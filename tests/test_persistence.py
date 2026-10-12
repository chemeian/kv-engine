"""Persistence tests: AOF replay, snapshots and restart recovery."""

import os
import tempfile
import unittest

from kvs.engine import Engine
from kvs.snapshot import SnapshotError, read_snapshot


class PersistenceTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.dir = self._tmp.name

    def tearDown(self):
        self._tmp.cleanup()

    def _engine(self, **kwargs):
        kwargs.setdefault("snapshot_writes", 0)      # no write-triggered snapshot
        kwargs.setdefault("snapshot_interval", 0)    # no time-triggered snapshot
        return Engine(self.dir, **kwargs)

    def _crash(self, engine):
        """Drop the file handles without running the graceful-close snapshot."""
        engine._stop.set()
        engine._thread.join(timeout=5.0)
        engine._aof.close()
        engine._opened = False

    # -- write-ahead logging ----------------------------------------------
    def test_write_is_appended_to_aof_immediately(self):
        engine = self._engine().open()
        engine.set("k", "v")
        aof_path = os.path.join(self.dir, "appendonly.aof")
        with open(aof_path, "rb") as handle:
            content = handle.read()
        self.assertIn(b"SET", content)
        self.assertIn(b"k", content)
        self.assertIn(b"v", content)
        engine.close()

    # -- recovery ----------------------------------------------------------
    def test_values_survive_graceful_restart(self):
        engine = self._engine().open()
        engine.set("a", "1")
        engine.set("b", "2")
        engine.delete("a")
        engine.set("c", "3")
        engine.close()

        reopened = self._engine().open()
        try:
            self.assertIsNone(reopened.get("a"))
            self.assertEqual(reopened.get("b"), "2")
            self.assertEqual(reopened.get("c"), "3")
        finally:
            reopened.close()

    def test_committed_writes_survive_simulated_crash(self):
        engine = self._engine(aof_fsync="always").open()
        for index in range(20):
            engine.set("k%d" % index, "v%d" % index)
        engine.delete("k0")
        self._crash(engine)  # never snapshotted: only the AOF can restore state

        reopened = self._engine().open()
        try:
            self.assertIsNone(reopened.get("k0"))
            self.assertEqual(reopened.get("k7"), "v7")
            self.assertEqual(reopened.get("k19"), "v19")
            self.assertEqual(len(reopened.keys("*")), 19)
        finally:
            reopened.close()

    def test_recovery_from_aof_alone_without_snapshot(self):
        engine = self._engine().open()
        engine.set("only", "aof")
        engine.set("second", "value")
        self._crash(engine)  # no snapshot is ever written
        self.assertFalse(os.path.exists(os.path.join(self.dir, "snapshot.kvs")))

        reopened = self._engine().open()
        try:
            self.assertEqual(reopened.get("only"), "aof")
            self.assertEqual(reopened.get("second"), "value")
        finally:
            reopened.close()

    def test_snapshot_then_aof_tail_is_replayed(self):
        engine = self._engine(snapshot_writes=5).open()
        for index in range(5):
            engine.set("k%d" % index, "v%d" % index)
        self.assertTrue(os.path.exists(os.path.join(self.dir, "snapshot.kvs")))
        # These two writes land after the snapshot and live only in the AOF.
        engine.set("tail1", "v")
        engine.set("tail2", "v")
        engine.close()

        reopened = self._engine().open()
        try:
            for index in range(5):
                self.assertEqual(reopened.get("k%d" % index), "v%d" % index)
            self.assertEqual(reopened.get("tail1"), "v")
            self.assertEqual(reopened.get("tail2"), "v")
        finally:
            reopened.close()

    def test_snapshot_compaction_shrinks_the_aof(self):
        engine = self._engine(snapshot_writes=1000).open()
        for index in range(50):
            engine.set("k", "v%d" % index)  # one key, many writes
        engine.snapshot()
        with open(os.path.join(self.dir, "appendonly.aof"), "rb") as handle:
            self.assertEqual(handle.read(), b"")  # everything folded into the snapshot
        engine.close()

        reopened = self._engine().open()
        try:
            self.assertEqual(reopened.get("k"), "v49")
        finally:
            reopened.close()

    # -- TTL across restarts ----------------------------------------------
    def test_ttl_survives_restart(self):
        engine = self._engine().open()
        engine.set("temp", "v", ex=100)
        engine.close()

        reopened = self._engine().open()
        try:
            self.assertTrue(0 < reopened.ttl("temp") <= 100)
            self.assertEqual(reopened.get("temp"), "v")
        finally:
            reopened.close()

    def test_expired_key_is_not_resurrected_by_restart(self):
        engine = self._engine().open()
        engine.set("gone", "v", expire_at=engine.now() - 1)
        engine.set("stays", "v")
        engine.close()

        reopened = self._engine().open()
        try:
            self.assertIsNone(reopened.get("gone"))
            self.assertFalse(reopened.exists("gone"))
            self.assertEqual(reopened.keys("*"), ["stays"])
        finally:
            reopened.close()

    def test_non_ascii_survives_restart(self):
        engine = self._engine().open()
        engine.set("\u7528\u6237:1", "\u5f20\u4e09")
        engine.set("emoji", "\U0001f680")
        engine.close()

        reopened = self._engine().open()
        try:
            self.assertEqual(reopened.get("\u7528\u6237:1"), "\u5f20\u4e09")
            self.assertEqual(reopened.get("emoji"), "\U0001f680")
            self.assertEqual(reopened.keys("\u7528\u6237:*"), ["\u7528\u6237:1"])
        finally:
            reopened.close()

    # -- durable file format ----------------------------------------------
    def test_snapshot_round_trip(self):
        engine = self._engine().open()
        engine.set("a", "1")
        engine.set("b", "2", ex=500)
        engine.snapshot()
        seq, items = read_snapshot(os.path.join(self.dir, "snapshot.kvs"))
        engine.close()

        items = dict((key, value) for key, value, _ in items)
        self.assertEqual(items, {"a": "1", "b": "2"})
        self.assertGreaterEqual(seq, 2)

    def test_corrupt_snapshot_is_detected(self):
        engine = self._engine().open()
        engine.set("a", "1")
        engine.snapshot()
        engine.close()
        path = os.path.join(self.dir, "snapshot.kvs")
        with open(path, "rb") as handle:
            payload = bytearray(handle.read())
        payload[12] ^= 0xFF  # flip a bit inside the payload
        with open(path, "wb") as handle:
            handle.write(payload)
        with self.assertRaises(SnapshotError):
            read_snapshot(path)

    def test_missing_engine_reads_nothing(self):
        reopened = self._engine().open()
        try:
            self.assertEqual(reopened.keys("*"), [])
        finally:
            reopened.close()


if __name__ == "__main__":
    unittest.main()