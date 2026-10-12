"""Restart recovery: snapshot loading plus append-only log replay."""

from __future__ import annotations

import glob
import os
import shutil
import tempfile
import time
import unittest

from kvengine import Config, Engine, resp


class PersistenceTestCase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.data_dir = os.path.join(self._tmp.name, "data")
        self.snapshot_interval = 3600.0
        self.snapshot_every_writes = 10 ** 9

    def make_engine(self, data_dir=None, **overrides):
        options = {
            "data_dir": data_dir or self.data_dir,
            "snapshot_interval": self.snapshot_interval,
            "snapshot_every_writes": self.snapshot_every_writes,
        }
        options.update(overrides)
        engine = Engine(Config(**options))
        self.addCleanup(engine.close)
        return engine

    def crash_copy(self, name="crash"):
        """Byte-for-byte copy of the data directory, as a crashed process leaves it."""
        target = os.path.join(self._tmp.name, name)
        shutil.copytree(self.data_dir, target)
        return target

    def files(self, directory=None):
        directory = directory or self.data_dir
        return sorted(os.path.basename(path) for path in glob.glob(os.path.join(directory, "*")))


class RestartTests(PersistenceTestCase):
    def test_values_survive_a_graceful_restart(self):
        engine = self.make_engine()
        engine.execute("SET", ["user:1", "alice"])
        engine.execute("SET", ["user:2", "bob"])
        engine.close()

        reopened = self.make_engine()
        self.assertEqual(reopened.store.get("user:1"), "alice")
        self.assertEqual(reopened.store.get("user:2"), "bob")

    def test_recovery_combines_snapshot_and_log_tail(self):
        engine = self.make_engine(snapshot_every_writes=3)
        engine.execute("SET", ["a", "1"])
        engine.execute("SET", ["b", "2"])
        engine.execute("SET", ["c", "3"])  # third write triggers snapshot generation 1
        engine.execute("DEL", ["a"])  # after the snapshot: only in aof generation 1
        self.assertEqual(self.files(), ["aof-00000001.aof", "snapshot-00000001.json"])

        recovered = self.make_engine(data_dir=self.crash_copy())
        self.assertIsNone(recovered.store.get("a"))
        self.assertEqual(recovered.store.get("b"), "2")
        self.assertEqual(recovered.store.get("c"), "3")

    def test_log_replay_works_when_no_snapshot_exists(self):
        engine = self.make_engine()
        for index in range(25):
            engine.execute("SET", [f"k{index}", str(index)])
        engine.execute("DEL", ["k0"])
        engine.close(snapshot=False)
        self.assertEqual(self.files(), ["aof-00000000.aof"])

        recovered = self.make_engine(data_dir=self.crash_copy())
        self.assertIsNone(recovered.store.get("k0"))
        self.assertEqual(recovered.store.get("k24"), "24")
        self.assertEqual(len(recovered.store.keys("*")), 24)

    def test_writes_before_an_ungraceful_stop_are_not_lost(self):
        engine = self.make_engine()
        for index in range(10):
            engine.execute("SET", [f"k{index}", str(index)])
        # No close(): the copied directory is all a crashed process would leave.
        recovered = self.make_engine(data_dir=self.crash_copy())
        self.assertEqual(recovered.store.keys("*"), [f"k{index}" for index in range(10)])

    def test_data_directory_is_created_on_demand(self):
        nested = os.path.join(self._tmp.name, "deep", "nested")
        engine = self.make_engine(data_dir=nested)
        self.assertTrue(os.path.isdir(nested))
        engine.close()

    def test_no_snapshot_is_written_when_nothing_was_written(self):
        engine = self.make_engine(snapshot_every_writes=1)
        engine.close()
        self.assertEqual([name for name in self.files() if name.startswith("snapshot")], [])


class TtlPersistenceTests(PersistenceTestCase):
    def test_ttl_is_preserved_across_a_restart(self):
        engine = self.make_engine()
        engine.execute("SET", ["k", "v", "EX", "100"])
        recovered = self.make_engine(data_dir=self.crash_copy())
        self.assertEqual(recovered.store.get("k"), "v")
        self.assertEqual(recovered.store.ttl("k"), 100)

    def test_key_without_ttl_keeps_no_ttl_after_a_restart(self):
        engine = self.make_engine()
        engine.execute("SET", ["k", "v"])
        recovered = self.make_engine(data_dir=self.crash_copy())
        self.assertEqual(recovered.store.ttl("k"), -1)

    def test_expired_keys_do_not_resurrect_after_a_restart(self):
        engine = self.make_engine()
        engine.store.set("gone", "v", expire_seconds=0.05)
        engine.store.set("stays", "v")
        time.sleep(0.2)
        recovered = self.make_engine(data_dir=self.crash_copy())
        self.assertIsNone(recovered.store.get("gone"))
        self.assertEqual(recovered.store.get("stays"), "v")

    def test_expire_command_survives_a_restart(self):
        engine = self.make_engine()
        engine.execute("SET", ["k", "v"])
        engine.execute("EXPIRE", ["k", "100"])
        recovered = self.make_engine(data_dir=self.crash_copy())
        self.assertEqual(recovered.store.ttl("k"), 100)


class SnapshotFileTests(PersistenceTestCase):
    def test_snapshot_generations_replace_each_other(self):
        engine = self.make_engine(snapshot_every_writes=3)
        for index in range(6):
            engine.execute("SET", [f"k{index}", str(index)])
        self.assertEqual(self.files(), ["aof-00000002.aof", "snapshot-00000002.json"])

    def test_append_log_uses_resp_encoded_commands(self):
        engine = self.make_engine()
        engine.execute("SET", ["k", "v"])
        engine.execute("SET", ["ttl", "v", "EX", "60"])
        engine.close(snapshot=False)
        path = glob.glob(os.path.join(self.data_dir, "aof-*.aof"))[0]
        with open(path, "rb") as handle:
            reader = resp.RespReader(handle)
            first = reader.read_command()
            second = reader.read_command()
        self.assertEqual(first, ["SET", "k", "v"])
        self.assertEqual(second[:4], ["SET", "ttl", "v", "PXAT"])

    def test_torn_log_tail_is_ignored_on_recovery(self):
        engine = self.make_engine()
        engine.execute("SET", ["a", "1"])
        engine.close(snapshot=False)
        path = glob.glob(os.path.join(self.data_dir, "aof-*.aof"))[0]
        with open(path, "ab") as handle:
            handle.write(b"*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$1\r\n")  # torn mid-append

        recovered = self.make_engine(data_dir=self.crash_copy())
        self.assertEqual(recovered.store.get("a"), "1")
        self.assertIsNone(recovered.store.get("b"))

    def test_corrupt_newest_snapshot_falls_back_to_the_previous_one(self):
        engine = self.make_engine(snapshot_every_writes=2)
        engine.execute("SET", ["a", "1"])
        engine.execute("SET", ["b", "2"])  # snapshot generation 1
        engine.close(snapshot=False)
        with open(os.path.join(self.data_dir, "snapshot-00000002.json"), "w", encoding="utf-8") as handle:
            handle.write("{ this is not json")

        recovered = self.make_engine(data_dir=self.data_dir)
        self.assertEqual(recovered.store.get("a"), "1")
        self.assertEqual(recovered.store.get("b"), "2")

    def test_snapshot_is_a_readable_json_document(self):
        import json

        engine = self.make_engine()
        engine.execute("SET", ["k", "v"])
        engine.execute("EXPIRE", ["k", "100"])
        engine.snapshot()
        path = os.path.join(self.data_dir, "snapshot-00000001.json")
        with open(path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
        self.assertEqual(payload["version"], 1)
        self.assertEqual(payload["state"]["data"], {"k": "v"})
        self.assertEqual(len(payload["state"]["expires"]), 1)


if __name__ == "__main__":
    unittest.main()

