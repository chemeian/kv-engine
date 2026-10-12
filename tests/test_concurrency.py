"""Thread safety of the store and of the whole engine."""

from __future__ import annotations

import os
import shutil
import tempfile
import threading
import time
import unittest

from kvengine import Config, Engine
from kvengine.store import KVStore


def run_threads(target, count):
    """Run ``target(index)`` in ``count`` threads released at the same moment."""
    barrier = threading.Barrier(count, timeout=15)
    errors = []

    def wrapper(index):
        try:
            barrier.wait()
            target(index)
        except Exception as exc:  # noqa: BLE001 - surfaced to the test
            errors.append(exc)

    threads = [threading.Thread(target=wrapper, args=(index,)) for index in range(count)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=60)
    for thread in threads:
        if thread.is_alive():
            raise AssertionError("a worker thread did not finish")
    return errors


class StoreConcurrencyTests(unittest.TestCase):
    def test_parallel_writes_reads_and_deletes_stay_consistent(self):
        store = KVStore()
        thread_count, per_thread = 8, 200

        def worker(index):
            for step in range(per_thread):
                key = f"k{index}:{step}"
                store.set(key, f"v{index}:{step}")
                self.assertEqual(store.get(key), f"v{index}:{step}")
                store.set("shared", str(step))
                store.get("shared")
                store.exists(key)
                store.keys(f"k{index}:*")
                store.ttl(key)
                if step % 3 == 0:
                    store.delete(key)

        self.assertEqual(run_threads(worker, thread_count), [])
        for index in range(thread_count):
            for step in range(per_thread):
                key = f"k{index}:{step}"
                expected = None if step % 3 == 0 else f"v{index}:{step}"
                self.assertEqual(store.get(key), expected)

    def test_expiry_stays_consistent_while_being_read_concurrently(self):
        store = KVStore()

        def writer(_index):
            for _ in range(300):
                store.set("k", "v", expire_seconds=0.02)

        def reader(_index):
            for _ in range(300):
                value = store.get("k")
                if value is not None:
                    self.assertEqual(value, "v")
                store.exists("k")
                store.keys("*")
                store.ttl("k")

        self.assertEqual(run_threads(writer, 4), [])
        self.assertEqual(run_threads(reader, 4), [])
        time.sleep(0.1)
        self.assertIsNone(store.get("k"))

    def test_delete_returns_one_only_for_the_winning_thread(self):
        store = KVStore()
        store.set("contested", "v")
        outcomes = []
        lock = threading.Lock()

        def worker(_index):
            removed = store.delete("contested")
            with lock:
                outcomes.append(removed)

        self.assertEqual(run_threads(worker, 16), [])
        self.assertEqual(sorted(outcomes).count(1), 1)
        self.assertEqual(sorted(outcomes).count(0), 15)


class EngineConcurrencyTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.data_dir = os.path.join(self._tmp.name, "data")

    def test_every_committed_write_survives_recovery_under_load(self):
        engine = Engine(
            Config(
                data_dir=self.data_dir,
                snapshot_interval=3600.0,
                snapshot_every_writes=50,
            )
        )
        self.addCleanup(engine.close)
        thread_count, per_thread = 6, 100

        def worker(index):
            for step in range(per_thread):
                key = f"k{index}:{step}"
                engine.execute("SET", [key, f"v{index}:{step}"])
                if step % 5 == 0:
                    engine.execute("EXPIRE", [key, "3600"])
                if step % 11 == 0:
                    engine.execute("DEL", [f"missing{index}:{step}"])

        self.assertEqual(run_threads(worker, thread_count), [])

        crash_dir = os.path.join(self._tmp.name, "crash")
        shutil.copytree(self.data_dir, crash_dir)
        recovered = Engine(Config(data_dir=crash_dir))
        self.addCleanup(recovered.close)
        for index in range(thread_count):
            for step in range(per_thread):
                key = f"k{index}:{step}"
                self.assertEqual(recovered.store.get(key), f"v{index}:{step}")
        self.assertGreater(recovered.store.ttl("k0:0"), 3500)
        self.assertLessEqual(recovered.store.ttl("k0:0"), 3600)


if __name__ == "__main__":
    unittest.main()

