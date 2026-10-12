"""Concurrency tests: many threads hammering the engine must stay consistent."""

import tempfile
import threading
import unittest

from kvs.engine import Engine


class ConcurrencyTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.dir = self._tmp.name

    def tearDown(self):
        self._tmp.cleanup()

    def test_parallel_set_get_del(self):
        with Engine(self.dir, snapshot_writes=200) as engine:
            thread_count = 8
            per_thread = 150
            errors = []
            barrier = threading.Barrier(thread_count)

            def worker(index):
                try:
                    barrier.wait(timeout=10)
                    for step in range(per_thread):
                        key = "t%d:k%d" % (index, step)
                        engine.set(key, str(step))
                        if engine.get(key) != str(step):
                            raise AssertionError("read-back mismatch for %s" % key)
                        if step % 3 == 0:
                            if engine.delete(key) != 1:
                                raise AssertionError("delete failed for %s" % key)
                except Exception as exc:  # collect, assert on the main thread
                    errors.append(exc)

            threads = [threading.Thread(target=worker, args=(i,)) for i in range(thread_count)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=60)

            self.assertEqual(errors, [])
            for index in range(thread_count):
                for step in range(per_thread):
                    key = "t%d:k%d" % (index, step)
                    if step % 3 == 0:
                        self.assertIsNone(engine.get(key), key)
                    else:
                        self.assertEqual(engine.get(key), str(step), key)

    def test_concurrent_writes_to_the_same_key_stay_consistent(self):
        with Engine(self.dir) as engine:
            thread_count = 16
            rounds = 200
            barrier = threading.Barrier(thread_count)

            def worker(index):
                barrier.wait(timeout=10)
                for _ in range(rounds):
                    engine.set("shared", "worker-%d" % index)
                    engine.exists("shared")

            threads = [threading.Thread(target=worker, args=(i,)) for i in range(thread_count)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=60)

            value = engine.get("shared")
            self.assertTrue(value.startswith("worker-"), value)

    def test_concurrent_readers_never_see_torn_state(self):
        with Engine(self.dir) as engine:
            for index in range(100):
                engine.set("k%03d" % index, "v%d" % index)
            stop = threading.Event()
            errors = []

            def reader():
                while not stop.is_set():
                    keys = sorted(engine.keys("*"))
                    if len(keys) != 100:
                        errors.append("unexpected key count: %d" % len(keys))
                        return
                    for key in keys:
                        if engine.get(key) is None:
                            errors.append("missing value for %s" % key)
                            return

            reader_thread = threading.Thread(target=reader, daemon=True)
            reader_thread.start()
            try:
                for index in range(100):
                    engine.set("k%03d" % index, "updated-%d" % index)
            finally:
                stop.set()
                reader_thread.join(timeout=10)
            self.assertEqual(errors, [])


if __name__ == "__main__":
    unittest.main()