"""TTL tests. A fake clock keeps them fast and deterministic."""

import unittest

from kvs.store import KVStore


class FakeClock:
    def __init__(self, start=1_000_000.0):
        self.now = start

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


class TTLTests(unittest.TestCase):
    def setUp(self):
        self.clock = FakeClock()
        self.store = KVStore(clock=self.clock)

    def test_key_without_expiry_reports_minus_one(self):
        self.store.set("k", "v")
        self.assertEqual(self.store.ttl("k"), -1)

    def test_missing_key_reports_minus_one(self):
        self.assertEqual(self.store.ttl("nope"), -1)

    def test_ttl_counts_down(self):
        self.store.set("k", "v", expire_at=self.clock.now + 10)
        self.assertEqual(self.store.ttl("k"), 10)
        self.clock.advance(4)
        self.assertEqual(self.store.ttl("k"), 6)

    def test_expired_key_is_deleted_lazily_by_get(self):
        self.store.set("k", "v", expire_at=self.clock.now + 5)
        self.assertEqual(self.store.get("k"), "v")
        self.clock.advance(6)
        self.assertIsNone(self.store.get("k"))

    def test_expired_key_is_deleted_lazily_by_exists(self):
        self.store.set("k", "v", expire_at=self.clock.now + 5)
        self.clock.advance(6)
        self.assertFalse(self.store.exists("k"))

    def test_expired_key_disappears_from_keys(self):
        self.store.set("keep", "1")
        self.store.set("drop", "2", expire_at=self.clock.now + 1)
        self.assertEqual(sorted(self.store.keys("*")), ["drop", "keep"])
        self.clock.advance(2)
        self.assertEqual(self.store.keys("*"), ["keep"])

    def test_expire_sets_new_deadline(self):
        self.store.set("k", "v")
        self.assertTrue(self.store.set_expire_at("k", self.clock.now + 30))
        self.assertEqual(self.store.ttl("k"), 30)
        self.clock.advance(10)
        self.assertTrue(self.store.set_expire_at("k", self.clock.now + 60))
        self.assertEqual(self.store.ttl("k"), 60)

    def test_expire_on_missing_key_returns_false(self):
        self.assertFalse(self.store.set_expire_at("nope", self.clock.now + 10))

    def test_overwriting_expired_key_clears_old_ttl(self):
        self.store.set("k", "v", expire_at=self.clock.now + 5)
        self.clock.advance(6)
        self.store.set("k", "fresh")
        self.assertEqual(self.store.get("k"), "fresh")
        self.assertEqual(self.store.ttl("k"), -1)

    def test_ttl_never_negative_for_live_key(self):
        self.store.set("k", "v", expire_at=self.clock.now + 0.4)
        self.assertEqual(self.store.ttl("k"), 1)

    def test_purge_expired_reports_count(self):
        self.store.set("a", "1", expire_at=self.clock.now + 1)
        self.store.set("b", "2", expire_at=self.clock.now + 1)
        self.store.set("c", "3")
        self.clock.advance(2)
        self.assertEqual(self.store.purge_expired(), 2)
        self.assertEqual(self.store.keys("*"), ["c"])


if __name__ == "__main__":
    unittest.main()