"""Expiry semantics: TTL, EXPIRE and lazy deletion."""

from __future__ import annotations

import time
import unittest

from kvengine.store import KVStore


class TtlTests(unittest.TestCase):
    def setUp(self):
        self.store = KVStore()

    def test_ttl_without_expiry_is_minus_one(self):
        self.store.set("k", "v")
        self.assertEqual(self.store.ttl("k"), -1)

    def test_ttl_for_missing_key_is_minus_one(self):
        self.assertEqual(self.store.ttl("missing"), -1)

    def test_ttl_after_set_with_expiry(self):
        self.store.set("k", "v", expire_seconds=10)
        self.assertEqual(self.store.ttl("k"), 10)

    def test_expire_sets_and_updates_deadline(self):
        self.store.set("k", "v")
        self.assertEqual(self.store.expire("k", 30), 1)
        self.assertEqual(self.store.ttl("k"), 30)
        self.assertEqual(self.store.expire("k", 5), 1)
        self.assertEqual(self.store.ttl("k"), 5)
        self.assertEqual(self.store.get("k"), "v")

    def test_expire_on_missing_key_returns_zero(self):
        self.assertEqual(self.store.expire("missing", 10), 0)

    def test_expire_with_non_positive_seconds_deletes_the_key(self):
        self.store.set("k", "v")
        self.assertEqual(self.store.expire("k", 0), 1)
        self.assertIsNone(self.store.get("k"))
        self.assertEqual(self.store.exists("k"), 0)

    def test_key_survives_until_deadline(self):
        self.store.set("k", "v", expire_seconds=0.3)
        time.sleep(0.05)
        self.assertEqual(self.store.get("k"), "v")
        self.assertEqual(self.store.ttl("k"), 1)

    def test_get_lazily_expires_and_purges_memory(self):
        self.store.set("k", "v", expire_seconds=0.1)
        time.sleep(0.25)
        self.assertIsNone(self.store.get("k"))
        self.assertNotIn("k", self.store.dump_state()["data"])
        self.assertNotIn("k", self.store.dump_state()["expires"])

    def test_exists_returns_zero_once_expired(self):
        self.store.set("k", "v", expire_seconds=0.1)
        time.sleep(0.25)
        self.assertEqual(self.store.exists("k"), 0)

    def test_keys_hides_expired_entries(self):
        self.store.set("user:soon", "v", expire_seconds=0.1)
        self.store.set("user:later", "v", expire_seconds=100)
        self.assertEqual(self.store.keys("user:*"), ["user:later", "user:soon"])
        time.sleep(0.25)
        self.assertEqual(self.store.keys("user:*"), ["user:later"])
        self.assertEqual(self.store.keys("*"), ["user:later"])

    def test_ttl_is_minus_one_once_expired(self):
        self.store.set("k", "v", expire_seconds=0.1)
        time.sleep(0.25)
        self.assertEqual(self.store.ttl("k"), -1)

    def test_expire_then_overwrite_without_ttl(self):
        self.store.set("k", "v", expire_seconds=0.1)
        self.store.set("k", "v2")
        time.sleep(0.25)
        self.assertEqual(self.store.get("k"), "v2")


if __name__ == "__main__":
    unittest.main()

