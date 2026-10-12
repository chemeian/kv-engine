"""Store tests: create/read/update/delete, EXISTS and KEYS patterns."""

import unittest

from kvs.store import KVStore


class StoreBasicsTests(unittest.TestCase):
    def setUp(self):
        self.store = KVStore()

    def test_get_missing_key_returns_none(self):
        self.assertIsNone(self.store.get("missing"))

    def test_set_then_get(self):
        self.store.set("k", "v")
        self.assertEqual(self.store.get("k"), "v")

    def test_overwrite_updates_value(self):
        self.store.set("k", "old")
        self.store.set("k", "new")
        self.assertEqual(self.store.get("k"), "new")

    def test_delete_returns_true_only_once(self):
        self.store.set("k", "v")
        self.assertTrue(self.store.delete("k"))
        self.assertFalse(self.store.delete("k"))
        self.assertIsNone(self.store.get("k"))

    def test_exists(self):
        self.assertFalse(self.store.exists("k"))
        self.store.set("k", "v")
        self.assertTrue(self.store.exists("k"))
        self.store.delete("k")
        self.assertFalse(self.store.exists("k"))

    def test_keys_star_matches_everything(self):
        self.store.set("user:1", "a")
        self.store.set("user:2", "b")
        self.store.set("session:9", "c")
        self.assertEqual(sorted(self.store.keys("*")),
                         ["session:9", "user:1", "user:2"])

    def test_keys_prefix_pattern(self):
        self.store.set("user:1", "a")
        self.store.set("user:2", "b")
        self.store.set("session:1", "c")
        self.assertEqual(sorted(self.store.keys("user:*")), ["user:1", "user:2"])

    def test_keys_pattern_with_no_match(self):
        self.store.set("user:1", "a")
        self.assertEqual(self.store.keys("nope:*"), [])

    def test_keys_middle_wildcard(self):
        self.store.set("user:1:name", "a")
        self.store.set("user:1:age", "b")
        self.store.set("user:2:name", "c")
        self.assertEqual(sorted(self.store.keys("user:1:*")),
                         ["user:1:age", "user:1:name"])

    def test_empty_key_and_value_are_allowed(self):
        self.store.set("", "")
        self.assertEqual(self.store.get(""), "")
        self.assertTrue(self.store.exists(""))
        self.assertEqual(self.store.keys("*"), [""])

    def test_non_ascii_round_trip(self):
        self.store.set("key-\u4e2d\u6587", "value-\u503c")
        self.assertEqual(self.store.get("key-\u4e2d\u6587"), "value-\u503c")
        self.assertEqual(self.store.keys("key-*"), ["key-\u4e2d\u6587"])

    def test_len_counts_live_keys(self):
        self.store.set("a", "1")
        self.store.set("b", "2")
        self.assertEqual(len(self.store), 2)


if __name__ == "__main__":
    unittest.main()