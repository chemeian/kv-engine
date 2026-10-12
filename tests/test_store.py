"""CRUD, pattern matching and journaling behaviour of the in-memory store."""

from __future__ import annotations

import time
import unittest

from kvengine.store import KVStore, match_pattern


class MatchPatternTests(unittest.TestCase):
    def test_exact_literal(self):
        self.assertTrue(match_pattern("foo", "foo"))
        self.assertFalse(match_pattern("foo", "foobar"))

    def test_star_matches_everything(self):
        self.assertTrue(match_pattern("*", ""))
        self.assertTrue(match_pattern("*", "anything"))

    def test_prefix_wildcard(self):
        self.assertTrue(match_pattern("user:*", "user:1"))
        self.assertTrue(match_pattern("user:*", "user:"))
        self.assertFalse(match_pattern("user:*", "users:1"))
        self.assertFalse(match_pattern("user:*", "user"))

    def test_suffix_and_middle_wildcards(self):
        self.assertTrue(match_pattern("*:1", "user:1"))
        self.assertTrue(match_pattern("a*c", "abc"))
        self.assertTrue(match_pattern("a*c", "ac"))
        self.assertTrue(match_pattern("a*b*c", "a-x-b-y-c"))
        self.assertFalse(match_pattern("a*b*c", "a-c-b"))

    def test_wildcards_do_not_overlap(self):
        self.assertFalse(match_pattern("ab*ba", "aba"))

    def test_regex_metacharacters_are_literal(self):
        self.assertTrue(match_pattern("a.c", "a.c"))
        self.assertFalse(match_pattern("a.c", "abc"))
        self.assertFalse(match_pattern("a?c", "abc"))
        self.assertTrue(match_pattern("a[bc]", "a[bc]"))


class StoreCrudTests(unittest.TestCase):
    def setUp(self):
        self.store = KVStore()

    def test_set_and_get(self):
        self.assertEqual(self.store.set("k", "v"), "OK")
        self.assertEqual(self.store.get("k"), "v")

    def test_get_missing_returns_none(self):
        self.assertIsNone(self.store.get("missing"))

    def test_set_overwrites_and_clears_previous_ttl(self):
        self.store.set("k", "v1", expire_seconds=100)
        self.assertEqual(self.store.ttl("k"), 100)
        self.store.set("k", "v2")
        self.assertEqual(self.store.get("k"), "v2")
        self.assertEqual(self.store.ttl("k"), -1)

    def test_set_rejects_non_positive_expiry(self):
        with self.assertRaises(ValueError):
            self.store.set("k", "v", expire_seconds=0)

    def test_delete(self):
        self.store.set("k", "v")
        self.assertEqual(self.store.delete("k"), 1)
        self.assertEqual(self.store.delete("k"), 0)
        self.assertIsNone(self.store.get("k"))

    def test_delete_multiple_keys_counts_only_existing(self):
        self.store.set("a", "1")
        self.store.set("b", "2")
        self.assertEqual(self.store.delete("a", "b", "c"), 2)

    def test_exists(self):
        self.store.set("a", "1")
        self.assertEqual(self.store.exists("a"), 1)
        self.assertEqual(self.store.exists("a", "missing"), 1)
        self.assertEqual(self.store.exists("missing"), 0)

    def test_keys_returns_sorted_snapshot(self):
        for key in ("user:2", "user:1", "other"):
            self.store.set(key, "x")
        self.assertEqual(self.store.keys("*"), ["other", "user:1", "user:2"])
        self.assertEqual(self.store.keys("user:*"), ["user:1", "user:2"])
        self.assertEqual(self.store.keys("nope:*"), [])

    def test_empty_string_value_is_still_present(self):
        self.store.set("k", "")
        self.assertEqual(self.store.exists("k"), 1)
        self.assertEqual(self.store.get("k"), "")


class JournalTests(unittest.TestCase):
    def test_only_writes_are_journaled(self):
        events = []
        store = KVStore(journal=events.append)
        store.set("a", "1")
        store.get("a")
        store.exists("a")
        store.ttl("a")
        store.keys("*")
        store.delete("a")
        self.assertEqual([event[0] for event in events], ["SET", "DEL"])
        self.assertEqual(events[0], ("SET", "a", "1"))

    def test_set_with_expiry_logs_absolute_milliseconds(self):
        events = []
        store = KVStore(journal=events.append)
        before = time.time()
        store.set("a", "1", expire_seconds=5)
        self.assertEqual(events[0][:3], ("SET", "a", "1"))
        self.assertEqual(events[0][3], "PXAT")
        deadline = int(events[0][4]) / 1000.0
        # Deadlines are stored on a millisecond grid, hence the tolerance.
        self.assertGreaterEqual(deadline, before + 5 - 0.001)
        self.assertLess(deadline, before + 6)

    def test_expire_logs_absolute_deadline(self):
        events = []
        store = KVStore(journal=events.append)
        store.set("a", "1")
        store.expire("a", 10)
        name, key, deadline = events[1]
        self.assertEqual((name, key), ("PEXPIREAT", "a"))
        self.assertGreater(int(deadline) / 1000.0, time.time())

    def test_expire_on_missing_key_is_not_journaled(self):
        events = []
        store = KVStore(journal=events.append)
        self.assertEqual(store.expire("missing", 10), 0)
        self.assertEqual(events, [])

    def test_a_failing_journal_prevents_the_mutation(self):
        def boom(argv):
            raise OSError("disk is on fire")

        store = KVStore(journal=boom)
        with self.assertRaises(OSError):
            store.set("k", "v")
        self.assertEqual(store.exists("k"), 0)
        self.assertEqual(store.keys("*"), [])

    def test_replay_applies_logged_commands_without_rejournaling(self):
        events = []
        store = KVStore(journal=events.append)
        store.replay(["SET", "k", "v"])
        store.replay(["PEXPIREAT", "k", str(int((time.time() + 60) * 1000))])
        self.assertEqual(store.get("k"), "v")
        self.assertEqual(store.ttl("k"), 60)
        self.assertEqual(events, [])

    def test_replay_rejects_unknown_commands(self):
        store = KVStore()
        with self.assertRaises(ValueError):
            store.replay(["BOGUS", "k"])


class DumpLoadTests(unittest.TestCase):
    def test_dump_and_load_round_trip(self):
        store = KVStore()
        store.set("a", "1")
        store.set("b", "2", expire_seconds=60)
        state = store.dump_state()

        restored = KVStore()
        restored.load_state(state)
        self.assertEqual(restored.get("a"), "1")
        self.assertEqual(restored.get("b"), "2")
        self.assertEqual(restored.ttl("b"), 60)


if __name__ == "__main__":
    unittest.main()

