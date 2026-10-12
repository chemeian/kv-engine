"""In-memory key/value store with TTL expiry and thread safety.

All values are strings and all mutation happens while holding a single
re-entrant lock, so readers and writers can never observe a half-applied
command.

The store can optionally take a ``journal`` callback.  Every mutating method
serialises the change into a canonical command and hands it to the journal
*before* touching memory; if the journal raises, nothing is applied.  That
ordering is what makes the append-only log safe: the log can only ever be
ahead of, never behind, the in-memory state.
"""

from __future__ import annotations

import math
import threading
import time
from typing import Callable, Dict, List, Optional, Sequence, Tuple

DEFAULT_PATTERN = "*"


def match_pattern(pattern: str, key: str) -> bool:
    """Glob match in which ``*`` is the only wildcard and all else is literal.

    Regex metacharacters such as ``.``, ``?`` or ``[`` carry no special
    meaning, matching how ``KEYS user:*`` is expected to behave.
    """
    if pattern == DEFAULT_PATTERN:
        return True
    parts = pattern.split("*")
    if len(parts) == 1:
        return key == pattern
    head, tail = parts[0], parts[-1]
    if not key.startswith(head) or not key.endswith(tail):
        return False
    position = len(head)
    limit = len(key) - len(tail)
    for part in parts[1:-1]:
        index = key.find(part, position)
        if index < 0 or index > limit:
            return False
        position = index + len(part)
    # The head and tail regions must not overlap for the match to be real.
    return position <= limit


def to_millis(epoch_seconds: float) -> int:
    """Convert an epoch timestamp to integer milliseconds (log representation)."""
    return int(round(float(epoch_seconds) * 1000.0))


def round_millis(epoch_seconds: float) -> float:
    """Snap an epoch timestamp to the millisecond grid used by the log.

    Keeping the in-memory deadline identical to the logged one means a restart
    can never shift a key's expiry.
    """
    return round(float(epoch_seconds) * 1000.0) / 1000.0


class KVStore:
    """A dict of ``str -> str`` with per-key expiry, safe for concurrent use."""

    def __init__(self, journal: Optional[Callable[[Tuple[str, ...]], None]] = None) -> None:
        self._lock = threading.RLock()
        self._data: Dict[str, str] = {}
        self._expires: Dict[str, float] = {}
        self._journal = journal

    @property
    def lock(self) -> threading.RLock:
        """The lock guarding both the data and the journal."""
        return self._lock

    # ------------------------------------------------------------------ writes
    def set(self, key: str, value: str, expire_seconds: Optional[float] = None) -> str:
        if expire_seconds is not None:
            expire_seconds = float(expire_seconds)
            if expire_seconds <= 0:
                raise ValueError("expire time must be positive")
        with self._lock:
            if expire_seconds is None:
                argv: Tuple[str, ...] = ("SET", key, value)
            else:
                deadline = round_millis(time.time() + expire_seconds)
                argv = ("SET", key, value, "PXAT", str(to_millis(deadline)))
            self._log(argv)
            return self._apply(argv[0], argv[1:])

    def delete(self, *keys: str) -> int:
        removed = 0
        with self._lock:
            for key in keys:
                if self._alive(key):
                    self._log(("DEL", key))
                    removed += self._apply("DEL", (key,))
        return removed

    def expire(self, key: str, seconds: float) -> int:
        seconds = float(seconds)
        with self._lock:
            if not self._alive(key):
                return 0
            if seconds <= 0:
                # A non-positive TTL means "gone now"; log it as a delete so the
                # effect is exact and replay is trivially deterministic.
                self._log(("DEL", key))
                return self._apply("DEL", (key,))
            deadline = round_millis(time.time() + seconds)
            argv = ("PEXPIREAT", key, str(to_millis(deadline)))
            self._log(argv)
            return self._apply(argv[0], argv[1:])

    # ------------------------------------------------------------------- reads
    def get(self, key: str) -> Optional[str]:
        with self._lock:
            if not self._alive(key):
                return None
            return self._data[key]

    def exists(self, *keys: str) -> int:
        with self._lock:
            return sum(1 for key in keys if self._alive(key))

    def keys(self, pattern: str = DEFAULT_PATTERN) -> List[str]:
        with self._lock:
            self._purge_expired()
            return sorted(key for key in self._data if match_pattern(pattern, key))

    def ttl(self, key: str) -> int:
        with self._lock:
            if not self._alive(key):
                return -1
            deadline = self._expires.get(key)
            if deadline is None:
                return -1
            # Round to whole milliseconds first so that the half-millisecond of
            # precision the log keeps cannot push the result up by one second.
            remaining_ms = int(round((deadline - time.time()) * 1000.0))
            return max(1, math.ceil(remaining_ms / 1000.0))

    # --------------------------------------------------------------- snapshots
    def dump_state(self) -> Dict[str, Dict[str, object]]:
        """Return a JSON-serialisable copy of the live state."""
        with self._lock:
            self._purge_expired()
            return {"data": dict(self._data), "expires": dict(self._expires)}

    def load_state(self, state: Optional[Dict[str, Dict[str, object]]]) -> None:
        state = state or {}
        with self._lock:
            self._data = {str(k): str(v) for k, v in dict(state.get("data") or {}).items()}
            self._expires = {
                str(k): float(v)
                for k, v in dict(state.get("expires") or {}).items()
                if str(k) in self._data
            }
            self._purge_expired()

    # ------------------------------------------------------------ log replay
    def replay(self, argv: Sequence[str]) -> object:
        """Apply a command from the append-only log, bypassing the journal."""
        with self._lock:
            return self._apply(argv[0], tuple(argv[1:]))

    def _apply(self, name: str, args: Tuple[str, ...]) -> object:
        name = name.upper()
        if name == "SET":
            return self._apply_set(args)
        if name == "DEL":
            return self._apply_del(args)
        if name == "EXPIRE":
            return self._set_deadline(args[0], time.time() + float(args[1]))
        if name == "PEXPIREAT":
            return self._set_deadline(args[0], int(args[1]) / 1000.0)
        raise ValueError(f"unknown command in log: {name!r}")

    def _apply_set(self, args: Tuple[str, ...]) -> str:
        key, value = args[0], args[1]
        self._data[key] = value
        self._expires.pop(key, None)
        index = 2
        while index < len(args):
            option = args[index].upper()
            if option == "PXAT":
                self._expires[key] = int(args[index + 1]) / 1000.0
            elif option == "EX":
                self._expires[key] = time.time() + float(args[index + 1])
            else:
                raise ValueError(f"unknown SET option in log: {option!r}")
            index += 2
        deadline = self._expires.get(key)
        if deadline is not None and deadline <= time.time():
            self._data.pop(key, None)
            self._expires.pop(key, None)
        return "OK"

    def _apply_del(self, args: Tuple[str, ...]) -> int:
        removed = 0
        for key in args:
            if self._data.pop(key, None) is not None:
                removed += 1
            self._expires.pop(key, None)
        return removed

    def _set_deadline(self, key: str, deadline: float) -> int:
        if not self._alive(key):
            return 0
        if deadline <= time.time():
            self._data.pop(key, None)
            self._expires.pop(key, None)
            return 1
        self._expires[key] = deadline
        return 1

    # ---------------------------------------------------------------- helpers
    def _log(self, argv: Tuple[str, ...]) -> None:
        if self._journal is not None:
            self._journal(tuple(argv))

    def _alive(self, key: str) -> bool:
        """Check presence, lazily dropping the key if its deadline has passed."""
        deadline = self._expires.get(key)
        if deadline is not None and deadline <= time.time():
            self._data.pop(key, None)
            self._expires.pop(key, None)
            return False
        return key in self._data

    def _purge_expired(self) -> None:
        if not self._expires:
            return
        now = time.time()
        for key in [key for key, deadline in self._expires.items() if deadline <= now]:
            self._data.pop(key, None)
            self._expires.pop(key, None)

