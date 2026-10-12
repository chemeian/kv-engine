"""In-memory, thread-safe key/value store with lazy TTL expiration.

This module deliberately knows nothing about persistence or networking so it
can be unit-tested in isolation.
"""

from __future__ import annotations

import fnmatch
import math
import threading
import time
from typing import Callable, Dict, Iterable, List, Optional, Tuple


class KVStore:
    """A thread-safe mapping of string keys to string values.

    Expiry timestamps are stored as absolute wall-clock seconds (see
    :func:`time.time`) rather than relative durations, so a TTL keeps ticking
    across process restarts when the value is reloaded from disk.
    """

    def __init__(self, clock: Callable[[], float] = time.time) -> None:
        self._clock = clock
        self._lock = threading.RLock()
        self._data: Dict[str, str] = {}
        self._expires: Dict[str, float] = {}

    # -- helpers -----------------------------------------------------------
    def now(self) -> float:
        """Current time according to the (possibly injected) clock."""
        return self._clock()

    def _drop_if_expired(self, key: str, now: float) -> bool:
        """Remove *key* when its TTL has elapsed. Caller must hold the lock."""
        expire_at = self._expires.get(key)
        if expire_at is not None and expire_at <= now:
            self._data.pop(key, None)
            self._expires.pop(key, None)
            return True
        return False

    def purge_expired(self) -> int:
        """Actively drop every key whose TTL has elapsed; return the count."""
        with self._lock:
            now = self.now()
            expired = [key for key, when in self._expires.items() if when <= now]
            for key in expired:
                self._data.pop(key, None)
                self._expires.pop(key, None)
            return len(expired)

    # -- writes ------------------------------------------------------------
    def set(self, key: str, value: str, expire_at: Optional[float] = None) -> None:
        with self._lock:
            self._data[key] = value
            if expire_at is None:
                self._expires.pop(key, None)
            else:
                self._expires[key] = float(expire_at)

    def delete(self, key: str) -> bool:
        with self._lock:
            self._drop_if_expired(key, self.now())
            existed = key in self._data
            self._data.pop(key, None)
            self._expires.pop(key, None)
            return existed

    def set_expire_at(self, key: str, expire_at: float) -> bool:
        """Set an absolute expiry. Returns False when the key does not exist."""
        with self._lock:
            self._drop_if_expired(key, self.now())
            if key not in self._data:
                return False
            self._expires[key] = float(expire_at)
            return True

    # -- reads -------------------------------------------------------------
    def get(self, key: str) -> Optional[str]:
        with self._lock:
            self._drop_if_expired(key, self.now())
            return self._data.get(key)

    def exists(self, key: str) -> bool:
        with self._lock:
            self._drop_if_expired(key, self.now())
            return key in self._data

    def keys(self, pattern: str = "*") -> List[str]:
        with self._lock:
            now = self.now()
            for key in list(self._expires):
                self._drop_if_expired(key, now)
            if pattern == "*":
                return list(self._data)
            return [key for key in self._data if fnmatch.fnmatchcase(key, pattern)]

    def ttl(self, key: str) -> int:
        """Remaining lifetime in seconds, or -1 when absent / no expiry."""
        with self._lock:
            self._drop_if_expired(key, self.now())
            if key not in self._data:
                return -1
            expire_at = self._expires.get(key)
            if expire_at is None:
                return -1
            return max(0, int(math.ceil(expire_at - self.now())))

    def expire_at_of(self, key: str) -> Optional[float]:
        with self._lock:
            self._drop_if_expired(key, self.now())
            return self._expires.get(key)

    # -- bulk access (used by the persistence layer) -----------------------
    def snapshot_items(self) -> List[Tuple[str, str, float]]:
        """Return a consistent copy of ``(key, value, expire_at)`` triples."""
        with self._lock:
            now = self.now()
            for key in list(self._expires):
                self._drop_if_expired(key, now)
            return [(key, value, self._expires.get(key, 0.0))
                    for key, value in self._data.items()]

    def load_items(self, items: Iterable[Tuple[str, str, float]]) -> None:
        with self._lock:
            for key, value, expire_at in items:
                self._data[key] = value
                if expire_at:
                    self._expires[key] = float(expire_at)
                else:
                    self._expires.pop(key, None)

    def clear(self) -> None:
        with self._lock:
            self._data.clear()
            self._expires.clear()

    def __len__(self) -> int:
        with self._lock:
            now = self.now()
            for key in list(self._expires):
                self._drop_if_expired(key, now)
            return len(self._data)