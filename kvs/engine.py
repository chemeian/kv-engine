"""The persistence engine: an in-memory store backed by an AOF + snapshots.

Durability model
----------------
* Every write (``SET`` / ``DEL`` / ``EXPIRE``) is appended to the AOF *first*
  and only then applied to memory, so a crash can never lose a write that was
  acknowledged.
* A snapshot is a full copy of the keyspace stamped with the sequence number
  of the last write it contains.  It is taken every ``snapshot_interval``
  seconds or after ``snapshot_writes`` writes, whichever comes first.
* On recovery the newest snapshot is loaded and the AOF is replayed from the
  point the snapshot ends, which is why no acknowledged write is ever lost.
"""

from __future__ import annotations

import os
import threading
import time
from typing import Callable, List, Optional

from .aof import AOF
from .snapshot import read_snapshot, write_snapshot
from .store import KVStore


class Engine:
    """Thread-safe, persistent key/value engine."""

    def __init__(self, directory: str, *, snapshot_interval: float = 60.0,
                 snapshot_writes: int = 1000, aof_fsync: str = "always",
                 aof_filename: str = "appendonly.aof",
                 snapshot_filename: str = "snapshot.kvs",
                 clock: Callable[[], float] = time.time) -> None:
        self.directory = directory
        self.snapshot_interval = snapshot_interval
        self.snapshot_writes = snapshot_writes
        self._store = KVStore(clock=clock)
        self._lock = threading.RLock()
        self._aof = AOF(os.path.join(directory, aof_filename), fsync_policy=aof_fsync)
        self._snapshot_path = os.path.join(directory, snapshot_filename)
        self._next_seq = 1
        self._writes_since_snapshot = 0
        self._last_snapshot = 0.0
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._opened = False

    # -- lifecycle ---------------------------------------------------------
    def open(self) -> "Engine":
        if self._opened:
            return self
        os.makedirs(self.directory, exist_ok=True)
        self._recover()
        self._aof.open()
        self._last_snapshot = self._store.now()
        self._opened = True
        self._thread = threading.Thread(target=self._maintenance_loop,
                                        name="kvs-maintenance", daemon=True)
        self._thread.start()
        return self

    def close(self) -> None:
        if not self._opened:
            return
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5.0)
            self._thread = None
        with self._lock:
            if self._writes_since_snapshot > 0:
                self._take_snapshot_locked()
            self._aof.close()
        self._opened = False

    def __enter__(self) -> "Engine":
        return self.open()

    def __exit__(self, *exc) -> None:
        self.close()

    # -- recovery ----------------------------------------------------------
    def _recover(self) -> None:
        snapshot = read_snapshot(self._snapshot_path)
        snapshot_seq = 0
        if snapshot is not None:
            snapshot_seq, items = snapshot
            self._store.load_items(items)
            self._next_seq = snapshot_seq + 1
        for seq, command, args in self._aof.replay():
            if seq <= snapshot_seq:
                continue  # already contained in the snapshot
            self._apply(command, args)
            if seq >= self._next_seq:
                self._next_seq = seq + 1

    def _apply(self, command: str, args: List[str]) -> None:
        """Apply a decoded command to memory only (no AOF write)."""
        name = command.upper()
        if name == "SET":
            if len(args) < 2:
                return
            key, value = args[0], args[1]
            expire_at = None
            if len(args) >= 4 and args[2].upper() in ("PXAT", "EXAT"):
                raw = float(args[3])
                expire_at = raw / 1000.0 if args[2].upper() == "PXAT" else raw
            self._store.set(key, value, expire_at=expire_at)
        elif name == "DEL":
            for key in args:
                self._store.delete(key)
        elif name == "PEXPIREAT":
            if len(args) >= 2:
                self._store.set_expire_at(args[0], float(args[1]) / 1000.0)

    # -- writes ------------------------------------------------------------
    def _commit(self, args: List[str]) -> None:
        """Write-ahead log then apply, under the engine lock."""
        with self._lock:
            self._aof.append(self._next_seq, args)
            self._next_seq += 1
            self._apply(args[0], args[1:])
            self._writes_since_snapshot += 1
            if self.snapshot_writes and self._writes_since_snapshot >= self.snapshot_writes:
                self._take_snapshot_locked()

    def set(self, key: str, value: str, ex: Optional[float] = None,
            expire_at: Optional[float] = None) -> bool:
        with self._lock:
            if expire_at is None and ex is not None:
                expire_at = self._store.now() + float(ex)
            args = ["SET", key, value]
            if expire_at is not None:
                args += ["PXAT", str(int(round(float(expire_at) * 1000)))]
            self._commit(args)
        return True

    def delete(self, key: str) -> int:
        with self._lock:
            if not self._store.exists(key):
                return 0
            self._commit(["DEL", key])
            return 1

    def expire(self, key: str, seconds: float) -> int:
        with self._lock:
            if not self._store.exists(key):
                return 0
            seconds = float(seconds)
            if seconds <= 0:
                self._commit(["DEL", key])
                return 1
            when = self._store.now() + seconds
            self._commit(["PEXPIREAT", key, str(int(round(when * 1000)))])
            return 1

    # -- reads -------------------------------------------------------------
    def get(self, key: str) -> Optional[str]:
        return self._store.get(key)

    def exists(self, key: str) -> int:
        return 1 if self._store.exists(key) else 0

    def ttl(self, key: str) -> int:
        return self._store.ttl(key)

    def keys(self, pattern: str = "*") -> List[str]:
        return self._store.keys(pattern)

    def now(self) -> float:
        return self._store.now()

    # -- snapshots ---------------------------------------------------------
    def snapshot(self) -> None:
        """Force a full snapshot (also compacts the AOF)."""
        with self._lock:
            self._take_snapshot_locked()

    def _take_snapshot_locked(self) -> None:
        items = self._store.snapshot_items()
        seq = self._next_seq - 1
        write_snapshot(self._snapshot_path, seq, items)
        self._aof.compact(self._next_seq)
        self._writes_since_snapshot = 0
        self._last_snapshot = self._store.now()

    def _maintenance_loop(self) -> None:
        while not self._stop.wait(1.0):
            try:
                self._store.purge_expired()
                with self._lock:
                    if self._aof.needs_sync():
                        self._aof.sync()
                    if self.snapshot_interval and self._writes_since_snapshot > 0:
                        elapsed = self._store.now() - self._last_snapshot
                        if elapsed >= self.snapshot_interval:
                            self._take_snapshot_locked()
            except Exception:
                # A maintenance failure must never take the server down.
                pass

    # -- introspection -----------------------------------------------------
    def stats(self) -> dict:
        return {
            "directory": self.directory,
            "keys": len(self._store),
            "aof_bytes": self._aof.size(),
            "writes_since_snapshot": self._writes_since_snapshot,
            "snapshot_interval": self.snapshot_interval,
            "snapshot_writes": self.snapshot_writes,
            "aof_fsync": self._aof.fsync_policy,
        }