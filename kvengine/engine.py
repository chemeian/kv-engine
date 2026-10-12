"""Command layer: RESP replies, expiry, log-then-apply, snapshot scheduling."""

from __future__ import annotations

import logging
import threading
import time
from typing import List, Optional, Sequence

from . import resp
from .config import Config
from .persistence import Persistence
from .store import KVStore

log = logging.getLogger("kvengine.engine")

WRITE_COMMANDS = frozenset({"SET", "DEL", "EXPIRE"})


class CommandError(Exception):
    """A user-facing error, rendered as a ``-ERR`` RESP reply."""


class Engine:
    """Wires a :class:`KVStore` to its append-only log and snapshotter."""

    def __init__(self, config: Optional[Config] = None) -> None:
        self.config = config or Config()
        self.persistence = Persistence(self.config.data_dir, fsync=self.config.fsync)
        state, commands = self.persistence.open()
        self.store = KVStore(journal=self.persistence.append)
        self.store.load_state(state)
        for argv in commands:
            try:
                self.store.replay(argv)
            except (ValueError, IndexError):
                log.warning("skipping unplayable log entry: %r", argv)

        self._writes_since_snapshot = 0
        self._last_snapshot = time.time()
        self._snapshot_lock = threading.RLock()
        self._stop = threading.Event()
        self._closed = False
        self._maintenance = threading.Thread(
            target=self._maintenance_loop, name="kvengine-maintenance", daemon=True
        )
        self._maintenance.start()

    # ------------------------------------------------------------- execution
    def execute(self, command: str, args: Sequence[str] = ()) -> bytes:
        """Run one command and return the encoded RESP reply."""
        name = (command or "").upper()
        arguments = list(args)
        try:
            reply = self._dispatch(name, arguments)
        except CommandError as exc:
            return resp.encode_error(f"ERR {exc}")
        except ValueError as exc:
            return resp.encode_error(f"ERR {exc}")
        except Exception:  # pragma: no cover - defensive
            log.exception("command %s failed", name)
            return resp.encode_error("ERR internal error")
        if name in WRITE_COMMANDS:
            self._after_write()
        return reply

    def _dispatch(self, name: str, args: List[str]) -> bytes:
        if name == "PING":
            return resp.encode_bulk(args[0]) if args else resp.encode_simple("PONG")
        if name == "ECHO":
            self._arity(name, args, 1)
            return resp.encode_bulk(args[0])
        if name == "SET":
            return self._cmd_set(args)
        if name == "GET":
            self._arity(name, args, 1)
            return resp.encode_bulk(self.store.get(args[0]))
        if name == "DEL":
            self._arity(name, args, 1, minimum=True)
            return resp.encode_integer(self.store.delete(*args))
        if name == "EXISTS":
            self._arity(name, args, 1, minimum=True)
            return resp.encode_integer(self.store.exists(*args))
        if name == "EXPIRE":
            self._arity(name, args, 2)
            return resp.encode_integer(self.store.expire(args[0], self._parse_int(args[1])))
        if name == "TTL":
            self._arity(name, args, 1)
            return resp.encode_integer(self.store.ttl(args[0]))
        if name == "KEYS":
            self._arity(name, args, 1)
            return resp.encode_array(resp.encode_bulk(key) for key in self.store.keys(args[0]))
        if name == "COMMAND":
            # redis-cli probes COMMAND DOCS on connect; an empty array keeps it happy.
            return resp.encode_array([])
        if name == "QUIT":
            return resp.encode_simple("OK")
        raise CommandError(f"unknown command '{name}'")

    def _cmd_set(self, args: List[str]) -> bytes:
        self._arity("SET", args, 2, minimum=True)
        key, value = args[0], args[1]
        expire: Optional[float] = None
        index = 2
        while index < len(args):
            option = args[index].upper()
            if option in ("EX", "PX"):
                if index + 1 >= len(args):
                    raise CommandError("syntax error")
                amount = self._parse_int(args[index + 1])
                if amount <= 0:
                    raise CommandError("invalid expire time in 'set' command")
                expire = float(amount) if option == "EX" else amount / 1000.0
                index += 2
            else:
                raise CommandError("syntax error")
        self.store.set(key, value, expire)
        return resp.encode_simple("OK")

    @staticmethod
    def _parse_int(raw: str) -> int:
        try:
            return int(raw)
        except (TypeError, ValueError):
            raise CommandError("value is not an integer or out of range") from None

    @staticmethod
    def _arity(name: str, args: Sequence[str], expected: int, minimum: bool = False) -> None:
        ok = len(args) >= expected if minimum else len(args) == expected
        if not ok:
            raise CommandError(f"wrong number of arguments for '{name.lower()}' command")

    # -------------------------------------------------------------- snapshots
    def _after_write(self) -> None:
        self._writes_since_snapshot += 1
        if self._writes_since_snapshot >= int(self.config.snapshot_every_writes):
            self.snapshot()

    def snapshot(self) -> Optional[int]:
        """Snapshot now, returning the new generation or ``None`` if one is running."""
        if not self._snapshot_lock.acquire(blocking=False):
            return None
        try:
            if self._writes_since_snapshot <= 0:
                return None
            with self.store.lock:
                seq = self.persistence.snapshot(self.store.dump_state())
                self._writes_since_snapshot = 0
                self._last_snapshot = time.time()
            log.debug("wrote snapshot generation %d", seq)
            return seq
        finally:
            self._snapshot_lock.release()

    def _maintenance_loop(self) -> None:
        interval = float(self.config.snapshot_interval)
        while not self._stop.wait(min(interval, 1.0)):
            if self._writes_since_snapshot <= 0:
                continue
            if time.time() - self._last_snapshot < interval:
                continue
            try:
                self.snapshot()
            except Exception:  # pragma: no cover - defensive
                log.exception("background snapshot failed")

    # ---------------------------------------------------------------- teardown
    def close(self, snapshot: bool = True) -> None:
        """Stop background work and release the log file.

        Pass ``snapshot=False`` to shut down without the final snapshot, which
        is how tests reproduce a process that died right after a write.
        """
        if self._closed:
            return
        self._closed = True
        self._stop.set()
        if self._maintenance.is_alive() and threading.current_thread() is not self._maintenance:
            self._maintenance.join(timeout=5.0)
        try:
            if snapshot and self._writes_since_snapshot > 0:
                self.snapshot()
        finally:
            self.persistence.close()

