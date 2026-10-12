"""Append-only-file persistence.

Every write command is serialised as a RESP array and appended to the AOF
*before* it is applied to memory (write-ahead logging).  Each record is
prefixed with a monotonically increasing sequence number so that recovery can
skip records that are already covered by a snapshot::

    *3\r\n$1\r\n7\r\n$3\r\nSET\r\n$3\r\nfoo\r\n
    ^         ^
    |         +-- command and arguments
    +------------ sequence number
"""

from __future__ import annotations

import os
from typing import Iterator, List, Sequence, Tuple

from . import resp

_FSYNC_POLICIES = ("always", "everysec", "no")


class AOF:
    """An append-only log of write commands."""

    def __init__(self, path: str, fsync_policy: str = "always") -> None:
        if fsync_policy not in _FSYNC_POLICIES:
            raise ValueError("fsync_policy must be one of %r" % (_FSYNC_POLICIES,))
        self.path = path
        self.fsync_policy = fsync_policy
        self._file = None
        self._dirty = False

    # -- lifecycle ---------------------------------------------------------
    def open(self) -> None:
        self._file = open(self.path, "ab", buffering=0)
        self._dirty = False

    def close(self) -> None:
        if self._file is None:
            return
        try:
            self._file.flush()
            os.fsync(self._file.fileno())
        finally:
            self._file.close()
            self._file = None

    # -- writing -----------------------------------------------------------
    def append(self, seq: int, args: Sequence[str]) -> None:
        """Append a single command record."""
        record = resp.encode_command([str(seq)] + [resp.to_bytes(arg) for arg in args])
        self._file.write(record)
        if self.fsync_policy == "always":
            self._file.flush()
            os.fsync(self._file.fileno())
            self._dirty = False
        else:
            self._dirty = True

    def sync(self) -> None:
        if self._file is None:
            return
        self._file.flush()
        try:
            os.fsync(self._file.fileno())
        except OSError:
            pass
        self._dirty = False

    def needs_sync(self) -> bool:
        return self.fsync_policy == "everysec" and self._dirty

    def size(self) -> int:
        try:
            return os.path.getsize(self.path)
        except OSError:
            return 0

    # -- reading -----------------------------------------------------------
    def iter_records(self) -> Iterator[Tuple[int, List[bytes]]]:
        """Yield ``(seq, args)`` for every complete record in the file.

        Parsing stops silently at a truncated tail, which is what a crash
        during ``write`` leaves behind.
        """
        if not os.path.exists(self.path):
            return
        with open(self.path, "rb") as handle:
            data = handle.read()
        position = 0
        while position < len(data):
            try:
                parsed = resp.parse_request(data, position)
            except resp.ProtocolError:
                break
            if parsed is None:
                break
            args, position = parsed
            if not args:
                continue
            try:
                seq = int(resp.to_str(args[0]))
            except ValueError:
                break
            yield seq, list(args[1:])

    def replay(self) -> Iterator[Tuple[int, str, List[str]]]:
        """Yield ``(seq, command, args)`` with everything decoded to ``str``."""
        for seq, args in self.iter_records():
            if not args:
                continue
            yield seq, resp.to_str(args[0]), [resp.to_str(a) for a in args[1:]]

    # -- maintenance -------------------------------------------------------
    def compact(self, min_seq: int) -> None:
        """Rewrite the file, keeping only records with ``seq >= min_seq``.

        The rewrite is atomic (temp file + rename), so a crash mid-compaction
        leaves the original AOF intact.
        """
        if self._file is not None:
            self._file.flush()
        kept = [resp.encode_command(args) for seq, args in self.iter_records() if seq >= min_seq]
        tmp = self.path + ".tmp"
        with open(tmp, "wb") as handle:
            for record in kept:
                handle.write(record)
            handle.flush()
            os.fsync(handle.fileno())
        if self._file is not None:
            self._file.close()
            self._file = None
        os.replace(tmp, self.path)
        self._file = open(self.path, "ab", buffering=0)
        self._dirty = False