"""Append-only log plus periodic full snapshots.

On-disk layout, where ``seq`` is a monotonically increasing generation number::

    <data_dir>/snapshot-00000001.json   full state, written atomically
    <data_dir>/aof-00000001.aof         writes that happened *after* that snapshot

Taking a snapshot means writing ``snapshot-<n+1>.json`` to a temporary file and
renaming it into place, and only then switching to a fresh ``aof-<n+1>.aof``
and dropping generation ``n``.  Because the whole sequence runs under one lock,
a crash at any point leaves either generation ``n`` or generation ``n+1``
complete on disk, and restart replays exactly the tail the snapshot does not
cover.
"""

from __future__ import annotations

import json
import os
import re
import threading
import time
from typing import Dict, List, Optional, Sequence, Tuple

from . import resp

FORMAT_VERSION = 1
_SNAPSHOT_RE = re.compile(r"^snapshot-(\d{8})\.json$")
_AOF_RE = re.compile(r"^aof-(\d{8})\.aof$")


def _remove(path: str) -> None:
    try:
        os.remove(path)
    except OSError:
        pass


class Persistence:
    """Owns the AOF handle and the snapshot lifecycle for one data directory."""

    def __init__(self, data_dir: str, fsync: bool = True) -> None:
        self.data_dir = os.path.abspath(os.fspath(data_dir))
        self.fsync = bool(fsync)
        self.seq = 0
        self._handle = None
        self._aof_path: Optional[str] = None
        self._lock = threading.RLock()
        os.makedirs(self.data_dir, exist_ok=True)

    @property
    def aof_path(self) -> Optional[str]:
        return self._aof_path

    # ------------------------------------------------------------------- paths
    def _snapshot_path(self, seq: int) -> str:
        return os.path.join(self.data_dir, f"snapshot-{seq:08d}.json")

    def _aof_path_for(self, seq: int) -> str:
        return os.path.join(self.data_dir, f"aof-{seq:08d}.aof")

    # ---------------------------------------------------------- startup / load
    def open(self) -> Tuple[Dict[str, dict], List[List[str]]]:
        """Load the newest usable snapshot and the log tail that follows it."""
        with self._lock:
            loaded = self._load_latest_snapshot()
            if loaded is None:
                self.seq, state = 0, {}
            else:
                self.seq, state = loaded
            commands = self._read_aof(self._aof_path_for(self.seq))
            self._open_aof(self.seq)
            self._cleanup(self.seq)
            return state, commands

    def _snapshot_seqs(self) -> List[int]:
        seqs = []
        for name in os.listdir(self.data_dir):
            match = _SNAPSHOT_RE.match(name)
            if match:
                seqs.append(int(match.group(1)))
        return sorted(seqs, reverse=True)

    def _load_latest_snapshot(self) -> Optional[Tuple[int, Dict[str, dict]]]:
        for seq in self._snapshot_seqs():
            try:
                return seq, self._read_snapshot(seq)
            except (OSError, ValueError, KeyError):
                continue
        return None

    def _read_snapshot(self, seq: int) -> Dict[str, dict]:
        with open(self._snapshot_path(seq), "r", encoding="utf-8") as handle:
            payload = json.load(handle)
        if not isinstance(payload, dict) or "state" not in payload:
            raise ValueError("snapshot does not have the expected shape")
        return payload["state"]

    def _read_aof(self, path: str) -> List[List[str]]:
        if not os.path.exists(path):
            return []
        commands: List[List[str]] = []
        with open(path, "rb") as handle:
            reader = resp.RespReader(handle)
            while True:
                try:
                    argv = reader.read_command()
                except (resp.ProtocolError, resp.ConnectionClosed):
                    # A torn tail is expected when the process died mid-append;
                    # everything before it was fully written and is replayed.
                    break
                if argv:
                    commands.append(argv)
        return commands

    def _open_aof(self, seq: int) -> None:
        self._aof_path = self._aof_path_for(seq)
        self._handle = open(self._aof_path, "ab")

    def _close_aof(self) -> None:
        handle, self._handle = self._handle, None
        if handle is not None:
            try:
                handle.flush()
                if self.fsync:
                    os.fsync(handle.fileno())
            finally:
                handle.close()

    # ------------------------------------------------------------------ append
    def append(self, argv: Sequence[str]) -> None:
        """Append one command to the log and make it durable before returning."""
        payload = resp.encode_command(list(argv))
        with self._lock:
            handle = self._handle
            if handle is None:
                return
            handle.write(payload)
            handle.flush()
            if self.fsync:
                os.fsync(handle.fileno())

    # ---------------------------------------------------------------- snapshot
    def snapshot(self, state: Dict[str, dict]) -> int:
        """Persist ``state`` and roll the log over to a new generation."""
        with self._lock:
            new_seq = self.seq + 1
            path = self._snapshot_path(new_seq)
            temporary = path + ".tmp"
            payload = {
                "version": FORMAT_VERSION,
                "seq": new_seq,
                "created_at": time.time(),
                "state": state,
            }
            with open(temporary, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, ensure_ascii=False)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, path)

            self._close_aof()
            self._open_aof(new_seq)
            self.seq = new_seq
            self._cleanup(new_seq)
            return new_seq

    def _cleanup(self, keep_seq: int) -> None:
        for name in os.listdir(self.data_dir):
            path = os.path.join(self.data_dir, name)
            snapshot = _SNAPSHOT_RE.match(name)
            if snapshot:
                if int(snapshot.group(1)) != keep_seq:
                    _remove(path)
                continue
            aof = _AOF_RE.match(name)
            if aof:
                if int(aof.group(1)) != keep_seq:
                    _remove(path)
                continue
            if name.endswith(".tmp"):
                _remove(path)

    def close(self) -> None:
        with self._lock:
            self._close_aof()

