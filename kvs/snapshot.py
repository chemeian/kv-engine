"""Crash-safe full-state snapshots.

The on-disk layout is a tiny binary format with a CRC32 trailer so that a
corrupt or partially written file is detected instead of silently loaded::

    magic "KVSNAP01" | seq u64 | count u64 | (klen u32 | key | vlen u32 | value | expire_at f64)* | crc32 u32
"""

from __future__ import annotations

import os
import struct
import zlib
from typing import Iterable, List, Optional, Tuple

MAGIC = b"KVSNAP01"
_CRC = struct.Struct("<I")
_EXPIRE = struct.Struct("<d")
_LENGTHS = struct.Struct("<II")


class SnapshotError(Exception):
    """Raised when a snapshot file cannot be trusted."""


def write_snapshot(path: str, seq: int, items: Iterable[Tuple[str, str, float]]) -> None:
    """Atomically write a snapshot covering all writes up to *seq*."""
    items = list(items)
    body = bytearray()
    body += struct.pack("<QQ", seq, len(items))
    for key, value, expire_at in items:
        key_bytes = key.encode("utf-8", "surrogateescape")
        val_bytes = value.encode("utf-8", "surrogateescape")
        body += _LENGTHS.pack(len(key_bytes), len(val_bytes))
        body += key_bytes
        body += val_bytes
        body += _EXPIRE.pack(float(expire_at or 0.0))
    checksum = zlib.crc32(bytes(body)) & 0xFFFFFFFF
    payload = MAGIC + bytes(body) + _CRC.pack(checksum)
    tmp = path + ".tmp"
    with open(tmp, "wb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, path)


def read_snapshot(path: str) -> Optional[Tuple[int, List[Tuple[str, str, float]]]]:
    """Load a snapshot, or return ``None`` when the file does not exist."""
    if not os.path.exists(path):
        return None
    with open(path, "rb") as handle:
        data = handle.read()
    if len(data) < len(MAGIC) + _CRC.size:
        raise SnapshotError("snapshot file is truncated")
    if data[:len(MAGIC)] != MAGIC:
        raise SnapshotError("bad snapshot magic")
    body = data[len(MAGIC):len(data) - _CRC.size]
    stored = _CRC.unpack(data[-_CRC.size:])[0]
    if zlib.crc32(body) & 0xFFFFFFFF != stored:
        raise SnapshotError("snapshot checksum mismatch")
    if len(body) < 16:
        raise SnapshotError("snapshot header is truncated")
    seq, count = struct.unpack_from("<QQ", body, 0)
    position = 16
    items: List[Tuple[str, str, float]] = []
    for _ in range(count):
        key_len, val_len = _LENGTHS.unpack_from(body, position)
        position += _LENGTHS.size
        if position + key_len + val_len + _EXPIRE.size > len(body):
            raise SnapshotError("snapshot entry overruns the payload")
        key = body[position:position + key_len].decode("utf-8", "surrogateescape")
        position += key_len
        value = body[position:position + val_len].decode("utf-8", "surrogateescape")
        position += val_len
        expire_at = _EXPIRE.unpack_from(body, position)[0]
        position += _EXPIRE.size
        items.append((key, value, expire_at))
    return seq, items