"""A minimal implementation of the RESP wire protocol (Redis protocol).

Only the pieces this project needs are implemented:

* request parsing, both multibulk (what real clients send) and inline
  commands (what ``netcat`` users type);
* reply encoding for the reply types this server produces;
* reply decoding, used by the bundled test/mini client.
"""

from __future__ import annotations

import shlex
from typing import List, Optional, Sequence, Tuple

CRLF = b"\r\n"

# RESP type markers.
_ARRAY = 0x2A       # '*'
_BULK = 0x24        # '$'
_SIMPLE = 0x2B      # '+'
_ERROR = 0x2D       # '-'
_INTEGER = 0x3A     # ':'


class ProtocolError(Exception):
    """Raised when a client sends bytes that violate the RESP grammar."""


class SimpleString(str):
    """A RESP simple string, e.g. the ``+OK`` reply."""


class ErrorReply(str):
    """A RESP error, e.g. ``-ERR unknown command``."""


def to_bytes(value) -> bytes:
    """Coerce ``str``/``bytes`` into ``bytes`` losslessly."""
    if isinstance(value, str):
        return value.encode("utf-8", "surrogateescape")
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value)
    return str(value).encode("utf-8", "surrogateescape")


def to_str(value) -> str:
    """Coerce ``bytes``/``str`` into ``str`` losslessly."""
    if isinstance(value, str):
        return value
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).decode("utf-8", "surrogateescape")
    return str(value)


# --------------------------------------------------------------------------
# Encoding
# --------------------------------------------------------------------------
def encode_command(args: Sequence) -> bytes:
    """Encode a command as a RESP array of bulk strings."""
    chunks = [b"*%d\r\n" % len(args)]
    for arg in args:
        blob = to_bytes(arg)
        chunks.append(b"$%d\r\n" % len(blob))
        chunks.append(blob)
        chunks.append(CRLF)
    return b"".join(chunks)


def encode_simple(text: str) -> bytes:
    return b"+" + to_bytes(text) + CRLF


def encode_error(text: str) -> bytes:
    return b"-" + to_bytes(text) + CRLF


def encode_integer(value: int) -> bytes:
    return b":%d\r\n" % value


def encode_bulk(value) -> bytes:
    """Encode a bulk string; ``None`` becomes the RESP null bulk string."""
    if value is None:
        return b"$-1\r\n"
    blob = to_bytes(value)
    return b"$%d\r\n" % len(blob) + blob + CRLF


def encode_array(items: Sequence) -> bytes:
    return b"".join([b"*%d\r\n" % len(items)] + [encode_bulk(i) for i in items])


# --------------------------------------------------------------------------
# Request parsing
# --------------------------------------------------------------------------
def _read_line(data, pos: int):
    """Return ``(line_without_terminator, position_after)`` or ``None``."""
    index = data.find(b"\n", pos)
    if index < 0:
        return None
    line = bytes(data[pos:index])
    if line.endswith(b"\r"):
        line = line[:-1]
    return line, index + 1


def parse_request(data, pos: int = 0) -> Optional[Tuple[List[bytes], int]]:
    """Parse one client command starting at *pos*.

    Returns ``(args, next_position)`` or ``None`` when more bytes are needed.
    Raises :class:`ProtocolError` for malformed input.
    """
    if pos >= len(data):
        return None
    if data[pos] == _ARRAY:
        return _parse_multibulk(data, pos)
    return _parse_inline(data, pos)


def _parse_multibulk(data, pos: int):
    head = _read_line(data, pos + 1)
    if head is None:
        return None
    line, position = head
    try:
        count = int(line.strip() or b"0")
    except ValueError:
        raise ProtocolError("invalid multibulk length")
    if count <= 0:
        return [], position
    args: List[bytes] = []
    for _ in range(count):
        if position >= len(data):
            return None
        if data[position] != _BULK:
            raise ProtocolError("expected '$', got %r" % bytes(data[position:position + 1]))
        head = _read_line(data, position + 1)
        if head is None:
            return None
        line, position = head
        try:
            length = int(line.strip())
        except ValueError:
            raise ProtocolError("invalid bulk length")
        if length < 0:
            raise ProtocolError("invalid bulk length")
        end = position + length
        if len(data) < end + 1:
            return None
        args.append(bytes(data[position:end]))
        tail = bytes(data[end:end + 2])
        if tail == CRLF:
            position = end + 2
        elif tail[:1] == b"\n":
            position = end + 1
        else:
            return None  # trailing CRLF not fully received yet
    return args, position


def _parse_inline(data, pos: int):
    head = _read_line(data, pos)
    if head is None:
        return None
    line, position = head
    text = line.decode("utf-8", "surrogateescape").strip()
    if not text:
        return [], position
    try:
        parts = shlex.split(text)
    except ValueError as exc:
        raise ProtocolError("unbalanced quotes in request: %s" % exc)
    return [to_bytes(part) for part in parts], position


# --------------------------------------------------------------------------
# Reply parsing (client side)
# --------------------------------------------------------------------------
def parse_reply(data, pos: int = 0):
    """Parse one RESP reply. Returns ``(value, next_position)`` or ``None``.

    Reply types map to Python as: simple -> :class:`SimpleString`,
    error -> :class:`ErrorReply`, integer -> ``int``, bulk -> ``bytes``
    (``None`` for the null bulk), array -> ``list``.
    """
    if pos >= len(data):
        return None
    prefix = data[pos]
    if prefix in (_SIMPLE, _ERROR, _INTEGER):
        head = _read_line(data, pos + 1)
        if head is None:
            return None
        line, position = head
        if prefix == _SIMPLE:
            return SimpleString(line.decode("utf-8", "surrogateescape")), position
        if prefix == _ERROR:
            return ErrorReply(line.decode("utf-8", "surrogateescape")), position
        try:
            return int(line.strip()), position
        except ValueError:
            raise ProtocolError("invalid integer reply")
    if prefix == _BULK:
        head = _read_line(data, pos + 1)
        if head is None:
            return None
        line, position = head
        try:
            length = int(line.strip())
        except ValueError:
            raise ProtocolError("invalid bulk length")
        if length < 0:
            return None, position
        end = position + length
        if len(data) < end + 2:
            return None
        return bytes(data[position:end]), end + 2
    if prefix == _ARRAY:
        head = _read_line(data, pos + 1)
        if head is None:
            return None
        line, position = head
        try:
            count = int(line.strip())
        except ValueError:
            raise ProtocolError("invalid array length")
        if count < 0:
            return None, position
        items = []
        for _ in range(count):
            parsed = parse_reply(data, position)
            if parsed is None:
                return None
            value, position = parsed
            items.append(value)
        return items, position
    raise ProtocolError("unknown reply prefix %r" % bytes(data[pos:pos + 1]))