"""Minimal RESP (REdis Serialization Protocol) helpers.

Only the subset this engine needs is implemented:

* client -> server: RESP arrays of bulk strings, plus plain "inline" commands
  so that ``nc``/``telnet`` users can type commands by hand;
* server -> client: simple strings, errors, integers, bulk strings and arrays.

The same encoder is reused to write the append-only log, which keeps the log
byte-exact and trivially parseable.
"""

from __future__ import annotations

from typing import Iterable, List, Optional, Sequence, Union

CRLF = b"\r\n"
MAX_LINE = 64 * 1024
MAX_BULK = 512 * 1024 * 1024


class ProtocolError(Exception):
    """The bytes on the wire are not valid RESP."""


class ConnectionClosed(Exception):
    """The peer went away in the middle of a command."""


def encode_simple(text: str) -> bytes:
    return b"+" + text.encode("utf-8") + CRLF


def encode_error(text: str) -> bytes:
    return b"-" + text.encode("utf-8") + CRLF


def encode_integer(value: int) -> bytes:
    return b":" + str(int(value)).encode("ascii") + CRLF


def encode_bulk(data: Union[str, bytes, None]) -> bytes:
    if data is None:
        return b"$-1\r\n"
    raw = data.encode("utf-8") if isinstance(data, str) else bytes(data)
    return b"$" + str(len(raw)).encode("ascii") + CRLF + raw + CRLF


def encode_array(items: Iterable[bytes]) -> bytes:
    encoded: List[bytes] = list(items)
    out = bytearray(b"*" + str(len(encoded)).encode("ascii") + CRLF)
    for item in encoded:
        out += item
    return bytes(out)


def encode_command(args: Sequence[str]) -> bytes:
    """Encode a command as a RESP array of bulk strings (used for the AOF)."""
    out = bytearray(b"*" + str(len(args)).encode("ascii") + CRLF)
    for arg in args:
        raw = arg.encode("utf-8")
        out += b"$" + str(len(raw)).encode("ascii") + CRLF + raw + CRLF
    return bytes(out)


class RespReader:
    """Pull commands from a binary stream (socket file object or open file).

    Line endings are accepted as CRLF or bare LF, which makes the server usable
    with ``netcat``.  Bulk payloads keep the strict RESP framing.
    """

    def __init__(self, stream) -> None:
        self._stream = stream

    def _read_line(self) -> bytes:
        line = self._stream.readline(MAX_LINE + 2)
        if not line:
            raise ConnectionClosed()
        if line.endswith(CRLF):
            return line[:-2]
        if line.endswith(b"\n"):
            return line[:-1]
        raise ProtocolError("unterminated line")

    def _read_exact(self, size: int) -> bytes:
        chunks = bytearray()
        while len(chunks) < size:
            chunk = self._stream.read(size - len(chunks))
            if not chunk:
                raise ConnectionClosed()
            chunks += chunk
        return bytes(chunks)

    def read_command(self) -> Optional[List[str]]:
        """Return the next command, or ``None`` for a blank inline line."""
        line = self._read_line()
        if not line:
            return None
        marker = line[:1]
        if marker == b"*":
            return self._read_multibulk(line)
        if marker in (b"+", b"-", b":", b"$"):
            raise ProtocolError("unexpected reply-style type from client")
        return line.decode("utf-8", "surrogateescape").split()

    def _read_multibulk(self, header: bytes) -> List[str]:
        try:
            count = int(header[1:])
        except ValueError:
            raise ProtocolError("invalid multibulk length") from None
        if count <= 0:
            return []
        args: List[str] = []
        for _ in range(count):
            head = self._read_line()
            if head[:1] != b"$":
                raise ProtocolError("expected a bulk string inside a command")
            try:
                length = int(head[1:])
            except ValueError:
                raise ProtocolError("invalid bulk length") from None
            if length < 0:
                raise ProtocolError("null bulk string inside a command")
            if length > MAX_BULK:
                raise ProtocolError("bulk string is too large")
            args.append(self._read_exact(length).decode("utf-8", "surrogateescape"))
            if self._read_exact(2) != CRLF:
                raise ProtocolError("bulk string is not terminated by CRLF")
        return args

