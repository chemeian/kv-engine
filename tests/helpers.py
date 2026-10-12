"""A minimal RESP client used by the socket-level tests."""

from __future__ import annotations

import socket
from typing import Any, List, Optional, Union

from kvengine import resp


class ErrorReply(str):
    """Marks a RESP error reply so tests can assert on it explicitly."""


class Client:
    """Blocking RESP client over a plain socket."""

    def __init__(self, sock: socket.socket) -> None:
        self._sock = sock
        self._file = sock.makefile("rb")

    def send_raw(self, data: bytes) -> None:
        self._sock.sendall(data)

    def call(self, *args: Any) -> Union[None, int, str, List[Any]]:
        self.send_raw(resp.encode_command([str(arg) for arg in args]))
        return self.read_reply()

    def read_reply(self) -> Union[None, int, str, List[Any]]:
        line = self._file.readline()
        if not line:
            raise EOFError("connection closed while reading a reply")
        marker, payload = line[:1], line[1:-2]
        if marker == b"+":
            return payload.decode()
        if marker == b"-":
            return ErrorReply(payload.decode())
        if marker == b":":
            return int(payload)
        if marker == b"$":
            length = int(payload)
            if length == -1:
                return None
            data = self._file.read(length)
            self._file.read(2)
            return data.decode()
        if marker == b"*":
            count = int(payload)
            if count == -1:
                return None
            return [self.read_reply() for _ in range(count)]
        raise ValueError(f"unexpected reply: {line!r}")

    def close(self) -> None:
        try:
            self._file.close()
        finally:
            self._sock.close()

