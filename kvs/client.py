"""A tiny RESP client: used by the tests and as a ``redis-cli`` substitute."""

from __future__ import annotations

import argparse
import shlex
import socket
from typing import Optional, Sequence

from . import resp


class KVClient:
    """A blocking RESP client over a single socket."""

    def __init__(self, host: str = "127.0.0.1", port: int = 6379,
                 timeout: float = 10.0) -> None:
        self._socket = socket.create_connection((host, port), timeout=timeout)
        self._buffer = bytearray()

    def close(self) -> None:
        try:
            self._socket.close()
        except OSError:
            pass

    def __enter__(self) -> "KVClient":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def send_command(self, *args) -> None:
        self._socket.sendall(resp.encode_command([resp.to_bytes(a) for a in args]))

    def read_reply(self):
        while True:
            parsed = resp.parse_reply(self._buffer, 0)
            if parsed is not None:
                value, consumed = parsed
                del self._buffer[:consumed]
                return value
            chunk = self._socket.recv(65536)
            if not chunk:
                raise ConnectionError("connection closed by server")
            self._buffer.extend(chunk)

    def command(self, *args):
        self.send_command(*args)
        return self.read_reply()


def _format(value) -> str:
    if isinstance(value, resp.ErrorReply):
        return "(error) %s" % value
    if value is None:
        return "(nil)"
    if isinstance(value, resp.SimpleString):
        return str(value)
    if isinstance(value, bool):
        return "(integer) %d" % int(value)
    if isinstance(value, int):
        return "(integer) %d" % value
    if isinstance(value, list):
        if not value:
            return "(empty array)"
        return "\n".join("%d) %s" % (i + 1, _format(item)) for i, item in enumerate(value))
    return resp.to_str(value)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="kvs.client",
        description="Minimal redis-cli substitute for the kv-engine server.",
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("command", nargs="*", help="optional command to run once")
    args = parser.parse_args(argv)

    with KVClient(args.host, args.port) as client:
        if args.command:
            print(_format(client.command(*args.command)))
            return 0
        prompt = "%s:%d> " % (args.host, args.port)
        while True:
            try:
                line = input(prompt)
            except (EOFError, KeyboardInterrupt):
                print()
                break
            line = line.strip()
            if not line:
                continue
            if line.lower() in ("exit", "quit"):
                break
            try:
                parts = shlex.split(line)
            except ValueError as exc:
                print("parse error:", exc)
                continue
            if not parts:
                continue
            try:
                print(_format(client.command(*parts)))
            except Exception as exc:
                print("error:", exc)
                break
    return 0


if __name__ == "__main__":
    raise SystemExit(main())