"""A threaded TCP server that speaks RESP.

Each connection gets its own thread; the shared :class:`~kvs.engine.Engine`
is thread-safe, so concurrent clients see a consistent keyspace.
"""

from __future__ import annotations

import argparse
import socketserver
from typing import Optional, Sequence

from . import commands, resp
from .engine import Engine


class _Handler(socketserver.BaseRequestHandler):
    def handle(self) -> None:
        connection = self.request
        engine = self.server.engine
        buffer = bytearray()
        while True:
            try:
                chunk = connection.recv(65536)
            except OSError:
                break
            if not chunk:
                break
            buffer.extend(chunk)
            if not self._process(connection, engine, buffer):
                break

    @staticmethod
    def _process(connection, engine, buffer: bytearray) -> bool:
        """Drain every complete command in *buffer*. Returns False to stop."""
        while True:
            try:
                parsed = resp.parse_request(buffer, 0)
            except resp.ProtocolError as exc:
                _safe_send(connection, resp.encode_error("ERR Protocol error: %s" % exc))
                return False
            if parsed is None:
                return True
            args, consumed = parsed
            del buffer[:consumed]
            if not args:
                continue
            try:
                reply = commands.execute(engine, args)
            except Exception as exc:  # never let a bad command kill the thread
                reply = resp.encode_error("ERR internal error: %s" % exc)
            if reply and not _safe_send(connection, reply):
                return False
            if resp.to_str(args[0]).upper() == "QUIT":
                return False


def _safe_send(connection, payload: bytes) -> bool:
    try:
        connection.sendall(payload)
        return True
    except OSError:
        return False


class _ThreadingServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, address, engine: Engine) -> None:
        self.engine = engine
        super().__init__(address, _Handler)


class KVSServer:
    """Owns a listening socket for an :class:`Engine`."""

    def __init__(self, engine: Engine, host: str = "127.0.0.1", port: int = 6379) -> None:
        self.engine = engine
        self._server = _ThreadingServer((host, port), engine)

    @property
    def address(self):
        return self._server.server_address

    def serve_forever(self) -> None:
        self._server.serve_forever()

    def shutdown(self) -> None:
        self._server.shutdown()

    def server_close(self) -> None:
        self._server.server_close()

    def __enter__(self) -> "KVSServer":
        return self

    def __exit__(self, *exc) -> None:
        self.server_close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="kvs.server",
        description="Run the persistent key/value server (RESP protocol).",
    )
    parser.add_argument("--host", default="127.0.0.1", help="bind address (default 127.0.0.1)")
    parser.add_argument("--port", type=int, default=6379, help="TCP port (default 6379)")
    parser.add_argument("--dir", default="./data", help="data directory (default ./data)")
    parser.add_argument("--snapshot-interval", type=float, default=60.0,
                        help="seconds between snapshots (default 60)")
    parser.add_argument("--snapshot-writes", type=int, default=1000,
                        help="writes between snapshots (default 1000)")
    parser.add_argument("--aof-fsync", choices=("always", "everysec", "no"), default="always",
                        help="AOF durability policy (default always)")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    engine = Engine(
        args.dir,
        snapshot_interval=args.snapshot_interval,
        snapshot_writes=args.snapshot_writes,
        aof_fsync=args.aof_fsync,
    )
    engine.open()
    server = KVSServer(engine, host=args.host, port=args.port)
    host, port = server.address
    print("kv-engine listening on %s:%d" % (host, port))
    print("data dir: %s | snapshot every %gs or %d writes | aof fsync: %s"
          % (args.dir, args.snapshot_interval, args.snapshot_writes, args.aof_fsync))
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nshutting down...")
    finally:
        server.server_close()
        engine.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())