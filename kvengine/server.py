"""A small threaded TCP server speaking the RESP wire protocol."""

from __future__ import annotations

import argparse
import logging
import socketserver
import sys
from typing import Optional, Sequence, Tuple

from . import resp
from .config import Config
from .engine import Engine

log = logging.getLogger("kvengine.server")


class _Handler(socketserver.StreamRequestHandler):
    disable_nagle_algorithm = True

    def handle(self) -> None:
        engine: Engine = self.server.engine
        reader = resp.RespReader(self.rfile)
        while True:
            try:
                argv = reader.read_command()
            except resp.ConnectionClosed:
                return
            except resp.ProtocolError as exc:
                self._reply(resp.encode_error(f"ERR Protocol error: {exc}"))
                return
            if not argv:
                continue
            if argv[0].upper() == "QUIT":
                self._reply(resp.encode_simple("OK"))
                return
            self._reply(engine.execute(argv[0], argv[1:]))

    def _reply(self, payload: bytes) -> None:
        self.wfile.write(payload)


class KVServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True
    request_queue_size = 128

    def __init__(self, address: Tuple[str, int], engine: Engine) -> None:
        self.engine = engine
        super().__init__(address, _Handler)

    def handle_error(self, request, client_address) -> None:
        exc = sys.exc_info()[1]
        if isinstance(exc, (ConnectionResetError, BrokenPipeError, ConnectionAbortedError)):
            log.debug("client %s disconnected: %s", client_address, exc)
            return
        log.exception("error while serving %s", client_address)


def build_config(args: argparse.Namespace) -> Config:
    return Config(
        host=args.host,
        port=args.port,
        data_dir=args.data_dir,
        snapshot_interval=args.snapshot_interval,
        snapshot_every_writes=args.snapshot_every,
        fsync=not args.no_fsync,
    )


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="kvengine",
        description="A tiny persistent, TTL-aware key/value server speaking RESP.",
    )
    parser.add_argument("--host", default="127.0.0.1", help="bind address (default 127.0.0.1)")
    parser.add_argument("--port", type=int, default=6379, help="TCP port (default 6379)")
    parser.add_argument("--data-dir", default="data", help="where AOF/snapshots live")
    parser.add_argument(
        "--snapshot-interval",
        type=float,
        default=60.0,
        help="seconds between periodic snapshots (default 60)",
    )
    parser.add_argument(
        "--snapshot-every",
        type=int,
        default=1000,
        help="write commands between snapshots (default 1000)",
    )
    parser.add_argument(
        "--no-fsync",
        action="store_true",
        help="skip fsync after each append (faster, less durable)",
    )
    parser.add_argument("--log-level", default="INFO", help="DEBUG/INFO/WARNING/ERROR")
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    config = build_config(args)
    engine = Engine(config)
    server = KVServer((config.host, config.port), engine)
    host, port = server.server_address[0], server.server_address[1]
    log.info("listening on %s:%s, data dir %s", host, port, config.data_dir)
    try:
        server.serve_forever(poll_interval=0.2)
    except KeyboardInterrupt:
        log.info("interrupted, shutting down")
    finally:
        server.server_close()
        engine.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())

