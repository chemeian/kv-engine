"""RESP command dispatch, shared by the TCP server and the unit tests.

Keeping this separate from the socket handling means the exact byte-level
behaviour of every command can be tested without opening a port.
"""

from __future__ import annotations

from typing import List, Tuple

from . import resp
from .engine import Engine


class CommandError(Exception):
    """A user-facing command error, reported to the client as ``-ERR ...``."""


def execute(engine: Engine, args: List[bytes]) -> bytes:
    """Run one command and return the encoded RESP reply."""
    if not args:
        return resp.encode_error("ERR empty command")
    name = resp.to_str(args[0]).upper()
    handler = _HANDLERS.get(name)
    if handler is None:
        return resp.encode_error("ERR unknown command '%s'" % resp.to_str(args[0]))
    try:
        return handler(engine, args[1:])
    except CommandError as exc:
        return resp.encode_error("ERR " + str(exc))


# --------------------------------------------------------------------------
# Argument helpers
# --------------------------------------------------------------------------
def _parse_expire_options(args: List[bytes], start: int) -> Tuple:
    ex = px = exat = pxat = None
    index = start
    while index < len(args):
        option = resp.to_str(args[index]).upper()
        if option in ("EX", "PX", "EXAT", "PXAT"):
            if index + 1 >= len(args):
                raise CommandError("syntax error")
            try:
                value = float(resp.to_str(args[index + 1]))
            except ValueError:
                raise CommandError("value is not a number")
            if option == "EX":
                ex = value
            elif option == "PX":
                px = value
            elif option == "EXAT":
                exat = value
            else:
                pxat = value
            index += 2
        else:
            raise CommandError("syntax error")
    return ex, px, exat, pxat


# --------------------------------------------------------------------------
# Command handlers
# --------------------------------------------------------------------------
def _cmd_set(engine: Engine, args: List[bytes]) -> bytes:
    if len(args) < 2:
        raise CommandError("wrong number of arguments for 'set' command")
    key, value = resp.to_str(args[0]), resp.to_str(args[1])
    ex, px, exat, pxat = _parse_expire_options(args, 2)
    now = engine.now()
    expire_at = None
    if exat is not None:
        expire_at = exat
    elif pxat is not None:
        expire_at = pxat / 1000.0
    elif ex is not None:
        expire_at = now + ex
    elif px is not None:
        expire_at = now + px / 1000.0
    if expire_at is not None and expire_at <= now:
        raise CommandError("invalid expire time in 'set' command")
    engine.set(key, value, expire_at=expire_at)
    return resp.encode_simple("OK")


def _cmd_get(engine: Engine, args: List[bytes]) -> bytes:
    if len(args) != 1:
        raise CommandError("wrong number of arguments for 'get' command")
    return resp.encode_bulk(engine.get(resp.to_str(args[0])))


def _cmd_del(engine: Engine, args: List[bytes]) -> bytes:
    if not args:
        raise CommandError("wrong number of arguments for 'del' command")
    removed = sum(engine.delete(resp.to_str(key)) for key in args)
    return resp.encode_integer(removed)


def _cmd_exists(engine: Engine, args: List[bytes]) -> bytes:
    if not args:
        raise CommandError("wrong number of arguments for 'exists' command")
    return resp.encode_integer(sum(engine.exists(resp.to_str(key)) for key in args))


def _cmd_expire(engine: Engine, args: List[bytes]) -> bytes:
    if len(args) != 2:
        raise CommandError("wrong number of arguments for 'expire' command")
    try:
        seconds = int(resp.to_str(args[1]))
    except ValueError:
        raise CommandError("value is not an integer or out of range")
    return resp.encode_integer(engine.expire(resp.to_str(args[0]), seconds))


def _cmd_ttl(engine: Engine, args: List[bytes]) -> bytes:
    if len(args) != 1:
        raise CommandError("wrong number of arguments for 'ttl' command")
    return resp.encode_integer(engine.ttl(resp.to_str(args[0])))


def _cmd_keys(engine: Engine, args: List[bytes]) -> bytes:
    if len(args) != 1:
        raise CommandError("wrong number of arguments for 'keys' command")
    return resp.encode_array(engine.keys(resp.to_str(args[0])))


def _cmd_dbsize(engine: Engine, args: List[bytes]) -> bytes:
    return resp.encode_integer(len(engine.keys("*")))


def _cmd_ping(engine: Engine, args: List[bytes]) -> bytes:
    if args:
        return resp.encode_bulk(resp.to_str(args[0]))
    return resp.encode_simple("PONG")


def _cmd_echo(engine: Engine, args: List[bytes]) -> bytes:
    if len(args) != 1:
        raise CommandError("wrong number of arguments for 'echo' command")
    return resp.encode_bulk(resp.to_str(args[0]))


def _cmd_select(engine: Engine, args: List[bytes]) -> bytes:
    return resp.encode_simple("OK")


def _cmd_quit(engine: Engine, args: List[bytes]) -> bytes:
    return resp.encode_simple("OK")


def _cmd_command(engine: Engine, args: List[bytes]) -> bytes:
    # redis-cli asks for COMMAND DOCS on connect; an empty array is accepted.
    return resp.encode_array([])


_HANDLERS = {
    "SET": _cmd_set,
    "GET": _cmd_get,
    "DEL": _cmd_del,
    "EXISTS": _cmd_exists,
    "EXPIRE": _cmd_expire,
    "TTL": _cmd_ttl,
    "KEYS": _cmd_keys,
    "DBSIZE": _cmd_dbsize,
    "PING": _cmd_ping,
    "ECHO": _cmd_echo,
    "SELECT": _cmd_select,
    "QUIT": _cmd_quit,
    "COMMAND": _cmd_command,
}