"""A tiny Redis-like key/value engine with TTL expiry, AOF + snapshot
persistence, thread safety and a RESP TCP server.  Standard library only."""

from .config import Config
from .engine import CommandError, Engine
from .store import KVStore, match_pattern

__all__ = ["Config", "Engine", "CommandError", "KVStore", "match_pattern", "__version__"]

__version__ = "1.0.0"

