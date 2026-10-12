"""A small persistent key/value storage engine (Redis subset, stdlib only)."""

from .engine import Engine
from .store import KVStore

__all__ = ["Engine", "KVStore"]
__version__ = "1.0.0"