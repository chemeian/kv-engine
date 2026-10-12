"""Runtime configuration for the engine and the TCP server."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass
class Config:
    """All tunables for a running engine instance.

    Defaults follow the brief: port 6379, a snapshot every 60 seconds or every
    1000 write commands, whichever comes first.
    """

    host: str = "127.0.0.1"
    port: int = 6379
    data_dir: str = "data"
    snapshot_interval: float = 60.0
    snapshot_every_writes: int = 1000
    fsync: bool = True

    def __post_init__(self) -> None:
        if not 0 < int(self.port) <= 65535:
            raise ValueError(f"port must be in 1..65535, got {self.port!r}")
        if float(self.snapshot_interval) <= 0:
            raise ValueError("snapshot_interval must be greater than 0 seconds")
        if int(self.snapshot_every_writes) <= 0:
            raise ValueError("snapshot_every_writes must be greater than 0")
        self.port = int(self.port)
        self.data_dir = os.path.abspath(os.fspath(self.data_dir))

