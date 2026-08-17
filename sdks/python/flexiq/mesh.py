"""Mesh scheduling configuration for decentralized task dispatch."""

from __future__ import annotations

import json
from typing import Any


class MeshWorker:
    """Configuration for mesh-enabled workers.

    Pass an instance to ``queue.run_worker(mesh=...)`` to enable
    gossip-based worker discovery, consistent-hashing task affinity,
    and work-stealing.

    Requires the ``mesh`` cargo feature at build time.
    """

    __slots__ = (
        "advertise_addr",
        "affinity_weight",
        "bind_addr",
        "encryption_key",
        "local_buffer",
        "port",
        "seeds",
        "steal",
        "steal_batch",
        "steal_rate_limit",
        "steal_threshold",
        "virtual_nodes",
    )

    def __init__(
        self,
        *,
        port: int = 7946,
        seeds: list[str] | None = None,
        steal: bool = True,
        affinity_weight: float = 0.7,
        local_buffer: int = 64,
        steal_batch: int = 4,
        steal_threshold: int = 2,
        virtual_nodes: int = 150,
        bind_addr: str = "0.0.0.0",
        advertise_addr: str | None = None,
        encryption_key: str | None = None,
        steal_rate_limit: int = 10,
    ) -> None:
        if not 1024 <= port <= 65535:
            raise ValueError(f"port must be 1024-65535, got {port}")
        if not 0.0 <= affinity_weight <= 1.0:
            raise ValueError(f"affinity_weight must be 0.0-1.0, got {affinity_weight}")
        if local_buffer < 1:
            raise ValueError(f"local_buffer must be >= 1, got {local_buffer}")
        if steal_batch < 1:
            raise ValueError(f"steal_batch must be >= 1, got {steal_batch}")
        if virtual_nodes < 1:
            raise ValueError(f"virtual_nodes must be >= 1, got {virtual_nodes}")

        self.port = port
        self.seeds = seeds or []
        self.steal = steal
        self.affinity_weight = affinity_weight
        self.local_buffer = local_buffer
        self.steal_batch = steal_batch
        self.steal_threshold = steal_threshold
        self.virtual_nodes = virtual_nodes
        self.bind_addr = bind_addr
        self.advertise_addr = advertise_addr
        self.encryption_key = encryption_key
        self.steal_rate_limit = steal_rate_limit

    def to_json(self) -> str:
        """Serialize to JSON for passing through the PyO3 boundary."""
        return json.dumps(self._as_rust_config())

    def _as_rust_config(self) -> dict[str, Any]:
        return {
            "gossip_port": self.port,
            "steal_port": self.port + 1,
            "bind_addr": self.bind_addr,
            "seeds": self.seeds,
            "protocol_period_ms": 500,
            "indirect_ping_count": 3,
            "suspicion_multiplier": 4,
            "virtual_nodes": self.virtual_nodes,
            "local_buffer_capacity": self.local_buffer,
            "max_steal_batch": self.steal_batch,
            "steal_threshold": self.steal_threshold,
            "affinity_weight": self.affinity_weight,
            "enable_stealing": self.steal,
            "advertise_addr": self.advertise_addr,
            "encryption_key": self.encryption_key,
            "steal_rate_limit": self.steal_rate_limit,
        }

    def __repr__(self) -> str:
        return (
            f"MeshWorker(port={self.port}, seeds={self.seeds!r}, "
            f"steal={self.steal}, affinity_weight={self.affinity_weight}, "
            f"local_buffer={self.local_buffer})"
        )
