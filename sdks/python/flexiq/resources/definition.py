"""Resource definition and scope types."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from flexiq.enums import coerce_enum


class ResourceScope(Enum):
    """Lifetime of a resource instance. Names and wire forms are shared across SDKs.

    ``REQUEST`` (a fresh instance per *resolve*) has no Python equivalent:
    resources arrive by injection, resolved once per task, so there is no second
    resolve to build for.
    """

    WORKER = "worker"
    """Built once, lazily, and shared by every task on the worker."""

    THREAD = "thread"
    """Built once per worker thread and shared by every task on that thread."""

    TASK = "task"
    """Built fresh per task and torn down when the task ends."""

    POOLED = "pooled"
    """Checked out of a bounded pool for the task's duration, returned at task end."""


@dataclass
class ResourceDefinition:
    """Describes how to create, health-check, and tear down a resource."""

    name: str
    factory: Callable[..., Any]
    teardown: Callable | None = None
    health_check: Callable | None = None
    health_check_interval: float = 0.0  # seconds, 0 = disabled
    max_recreation_attempts: int = 3
    scope: ResourceScope = ResourceScope.WORKER
    depends_on: list[str] = field(default_factory=list)
    # Pool config (POOLED scope only)
    pool_size: int | None = None  # None = worker thread count
    pool_min: int = 0
    acquire_timeout: float = 10.0
    max_lifetime: float = 3600.0
    idle_timeout: float = 300.0
    # Behavior flags
    reloadable: bool = False
    frozen: bool = False

    def __post_init__(self) -> None:
        # Coerce first: a wire string is accepted here (TOML config, hand-built
        # definitions), and the runtime dispatches on enum identity — a raw string
        # would match no branch and leave the resource silently uninitialized.
        self.scope = coerce_enum(ResourceScope, self.scope, param="scope")
        # Pool tuning on a non-pooled scope is the one way the rename can go wrong
        # silently: `scope=TASK` used to mean "checkout from a pool", and now means
        # "fresh per task". Reject it here so that misread fails loudly.
        if self.scope is not ResourceScope.POOLED and (
            self.pool_size is not None or self.pool_min
        ):
            raise ValueError(
                f"resource {self.name!r}: pool_size/pool_min require "
                f"scope={ResourceScope.POOLED}, got scope={self.scope}"
            )
