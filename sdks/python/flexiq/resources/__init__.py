"""Worker resource runtime — dependency injection for flexiq tasks."""

from flexiq.resources.definition import ResourceDefinition, ResourceScope
from flexiq.resources.frozen import FrozenResource
from flexiq.resources.graph import detect_cycle, topological_sort
from flexiq.resources.health import HealthChecker
from flexiq.resources.pool import PoolConfig, ResourcePool
from flexiq.resources.runtime import ResourceRuntime
from flexiq.resources.thread_local import ThreadLocalStore

__all__ = [
    "FrozenResource",
    "HealthChecker",
    "PoolConfig",
    "ResourceDefinition",
    "ResourcePool",
    "ResourceRuntime",
    "ResourceScope",
    "ThreadLocalStore",
    "detect_cycle",
    "topological_sort",
]
