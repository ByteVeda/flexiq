"""ResourceRuntime — initializes, resolves, and tears down worker resources."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from typing import Any

from flexiq.async_support.helpers import run_maybe_async
from flexiq.exceptions import (
    ResourceInitError,
    ResourceNotFoundError,
    ResourceUnavailableError,
)
from flexiq.resources.definition import ResourceDefinition, ResourceScope
from flexiq.resources.frozen import FrozenResource
from flexiq.resources.graph import topological_sort
from flexiq.resources.pool import PoolConfig, ResourcePool
from flexiq.resources.thread_local import ThreadLocalStore

logger = logging.getLogger("flexiq.resources")


class ResourceRuntime:
    """Manages the lifecycle of scoped resources.

    Worker-scoped resources are initialized eagerly in dependency order.
    Pooled resources get a checkout pool. Thread-scoped resources use thread-local storage.
    Task-scoped resources are created fresh per task.
    """

    def __init__(self, definitions: dict[str, ResourceDefinition]) -> None:
        self._definitions = dict(definitions)
        self._instances: dict[str, Any] = {}
        self._init_order: list[str] = []
        self._unhealthy: set[str] = set()
        self._recreation_count: dict[str, int] = {}
        self._init_duration: dict[str, float] = {}
        self._pools: dict[str, Any] = {}  # ResourcePool instances
        self._thread_locals: dict[str, Any] = {}  # ThreadLocalStore instances

    def initialize(self) -> None:
        """Create all resources in topological (dependency-first) order."""
        self._init_order = topological_sort(self._definitions)
        for name in self._init_order:
            defn = self._definitions[name]
            start = time.monotonic()

            if defn.scope == ResourceScope.WORKER:
                self._create_resource(name)
                if defn.frozen and name in self._instances:
                    self._instances[name] = FrozenResource(self._instances[name], name)
            elif defn.scope == ResourceScope.POOLED:
                self._build_pool(name)
            elif defn.scope == ResourceScope.THREAD:
                dep_kwargs = {dep: self._instances[dep] for dep in defn.depends_on}
                store = ThreadLocalStore(
                    name=name,
                    factory=defn.factory,
                    teardown=defn.teardown,
                    dep_kwargs=dep_kwargs,
                )
                self._thread_locals[name] = store
            elif defn.scope == ResourceScope.TASK:
                pass  # built fresh per task in acquire_for_task

            self._init_duration[name] = time.monotonic() - start

    def _build_pool(self, name: str) -> None:
        """Build (or rebuild) a pooled resource's checkout pool.

        Replacing the pool object is how a reload swaps it: in-flight checkouts
        release into the old, shut-down pool, which tears them down instead of
        handing them out again.
        """
        defn = self._definitions[name]
        previous = self._pools.get(name)
        dep_kwargs = {dep: self._instances[dep] for dep in defn.depends_on}
        pool = ResourcePool(
            name=name,
            factory=defn.factory,
            teardown=defn.teardown,
            config=PoolConfig(
                pool_size=defn.pool_size or 4,
                pool_min=defn.pool_min,
                acquire_timeout=defn.acquire_timeout,
                max_lifetime=defn.max_lifetime,
                idle_timeout=defn.idle_timeout,
            ),
            dep_kwargs=dep_kwargs,
        )
        if defn.pool_min > 0:
            pool.prewarm()
        self._pools[name] = pool
        if previous is not None:
            previous.shutdown()

    def _create_resource(self, name: str) -> None:
        """Invoke a resource factory, injecting its declared dependencies."""
        defn = self._definitions[name]
        dep_kwargs = {dep: self._instances[dep] for dep in defn.depends_on}
        try:
            result = run_maybe_async(defn.factory(**dep_kwargs))
            self._instances[name] = result
            self._unhealthy.discard(name)
        except Exception as exc:
            raise ResourceInitError(f"Failed to initialize resource '{name}': {exc}") from exc

    def resolve(self, name: str) -> Any:
        """Return a live resource instance by name (worker scope only).

        For task- and pooled-scoped resources, use acquire_for_task() instead.

        Raises:
            ResourceNotFoundError: If the name was never registered.
            ResourceUnavailableError: If the resource is permanently unhealthy.
        """
        if name not in self._definitions and name not in self._instances:
            raise ResourceNotFoundError(f"Resource '{name}' is not registered")
        if name in self._unhealthy:
            raise ResourceUnavailableError(f"Resource '{name}' is permanently unhealthy")

        # Worker scope — direct return
        if name in self._instances:
            return self._instances[name]

        # Thread scope — thread-local access
        if name in self._thread_locals:
            return self._thread_locals[name].get_or_create()

        # Pooled instances are checked out per task, never resolved as singletons.
        if name in self._pools:
            raise ResourceUnavailableError(
                f"Resource '{name}' is pooled — use acquire_for_task() instead"
            )

        raise ResourceNotFoundError(f"Resource '{name}' is not initialized")

    def acquire_for_task(self, name: str) -> tuple[Any, Callable[[], None] | None]:
        """Acquire a resource for a single task execution.

        Returns:
            (instance, release_callback) where release_callback is None for
            worker/thread scopes and must be called after task completion for
            task (teardown) and pooled (return to pool) scopes.
        """
        if name not in self._definitions and name not in self._instances:
            raise ResourceNotFoundError(f"Resource '{name}' is not registered")
        if name in self._unhealthy:
            raise ResourceUnavailableError(f"Resource '{name}' is permanently unhealthy")

        defn = self._definitions.get(name)
        scope = defn.scope if defn else ResourceScope.WORKER

        if scope == ResourceScope.WORKER:
            return self._instances[name], None

        if scope == ResourceScope.THREAD:
            return self._thread_locals[name].get_or_create(), None

        if scope == ResourceScope.POOLED:
            pool = self._pools[name]
            instance = pool.acquire()

            def release() -> None:
                pool.release(instance)

            return instance, release

        if scope == ResourceScope.TASK:
            deps = defn.depends_on if defn else []
            dep_kwargs = {dep: self._instances.get(dep) for dep in deps}
            instance = defn.factory(**dep_kwargs) if defn else None
            instance = run_maybe_async(instance)

            def teardown_task() -> None:
                if defn and defn.teardown is not None and instance is not None:
                    try:
                        run_maybe_async(defn.teardown(instance))
                    except Exception:
                        logger.exception("Error tearing down task resource '%s'", name)

            return instance, teardown_task

        return self._instances.get(name), None

    def recreate(self, name: str) -> bool:
        """Attempt to recreate a single resource. Returns True on success."""
        try:
            old = self._instances.get(name)
            defn = self._definitions[name]
            if defn.scope == ResourceScope.POOLED:
                self._build_pool(name)
                self._recreation_count[name] = self._recreation_count.get(name, 0) + 1
                return True
            if old is not None and defn.teardown is not None:
                run_maybe_async(defn.teardown(old))
            self._create_resource(name)
            self._recreation_count[name] = self._recreation_count.get(name, 0) + 1
            return True
        except ResourceInitError:
            return False

    def reload(self, names: list[str] | None = None) -> dict[str, bool]:
        """Reload reloadable resources. Returns {name: success} dict."""
        results: dict[str, bool] = {}
        targets = names or [n for n, d in self._definitions.items() if d.reloadable]
        # Reload in dependency order
        for name in self._init_order:
            if name not in targets:
                continue
            defn = self._definitions.get(name)
            if defn is None or (names is None and not defn.reloadable):
                continue
            results[name] = self.recreate(name)
        return results

    def mark_unhealthy(self, name: str) -> None:
        """Mark a resource as permanently unhealthy."""
        self._unhealthy.add(name)
        logger.error("Resource '%s' marked permanently unhealthy", name)

    def teardown(self) -> None:
        """Tear down all resources in reverse initialization order."""
        # Shutdown pools first
        for pool in self._pools.values():
            pool.shutdown()
        self._pools.clear()

        # Teardown thread-local stores
        for store in self._thread_locals.values():
            store.teardown_all()
        self._thread_locals.clear()

        # Teardown worker-scoped resources in reverse order
        for name in reversed(self._init_order):
            defn = self._definitions.get(name)
            instance = self._instances.pop(name, None)
            if defn is not None and defn.teardown is not None and instance is not None:
                try:
                    run_maybe_async(defn.teardown(instance))
                except Exception:
                    logger.exception("Error tearing down resource '%s'", name)
        self._init_order.clear()

    def status(self) -> list[dict[str, Any]]:
        """Return a summary of each resource's current state."""
        result: list[dict[str, Any]] = []
        for name in self._init_order:
            defn = self._definitions.get(name)
            if name in self._unhealthy:
                health = "unhealthy"
            elif name in self._instances or name in self._pools or name in self._thread_locals:
                health = "healthy"
            else:
                health = "unknown"
            entry: dict[str, Any] = {
                "name": name,
                "scope": defn.scope.value if defn else "worker",
                "health": health,
                "init_duration_ms": round(self._init_duration.get(name, 0) * 1000, 2),
                "recreations": self._recreation_count.get(name, 0),
                "depends_on": defn.depends_on if defn else [],
            }
            # Add pool stats for pooled resources
            if name in self._pools:
                entry["pool"] = self._pools[name].stats()
            result.append(entry)
        return result

    @classmethod
    def from_test_overrides(cls, instances: dict[str, Any]) -> ResourceRuntime:
        """Create a runtime pre-populated with mock/test instances.

        No factories are called — the provided instances are used directly.
        """
        rt = cls(definitions={})
        rt._instances = dict(instances)
        rt._init_order = list(instances.keys())
        return rt
