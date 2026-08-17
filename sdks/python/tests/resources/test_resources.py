"""Tests for the worker resource runtime (dependency injection)."""

from __future__ import annotations

from typing import Any

import pytest

from flexiq import (
    CircularDependencyError,
    Queue,
    ResourceInitError,
    ResourceNotFoundError,
)
from flexiq.resources import (
    ResourceDefinition,
    ResourceRuntime,
    detect_cycle,
    topological_sort,
)

# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------


def test_worker_resource_decorator_registers(queue: Queue) -> None:
    """@queue.worker_resource stores a ResourceDefinition."""

    @queue.worker_resource("cache")
    def create_cache() -> dict[str, int]:
        return {"hits": 0}

    assert "cache" in queue._resource_definitions
    defn = queue._resource_definitions["cache"]
    assert defn.name == "cache"
    assert defn.factory is create_cache


def test_register_resource_programmatic(queue: Queue) -> None:
    """register_resource() stores a definition without the decorator."""

    def factory() -> str:
        return "hello"

    queue.register_resource(ResourceDefinition(name="greeter", factory=factory))
    assert "greeter" in queue._resource_definitions
    assert queue._resource_definitions["greeter"].factory is factory


# ---------------------------------------------------------------------------
# Graph
# ---------------------------------------------------------------------------


def test_circular_dependency_detected(queue: Queue) -> None:
    """Circular deps raise CircularDependencyError at registration time."""

    @queue.worker_resource("a", depends_on=["b"])
    def make_a(b: Any) -> str:
        return "a"

    with pytest.raises(CircularDependencyError):

        @queue.worker_resource("b", depends_on=["a"])
        def make_b(a: Any) -> str:
            return "b"


def test_topological_sort_order() -> None:
    """Dependencies are initialized before dependents."""
    defs = {
        "config": ResourceDefinition(name="config", factory=lambda: {}),
        "db": ResourceDefinition(name="db", factory=lambda config: {}, depends_on=["config"]),
        "cache": ResourceDefinition(
            name="cache", factory=lambda config: {}, depends_on=["config"]
        ),
    }
    order = topological_sort(defs)
    assert order.index("config") < order.index("db")
    assert order.index("config") < order.index("cache")


def test_detect_cycle_returns_none_when_no_cycle() -> None:
    defs = {
        "a": ResourceDefinition(name="a", factory=lambda: 1),
        "b": ResourceDefinition(name="b", factory=lambda a: 2, depends_on=["a"]),
    }
    assert detect_cycle(defs) is None


def test_detect_cycle_returns_path() -> None:
    defs = {
        "a": ResourceDefinition(name="a", factory=lambda b: 1, depends_on=["b"]),
        "b": ResourceDefinition(name="b", factory=lambda a: 2, depends_on=["a"]),
    }
    cycle = detect_cycle(defs)
    assert cycle is not None
    assert len(cycle) >= 2


# ---------------------------------------------------------------------------
# Runtime
# ---------------------------------------------------------------------------


def test_dependency_injection_into_factory() -> None:
    """Factory receives its depends_on resources as kwargs."""
    defs = {
        "config": ResourceDefinition(name="config", factory=lambda: {"url": "sqlite://"}),
        "db": ResourceDefinition(
            name="db", factory=lambda config: f"connected:{config['url']}", depends_on=["config"]
        ),
    }
    rt = ResourceRuntime(defs)
    rt.initialize()
    assert rt.resolve("db") == "connected:sqlite://"
    rt.teardown()


def test_missing_resource_raises() -> None:
    """Resolving an unregistered name raises ResourceNotFoundError."""
    rt = ResourceRuntime({})
    with pytest.raises(ResourceNotFoundError):
        rt.resolve("nope")


def test_teardown_reverse_order() -> None:
    """Resources are torn down in reverse initialization order."""
    teardown_log: list[str] = []

    def td_config(inst: Any) -> None:
        teardown_log.append("config")

    def td_db(inst: Any) -> None:
        teardown_log.append("db")

    defs = {
        "config": ResourceDefinition(name="config", factory=lambda: "cfg", teardown=td_config),
        "db": ResourceDefinition(
            name="db",
            factory=lambda config: "db_conn",
            depends_on=["config"],
            teardown=td_db,
        ),
    }
    rt = ResourceRuntime(defs)
    rt.initialize()
    rt.teardown()
    assert teardown_log == ["db", "config"]


def test_init_failure_raises_resource_init_error() -> None:
    """A factory that raises is wrapped in ResourceInitError."""
    defs = {
        "broken": ResourceDefinition(
            name="broken",
            factory=lambda: (_ for _ in ()).throw(RuntimeError("boom")),
        ),
    }
    rt = ResourceRuntime(defs)
    with pytest.raises(ResourceInitError, match="boom"):
        rt.initialize()


def test_from_test_overrides() -> None:
    """from_test_overrides pre-populates instances without factories."""
    rt = ResourceRuntime.from_test_overrides({"db": "mock_db", "cache": "mock_cache"})
    assert rt.resolve("db") == "mock_db"
    assert rt.resolve("cache") == "mock_cache"


def test_reload_resources_without_worker_returns_empty(queue: Queue) -> None:
    """queue.reload_resources() is an empty no-op when no runtime is active."""
    assert queue.reload_resources() == {}


def test_reload_resources_recreates_reloadable(queue: Queue) -> None:
    """queue.reload_resources() re-runs reloadable factories in the live runtime."""
    counter = {"n": 0}

    @queue.worker_resource("svc", reloadable=True)
    def svc() -> int:
        counter["n"] += 1
        return counter["n"]

    queue._resource_runtime = ResourceRuntime(queue._resource_definitions)
    queue._resource_runtime.initialize()
    try:
        assert queue._resource_runtime.resolve("svc") == 1
        assert queue.reload_resources() == {"svc": True}
        assert queue._resource_runtime.resolve("svc") == 2
        assert queue.reload_resources(["svc"]) == {"svc": True}
        assert queue._resource_runtime.resolve("svc") == 3
    finally:
        queue._resource_runtime.teardown()
        queue._resource_runtime = None


# ---------------------------------------------------------------------------
# Async factory
# ---------------------------------------------------------------------------


def test_async_factory() -> None:
    """Async factories are awaited during initialize."""

    async def make_client() -> str:
        return "async_client"

    defs = {
        "client": ResourceDefinition(name="client", factory=make_client),
    }
    rt = ResourceRuntime(defs)
    rt.initialize()
    assert rt.resolve("client") == "async_client"
    rt.teardown()


# ---------------------------------------------------------------------------
# Injection into tasks
# ---------------------------------------------------------------------------


def test_resource_injected_into_task(queue: Queue) -> None:
    """Task with inject=["db"] receives the resource as a kwarg."""

    @queue.worker_resource("db")
    def create_db() -> str:
        return "live_db"

    results_holder: list[Any] = []

    @queue.task(inject=["db"])
    def my_task(x: int, db: Any = None) -> None:
        results_holder.append((x, db))

    with queue.test_mode(resources={"db": "test_db"}) as results:
        my_task.delay(42)

    assert len(results) == 1
    assert results[0].succeeded
    assert results_holder == [(42, "test_db")]


def test_explicit_kwarg_wins_over_inject(queue: Queue) -> None:
    """Caller-provided kwargs are not overridden by injection."""

    @queue.worker_resource("db")
    def create_db() -> str:
        return "injected_db"

    results_holder: list[Any] = []

    @queue.task(inject=["db"])
    def my_task(db: Any = None) -> None:
        results_holder.append(db)

    with queue.test_mode(resources={"db": "injected_db"}):
        my_task.delay(db="explicit_db")

    assert results_holder == ["explicit_db"]


def test_test_mode_with_resources(queue: Queue) -> None:
    """test_mode(resources=...) injects mock resources."""

    @queue.worker_resource("cache")
    def create_cache() -> dict[str, str]:
        return {}

    captured: list[Any] = []

    @queue.task(inject=["cache"])
    def use_cache(cache: Any = None) -> None:
        captured.append(cache)

    mock_cache = {"key": "value"}
    with queue.test_mode(resources={"cache": mock_cache}) as results:
        use_cache.delay()

    assert captured == [mock_cache]
    assert results[0].succeeded
    # Runtime is cleaned up after exiting test mode
    assert queue._resource_runtime is None


def test_test_mode_restores_previous_runtime(queue: Queue) -> None:
    """Exiting test_mode restores whatever runtime was set before."""
    assert queue._resource_runtime is None

    with queue.test_mode(resources={"x": 1}):
        assert queue._resource_runtime is not None

    assert queue._resource_runtime is None


# ---------------------------------------------------------------------------
# Health checking
# ---------------------------------------------------------------------------


def test_health_check_recreation(poll_until: Any) -> None:
    """Unhealthy resource is recreated; permanent failure marks it unavailable."""
    from flexiq.exceptions import ResourceUnavailableError
    from flexiq.resources.health import HealthChecker

    call_count = 0

    def make_svc() -> str:
        nonlocal call_count
        call_count += 1
        if call_count > 1:
            raise RuntimeError("factory broken")
        return f"svc_v{call_count}"

    def check_health(inst: Any) -> bool:
        # Always fail after initial creation
        return False

    defs = {
        "svc": ResourceDefinition(
            name="svc",
            factory=make_svc,
            health_check=check_health,
            health_check_interval=0.2,
            max_recreation_attempts=2,
        ),
    }
    rt = ResourceRuntime(defs)
    rt.initialize()
    assert rt.resolve("svc") == "svc_v1"

    checker = HealthChecker(rt)
    checker.start()
    poll_until(
        lambda: "svc" in rt._unhealthy,
        timeout=5,
        message="resource never marked unhealthy after exhausting attempts",
    )
    checker.stop()

    assert "svc" in rt._unhealthy
    with pytest.raises(ResourceUnavailableError):
        rt.resolve("svc")


# ---------------------------------------------------------------------------
# Banner
# ---------------------------------------------------------------------------


def test_banner_shows_resources(queue: Queue, capsys: pytest.CaptureFixture[str]) -> None:
    """Resources section appears in the startup banner."""

    @queue.worker_resource("db", depends_on=["config"])
    def create_db(config: Any) -> str:
        return "db"

    @queue.worker_resource("config")
    def create_config() -> dict[str, str]:
        return {}

    queue._print_banner(["default"])
    captured = capsys.readouterr().out
    assert "[resources]" in captured
    assert "config" in captured
    assert "db" in captured
    assert "depends: config" in captured


# ---------------------------------------------------------------------------
# TaskWrapper.inject property
# ---------------------------------------------------------------------------


def test_task_wrapper_inject_property(queue: Queue) -> None:
    """TaskWrapper exposes the inject list."""

    @queue.task(inject=["db", "cache"])
    def my_task(db: Any = None, cache: Any = None) -> None:
        pass

    assert my_task.inject == ["db", "cache"]


def test_task_wrapper_inject_default(queue: Queue) -> None:
    """TaskWrapper.inject defaults to empty list."""

    @queue.task()
    def my_task() -> None:
        pass

    assert my_task.inject == []
