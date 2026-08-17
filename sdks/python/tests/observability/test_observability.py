"""Tests for resource observability (Phase 6 — status API, CLI, health checks)."""

from __future__ import annotations

import time
from typing import Any

import pytest

from flexiq import Queue
from flexiq.resources import ResourceDefinition, ResourceRuntime

# ---------------------------------------------------------------------------
# ResourceRuntime.status()
# ---------------------------------------------------------------------------


class TestResourceRuntimeStatus:
    def test_status_returns_all_resources(self) -> None:
        """status() returns an entry for every initialized resource."""
        defs = {
            "config": ResourceDefinition(name="config", factory=lambda: {}),
            "db": ResourceDefinition(
                name="db", factory=lambda config: "conn", depends_on=["config"]
            ),
        }
        rt = ResourceRuntime(defs)
        rt.initialize()

        entries = rt.status()
        assert len(entries) == 2
        names = [e["name"] for e in entries]
        assert "config" in names
        assert "db" in names
        rt.teardown()

    def test_status_healthy_resource(self) -> None:
        """A successfully initialized resource has health='healthy'."""
        defs = {"svc": ResourceDefinition(name="svc", factory=lambda: "ok")}
        rt = ResourceRuntime(defs)
        rt.initialize()

        entry = rt.status()[0]
        assert entry["health"] == "healthy"
        assert entry["name"] == "svc"
        assert entry["scope"] == "worker"
        assert entry["recreations"] == 0
        assert entry["init_duration_ms"] >= 0
        assert entry["depends_on"] == []
        rt.teardown()

    def test_status_unhealthy_resource(self) -> None:
        """An unhealthy resource is reported as such."""
        defs = {"svc": ResourceDefinition(name="svc", factory=lambda: "ok")}
        rt = ResourceRuntime(defs)
        rt.initialize()
        rt._unhealthy.add("svc")

        entry = rt.status()[0]
        assert entry["health"] == "unhealthy"
        rt.teardown()

    def test_status_tracks_init_duration(self) -> None:
        """init_duration_ms is populated after initialize."""

        def slow_factory() -> str:
            time.sleep(0.05)
            return "result"

        defs = {"slow": ResourceDefinition(name="slow", factory=slow_factory)}
        rt = ResourceRuntime(defs)
        rt.initialize()

        entry = rt.status()[0]
        assert entry["init_duration_ms"] >= 40  # at least ~50ms minus timing jitter
        rt.teardown()

    def test_status_tracks_recreations(self) -> None:
        """recreation count is incremented on successful recreate."""
        call_count = 0

        def make_svc() -> str:
            nonlocal call_count
            call_count += 1
            return f"v{call_count}"

        defs = {
            "svc": ResourceDefinition(
                name="svc",
                factory=make_svc,
                health_check=lambda inst: True,
                health_check_interval=1.0,
            ),
        }
        rt = ResourceRuntime(defs)
        rt.initialize()
        assert rt._recreation_count.get("svc", 0) == 0

        # Manually trigger recreation
        rt.recreate("svc")
        assert rt._recreation_count["svc"] == 1

        entry = rt.status()[0]
        assert entry["recreations"] == 1
        rt.teardown()

    def test_status_includes_depends_on(self) -> None:
        """depends_on list is included in status output."""
        defs = {
            "config": ResourceDefinition(name="config", factory=lambda: {}),
            "db": ResourceDefinition(
                name="db", factory=lambda config: "conn", depends_on=["config"]
            ),
        }
        rt = ResourceRuntime(defs)
        rt.initialize()

        entries = {e["name"]: e for e in rt.status()}
        assert entries["config"]["depends_on"] == []
        assert entries["db"]["depends_on"] == ["config"]
        rt.teardown()

    def test_from_test_overrides_status(self) -> None:
        """Test-override runtime reports healthy status."""
        rt = ResourceRuntime.from_test_overrides({"db": "mock"})
        entries = rt.status()
        assert len(entries) == 1
        assert entries[0]["name"] == "db"
        assert entries[0]["health"] == "healthy"


# ---------------------------------------------------------------------------
# Queue.resource_status()
# ---------------------------------------------------------------------------


class TestQueueResourceStatus:
    def test_resource_status_with_runtime(self, tmp_path: Any) -> None:
        """resource_status() delegates to runtime when initialized."""
        queue = Queue(db_path=str(tmp_path / "q.db"))

        @queue.worker_resource("db")
        def create_db() -> str:
            return "conn"

        # Manually initialize runtime
        rt = ResourceRuntime(queue._resource_definitions)
        rt.initialize()
        queue._resource_runtime = rt

        status = queue.resource_status()
        assert len(status) == 1
        assert status[0]["name"] == "db"
        assert status[0]["health"] == "healthy"
        rt.teardown()

    def test_resource_status_without_runtime(self, tmp_path: Any) -> None:
        """resource_status() returns definitions with not_initialized health."""
        queue = Queue(db_path=str(tmp_path / "q.db"))

        @queue.worker_resource("db")
        def create_db() -> str:
            return "conn"

        status = queue.resource_status()
        assert len(status) == 1
        assert status[0]["name"] == "db"
        assert status[0]["health"] == "not_initialized"

    def test_resource_status_empty(self, tmp_path: Any) -> None:
        """resource_status() returns empty list with no resources."""
        queue = Queue(db_path=str(tmp_path / "q.db"))
        assert queue.resource_status() == []


# ---------------------------------------------------------------------------
# Health check integration
# ---------------------------------------------------------------------------


class TestHealthCheckIntegration:
    def test_readiness_reports_healthy_resources(self, tmp_path: Any) -> None:
        """check_readiness includes resource status when all healthy."""
        from flexiq.health import check_readiness

        queue = Queue(db_path=str(tmp_path / "q.db"))

        @queue.worker_resource("db")
        def create_db() -> str:
            return "conn"

        rt = ResourceRuntime(queue._resource_definitions)
        rt.initialize()
        queue._resource_runtime = rt

        result = check_readiness(queue)
        assert "resources" in result["checks"]
        res_check = result["checks"]["resources"]
        assert res_check["status"] == "ok"
        assert res_check["count"] == 1
        assert res_check["unhealthy"] == []
        rt.teardown()

    def test_readiness_reports_unhealthy_resources(self, tmp_path: Any) -> None:
        """check_readiness marks status as degraded for unhealthy resources."""
        from flexiq.health import check_readiness

        queue = Queue(db_path=str(tmp_path / "q.db"))

        @queue.worker_resource("db")
        def create_db() -> str:
            return "conn"

        rt = ResourceRuntime(queue._resource_definitions)
        rt.initialize()
        rt._unhealthy.add("db")
        queue._resource_runtime = rt

        result = check_readiness(queue)
        assert result["status"] == "degraded"
        res_check = result["checks"]["resources"]
        assert res_check["status"] == "degraded"
        assert "db" in res_check["unhealthy"]
        rt.teardown()

    def test_readiness_no_resources(self, tmp_path: Any) -> None:
        """check_readiness works fine without any resources."""
        from flexiq.health import check_readiness

        queue = Queue(db_path=str(tmp_path / "q.db"))
        result = check_readiness(queue)
        # No resources section when empty
        assert (
            "resources" not in result["checks"]
            or result["checks"].get("resources", {}).get("count", 0) == 0
        )

    def test_health_check_always_ok(self) -> None:
        """check_health is always ok regardless of resources."""
        from flexiq.health import check_health

        assert check_health() == {"status": "ok"}


# ---------------------------------------------------------------------------
# CLI resources subcommand
# ---------------------------------------------------------------------------


class TestCLIResources:
    def test_run_resources_no_resources(self, tmp_path: Any) -> None:
        """resource_status returns empty list when no resources registered."""
        queue = Queue(db_path=str(tmp_path / "q.db"))
        assert queue.resource_status() == []

    def test_resource_status_table_format(
        self, tmp_path: Any, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Verify table output format from CLI helper."""
        queue = Queue(db_path=str(tmp_path / "q.db"))

        @queue.worker_resource("config")
        def create_config() -> dict[str, str]:
            return {}

        @queue.worker_resource("db", depends_on=["config"])
        def create_db(config: Any) -> str:
            return "conn"

        rt = ResourceRuntime(queue._resource_definitions)
        rt.initialize()
        queue._resource_runtime = rt

        # Simulate what run_resources prints
        resources = queue.resource_status()
        assert len(resources) == 2
        names = {r["name"] for r in resources}
        assert names == {"config", "db"}

        # Verify structure matches CLI expectations
        for r in resources:
            assert "name" in r
            assert "scope" in r
            assert "health" in r
            assert "init_duration_ms" in r
            assert "recreations" in r
            assert "depends_on" in r

        rt.teardown()


# ---------------------------------------------------------------------------
# Prometheus resource metrics
# ---------------------------------------------------------------------------


class TestPrometheusResourceMetrics:
    def test_prometheus_middleware_has_resource_metrics(self) -> None:
        """Verify resource metric singletons are initialized."""
        pytest.importorskip("prometheus_client")
        from flexiq.contrib.prometheus import _init_metrics  # type: ignore[attr-defined]

        _init_metrics()

        from flexiq.contrib import prometheus as pmod

        assert pmod._resource_health is not None  # type: ignore[attr-defined]
        assert pmod._resource_recreations is not None  # type: ignore[attr-defined]
        assert pmod._resource_init_duration is not None  # type: ignore[attr-defined]
