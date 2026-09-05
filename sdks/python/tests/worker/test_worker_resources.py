"""Tests for worker resource advertisement (Phase 4 — storage extension)."""

from __future__ import annotations

import json
import threading
from typing import Any

from conftest import join_worker
from flexiq import Queue

# ---------------------------------------------------------------------------
# Direct storage-level tests (no worker needed)
# ---------------------------------------------------------------------------


class TestWorkerAdvertisement:
    def test_no_resources_returns_none(self, tmp_path: Any) -> None:
        """_build_resource_health_json returns None when no resources."""
        queue = Queue(db_path=str(tmp_path / "q.db"))
        assert queue._build_resource_health_json() is None

    def test_build_resource_health_json_with_resources(self, tmp_path: Any) -> None:
        """_build_resource_health_json returns correct JSON."""
        queue = Queue(db_path=str(tmp_path / "q.db"))

        @queue.worker_resource("db")
        def create_db() -> str:
            return "db_instance"

        @queue.worker_resource("cache")
        def create_cache() -> str:
            return "cache_instance"

        health_json = queue._build_resource_health_json()
        assert health_json is not None
        health = json.loads(health_json)
        assert health == {"db": "healthy", "cache": "healthy"}

    def test_build_resource_health_reflects_unhealthy(self, tmp_path: Any) -> None:
        """_build_resource_health_json marks unhealthy resources."""
        from flexiq.resources.runtime import ResourceRuntime

        queue = Queue(db_path=str(tmp_path / "q.db"))

        @queue.worker_resource("db")
        def create_db() -> str:
            return "db_instance"

        # Simulate an initialized runtime with an unhealthy resource
        runtime = ResourceRuntime(queue._resource_definitions)
        runtime._instances = {"db": "db_instance"}
        runtime._init_order = ["db"]
        runtime._unhealthy = {"db"}
        queue._resource_runtime = runtime

        health_json = queue._build_resource_health_json()
        assert health_json is not None
        health = json.loads(health_json)
        assert health["db"] == "unhealthy"

    def test_worker_heartbeat_method(self, tmp_path: Any) -> None:
        """worker_heartbeat can be called without error."""
        queue = Queue(db_path=str(tmp_path / "q.db"))
        # Heartbeat for a non-existent worker is a no-op (updates 0 rows)
        queue._inner.worker_heartbeat("nonexistent-worker")

    def test_worker_heartbeat_with_health(self, tmp_path: Any) -> None:
        """worker_heartbeat accepts resource_health JSON."""
        queue = Queue(db_path=str(tmp_path / "q.db"))
        health = json.dumps({"db": "healthy"})
        queue._inner.worker_heartbeat("w-test", health)

    def test_list_workers_empty(self, tmp_path: Any) -> None:
        """list_workers returns empty list when no workers registered."""
        queue = Queue(db_path=str(tmp_path / "q.db"))
        workers = queue.workers()
        assert workers == []


# ---------------------------------------------------------------------------
# Integration tests with actual worker
# ---------------------------------------------------------------------------


class TestWorkerResourceIntegration:
    def test_worker_advertises_resources_and_threads(self, tmp_path: Any, poll_until: Any) -> None:
        """A running worker stores resources and threads in storage."""
        queue = Queue(db_path=str(tmp_path / "q.db"), workers=2)

        @queue.worker_resource("db")
        def create_db() -> str:
            return "db_instance"

        @queue.task()
        def noop() -> None:
            pass

        # Start worker in thread, wait for it to register
        thread = threading.Thread(target=queue.run_worker, daemon=True)
        thread.start()

        try:
            # Poll until a worker appears with resource_health populated
            # (initial registration has None; first heartbeat sets it).
            poll_until(
                lambda: bool((ws := queue.workers()) and ws[0].get("resource_health") is not None),
                timeout=15,
                message="worker did not publish resource_health",
            )
            workers: list[dict[str, Any]] = queue.workers()
            assert len(workers) >= 1
            w = workers[0]
            assert "resources" in w
            assert "resource_health" in w
            assert "threads" in w
            assert w["threads"] == 2

            # Parse resources JSON
            resources = json.loads(w["resources"])
            assert "db" in resources

            # Parse health JSON
            health = json.loads(w["resource_health"])
            assert health.get("db") == "healthy"
        finally:
            queue._inner.request_shutdown()
            join_worker(thread)

    def test_worker_no_resources(self, tmp_path: Any, poll_until: Any) -> None:
        """Worker without resources stores None for resource fields."""
        queue = Queue(db_path=str(tmp_path / "q.db"), workers=1)

        @queue.task()
        def noop() -> None:
            pass

        thread = threading.Thread(target=queue.run_worker, daemon=True)
        thread.start()

        try:
            poll_until(
                lambda: bool(queue.workers()), timeout=10, message="worker did not register"
            )
            workers: list[dict[str, Any]] = queue.workers()
            assert len(workers) >= 1
            w = workers[0]
            assert w["resources"] is None
            assert w["resource_health"] is None
        finally:
            queue._inner.request_shutdown()
            join_worker(thread)

    def test_heartbeat_updates_health(self, tmp_path: Any, poll_until: Any) -> None:
        """Heartbeat thread updates resource_health in storage."""
        queue = Queue(db_path=str(tmp_path / "q.db"), workers=1)

        @queue.worker_resource("db")
        def create_db() -> str:
            return "db_instance"

        @queue.task()
        def noop() -> None:
            pass

        thread = threading.Thread(target=queue.run_worker, daemon=True)
        thread.start()

        try:
            # Wait for worker + first heartbeat
            poll_until(
                lambda: bool((ws := queue.workers()) and ws[0].get("resource_health")),
                timeout=10,
                message="heartbeat never published resource_health",
            )
            workers: list[dict[str, Any]] = queue.workers()
            assert len(workers) >= 1
            health = json.loads(workers[0]["resource_health"])
            assert "db" in health
        finally:
            queue._inner.request_shutdown()
            join_worker(thread)
