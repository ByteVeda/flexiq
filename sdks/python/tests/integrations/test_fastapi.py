"""Tests for FastAPI integration."""

import threading
from typing import Any

import pytest

# Skip entire module if fastapi is not installed
fastapi = pytest.importorskip("fastapi")
httpx = pytest.importorskip("httpx")

from fastapi import FastAPI  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from flexiq import Queue  # noqa: E402
from flexiq.contrib.fastapi import FlexiQRouter  # noqa: E402


@pytest.fixture
def app(queue: Queue) -> FastAPI:
    """Create a FastAPI app with FlexiQRouter."""
    app = FastAPI()
    app.include_router(FlexiQRouter(queue), prefix="/tasks")
    return app


@pytest.fixture
def client(app: FastAPI) -> TestClient:
    """Create a TestClient."""
    return TestClient(app)


@pytest.fixture
def populated(queue: Queue, client: TestClient) -> tuple[Queue, TestClient, list[Any], Any]:
    """Queue with a task and some jobs."""

    @queue.task()
    def add(a: int, b: int) -> int:
        return a + b

    jobs = [add.delay(i, i + 1) for i in range(5)]
    return queue, client, jobs, add


# ── Stats ────────────────────────────────────────────────


def test_stats(populated: tuple[Queue, TestClient, list[Any], Any]) -> None:
    _queue, client, _jobs, _add = populated
    resp = client.get("/tasks/stats")
    assert resp.status_code == 200
    data = resp.json()
    assert data["pending"] == 5
    assert data["running"] == 0


# ── Job detail ───────────────────────────────────────────


def test_get_job(populated: tuple[Queue, TestClient, list[Any], Any]) -> None:
    _queue, client, jobs, _add = populated
    job_id = jobs[0].id
    resp = client.get(f"/tasks/jobs/{job_id}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["id"] == job_id
    assert data["status"] == "pending"


def test_get_job_not_found(client: TestClient) -> None:
    resp = client.get("/tasks/jobs/nonexistent")
    assert resp.status_code == 404


# ── Job errors ───────────────────────────────────────────


def test_get_job_errors_empty(populated: tuple[Queue, TestClient, list[Any], Any]) -> None:
    _queue, client, jobs, _add = populated
    job_id = jobs[0].id
    resp = client.get(f"/tasks/jobs/{job_id}/errors")
    assert resp.status_code == 200
    assert resp.json() == []


# ── Job result ───────────────────────────────────────────


def test_get_job_result_pending(populated: tuple[Queue, TestClient, list[Any], Any]) -> None:
    _queue, client, jobs, _add = populated
    job_id = jobs[0].id
    resp = client.get(f"/tasks/jobs/{job_id}/result")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "pending"
    assert data["result"] is None


def test_get_job_result_completed(populated: tuple[Queue, TestClient, list[Any], Any]) -> None:
    queue, client, jobs, _add = populated

    worker = threading.Thread(target=queue.run_worker, daemon=True)
    worker.start()

    # Wait for first job to complete
    jobs[0].result(timeout=10)

    resp = client.get(f"/tasks/jobs/{jobs[0].id}/result")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "complete"
    assert data["result"] == 1  # add(0, 1) = 1


# ── Cancel ───────────────────────────────────────────────


def test_cancel_job(populated: tuple[Queue, TestClient, list[Any], Any]) -> None:
    _queue, client, jobs, _add = populated
    job_id = jobs[0].id
    resp = client.post(f"/tasks/jobs/{job_id}/cancel")
    assert resp.status_code == 200
    assert resp.json()["cancelled"] is True

    # Cancel again — should be false
    resp = client.post(f"/tasks/jobs/{job_id}/cancel")
    assert resp.json()["cancelled"] is False


# ── Dead letters ─────────────────────────────────────────


def test_dead_letters_empty(client: TestClient) -> None:
    resp = client.get("/tasks/dead-letters")
    assert resp.status_code == 200
    assert resp.json() == []


# ── Progress SSE ─────────────────────────────────────────


def test_progress_stream(populated: tuple[Queue, TestClient, list[Any], Any]) -> None:
    queue, client, jobs, _add = populated

    # Start worker so the job completes
    worker = threading.Thread(target=queue.run_worker, daemon=True)
    worker.start()

    job_id = jobs[0].id
    # Wait for job to finish first
    jobs[0].result(timeout=10)

    with client.stream("GET", f"/tasks/jobs/{job_id}/progress") as resp:
        assert resp.status_code == 200
        lines: list[str] = []
        for line in resp.iter_lines():
            if line.startswith("data:"):
                lines.append(line)
                break  # Just check first event

    assert len(lines) >= 1
    # The first (and only) event should show complete status
    import json

    data = json.loads(lines[0].replace("data: ", ""))
    assert data["status"] == "complete"


def test_progress_stream_not_found(client: TestClient) -> None:
    resp = client.get("/tasks/jobs/nonexistent/progress")
    assert resp.status_code == 404


# ── Router config ────────────────────────────────────────


def test_router_custom_tags(queue: Queue) -> None:
    """FlexiQRouter accepts standard APIRouter kwargs."""
    router = FlexiQRouter(queue, tags=["my-tasks"])
    assert "my-tasks" in router.tags


def test_router_custom_prefix(queue: Queue) -> None:
    """Router can be mounted with a custom prefix."""
    app = FastAPI()
    app.include_router(FlexiQRouter(queue), prefix="/api/v1/queue")
    client = TestClient(app)

    resp = client.get("/api/v1/queue/stats")
    assert resp.status_code == 200
