"""Tests for dashboard API endpoints and list_jobs/to_dict."""

import json
import threading
import urllib.error
import urllib.request
from collections.abc import Generator
from pathlib import Path
from typing import Any

import pytest

from flexiq import Queue
from flexiq.dashboard._testing import AuthedClient, seed_admin_and_session


@pytest.fixture
def queue(tmp_path: Path) -> Queue:
    """Create a fresh queue with some test data pre-registered."""
    db_path = str(tmp_path / "test_dashboard.db")
    q = Queue(db_path=db_path, workers=2)

    @q.task(queue="default")
    def task_a(x: int) -> int:
        return x * 2

    @q.task(queue="email")
    def task_b(x: int) -> int:
        return x + 1

    return q


@pytest.fixture
def populated_queue(queue: Queue) -> tuple[Queue, list[Any]]:
    """Queue with several jobs enqueued."""
    task_a_name: str = ""
    task_b_name: str = ""
    for name, _fn in queue._task_registry.items():
        if "task_a" in name:
            task_a_name = name
        elif "task_b" in name:
            task_b_name = name

    jobs: list[Any] = []
    for i in range(5):
        jobs.append(queue.enqueue(task_a_name, args=(i,)))
    for i in range(3):
        jobs.append(queue.enqueue(task_b_name, args=(i,), queue="email"))
    return queue, jobs


# ── list_jobs tests ──────────────────────────────────────


def test_list_jobs_returns_all(populated_queue: tuple[Queue, list[Any]]) -> None:
    """list_jobs() with no filters returns all jobs."""
    queue, _ = populated_queue
    result = queue.list_jobs()
    assert len(result) == 8


def test_list_jobs_filter_by_queue(populated_queue: tuple[Queue, list[Any]]) -> None:
    """list_jobs() can filter by queue name."""
    queue, _ = populated_queue
    result = queue.list_jobs(queue="email")
    assert len(result) == 3
    for j in result:
        d = j.to_dict()
        assert d["queue"] == "email"


def test_list_jobs_filter_by_status(populated_queue: tuple[Queue, list[Any]]) -> None:
    """list_jobs() can filter by status."""
    queue, _ = populated_queue
    result = queue.list_jobs(status="pending")
    assert len(result) == 8  # all are pending

    result = queue.list_jobs(status="running")
    assert len(result) == 0


def test_list_jobs_filter_by_task_name(populated_queue: tuple[Queue, list[Any]]) -> None:
    """list_jobs() can filter by task name."""
    queue, _ = populated_queue
    # Find the task_a name
    task_a_name = None
    for name in queue._task_registry:
        if "task_a" in name:
            task_a_name = name
            break

    result = queue.list_jobs(task_name=task_a_name)
    assert len(result) == 5


def test_list_jobs_pagination(populated_queue: tuple[Queue, list[Any]]) -> None:
    """list_jobs() respects limit and offset."""
    queue, _ = populated_queue
    page1 = queue.list_jobs(limit=3, offset=0)
    page2 = queue.list_jobs(limit=3, offset=3)

    assert len(page1) == 3
    assert len(page2) == 3

    ids1 = {j.id for j in page1}
    ids2 = {j.id for j in page2}
    assert ids1.isdisjoint(ids2)


def test_list_jobs_invalid_status(queue: Queue) -> None:
    """list_jobs() raises on invalid status string."""
    with pytest.raises(ValueError):
        queue.list_jobs(status="bogus")


# ── to_dict tests ────────────────────────────────────────


def test_to_dict_fields(queue: Queue) -> None:
    """to_dict() returns all expected fields."""

    @queue.task()
    def dummy() -> None:
        pass

    job = dummy.delay()
    d = job.to_dict()

    expected_keys = {
        "id",
        "queue",
        "task_name",
        "status",
        "priority",
        "progress",
        "retry_count",
        "max_retries",
        "created_at",
        "scheduled_at",
        "started_at",
        "completed_at",
        "error",
        "timeout_ms",
        "unique_key",
        "metadata",
        "notes",
        "namespace",
    }
    assert set(d.keys()) == expected_keys
    assert d["status"] == "pending"
    assert d["id"] == job.id


def test_to_dict_is_json_serializable(queue: Queue) -> None:
    """to_dict() output can be serialized to JSON."""

    @queue.task()
    def dummy() -> None:
        pass

    job = dummy.delay()
    d = job.to_dict()
    serialized = json.dumps(d)
    assert isinstance(serialized, str)


# ── Dashboard HTTP tests ─────────────────────────────────


def _start_dashboard(queue: Queue, *, static_assets: Any = None) -> tuple[str, Any]:
    """Boot a dashboard server bound to a random port.

    Returns the (url, server) pair so callers can shut it down explicitly.
    Optionally takes a ``StaticAssets`` instance — production code uses
    the package-bundled default; tests inject their own.
    """
    from http.server import ThreadingHTTPServer

    from flexiq.dashboard import _make_handler

    handler = _make_handler(queue, static_assets=static_assets)
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return f"http://127.0.0.1:{port}", server


@pytest.fixture
def dashboard_server(
    populated_queue: tuple[Queue, list[Any]],
) -> Generator[tuple[AuthedClient, Queue, list[Any]]]:
    """Start a dashboard server on a random port and pre-seed an admin session.

    Yields ``(client, queue, jobs)`` — the client transparently attaches the
    session cookie and CSRF header to every request.
    """
    queue, jobs = populated_queue
    url, server = _start_dashboard(queue)
    session = seed_admin_and_session(queue)
    client = AuthedClient(base=url, session=session)
    try:
        yield client, queue, jobs
    finally:
        server.shutdown()


def test_api_stats(dashboard_server: tuple[AuthedClient, Queue, list[Any]]) -> None:
    """GET /api/stats returns valid stats dict."""
    client, _, __ = dashboard_server
    data = client.get("/api/stats")
    assert "pending" in data
    assert data["pending"] == 8


def test_api_jobs_list(dashboard_server: tuple[AuthedClient, Queue, list[Any]]) -> None:
    """GET /api/jobs returns job list."""
    client, _, __ = dashboard_server
    data = client.get("/api/jobs")
    assert isinstance(data, list)
    assert len(data) == 8


def test_api_jobs_filter_status(
    dashboard_server: tuple[AuthedClient, Queue, list[Any]],
) -> None:
    """GET /api/jobs?status=pending filters correctly."""
    client, _, __ = dashboard_server
    data = client.get("/api/jobs?status=pending")
    assert len(data) == 8

    data = client.get("/api/jobs?status=running")
    assert len(data) == 0


def test_api_jobs_filter_queue(
    dashboard_server: tuple[AuthedClient, Queue, list[Any]],
) -> None:
    """GET /api/jobs?queue=email filters correctly."""
    client, _, __ = dashboard_server
    data = client.get("/api/jobs?queue=email")
    assert len(data) == 3


def test_api_jobs_pagination(
    dashboard_server: tuple[AuthedClient, Queue, list[Any]],
) -> None:
    """GET /api/jobs?limit=3&offset=0 paginates."""
    client, _, __ = dashboard_server
    data = client.get("/api/jobs?limit=3&offset=0")
    assert len(data) == 3


def test_api_job_detail(dashboard_server: tuple[AuthedClient, Queue, list[Any]]) -> None:
    """GET /api/jobs/{id} returns job dict."""
    client, _, jobs = dashboard_server
    job_id = jobs[0].id
    data = client.get(f"/api/jobs/{job_id}")
    assert data["id"] == job_id
    assert "status" in data


def test_api_job_not_found(
    dashboard_server: tuple[AuthedClient, Queue, list[Any]],
) -> None:
    """GET /api/jobs/nonexistent returns 404."""
    client, _, __ = dashboard_server
    try:
        client.get("/api/jobs/nonexistent-id")
        raise AssertionError("Expected 404")
    except urllib.error.HTTPError as e:
        assert e.code == 404


def test_api_cancel_job(dashboard_server: tuple[AuthedClient, Queue, list[Any]]) -> None:
    """POST /api/jobs/{id}/cancel cancels a pending job."""
    client, _, jobs = dashboard_server
    job_id = jobs[0].id
    data = client.post(f"/api/jobs/{job_id}/cancel")
    assert data["cancelled"] is True


def test_api_dead_letters_empty(
    dashboard_server: tuple[AuthedClient, Queue, list[Any]],
) -> None:
    """GET /api/dead-letters returns empty list initially."""
    client, _, __ = dashboard_server
    data = client.get("/api/dead-letters")
    assert data == []


def test_spa_html_served(
    populated_queue: tuple[Queue, list[Any]],
    tmp_path: Path,
) -> None:
    """GET / serves the SPA index.html when assets are bundled.

    Tests inject a ``StaticAssets`` rooted at a tmp directory so the test
    is self-contained — no dependency on a prior frontend build.
    """
    from flexiq.dashboard import StaticAssets

    tmp_path.joinpath("index.html").write_text(
        '<!doctype html><html><body><div id="app"></div></body></html>',
        encoding="utf-8",
    )
    queue, _ = populated_queue
    url, server = _start_dashboard(queue, static_assets=StaticAssets(tmp_path))
    try:
        with urllib.request.urlopen(url) as resp:
            assert resp.status == 200
            assert resp.headers.get("Content-Type", "").startswith("text/html")
            html = resp.read().decode()
            assert "<!doctype html>" in html.lower()
            assert 'id="app"' in html
    finally:
        server.shutdown()


def test_spa_assets_resolved_under_root(
    populated_queue: tuple[Queue, list[Any]],
    tmp_path: Path,
) -> None:
    """Hashed asset paths resolve under the bundle root and get long
    immutable cache headers."""
    from flexiq.dashboard import StaticAssets

    tmp_path.joinpath("index.html").write_text("<html><body></body></html>", encoding="utf-8")
    assets_dir = tmp_path / "assets"
    assets_dir.mkdir()
    assets_dir.joinpath("index-abc.js").write_text("export {};", encoding="utf-8")

    queue, _ = populated_queue
    url, server = _start_dashboard(queue, static_assets=StaticAssets(tmp_path))
    try:
        with urllib.request.urlopen(f"{url}/assets/index-abc.js") as resp:
            assert resp.status == 200
            assert resp.headers.get("Content-Type", "").startswith("application/javascript")
            assert "immutable" in resp.headers.get("Cache-Control", "")
            assert resp.read().decode() == "export {};"
    finally:
        server.shutdown()


def test_spa_unknown_route_falls_back_to_index(
    populated_queue: tuple[Queue, list[Any]],
    tmp_path: Path,
) -> None:
    """Client-side routes (anything that's not /assets/* or a real file)
    resolve to index.html so deep links keep working."""
    from flexiq.dashboard import StaticAssets

    tmp_path.joinpath("index.html").write_text(
        '<!doctype html><html><body><div id="app"></div></body></html>',
        encoding="utf-8",
    )
    queue, _ = populated_queue
    url, server = _start_dashboard(queue, static_assets=StaticAssets(tmp_path))
    try:
        with urllib.request.urlopen(f"{url}/jobs/some-id") as resp:
            assert resp.status == 200
            assert 'id="app"' in resp.read().decode()
    finally:
        server.shutdown()


def test_spa_missing_asset_under_assets_returns_404(
    populated_queue: tuple[Queue, list[Any]],
    tmp_path: Path,
) -> None:
    """A miss inside ``/assets/`` returns 404 — never the index fallback,
    so a stale page can't confuse the browser into running old chunks."""
    from flexiq.dashboard import StaticAssets

    tmp_path.joinpath("index.html").write_text("<html></html>", encoding="utf-8")
    queue, _ = populated_queue
    url, server = _start_dashboard(queue, static_assets=StaticAssets(tmp_path))
    try:
        try:
            urllib.request.urlopen(f"{url}/assets/missing.js")
            pytest.fail("Expected 404")
        except urllib.error.HTTPError as exc:
            assert exc.code == 404
    finally:
        server.shutdown()


def test_spa_missing_assets_returns_503(
    populated_queue: tuple[Queue, list[Any]],
) -> None:
    """When the frontend build wasn't run, the dashboard returns 503 with
    actionable rebuild instructions rather than silently 404-ing."""
    from flexiq.dashboard import StaticAssets

    queue, _ = populated_queue
    url, server = _start_dashboard(queue, static_assets=StaticAssets(None))
    try:
        try:
            urllib.request.urlopen(url)
            pytest.fail("Expected 503")
        except urllib.error.HTTPError as exc:
            assert exc.code == 503
            body = exc.read().decode()
            assert "not bundled" in body.lower()
            assert "pnpm" in body.lower()
    finally:
        server.shutdown()
