"""Tests for the retention echo.

Covers:
- ``Queue.effective_retention`` before and after a worker sweep
- ``GET /api/retention``
- the reserved ``retention:`` settings namespace
"""

from __future__ import annotations

import threading
import urllib.error
from collections.abc import Callable, Generator
from contextlib import contextmanager
from pathlib import Path

import pytest

from flexiq import Queue, Retention
from flexiq.dashboard._testing import AuthedClient, seed_admin_and_session

DAY_MS = 86_400_000

PollUntil = Callable[..., None]


@pytest.fixture
def queue(tmp_path: Path) -> Queue:
    # cleanup_interval=1 sweeps on the first maintenance tick, so the cleaner
    # publishes without the test waiting out the production cadence.
    return Queue(db_path=str(tmp_path / "retention.db"), scheduler_cleanup_interval=1)


@pytest.fixture
def dashboard_server(queue: Queue) -> Generator[tuple[AuthedClient, Queue]]:
    from http.server import ThreadingHTTPServer

    from flexiq.dashboard import _make_handler

    handler = _make_handler(queue)
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    session = seed_admin_and_session(queue)
    client = AuthedClient(base=f"http://127.0.0.1:{port}", session=session)
    try:
        yield client, queue
    finally:
        server.shutdown()


@contextmanager
def _worker_that_published(queue: Queue, poll_until: PollUntil) -> Generator[None]:
    """Run a worker until it has published its retention windows."""
    thread = threading.Thread(target=queue.run_worker, daemon=True)
    thread.start()
    try:
        poll_until(
            lambda: queue.effective_retention() is not None,
            timeout=10,
            message="the cleaner never published its retention windows",
        )
        yield
    finally:
        queue.shutdown()
        thread.join(timeout=5)


# ── Python API ──────────────────────────────────────────


def test_effective_retention_is_none_before_any_sweep(queue: Queue) -> None:
    # Unreported is not "off": no worker has run, so nothing is known yet.
    assert queue.effective_retention() is None


def test_worker_publishes_the_recommended_defaults(queue: Queue, poll_until: PollUntil) -> None:
    with _worker_that_published(queue, poll_until):
        snapshot = queue.effective_retention()
        assert snapshot is not None
        assert snapshot.enabled
        assert snapshot.defaulted, "nothing was configured, so these are the defaults"
        assert snapshot.namespace == "default"
        assert snapshot.reported_at > 0
        assert snapshot.windows == {
            "task_logs": 3 * DAY_MS,
            "archived_jobs": 7 * DAY_MS,
            "job_errors": 7 * DAY_MS,
            "task_metrics": 7 * DAY_MS,
            "dead_letter": 30 * DAY_MS,
        }


def test_worker_publishes_explicit_windows(tmp_path: Path, poll_until: PollUntil) -> None:
    queue = Queue(
        db_path=str(tmp_path / "explicit.db"),
        scheduler_cleanup_interval=1,
        retention=Retention(task_logs=3600, dead_letter=86400),
    )
    with _worker_that_published(queue, poll_until):
        snapshot = queue.effective_retention()
        assert snapshot is not None
        assert snapshot.enabled
        assert not snapshot.defaulted
        assert snapshot.windows["task_logs"] == 3_600_000
        assert snapshot.windows["dead_letter"] == 86_400_000
        # Tables the operator left out keep forever.
        assert snapshot.windows["archived_jobs"] is None


def test_worker_publishes_a_disabled_policy(tmp_path: Path, poll_until: PollUntil) -> None:
    # An empty Retention() opts out — that is a policy worth showing, so it
    # publishes as disabled rather than staying unreported.
    queue = Queue(
        db_path=str(tmp_path / "disabled.db"),
        scheduler_cleanup_interval=1,
        retention=Retention(),
    )
    with _worker_that_published(queue, poll_until):
        snapshot = queue.effective_retention()
        assert snapshot is not None
        assert not snapshot.enabled
        assert not snapshot.defaulted
        assert all(window is None for window in snapshot.windows.values())


def test_effective_retention_is_namespace_scoped(tmp_path: Path, poll_until: PollUntil) -> None:
    db = str(tmp_path / "shared.db")
    worker_queue = Queue(db_path=db, scheduler_cleanup_interval=1, namespace="tenant-a")
    with _worker_that_published(worker_queue, poll_until):
        # A second tenant on the same storage has its own policy — and none yet.
        other = Queue(db_path=db, namespace="tenant-b")
        assert other.effective_retention() is None
        assert worker_queue.effective_retention() is not None


# ── HTTP endpoint ───────────────────────────────────────


def test_retention_endpoint_reports_nothing_before_a_sweep(
    dashboard_server: tuple[AuthedClient, Queue],
) -> None:
    client, _ = dashboard_server
    body = client.get("/api/retention")
    assert body["reported"] is False
    assert body["enabled"] is False
    assert body["namespace"] is None
    assert body["reported_at"] is None
    assert body["windows"] == {
        "task_logs_ttl_ms": None,
        "archived_jobs_ttl_ms": None,
        "job_errors_ttl_ms": None,
        "task_metrics_ttl_ms": None,
        "dead_letter_ttl_ms": None,
    }


def test_retention_endpoint_echoes_the_published_windows(
    dashboard_server: tuple[AuthedClient, Queue], poll_until: PollUntil
) -> None:
    client, queue = dashboard_server
    with _worker_that_published(queue, poll_until):
        body = client.get("/api/retention")
        assert body["reported"] is True
        assert body["enabled"] is True
        assert body["defaulted"] is True
        assert body["namespace"] == "default"
        assert body["reported_at"] > 0
        assert body["windows"]["task_logs_ttl_ms"] == 3 * DAY_MS
        assert body["windows"]["dead_letter_ttl_ms"] == 30 * DAY_MS


def test_retention_dry_run_endpoint_reports_counts(
    dashboard_server: tuple[AuthedClient, Queue],
) -> None:
    # The dry-run is computed in-process, so it answers immediately — no worker
    # sweep needed. On an empty queue every count is zero, but the defaults are
    # still reported as on.
    client, _ = dashboard_server
    body = client.get("/api/retention/dry-run")
    assert body["enabled"] is True
    assert body["defaulted"] is True
    assert body["namespace"] == "default"
    assert body["reference_time"] > 0
    assert body["total"] == 0
    assert body["counts"] == {
        "task_logs": 0,
        "archived_jobs": 0,
        "job_errors": 0,
        "task_metrics": 0,
        "dead_letter": 0,
    }
    assert body["windows"]["task_logs_ttl_ms"] == 3 * DAY_MS


# ── Reserved settings namespace ─────────────────────────


def test_published_windows_are_not_an_editable_setting(
    dashboard_server: tuple[AuthedClient, Queue], poll_until: PollUntil
) -> None:
    client, queue = dashboard_server
    with _worker_that_published(queue, poll_until):
        # The published policy is a report, not a knob: it must not show up as
        # an editable settings row, and must not be spoofable through the KV API.
        assert not any(key.startswith("retention:") for key in client.get("/api/settings"))

        with pytest.raises(urllib.error.HTTPError) as read:
            client.get("/api/settings/retention:effective:default")
        assert read.value.code == 404

        with pytest.raises(urllib.error.HTTPError) as write:
            client.put("/api/settings/retention:effective:default", {"value": "{}"})
        assert write.value.code == 400


async def test_async_effective_retention(queue: Queue, poll_until: PollUntil) -> None:
    with _worker_that_published(queue, poll_until):
        snapshot = await queue.aeffective_retention()
        assert snapshot is not None
        assert snapshot.enabled
