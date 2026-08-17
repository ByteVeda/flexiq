"""Tests for the webhook delivery log + replay endpoints."""

from __future__ import annotations

import json
import threading
import urllib.error
import urllib.request
from collections.abc import Generator
from http.server import BaseHTTPRequestHandler, HTTPServer, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import pytest

from flexiq import Queue
from flexiq.dashboard import _make_handler
from flexiq.dashboard._testing import AuthedClient, seed_admin_and_session
from flexiq.dashboard.delivery_store import DeliveryStatus, DeliveryStore
from flexiq.events import EventType


@pytest.fixture
def queue(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Queue:
    monkeypatch.setenv("FLEXIQ_WEBHOOKS_ALLOW_PRIVATE", "1")
    return Queue(db_path=str(tmp_path / "deliveries.db"))


@pytest.fixture
def echo_server() -> Generator[tuple[str, list[dict[str, Any]]]]:
    """A local server that captures the bodies it receives."""
    received: list[dict[str, Any]] = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length)
            received.append({"body": json.loads(body), "headers": dict(self.headers)})
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")

        def log_message(self, *args: Any) -> None:
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}", received
    finally:
        server.shutdown()


@pytest.fixture
def fail_server() -> Generator[str]:
    """Always returns 500 to exercise the dead-letter path."""

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            self.send_response(500)
            self.end_headers()
            self.wfile.write(b"server error")

        def log_message(self, *args: Any) -> None:
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()


@pytest.fixture
def dashboard(queue: Queue) -> Generator[tuple[AuthedClient, Queue]]:
    handler = _make_handler(queue)
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    session = seed_admin_and_session(queue)
    client = AuthedClient(base=f"http://127.0.0.1:{server.server_address[1]}", session=session)
    try:
        yield client, queue
    finally:
        server.shutdown()


# ── DeliveryStore ──────────────────────────────────────────────────────


def test_delivery_store_starts_empty(queue: Queue) -> None:
    store = DeliveryStore(queue)
    assert store.list_for("missing") == []
    assert store.count_for("missing") == 0


def test_record_attempt_appends(queue: Queue) -> None:
    store = DeliveryStore(queue)
    record = store.record_attempt(
        "sub1",
        event="job.completed",
        payload={"job_id": "x"},
        status=DeliveryStatus.DELIVERED,
        attempts=1,
        response_code=200,
        latency_ms=10,
    )
    assert record.subscription_id == "sub1"
    assert record.status == "delivered"
    assert store.count_for("sub1") == 1
    listed = store.list_for("sub1")
    assert len(listed) == 1
    assert listed[0].id == record.id


def test_record_attempt_caps_history(queue: Queue) -> None:
    store = DeliveryStore(queue, max_per_webhook=3)
    for i in range(5):
        store.record_attempt(
            "sub1",
            event="job.completed",
            payload={"job_id": str(i)},
            status=DeliveryStatus.DELIVERED,
            attempts=1,
        )
    items = store.list_for("sub1")
    assert len(items) == 3
    # Newest first; oldest (i=0, i=1) evicted.
    assert items[0].payload["job_id"] == "4"
    assert items[-1].payload["job_id"] == "2"


def test_record_attempt_truncates_response_body(queue: Queue) -> None:
    store = DeliveryStore(queue)
    big = "x" * 100_000
    record = store.record_attempt(
        "sub1",
        event="job.completed",
        payload={},
        status=DeliveryStatus.FAILED,
        attempts=1,
        response_body=big,
    )
    assert record.response_body is not None
    assert len(record.response_body.encode("utf-8")) <= 2048 + 4  # +ellipsis


def test_list_for_filters_by_status_and_event(queue: Queue) -> None:
    store = DeliveryStore(queue)
    store.record_attempt(
        "sub1", event="job.completed", payload={}, status=DeliveryStatus.DELIVERED, attempts=1
    )
    store.record_attempt(
        "sub1", event="job.failed", payload={}, status=DeliveryStatus.FAILED, attempts=1
    )
    store.record_attempt(
        "sub1", event="job.completed", payload={}, status=DeliveryStatus.FAILED, attempts=1
    )

    delivered = store.list_for("sub1", status=DeliveryStatus.DELIVERED)
    assert len(delivered) == 1
    failed = store.list_for("sub1", status=DeliveryStatus.FAILED)
    assert len(failed) == 2
    completed_event = store.list_for("sub1", event="job.completed")
    assert len(completed_event) == 2


# ── End-to-end delivery recording ──────────────────────────────────────


def test_successful_delivery_recorded(
    queue: Queue, echo_server: tuple[str, list[dict[str, Any]]], poll_until: Any
) -> None:
    url, _ = echo_server
    sub = queue.add_webhook(url, events=[EventType.JOB_COMPLETED])
    queue._webhook_manager.notify(EventType.JOB_COMPLETED, {"job_id": "abc"})
    poll_until(
        lambda: DeliveryStore(queue).count_for(sub.id) >= 1,
        message="delivery not recorded",
    )
    items = DeliveryStore(queue).list_for(sub.id)
    assert len(items) == 1
    assert items[0].status == "delivered"
    assert items[0].response_code == 200
    assert items[0].latency_ms is not None


def test_failed_delivery_marked_dead(queue: Queue, fail_server: str, poll_until: Any) -> None:
    sub = queue.add_webhook(fail_server, events=[EventType.JOB_FAILED], max_retries=2)
    queue._webhook_manager.notify(EventType.JOB_FAILED, {"job_id": "x", "error": "boom"})
    poll_until(
        lambda: DeliveryStore(queue).count_for(sub.id) >= 1,
        message="delivery never recorded",
    )
    items = DeliveryStore(queue).list_for(sub.id)
    assert len(items) == 1
    assert items[0].status == "dead"
    assert items[0].attempts == 2
    assert items[0].response_code == 500


# ── Dashboard endpoints ────────────────────────────────────────────────


def test_list_deliveries_endpoint(
    dashboard: tuple[AuthedClient, Queue],
    echo_server: tuple[str, list[dict[str, Any]]],
    poll_until: Any,
) -> None:
    client, queue = dashboard
    url, _ = echo_server
    sub = queue.add_webhook(url, events=[EventType.JOB_COMPLETED])
    queue._webhook_manager.notify(EventType.JOB_COMPLETED, {"job_id": "1"})
    poll_until(lambda: DeliveryStore(queue).count_for(sub.id) >= 1)

    page = client.get(f"/api/webhooks/{sub.id}/deliveries")
    assert page["total"] == 1
    assert page["items"][0]["status"] == "delivered"


def test_list_deliveries_filters_by_status(
    dashboard: tuple[AuthedClient, Queue], fail_server: str, poll_until: Any
) -> None:
    client, queue = dashboard
    sub = queue.add_webhook(fail_server, max_retries=1)
    queue._webhook_manager.notify(EventType.JOB_COMPLETED, {"job_id": "1"})
    poll_until(lambda: DeliveryStore(queue).count_for(sub.id) >= 1)

    only_failed = client.get(f"/api/webhooks/{sub.id}/deliveries?status=dead")
    assert only_failed["total"] >= 1
    assert all(r["status"] == "dead" for r in only_failed["items"])

    delivered = client.get(f"/api/webhooks/{sub.id}/deliveries?status=delivered")
    assert delivered["items"] == []


def test_get_delivery_endpoint(
    dashboard: tuple[AuthedClient, Queue],
    echo_server: tuple[str, list[dict[str, Any]]],
    poll_until: Any,
) -> None:
    client, queue = dashboard
    url, _ = echo_server
    sub = queue.add_webhook(url)
    queue._webhook_manager.notify(EventType.JOB_COMPLETED, {"job_id": "x"})
    poll_until(lambda: DeliveryStore(queue).count_for(sub.id) >= 1)
    record_id = DeliveryStore(queue).list_for(sub.id)[0].id

    record = client.get(f"/api/webhooks/{sub.id}/deliveries/{record_id}")
    assert record["id"] == record_id
    assert record["status"] == "delivered"


def test_replay_delivery_endpoint(
    dashboard: tuple[AuthedClient, Queue],
    echo_server: tuple[str, list[dict[str, Any]]],
    poll_until: Any,
) -> None:
    client, queue = dashboard
    url, received = echo_server
    sub = queue.add_webhook(url)
    queue._webhook_manager.notify(EventType.JOB_COMPLETED, {"job_id": "x"})
    poll_until(lambda: len(received) >= 1)
    poll_until(lambda: DeliveryStore(queue).count_for(sub.id) >= 1)
    delivery_id = DeliveryStore(queue).list_for(sub.id)[0].id

    result = client.post(f"/api/webhooks/{sub.id}/deliveries/{delivery_id}/replay")
    assert result["delivered"] is True
    assert result["status"] == 200
    assert result["replayed_of"] == delivery_id

    # Replay produces a NEW delivery record AND a new POST.
    poll_until(lambda: len(received) >= 2)
    poll_until(lambda: DeliveryStore(queue).count_for(sub.id) >= 2)
    items = DeliveryStore(queue).list_for(sub.id)
    assert any(r.payload.get("replay_of") == delivery_id for r in items)


def test_list_deliveries_404_for_unknown_subscription(
    dashboard: tuple[AuthedClient, Queue],
) -> None:
    client, _ = dashboard
    with pytest.raises(urllib.error.HTTPError) as exc_info:
        client.get("/api/webhooks/nope/deliveries")
    assert exc_info.value.code == 404


def test_get_delivery_404_when_missing(
    dashboard: tuple[AuthedClient, Queue],
    echo_server: tuple[str, list[dict[str, Any]]],
) -> None:
    client, queue = dashboard
    url, _ = echo_server
    sub = queue.add_webhook(url)
    with pytest.raises(urllib.error.HTTPError) as exc_info:
        client.get(f"/api/webhooks/{sub.id}/deliveries/nonexistent")
    assert exc_info.value.code == 404


def test_list_deliveries_rejects_bad_status(
    dashboard: tuple[AuthedClient, Queue],
    echo_server: tuple[str, list[dict[str, Any]]],
) -> None:
    client, queue = dashboard
    url, _ = echo_server
    sub = queue.add_webhook(url)
    with pytest.raises(urllib.error.HTTPError) as exc_info:
        client.get(f"/api/webhooks/{sub.id}/deliveries?status=not-real")
    assert exc_info.value.code == 400
