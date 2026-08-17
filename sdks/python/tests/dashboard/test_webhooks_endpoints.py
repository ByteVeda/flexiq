"""Tests for the persistent webhook subscription store + dashboard CRUD endpoints."""

from __future__ import annotations

import hashlib
import hmac
import json
import threading
import urllib.error
import urllib.request
from collections.abc import Generator
from http.server import BaseHTTPRequestHandler, HTTPServer, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import pytest

from taskito import Queue
from taskito.dashboard import _make_handler
from taskito.dashboard._testing import AuthedClient, seed_admin_and_session
from taskito.dashboard.url_safety import UnsafeWebhookUrl, validate_webhook_url
from taskito.dashboard.webhook_store import WebhookSubscriptionStore
from taskito.events import EventType

# ── Fixtures ───────────────────────────────────────────────────────────


@pytest.fixture
def queue(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Queue:
    # Tests in this file create webhooks against 127.0.0.1 servers, which the
    # SSRF guard would otherwise reject.
    monkeypatch.setenv("FLEXIQ_WEBHOOKS_ALLOW_PRIVATE", "1")
    return Queue(db_path=str(tmp_path / "webhooks.db"))


@pytest.fixture
def echo_server() -> Generator[tuple[str, list[dict[str, Any]]]]:
    """A local HTTP server that captures incoming webhook bodies."""
    received: list[dict[str, Any]] = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length)
            received.append({"body": json.loads(body), "headers": dict(self.headers)})
            self.send_response(200)
            self.end_headers()

        def log_message(self, *args: Any) -> None:
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}", received
    finally:
        server.shutdown()


@pytest.fixture
def dashboard(queue: Queue) -> Generator[tuple[AuthedClient, Queue]]:
    handler = _make_handler(queue)
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    session = seed_admin_and_session(queue)
    client = AuthedClient(base=f"http://127.0.0.1:{server.server_address[1]}", session=session)
    try:
        yield client, queue
    finally:
        server.shutdown()


# ── SSRF guard ─────────────────────────────────────────────────────────


def test_url_safety_rejects_loopback(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FLEXIQ_WEBHOOKS_ALLOW_PRIVATE", raising=False)
    with pytest.raises(UnsafeWebhookUrl):
        validate_webhook_url("http://127.0.0.1:8080/x")
    with pytest.raises(UnsafeWebhookUrl):
        validate_webhook_url("http://localhost/x")
    with pytest.raises(UnsafeWebhookUrl):
        validate_webhook_url("http://something.internal/x")


def test_url_safety_rejects_private_ranges(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FLEXIQ_WEBHOOKS_ALLOW_PRIVATE", raising=False)
    with pytest.raises(UnsafeWebhookUrl):
        validate_webhook_url("http://10.0.0.5/x")
    with pytest.raises(UnsafeWebhookUrl):
        validate_webhook_url("http://192.168.1.1/x")
    with pytest.raises(UnsafeWebhookUrl):
        validate_webhook_url("http://169.254.169.254/latest/meta-data")


def test_url_safety_rejects_bad_scheme(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FLEXIQ_WEBHOOKS_ALLOW_PRIVATE", raising=False)
    with pytest.raises(UnsafeWebhookUrl):
        validate_webhook_url("ftp://example.com/x")
    with pytest.raises(UnsafeWebhookUrl):
        validate_webhook_url("javascript:alert(1)")


def test_url_safety_allows_private_with_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FLEXIQ_WEBHOOKS_ALLOW_PRIVATE", "1")
    # No exception
    validate_webhook_url("http://127.0.0.1:8080/x")
    validate_webhook_url("http://10.0.0.5/x")


# ── Store / Python API ─────────────────────────────────────────────────


def test_store_starts_empty(queue: Queue) -> None:
    assert WebhookSubscriptionStore(queue).list_all() == []


def test_create_and_get_subscription(queue: Queue) -> None:
    sub = queue.add_webhook(
        "http://127.0.0.1:9999/x", events=[EventType.JOB_FAILED], secret="topsecret"
    )
    fetched = queue.get_webhook(sub.id)
    assert fetched is not None
    assert fetched.url == "http://127.0.0.1:9999/x"
    assert fetched.events == ["job.failed"]
    assert fetched.secret == "topsecret"


def test_subscriptions_persist_across_queue_instances(tmp_path: Path) -> None:
    """A fresh Queue against the same DB sees prior subscriptions."""
    import os

    os.environ["FLEXIQ_WEBHOOKS_ALLOW_PRIVATE"] = "1"
    try:
        db = str(tmp_path / "persist.db")
        q1 = Queue(db_path=db)
        sub = q1.add_webhook("http://127.0.0.1:9999/x")

        q2 = Queue(db_path=db)
        all_subs = q2.list_webhooks()
        assert any(s.id == sub.id for s in all_subs)
    finally:
        del os.environ["FLEXIQ_WEBHOOKS_ALLOW_PRIVATE"]


def test_update_webhook(queue: Queue) -> None:
    sub = queue.add_webhook("http://127.0.0.1:9999/x", max_retries=3)
    updated = queue.update_webhook(sub.id, max_retries=7, enabled=False)
    assert updated.max_retries == 7
    assert updated.enabled is False
    fresh = queue.get_webhook(sub.id)
    assert fresh is not None and fresh.max_retries == 7


def test_remove_webhook(queue: Queue) -> None:
    sub = queue.add_webhook("http://127.0.0.1:9999/x")
    assert queue.remove_webhook(sub.id) is True
    assert queue.remove_webhook(sub.id) is False
    assert queue.get_webhook(sub.id) is None


def test_rotate_secret(queue: Queue) -> None:
    sub = queue.add_webhook("http://127.0.0.1:9999/x", secret="old")
    new_secret = queue.rotate_webhook_secret(sub.id)
    assert new_secret != "old"
    fresh = queue.get_webhook(sub.id)
    assert fresh is not None and fresh.secret == new_secret


def test_disabled_webhook_does_not_deliver(
    queue: Queue, echo_server: tuple[str, list[dict[str, Any]]], poll_until: Any
) -> None:
    url, received = echo_server
    sub = queue.add_webhook(url, events=[EventType.JOB_COMPLETED])
    queue.update_webhook(sub.id, enabled=False)
    queue._webhook_manager.notify(EventType.JOB_COMPLETED, {"job_id": "1"})
    # Give the dispatcher a chance.
    import time

    time.sleep(0.3)
    assert received == []


def test_task_filter_restricts_delivery(
    queue: Queue, echo_server: tuple[str, list[dict[str, Any]]], poll_until: Any
) -> None:
    url, received = echo_server
    queue.add_webhook(url, task_filter=["only_me"])
    queue._webhook_manager.notify(EventType.JOB_COMPLETED, {"job_id": "1", "task_name": "other"})
    queue._webhook_manager.notify(EventType.JOB_COMPLETED, {"job_id": "2", "task_name": "only_me"})
    poll_until(lambda: len(received) >= 1, message="task-filtered webhook not delivered")
    assert len(received) == 1
    assert received[0]["body"]["task_name"] == "only_me"


def test_manager_reload_picks_up_new_subscription(
    queue: Queue, echo_server: tuple[str, list[dict[str, Any]]], poll_until: Any
) -> None:
    """Subscriptions written by another worker show up after reload."""
    url, received = echo_server
    # Bypass the Queue API and write directly to the store to simulate a peer.
    WebhookSubscriptionStore(queue).create(url=url)
    queue._webhook_manager.reload()
    queue._webhook_manager.notify(EventType.JOB_COMPLETED, {"job_id": "1"})
    poll_until(lambda: len(received) >= 1, message="reloaded webhook not delivered")


def test_subscription_secret_signs_payload(
    queue: Queue, echo_server: tuple[str, list[dict[str, Any]]], poll_until: Any
) -> None:
    url, received = echo_server
    sub = queue.add_webhook(url, secret="signing-key")
    queue._webhook_manager.notify(EventType.JOB_COMPLETED, {"job_id": "x"})
    poll_until(lambda: len(received) >= 1)

    sig_header = received[0]["headers"].get("X-FlexiQ-Signature")
    assert sig_header is not None
    body_bytes = json.dumps(received[0]["body"], default=str).encode("utf-8")
    expected = hmac.new(b"signing-key", body_bytes, hashlib.sha256).hexdigest()
    assert sig_header == f"sha256={expected}"
    assert sub.secret == "signing-key"


# ── Dashboard HTTP endpoints ──────────────────────────────────────────


def test_list_webhooks_returns_empty(dashboard: tuple[AuthedClient, Queue]) -> None:
    client, _ = dashboard
    assert client.get("/api/webhooks") == []


def test_event_types_listing(dashboard: tuple[AuthedClient, Queue]) -> None:
    client, _ = dashboard
    events = client.get("/api/event-types")
    assert "job.completed" in events
    assert "job.failed" in events
    assert sorted(events) == events  # always sorted


def test_create_webhook_endpoint(
    dashboard: tuple[AuthedClient, Queue], echo_server: tuple[str, list[dict[str, Any]]]
) -> None:
    client, _queue = dashboard
    url, _ = echo_server
    created = client.post(
        "/api/webhooks",
        {
            "url": url,
            "events": ["job.failed"],
            "task_filter": ["send_email"],
            "max_retries": 5,
            "description": "ops failures",
            "generate_secret": True,
        },
    )
    assert created["url"] == url
    assert created["events"] == ["job.failed"]
    assert created["task_filter"] == ["send_email"]
    assert created["max_retries"] == 5
    # Secret is revealed exactly once on create.
    assert "secret" in created
    assert created["has_secret"] is True

    listed = client.get("/api/webhooks")
    assert len(listed) == 1
    # ``secret`` is redacted from list/get responses.
    assert "secret" not in listed[0]
    assert listed[0]["has_secret"] is True


def test_create_webhook_rejects_unsafe_url(dashboard: tuple[AuthedClient, Queue]) -> None:
    client, _ = dashboard
    # The fixture has FLEXIQ_WEBHOOKS_ALLOW_PRIVATE=1; remove it for this test only.
    import os

    saved = os.environ.pop("FLEXIQ_WEBHOOKS_ALLOW_PRIVATE", None)
    try:
        with pytest.raises(urllib.error.HTTPError) as exc_info:
            client.post("/api/webhooks", {"url": "http://127.0.0.1/x"})
        assert exc_info.value.code == 400
    finally:
        if saved is not None:
            os.environ["FLEXIQ_WEBHOOKS_ALLOW_PRIVATE"] = saved


def test_create_webhook_rejects_unknown_event(
    dashboard: tuple[AuthedClient, Queue], echo_server: tuple[str, list[dict[str, Any]]]
) -> None:
    client, _ = dashboard
    url, _ = echo_server
    with pytest.raises(urllib.error.HTTPError) as exc_info:
        client.post("/api/webhooks", {"url": url, "events": ["not.a.real.event"]})
    assert exc_info.value.code == 400


def test_update_webhook_endpoint(
    dashboard: tuple[AuthedClient, Queue], echo_server: tuple[str, list[dict[str, Any]]]
) -> None:
    client, _ = dashboard
    url, _ = echo_server
    created = client.post("/api/webhooks", {"url": url})

    updated = client.put(
        f"/api/webhooks/{created['id']}",
        {"max_retries": 10, "enabled": False, "description": "paused"},
    )
    assert updated["max_retries"] == 10
    assert updated["enabled"] is False
    assert updated["description"] == "paused"


def test_delete_webhook_endpoint(
    dashboard: tuple[AuthedClient, Queue], echo_server: tuple[str, list[dict[str, Any]]]
) -> None:
    client, _ = dashboard
    url, _ = echo_server
    created = client.post("/api/webhooks", {"url": url})
    assert client.delete(f"/api/webhooks/{created['id']}") == {"deleted": True}
    with pytest.raises(urllib.error.HTTPError) as exc_info:
        client.delete(f"/api/webhooks/{created['id']}")
    assert exc_info.value.code == 404


def test_rotate_secret_endpoint(
    dashboard: tuple[AuthedClient, Queue], echo_server: tuple[str, list[dict[str, Any]]]
) -> None:
    client, _ = dashboard
    url, _ = echo_server
    created = client.post("/api/webhooks", {"url": url, "secret": "old"})
    rotated = client.post(f"/api/webhooks/{created['id']}/rotate-secret")
    assert rotated["secret"] != "old"
    assert rotated["id"] == created["id"]


def test_test_webhook_endpoint_returns_status(
    dashboard: tuple[AuthedClient, Queue], echo_server: tuple[str, list[dict[str, Any]]]
) -> None:
    client, _ = dashboard
    url, received = echo_server
    created = client.post("/api/webhooks", {"url": url})
    result = client.post(f"/api/webhooks/{created['id']}/test")
    assert result["delivered"] is True
    assert result["status"] == 200
    # A test event landed at the echo server.
    assert any(r["body"].get("event") == "test.ping" for r in received)


def test_test_webhook_endpoint_reports_failure(
    dashboard: tuple[AuthedClient, Queue],
) -> None:
    """When the target server returns 4xx, the test endpoint surfaces it."""
    received_count = [0]

    class FailHandler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            received_count[0] += 1
            self.send_response(418)
            self.end_headers()

        def log_message(self, *args: Any) -> None:
            pass

    server = HTTPServer(("127.0.0.1", 0), FailHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        client, _ = dashboard
        created = client.post(
            "/api/webhooks",
            {"url": f"http://127.0.0.1:{server.server_address[1]}/x"},
        )
        result = client.post(f"/api/webhooks/{created['id']}/test")
        assert result["delivered"] is False
        assert result["status"] == 418
    finally:
        server.shutdown()
