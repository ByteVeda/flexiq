"""Tests for webhook delivery system."""

import hashlib
import hmac
import json
import threading
from collections.abc import Generator
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

import pytest

from flexiq.events import EventType
from flexiq.webhooks import WebhookManager

PollUntil = Any  # the conftest fixture's runtime type


@pytest.fixture
def webhook_server(
    monkeypatch: pytest.MonkeyPatch,
) -> Generator[tuple[str, list[dict[str, Any]]]]:
    """Start a local HTTP server that records webhook deliveries.

    Delivery now validates the target URL and refuses private/loopback hosts,
    so the 127.0.0.1 test server needs the documented dev escape hatch.
    """
    monkeypatch.setenv("FLEXIQ_WEBHOOKS_ALLOW_PRIVATE", "1")
    received: list[dict[str, Any]] = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length)
            received.append(
                {
                    "body": json.loads(body),
                    "headers": dict(self.headers),
                }
            )
            self.send_response(200)
            self.end_headers()

        def log_message(self, *args: Any) -> None:
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    yield f"http://127.0.0.1:{port}", received

    server.shutdown()


def test_webhook_delivery(
    webhook_server: tuple[str, list[dict[str, Any]]], poll_until: PollUntil
) -> None:
    """Webhooks are delivered to registered URLs."""
    url, received = webhook_server
    mgr = WebhookManager()
    mgr.add_webhook(url)

    mgr.notify(EventType.JOB_COMPLETED, {"job_id": "abc"})
    poll_until(lambda: len(received) >= 1, message="webhook not delivered")

    assert len(received) == 1
    assert received[0]["body"]["event"] == "job.completed"
    assert received[0]["body"]["job_id"] == "abc"


def test_webhook_event_filtering(
    webhook_server: tuple[str, list[dict[str, Any]]], poll_until: PollUntil
) -> None:
    """Webhooks with event filters only receive matching events."""
    url, received = webhook_server
    mgr = WebhookManager()
    mgr.add_webhook(url, events=[EventType.JOB_FAILED])

    mgr.notify(EventType.JOB_COMPLETED, {"job_id": "1"})
    mgr.notify(EventType.JOB_FAILED, {"job_id": "2", "error": "boom"})
    poll_until(lambda: len(received) >= 1, message="filtered webhook not delivered")

    assert len(received) == 1
    assert received[0]["body"]["event"] == "job.failed"


def test_webhook_hmac_signing(
    webhook_server: tuple[str, list[dict[str, Any]]], poll_until: PollUntil
) -> None:
    """Webhooks with a secret include a valid HMAC signature."""
    url, received = webhook_server
    secret = "my-secret-key"
    mgr = WebhookManager()
    mgr.add_webhook(url, secret=secret)

    mgr.notify(EventType.JOB_ENQUEUED, {"job_id": "xyz"})
    poll_until(lambda: len(received) >= 1, message="signed webhook not delivered")

    assert len(received) == 1
    sig_header = received[0]["headers"].get("X-Flexiq-Signature")
    assert sig_header is not None
    assert sig_header.startswith("sha256=")

    # Verify the signature
    body_bytes = json.dumps(received[0]["body"], default=str).encode("utf-8")
    expected_sig = hmac.new(secret.encode(), body_bytes, hashlib.sha256).hexdigest()
    assert sig_header == f"sha256={expected_sig}"


def test_webhook_url_validation() -> None:
    """Only http:// and https:// URLs are accepted."""
    mgr = WebhookManager()

    with pytest.raises(ValueError, match="http:// or https://"):
        mgr.add_webhook("ftp://example.com/hook")

    with pytest.raises(ValueError, match="http:// or https://"):
        mgr.add_webhook("javascript:alert(1)")

    # Valid URLs should not raise
    mgr.add_webhook("http://localhost:8080/hook")
    mgr.add_webhook("https://example.com/hook")


def test_webhook_custom_headers(
    webhook_server: tuple[str, list[dict[str, Any]]], poll_until: PollUntil
) -> None:
    """Custom headers are included in webhook requests."""
    url, received = webhook_server
    mgr = WebhookManager()
    mgr.add_webhook(url, headers={"X-Custom": "test-value"})

    mgr.notify(EventType.JOB_COMPLETED, {"job_id": "1"})
    poll_until(lambda: len(received) >= 1, message="custom-headers webhook not delivered")

    assert len(received) == 1
    assert received[0]["headers"].get("X-Custom") == "test-value"


def test_webhook_no_subscribers() -> None:
    """Notifying with no matching webhooks doesn't raise."""
    mgr = WebhookManager()
    mgr.notify(EventType.JOB_COMPLETED, {"job_id": "1"})
