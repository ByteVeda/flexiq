"""HTTP route tests for ``/api/topics/*``.

Boots a real dashboard server on a random port and drives it through the
standard AuthedClient + admin session pattern. The queue is seeded with a
few durable subscriptions and some published-but-unworked deliveries so the
aggregation and per-topic filtering can be asserted against known counts.
"""

from __future__ import annotations

import threading
from collections.abc import Generator
from pathlib import Path
from typing import Any

import pytest

from flexiq import Queue
from flexiq.dashboard._testing import AuthedClient, seed_admin_and_session


def _start_dashboard(queue: Queue) -> tuple[str, Any]:
    from http.server import ThreadingHTTPServer

    from flexiq.dashboard import _make_handler

    handler = _make_handler(queue, static_assets=None)
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return f"http://127.0.0.1:{port}", server


@pytest.fixture
def topics_dashboard(tmp_path: Path) -> Generator[tuple[AuthedClient, Queue]]:
    db_path = str(tmp_path / "topics_dashboard.db")
    q = Queue(db_path=db_path, workers=2)

    @q.subscriber("orders", name="fulfillment")
    def fulfillment(order_id: int) -> None:
        pass

    @q.subscriber("orders", name="analytics")
    def analytics(order_id: int) -> None:
        pass

    @q.subscriber("audit", name="audit_log")
    def audit_log(event: str) -> None:
        pass

    # Producer-only flush so publish() sees the durable subscriptions.
    q.declare_subscriptions()
    # Two orders publishes fan out to both order subscribers (4 jobs); one
    # audit publish fans out to the single audit subscriber (1 job). No worker
    # runs, so every delivery stays pending.
    q.publish("orders", 1)
    q.publish("orders", 2)
    q.publish("audit", "login")

    url, server = _start_dashboard(q)
    session = seed_admin_and_session(q)
    client = AuthedClient(base=url, session=session)
    try:
        yield client, q
    finally:
        server.shutdown()


# ── List endpoint ─────────────────────────────────────────────────────────


def test_list_topics_aggregates_per_topic(
    topics_dashboard: tuple[AuthedClient, Queue],
) -> None:
    client, _ = topics_dashboard
    data = client.get("/api/topics")
    assert isinstance(data, list)
    by_topic = {row["topic"]: row for row in data}
    assert set(by_topic) == {"orders", "audit"}

    orders = by_topic["orders"]
    assert orders["subscription_count"] == 2
    # 2 publishes x 2 subscribers = 4 pending, 0 running.
    assert orders["backlog"] == 4
    assert orders["dead"] == 0

    audit = by_topic["audit"]
    assert audit["subscription_count"] == 1
    assert audit["backlog"] == 1
    assert audit["dead"] == 0


# ── Detail endpoint ───────────────────────────────────────────────────────


def test_topic_detail_filters_to_topic(
    topics_dashboard: tuple[AuthedClient, Queue],
) -> None:
    client, _ = topics_dashboard
    rows = client.get("/api/topics/orders")
    assert isinstance(rows, list)
    assert len(rows) == 2
    assert {r["subscription"] for r in rows} == {"fulfillment", "analytics"}
    for row in rows:
        assert row["topic"] == "orders"
        assert row["pending"] == 2  # one delivery per publish
        assert row["running"] == 0
        assert row["active"] is True
        assert row["durable"] is True
        for key in ("task_name", "queue", "dead", "oldest_pending_age_ms"):
            assert key in row


def test_topic_detail_unknown_topic_is_empty(
    topics_dashboard: tuple[AuthedClient, Queue],
) -> None:
    client, _ = topics_dashboard
    assert client.get("/api/topics/does-not-exist") == []


# ── Actions ───────────────────────────────────────────────────────────────


def test_pause_and_resume_subscription(
    topics_dashboard: tuple[AuthedClient, Queue],
) -> None:
    client, _ = topics_dashboard

    paused = client.post("/api/topics/orders/subscriptions/fulfillment/pause")
    assert paused == {"paused": True}
    rows = {r["subscription"]: r for r in client.get("/api/topics/orders")}
    assert rows["fulfillment"]["active"] is False

    resumed = client.post("/api/topics/orders/subscriptions/fulfillment/resume")
    assert resumed == {"active": True}
    rows = {r["subscription"]: r for r in client.get("/api/topics/orders")}
    assert rows["fulfillment"]["active"] is True


def test_unsubscribe_removes_subscription(
    topics_dashboard: tuple[AuthedClient, Queue],
) -> None:
    client, _ = topics_dashboard

    result = client.delete("/api/topics/orders/subscriptions/analytics")
    assert result == {"unsubscribed": True}

    rows = client.get("/api/topics/orders")
    assert {r["subscription"] for r in rows} == {"fulfillment"}
