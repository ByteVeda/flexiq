"""Tests for task & queue runtime overrides."""

from __future__ import annotations

import threading
import urllib.error
from collections.abc import Generator
from http.server import ThreadingHTTPServer
from pathlib import Path

import pytest

from flexiq import Queue
from flexiq.dashboard import _make_handler
from flexiq.dashboard._testing import AuthedClient, seed_admin_and_session
from flexiq.dashboard.overrides_store import OverridesStore


@pytest.fixture
def queue(tmp_path: Path) -> Queue:
    q = Queue(db_path=str(tmp_path / "overrides.db"))

    @q.task(queue="default", max_retries=3, timeout=300)
    def send_email(to: str) -> str:
        return to

    @q.task(queue="email", max_retries=5, rate_limit="100/m", max_concurrent=10)
    def deliver(message: str) -> str:
        return message

    return q


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


# ── Store ──────────────────────────────────────────────────────────────


def test_overrides_store_starts_empty(queue: Queue) -> None:
    store = OverridesStore(queue)
    assert store.list_tasks() == {}
    assert store.list_queues() == {}


def test_set_task_override_persists(queue: Queue) -> None:
    store = OverridesStore(queue)
    override = store.set_task("foo", {"max_retries": 7, "rate_limit": "50/s"})
    assert override.max_retries == 7
    assert override.rate_limit == "50/s"
    fetched = store.get_task("foo")
    assert fetched is not None and fetched.max_retries == 7


def test_set_task_override_validates(queue: Queue) -> None:
    store = OverridesStore(queue)
    with pytest.raises(ValueError, match="rate_limit"):
        store.set_task("foo", {"rate_limit": "no-slash"})
    with pytest.raises(ValueError, match="max_concurrent"):
        store.set_task("foo", {"max_concurrent": -1})
    with pytest.raises(ValueError, match="unknown task override"):
        store.set_task("foo", {"not_a_field": 1})


def test_set_task_override_merges_with_existing(queue: Queue) -> None:
    store = OverridesStore(queue)
    store.set_task("foo", {"max_retries": 7})
    store.set_task("foo", {"rate_limit": "50/s"})
    merged = store.get_task("foo")
    assert merged is not None
    assert merged.max_retries == 7
    assert merged.rate_limit == "50/s"


def test_set_task_override_clears_field_with_none(queue: Queue) -> None:
    store = OverridesStore(queue)
    store.set_task("foo", {"max_retries": 7, "rate_limit": "50/s"})
    store.set_task("foo", {"max_retries": None})
    fetched = store.get_task("foo")
    assert fetched is not None
    assert fetched.max_retries is None
    assert fetched.rate_limit == "50/s"


def test_clear_task_override(queue: Queue) -> None:
    store = OverridesStore(queue)
    store.set_task("foo", {"max_retries": 7})
    assert store.clear_task("foo") is True
    assert store.clear_task("foo") is False
    assert store.get_task("foo") is None


def test_queue_override_basics(queue: Queue) -> None:
    store = OverridesStore(queue)
    store.set_queue("default", {"max_concurrent": 5, "paused": True})
    fetched = store.get_queue("default")
    assert fetched is not None
    assert fetched.max_concurrent == 5
    assert fetched.paused is True


def test_apply_task_overrides_mutates_configs(queue: Queue) -> None:
    """Mutating the in-memory PyTaskConfig is what makes overrides reach the
    Rust scheduler at worker start."""
    store = OverridesStore(queue)
    send_email = next(c for c in queue._task_configs if "send_email" in c.name)
    store.set_task(send_email.name, {"max_retries": 99, "rate_limit": "1/s"})
    store.apply_task_overrides(queue._task_configs)
    assert send_email.max_retries == 99
    assert send_email.rate_limit == "1/s"


def test_apply_task_overrides_reports_paused(queue: Queue) -> None:
    store = OverridesStore(queue)
    send_email = next(c for c in queue._task_configs if "send_email" in c.name)
    store.set_task(send_email.name, {"paused": True})
    paused = store.apply_task_overrides(queue._task_configs)
    assert send_email.name in paused


def test_apply_queue_overrides_merges(queue: Queue) -> None:
    store = OverridesStore(queue)
    queue.set_queue_concurrency("email", 10)  # configured-from-Python
    store.set_queue("email", {"rate_limit": "200/m"})
    merged = store.apply_queue_overrides(queue._queue_configs)
    assert merged["email"]["max_concurrent"] == 10  # decorator-set survives
    assert merged["email"]["rate_limit"] == "200/m"  # override wins


# ── Queue.registered_tasks() ──────────────────────────────────────────


def test_registered_tasks_lists_defaults_and_overrides(queue: Queue) -> None:
    tasks = queue.registered_tasks()
    assert len(tasks) == 2
    by_name = {t["name"]: t for t in tasks}
    deliver = next(t for n, t in by_name.items() if "deliver" in n)
    assert deliver["defaults"]["rate_limit"] == "100/m"
    assert deliver["defaults"]["max_retries"] == 5
    assert deliver["override"] is None
    assert deliver["effective"]["rate_limit"] == "100/m"


def test_registered_tasks_reflects_override(queue: Queue) -> None:
    send_email = next(t for t in queue.registered_tasks() if "send_email" in t["name"])
    queue.set_task_override(send_email["name"], max_retries=99)
    fresh = next(t for t in queue.registered_tasks() if t["name"] == send_email["name"])
    assert fresh["override"] == {"max_retries": 99}
    assert fresh["effective"]["max_retries"] == 99
    assert fresh["defaults"]["max_retries"] == 3  # original decorator value


# ── HTTP endpoints ────────────────────────────────────────────────────


def test_list_tasks_endpoint(dashboard: tuple[AuthedClient, Queue]) -> None:
    client, _ = dashboard
    tasks = client.get("/api/tasks")
    assert len(tasks) == 2
    for entry in tasks:
        assert "name" in entry and "defaults" in entry and "effective" in entry


def test_put_task_override(dashboard: tuple[AuthedClient, Queue]) -> None:
    client, queue = dashboard
    name = next(c.name for c in queue._task_configs if "send_email" in c.name)
    result = client.put(
        f"/api/tasks/{name}/override",
        {"max_retries": 7, "rate_limit": "50/s"},
    )
    assert result["max_retries"] == 7
    assert result["rate_limit"] == "50/s"

    fetched = client.get(f"/api/tasks/{name}/override")
    assert fetched["max_retries"] == 7


def test_put_task_override_rejects_unknown_field(
    dashboard: tuple[AuthedClient, Queue],
) -> None:
    client, queue = dashboard
    name = next(c.name for c in queue._task_configs if "send_email" in c.name)
    with pytest.raises(urllib.error.HTTPError) as exc_info:
        client.put(f"/api/tasks/{name}/override", {"made_up": 1})
    assert exc_info.value.code == 400


def test_delete_task_override(dashboard: tuple[AuthedClient, Queue]) -> None:
    client, queue = dashboard
    name = next(c.name for c in queue._task_configs if "send_email" in c.name)
    client.put(f"/api/tasks/{name}/override", {"max_retries": 7})
    assert client.delete(f"/api/tasks/{name}/override") == {"cleared": True}
    assert client.delete(f"/api/tasks/{name}/override") == {"cleared": False}


def test_get_task_override_404_when_none(dashboard: tuple[AuthedClient, Queue]) -> None:
    client, _ = dashboard
    with pytest.raises(urllib.error.HTTPError) as exc_info:
        client.get("/api/tasks/nonexistent/override")
    assert exc_info.value.code == 404


def test_put_queue_override_pauses_queue(dashboard: tuple[AuthedClient, Queue]) -> None:
    """Pausing via queue override must also update the live paused_queues
    state so a running worker stops dequeueing immediately."""
    client, queue = dashboard
    client.put("/api/queues/email/override", {"paused": True})
    assert "email" in queue.paused_queues()
    client.put("/api/queues/email/override", {"paused": False})
    assert "email" not in queue.paused_queues()


def test_list_queues_endpoint(dashboard: tuple[AuthedClient, Queue]) -> None:
    client, _ = dashboard
    queues = client.get("/api/queues")
    names = {q["name"] for q in queues}
    assert {"default", "email"} <= names


def test_put_queue_override_validates(dashboard: tuple[AuthedClient, Queue]) -> None:
    client, _ = dashboard
    with pytest.raises(urllib.error.HTTPError) as exc_info:
        client.put("/api/queues/default/override", {"max_concurrent": -1})
    assert exc_info.value.code == 400
