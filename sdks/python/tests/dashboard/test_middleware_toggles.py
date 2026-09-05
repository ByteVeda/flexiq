"""Tests for per-task middleware enable/disable from the dashboard."""

from __future__ import annotations

import threading
import urllib.error
from collections.abc import Generator
from http.server import ThreadingHTTPServer
from pathlib import Path
from typing import Any

import pytest

from conftest import join_worker
from flexiq import Queue
from flexiq.context import JobContext
from flexiq.dashboard import _make_handler
from flexiq.dashboard._testing import AuthedClient, seed_admin_and_session
from flexiq.dashboard.middleware_store import MiddlewareDisableStore
from flexiq.middleware import TaskMiddleware


class RecordingMiddleware(TaskMiddleware):
    """Captures every ``before`` invocation so the test can assert which
    tasks the middleware fired for."""

    name = "test.recording"

    def __init__(self) -> None:
        super().__init__()
        self.invocations: list[str] = []

    def before(self, ctx: JobContext) -> None:
        self.invocations.append(ctx.task_name)


class OtherMiddleware(TaskMiddleware):
    name = "test.other"

    def __init__(self) -> None:
        super().__init__()
        self.invocations: list[str] = []

    def before(self, ctx: JobContext) -> None:
        self.invocations.append(ctx.task_name)


@pytest.fixture
def middleware_pair() -> tuple[RecordingMiddleware, OtherMiddleware]:
    return RecordingMiddleware(), OtherMiddleware()


@pytest.fixture
def queue(tmp_path: Path, middleware_pair: tuple[RecordingMiddleware, OtherMiddleware]) -> Queue:
    rec, other = middleware_pair
    q = Queue(db_path=str(tmp_path / "mw.db"), middleware=[rec, other])

    @q.task()
    def alpha() -> str:
        return "a"

    @q.task()
    def beta() -> str:
        return "b"

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


def test_store_starts_empty(queue: Queue) -> None:
    store = MiddlewareDisableStore(queue)
    assert store.list_all() == {}
    assert store.get_for("alpha") == []


def test_set_disabled_adds_and_removes(queue: Queue) -> None:
    store = MiddlewareDisableStore(queue)
    store.set_disabled("alpha", "test.other", True)
    assert store.get_for("alpha") == ["test.other"]
    # Idempotent — same disable twice still has just one entry.
    store.set_disabled("alpha", "test.other", True)
    assert store.get_for("alpha") == ["test.other"]
    # Re-enable clears just that one.
    store.set_disabled("alpha", "test.other", False)
    assert store.get_for("alpha") == []


def test_clear_for_drops_setting_key(queue: Queue) -> None:
    store = MiddlewareDisableStore(queue)
    store.set_disabled("alpha", "test.other", True)
    assert store.clear_for("alpha") is True
    assert store.clear_for("alpha") is False
    assert store.get_for("alpha") == []


# ── Wiring into the middleware chain ──────────────────────────────────


def test_chain_skips_disabled_middleware(queue: Queue) -> None:
    """``_get_middleware_chain`` returns a chain that respects the disable
    list at lookup time — no worker restart required."""
    full = queue._get_middleware_chain("alpha")
    assert {mw.name for mw in full} == {"test.recording", "test.other"}
    queue.disable_middleware_for_task("alpha", "test.other")
    filtered = queue._get_middleware_chain("alpha")
    assert {mw.name for mw in filtered} == {"test.recording"}
    # Other tasks unaffected.
    assert {mw.name for mw in queue._get_middleware_chain("beta")} == {
        "test.recording",
        "test.other",
    }


def test_clear_re_enables_all(queue: Queue) -> None:
    queue.disable_middleware_for_task("alpha", "test.other")
    queue.disable_middleware_for_task("alpha", "test.recording")
    assert queue._get_middleware_chain("alpha") == []
    queue.clear_middleware_disables("alpha")
    assert len(queue._get_middleware_chain("alpha")) == 2


# ── Discovery ─────────────────────────────────────────────────────────


def test_list_middleware_reports_globals(queue: Queue) -> None:
    items = queue.list_middleware()
    names = {item["name"] for item in items}
    assert {"test.recording", "test.other"} <= names
    for entry in items:
        assert any(scope["kind"] == "global" for scope in entry["scopes"])


# ── HTTP endpoints ────────────────────────────────────────────────────


def test_list_middleware_endpoint(dashboard: tuple[AuthedClient, Queue]) -> None:
    client, _ = dashboard
    items = client.get("/api/middleware")
    names = {item["name"] for item in items}
    assert {"test.recording", "test.other"} <= names


def test_get_task_middleware_endpoint(dashboard: tuple[AuthedClient, Queue]) -> None:
    client, _ = dashboard
    result = client.get("/api/tasks/alpha/middleware")
    by_name = {entry["name"]: entry for entry in result["middleware"]}
    assert by_name["test.recording"]["disabled"] is False
    assert by_name["test.recording"]["effective"] is True


def test_put_task_middleware_disables(dashboard: tuple[AuthedClient, Queue]) -> None:
    client, queue = dashboard
    name = next(c.name for c in queue._task_configs if c.name.endswith("alpha"))
    result = client.put(f"/api/tasks/{name}/middleware/test.other", {"enabled": False})
    assert "test.other" in result["disabled"]
    # Reflected in the chain.
    chain_names = {mw.name for mw in queue._get_middleware_chain(name)}
    assert "test.other" not in chain_names
    # Re-enabling clears it.
    client.put(f"/api/tasks/{name}/middleware/test.other", {"enabled": True})
    chain_names = {mw.name for mw in queue._get_middleware_chain(name)}
    assert "test.other" in chain_names


def test_put_task_middleware_rejects_unknown_middleware(
    dashboard: tuple[AuthedClient, Queue],
) -> None:
    client, queue = dashboard
    name = next(c.name for c in queue._task_configs if c.name.endswith("alpha"))
    with pytest.raises(urllib.error.HTTPError) as exc_info:
        client.put(f"/api/tasks/{name}/middleware/not.a.real.mw", {"enabled": False})
    assert exc_info.value.code == 404


def test_put_task_middleware_rejects_bad_body(
    dashboard: tuple[AuthedClient, Queue],
) -> None:
    client, queue = dashboard
    name = next(c.name for c in queue._task_configs if c.name.endswith("alpha"))
    with pytest.raises(urllib.error.HTTPError) as exc_info:
        client.put(f"/api/tasks/{name}/middleware/test.other", {"enabled": "yes"})
    assert exc_info.value.code == 400


def test_put_task_middleware_handles_url_encoded_name(
    dashboard: tuple[AuthedClient, Queue],
) -> None:
    """Browser clients ``encodeURIComponent`` task names containing
    ``<``, ``>``, ``/`` etc. The server has to decode the captured
    group before looking up the disable list — otherwise the toggle
    silently no-ops because the disable is keyed by one name but the
    chain lookup uses another. Regression test for that path."""
    import urllib.parse

    client, queue = dashboard
    name = next(c.name for c in queue._task_configs if c.name.endswith("alpha"))
    encoded = urllib.parse.quote(name, safe="")
    assert "%" in encoded, "test setup: pick a task whose qualname needs encoding"
    result = client.put(f"/api/tasks/{encoded}/middleware/test.other", {"enabled": False})
    assert "test.other" in result["disabled"]
    # The chain lookup uses the decoded name, so it must reflect the
    # disable that was written by the encoded URL.
    chain_names = {mw.name for mw in queue._get_middleware_chain(name)}
    assert "test.other" not in chain_names


def test_delete_task_middleware_clears_all(
    dashboard: tuple[AuthedClient, Queue],
) -> None:
    client, queue = dashboard
    name = next(c.name for c in queue._task_configs if c.name.endswith("alpha"))
    client.put(f"/api/tasks/{name}/middleware/test.other", {"enabled": False})
    client.put(f"/api/tasks/{name}/middleware/test.recording", {"enabled": False})
    assert queue._get_middleware_chain(name) == []
    result = client.delete(f"/api/tasks/{name}/middleware")
    assert result == {"cleared": True}
    assert len(queue._get_middleware_chain(name)) == 2


# ── End-to-end: disabled middleware doesn't fire ─────────────────────


def test_disabled_middleware_does_not_fire(
    queue: Queue,
    middleware_pair: tuple[RecordingMiddleware, OtherMiddleware],
    poll_until: Any,
) -> None:
    rec, other = middleware_pair
    alpha_name = next(c.name for c in queue._task_configs if c.name.endswith("alpha"))
    queue.disable_middleware_for_task(alpha_name, "test.other")

    thread = threading.Thread(target=queue.run_worker, daemon=True)
    thread.start()
    try:
        queue.enqueue(alpha_name)
        poll_until(lambda: alpha_name in rec.invocations, message="task didn't run")
    finally:
        queue._inner.request_shutdown()
        join_worker(thread)

    assert alpha_name in rec.invocations  # global fired
    assert alpha_name not in other.invocations  # disabled for this task
