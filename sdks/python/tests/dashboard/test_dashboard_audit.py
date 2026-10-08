"""Dashboard changes land in the audit trail (#1020).

Boots a real dashboard, drives it over HTTP, and reads ``audit_log`` straight
from the SQLite file — the row shape is a cross-SDK contract, so the test
checks the stored columns rather than any SDK view of them.
"""

from __future__ import annotations

import sqlite3
import threading
import urllib.error
import urllib.request
from collections.abc import Iterator
from contextlib import contextmanager
from http.server import ThreadingHTTPServer
from pathlib import Path
from typing import Any

import pytest

from flexiq import Queue
from flexiq.dashboard import _make_handler
from flexiq.dashboard import server as dashboard_server
from flexiq.dashboard._testing import AuthedClient, seed_admin_and_session
from flexiq.dashboard.auth import AuthStore
from flexiq.dashboard.server import serve_dashboard


@contextmanager
def _serve(queue: Queue, *, auth_enabled: bool) -> Iterator[str]:
    """A live dashboard; on exit its audit recorder is flushed."""
    handler = _make_handler(queue, auth_enabled=auth_enabled)
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    queue._inner.start_dashboard_audit(90)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()
        queue._inner.close_dashboard_audit()


@pytest.fixture
def db_path(tmp_path: Path) -> str:
    return str(tmp_path / "audit.db")


@pytest.fixture
def queue(db_path: str) -> Queue:
    return Queue(db_path=db_path, workers=1)


def _trail(db_path: str) -> list[dict[str, object]]:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT namespace, principal_kind, token_id, principal, operation,"
            " target_kind, target, outcome, access FROM audit_log ORDER BY at_ms, id"
        ).fetchall()
    return [dict(row) for row in rows]


def _post(url: str) -> int:
    req = urllib.request.Request(url, method="POST", data=b"")
    try:
        with urllib.request.urlopen(req) as resp:
            return int(resp.status)
    except urllib.error.HTTPError as e:
        return e.code


def test_a_signed_in_change_and_a_refused_one_are_recorded(queue: Queue, db_path: str) -> None:
    with _serve(queue, auth_enabled=True) as url:
        admin = AuthedClient(base=url, session=seed_admin_and_session(queue))
        admin.post("/api/queues/emails/pause")
        admin.get("/api/queues/paused")

        AuthStore(queue).create_user("vera", "viewer-pass-1234", role="viewer")
        viewer_user = AuthStore(queue).get_user("vera")
        assert viewer_user is not None
        viewer = AuthedClient(base=url, session=AuthStore(queue).create_session(viewer_user))
        with pytest.raises(urllib.error.HTTPError) as refused:
            viewer.post("/api/queues/emails/resume")
        assert refused.value.code == 403

        # No session: nobody to name, so nothing is recorded.
        assert _post(f"{url}/api/queues/emails/resume") == 401

    assert _trail(db_path) == [
        {
            "namespace": "default",
            "principal_kind": "user",
            "token_id": "test-admin",
            "principal": "test-admin",
            "operation": "dashboard POST /api/queues/{queue}/pause",
            "target_kind": "queue",
            "target": "emails",
            "outcome": "OK",
            "access": "write",
        },
        {
            "namespace": "default",
            "principal_kind": "user",
            "token_id": "vera",
            "principal": "vera",
            "operation": "dashboard POST /api/queues/{queue}/resume",
            "target_kind": "queue",
            "target": "emails",
            "outcome": "PERMISSION_DENIED",
            "access": "write",
        },
    ]


def test_an_open_dashboard_records_anonymously(queue: Queue, db_path: str) -> None:
    with _serve(queue, auth_enabled=False) as url:
        assert _post(f"{url}/api/dead-letters/purge") == 200
        # The auth routes are off with auth off; nothing changed.
        assert _post(f"{url}/api/auth/logout") == 404

    trail = _trail(db_path)
    assert len(trail) == 1, trail
    assert trail[0]["principal_kind"] == "anonymous"
    assert trail[0]["token_id"] == ""
    assert trail[0]["operation"] == "dashboard POST /api/dead-letters/purge"
    assert trail[0]["target_kind"] is None


def test_the_retention_window_is_at_least_a_day(
    queue: Queue, monkeypatch: pytest.MonkeyPatch
) -> None:
    with pytest.raises(ValueError, match="audit_retention_days"):
        serve_dashboard(queue, port=0, audit_retention_days=0)
    monkeypatch.setenv("FLEXIQ_AUDIT_RETENTION_DAYS", "7d")
    with pytest.raises(ValueError, match="FLEXIQ_AUDIT_RETENTION_DAYS"):
        serve_dashboard(queue, port=0)
    with pytest.raises(ValueError, match="at least 1 day"):
        queue._inner.start_dashboard_audit(0)


class _RecordingAudit:
    """A binding that keeps every dashboard action handed to it."""

    def __init__(self) -> None:
        self.actions: list[tuple[str, str, int, str | None]] = []

    def record_dashboard_action(
        self, method: str, path: str, status: int, username: str | None = None
    ) -> None:
        self.actions.append((method, path, status, username))


class _RecordingQueue:
    def __init__(self) -> None:
        self._inner = _RecordingAudit()


class _HungUp:
    """A socket file whose peer has gone: every write fails."""

    def write(self, data: bytes) -> int:
        raise BrokenPipeError("peer closed")

    def flush(self) -> None:
        pass


def test_a_caller_gone_before_the_headers_is_recorded_499() -> None:
    queue = _RecordingQueue()
    handler_class = _make_handler(queue)  # type: ignore[arg-type]
    # Built without a socket, so no request is served on construction.
    handler: Any = object.__new__(handler_class)
    handler.wfile = _HungUp()
    handler.request_version = "HTTP/1.1"
    handler.requestline = "POST /api/queues/emails/pause HTTP/1.1"
    handler.command = "POST"
    handler.path = "/api/queues/emails/pause"
    handler.client_address = ("127.0.0.1", 0)

    def change() -> None:
        handler._audit_caller = "alice"
        handler._json_response({"ok": True})

    handler._serve_change("POST", change)
    assert queue._inner.actions == [("POST", "/api/queues/emails/pause", 499, "alice")]


class _FailingAudit:
    """A binding whose audit writer cannot start."""

    def start_dashboard_audit(self, retention_days: int) -> None:
        raise RuntimeError("audit writer did not start")

    def close_dashboard_audit(self) -> None:
        pass


class _FailingQueue:
    _inner = _FailingAudit()


def test_a_failed_audit_start_still_closes_the_socket(monkeypatch: pytest.MonkeyPatch) -> None:
    closed: list[bool] = []

    class _TrackedServer(ThreadingHTTPServer):
        def server_close(self) -> None:
            closed.append(True)
            super().server_close()

    monkeypatch.setattr(dashboard_server, "ThreadingHTTPServer", _TrackedServer)
    with pytest.raises(RuntimeError, match="did not start"):
        serve_dashboard(_FailingQueue(), port=0)  # type: ignore[arg-type]
    assert closed == [True]
