"""Tests for dashboard authentication.

Covers the auth helpers in :mod:`flexiq.dashboard.auth` and the HTTP
endpoints under ``/api/auth/*``, plus the session-gating behaviour the
server applies to every other API route.
"""

from __future__ import annotations

import json
import threading
import urllib.error
import urllib.request
from collections.abc import Generator
from http.server import ThreadingHTTPServer
from pathlib import Path
from typing import Any

import pytest

from flexiq import Queue
from flexiq.dashboard import _make_handler
from flexiq.dashboard import server as dashboard_module
from flexiq.dashboard.auth import (
    SESSION_PREFIX,
    AuthStore,
    Role,
    bootstrap_admin_from_env,
    hash_password,
    verify_password,
)


@pytest.fixture
def queue(tmp_path: Path) -> Queue:
    return Queue(db_path=str(tmp_path / "auth.db"))


# ── Password hashing primitives ─────────────────────────────────────────


def test_hash_password_round_trip() -> None:
    encoded = hash_password("hunter2-correct-horse")
    assert verify_password("hunter2-correct-horse", encoded) is True
    assert verify_password("wrong", encoded) is False


def test_hash_password_produces_unique_salts() -> None:
    a = hash_password("same-password")
    b = hash_password("same-password")
    assert a != b, "different salts must produce different hashes"
    assert verify_password("same-password", a)
    assert verify_password("same-password", b)


def test_verify_password_rejects_malformed_encoding() -> None:
    assert verify_password("anything", "not-a-real-hash") is False
    assert verify_password("anything", "scrypt$xxx$yyy$zzz") is False
    assert verify_password("anything", "pbkdf2_sha256$abc$def$ghi") is False


# ── AuthStore: users ────────────────────────────────────────────────────


def test_count_users_starts_at_zero(queue: Queue) -> None:
    assert AuthStore(queue).count_users() == 0


def test_create_user_persists(queue: Queue) -> None:
    store = AuthStore(queue)
    user = store.create_user("alice", "hunter2-secret")
    assert user.username == "alice"
    assert user.role == "admin"
    assert store.count_users() == 1
    assert store.get_user("alice") is not None
    assert store.get_user("missing") is None


def test_create_user_rejects_duplicate(queue: Queue) -> None:
    store = AuthStore(queue)
    store.create_user("alice", "hunter2-secret")
    with pytest.raises(ValueError, match="already exists"):
        store.create_user("alice", "another-pass")


def test_create_user_validates_username(queue: Queue) -> None:
    store = AuthStore(queue)
    with pytest.raises(ValueError, match="empty"):
        store.create_user("", "hunter2-secret")
    with pytest.raises(ValueError, match="may only contain"):
        store.create_user("alice bob", "hunter2-secret")


def test_create_user_validates_password(queue: Queue) -> None:
    store = AuthStore(queue)
    with pytest.raises(ValueError, match=">= 8 chars"):
        store.create_user("alice", "short")


def test_authenticate(queue: Queue) -> None:
    store = AuthStore(queue)
    store.create_user("alice", "hunter2-secret")
    assert store.authenticate("alice", "hunter2-secret") is not None
    assert store.authenticate("alice", "wrong") is None
    # Unknown username also returns None, without timing leak (we don't
    # assert timing here, just behaviour).
    assert store.authenticate("bob", "anything") is None


def test_authenticate_updates_last_login(queue: Queue) -> None:
    store = AuthStore(queue)
    store.create_user("alice", "hunter2-secret")
    assert store.get_user("alice").last_login_at is None  # type: ignore[union-attr]
    store.authenticate("alice", "hunter2-secret")
    assert store.get_user("alice").last_login_at is not None  # type: ignore[union-attr]


def test_delete_user(queue: Queue) -> None:
    store = AuthStore(queue)
    store.create_user("alice", "hunter2-secret")
    assert store.delete_user("alice") is True
    assert store.delete_user("alice") is False
    assert store.get_user("alice") is None


def test_update_password(queue: Queue) -> None:
    store = AuthStore(queue)
    store.create_user("alice", "hunter2-secret")
    store.update_password("alice", "new-secure-pass")
    assert store.authenticate("alice", "new-secure-pass") is not None
    assert store.authenticate("alice", "hunter2-secret") is None


# ── AuthStore: sessions ────────────────────────────────────────────────


def test_create_and_get_session(queue: Queue) -> None:
    store = AuthStore(queue)
    user = store.create_user("alice", "hunter2-secret")
    session = store.create_session(user)
    fetched = store.get_session(session.token)
    assert fetched is not None
    assert fetched.username == "alice"
    assert fetched.csrf_token == session.csrf_token
    assert not fetched.is_expired()


def test_session_role_is_the_enum(queue: Queue) -> None:
    store = AuthStore(queue)
    session = store.create_session(store.create_user("alice", "hunter2-secret"))
    fetched = store.get_session(session.token)
    assert fetched is not None
    assert fetched.role is Role.ADMIN


def test_unreadable_persisted_role_falls_back_to_viewer(queue: Queue) -> None:
    """A stored role nothing recognizes must not be handed back as-is."""
    store = AuthStore(queue)
    session = store.create_session(store.create_user("alice", "hunter2-secret"))
    raw = json.loads(queue.get_setting(SESSION_PREFIX + session.token) or "{}")
    raw["role"] = "superuser"
    queue.set_setting(SESSION_PREFIX + session.token, json.dumps(raw))

    fetched = store.get_session(session.token)
    assert fetched is not None
    assert fetched.role is Role.VIEWER


def test_get_session_unknown_token_returns_none(queue: Queue) -> None:
    assert AuthStore(queue).get_session("nope") is None
    assert AuthStore(queue).get_session("") is None


def test_delete_session(queue: Queue) -> None:
    store = AuthStore(queue)
    user = store.create_user("alice", "hunter2-secret")
    session = store.create_session(user)
    assert store.delete_session(session.token) is True
    assert store.get_session(session.token) is None
    assert store.delete_session(session.token) is False


def test_expired_sessions_pruned_on_lookup(queue: Queue) -> None:
    store = AuthStore(queue)
    user = store.create_user("alice", "hunter2-secret")
    session = store.create_session(user, ttl_seconds=0)
    # ttl_seconds=0 means it expires immediately.
    assert store.get_session(session.token) is None


def test_prune_expired_sessions(queue: Queue) -> None:
    store = AuthStore(queue)
    user = store.create_user("alice", "hunter2-secret")
    long_lived = store.create_session(user, ttl_seconds=3600)
    short_lived = store.create_session(user, ttl_seconds=0)
    removed = store.prune_expired_sessions()
    assert removed >= 1
    assert store.get_session(long_lived.token) is not None
    assert store.get_session(short_lived.token) is None


# ── Env bootstrap ──────────────────────────────────────────────────────


def test_bootstrap_admin_from_env(queue: Queue, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FLEXIQ_DASHBOARD_ADMIN_USER", "envadmin")
    monkeypatch.setenv("FLEXIQ_DASHBOARD_ADMIN_PASSWORD", "from-environ-pass")
    user = bootstrap_admin_from_env(queue)
    assert user is not None
    assert user.username == "envadmin"

    # Idempotent — second call is a no-op.
    again = bootstrap_admin_from_env(queue)
    assert again is None


def test_bootstrap_admin_noop_without_env(queue: Queue, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FLEXIQ_DASHBOARD_ADMIN_USER", raising=False)
    monkeypatch.delenv("FLEXIQ_DASHBOARD_ADMIN_PASSWORD", raising=False)
    assert bootstrap_admin_from_env(queue) is None
    assert AuthStore(queue).count_users() == 0


# ── OAuth users ────────────────────────────────────────────────────────


def test_verify_password_rejects_oauth_sentinel_hash() -> None:
    assert verify_password("anything", "oauth:google") is False
    assert verify_password("anything", "oauth:okta") is False


def test_get_or_create_oauth_user_first_user_is_viewer_without_allowlist(
    queue: Queue,
) -> None:
    # Even on an empty table, admin comes only from the allowlist — never
    # from first-login-wins.
    store = AuthStore(queue)
    user = store.get_or_create_oauth_user(
        slot="google",
        subject="1184283742",
        email="alice@acme.com",
        name="Alice Example",
        email_verified=True,
    )
    assert user.username == "google:1184283742"
    assert user.role == "viewer"
    assert user.email == "alice@acme.com"
    assert user.display_name == "Alice Example"
    assert user.is_oauth is True


def test_get_or_create_oauth_user_allowlisted_first_user_is_admin(queue: Queue) -> None:
    store = AuthStore(queue)
    user = store.get_or_create_oauth_user(
        slot="google",
        subject="1184283742",
        email="alice@acme.com",
        name="Alice Example",
        email_verified=True,
        admin_emails=("alice@acme.com",),
    )
    assert user.role == "admin"


def test_get_or_create_oauth_user_subsequent_user_is_viewer(queue: Queue) -> None:
    store = AuthStore(queue)
    store.get_or_create_oauth_user(
        slot="google",
        subject="111",
        email="alice@acme.com",
        name="Alice",
        email_verified=True,
    )
    second = store.get_or_create_oauth_user(
        slot="google",
        subject="222",
        email="bob@acme.com",
        name="Bob",
        email_verified=True,
    )
    assert second.role == "viewer"


def test_get_or_create_oauth_user_admin_emails_take_precedence(queue: Queue) -> None:
    store = AuthStore(queue)
    # Pre-seed a password user so the table is not empty.
    store.create_user("primary", "hunter2-secret")
    listed = store.get_or_create_oauth_user(
        slot="google",
        subject="111",
        email="alice@acme.com",
        name="Alice",
        email_verified=True,
        admin_emails=("alice@acme.com",),
    )
    assert listed.role == "admin"

    unlisted = store.get_or_create_oauth_user(
        slot="google",
        subject="222",
        email="eve@evil.com",
        name="Eve",
        email_verified=True,
        admin_emails=("alice@acme.com",),
    )
    assert unlisted.role == "viewer"


def test_get_or_create_oauth_user_unverified_email_never_gets_admin(queue: Queue) -> None:
    store = AuthStore(queue)
    # Even on empty table, an unverified email cannot become admin.
    user = store.get_or_create_oauth_user(
        slot="github",
        subject="42",
        email="claimed@acme.com",
        name=None,
        email_verified=False,
        admin_emails=("claimed@acme.com",),
    )
    assert user.role == "viewer"


def test_get_or_create_oauth_user_email_match_is_case_insensitive(queue: Queue) -> None:
    store = AuthStore(queue)
    store.create_user("primary", "hunter2-secret")
    user = store.get_or_create_oauth_user(
        slot="google",
        subject="123",
        email="Alice@ACME.com",
        name=None,
        email_verified=True,
        admin_emails=("alice@acme.com",),
    )
    assert user.role == "admin"


def test_get_or_create_oauth_user_returning_user_refreshes_display_fields(
    queue: Queue,
) -> None:
    store = AuthStore(queue)
    first = store.get_or_create_oauth_user(
        slot="google",
        subject="555",
        email="alice@acme.com",
        name="Alice",
        email_verified=True,
    )
    again = store.get_or_create_oauth_user(
        slot="google",
        subject="555",
        email="alice-new@acme.com",
        name="Alice Renamed",
        email_verified=True,
    )
    assert again.username == first.username
    assert again.email == "alice-new@acme.com"
    assert again.display_name == "Alice Renamed"
    # Role is not re-evaluated on subsequent logins.
    assert again.role == first.role


def test_oauth_users_namespace_by_slot(queue: Queue) -> None:
    store = AuthStore(queue)
    a = store.get_or_create_oauth_user(
        slot="okta", subject="abc", email=None, name=None, email_verified=False
    )
    b = store.get_or_create_oauth_user(
        slot="microsoft", subject="abc", email=None, name=None, email_verified=False
    )
    assert a.username != b.username
    assert store.count_users() == 2


# ── HTTP endpoints ─────────────────────────────────────────────────────


@pytest.fixture
def dashboard_server(queue: Queue) -> Generator[tuple[str, Queue]]:
    handler = _make_handler(queue, auth_enabled=True)
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{port}", queue
    finally:
        server.shutdown()


@pytest.fixture
def open_dashboard_server(queue: Queue) -> Generator[tuple[str, Queue]]:
    """Server with the default configuration — auth disabled."""
    handler = _make_handler(queue)
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{port}", queue
    finally:
        server.shutdown()


def _get(
    url: str,
    *,
    cookies: dict[str, str] | None = None,
    headers: dict[str, str] | None = None,
) -> tuple[int, Any, dict[str, str]]:
    req = urllib.request.Request(url, method="GET")
    if cookies:
        req.add_header("Cookie", "; ".join(f"{k}={v}" for k, v in cookies.items()))
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    try:
        resp = urllib.request.urlopen(req)
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}"), dict(e.headers or {})
    body = json.loads(resp.read() or b"{}")
    set_cookies = resp.headers.get_all("Set-Cookie") or []
    return resp.status, body, {"Set-Cookie": "\n".join(set_cookies)}


def _post(
    url: str,
    body: dict | None = None,
    *,
    cookies: dict[str, str] | None = None,
    headers: dict[str, str] | None = None,
) -> tuple[int, Any, dict[str, str]]:
    data = json.dumps(body or {}).encode()
    req = urllib.request.Request(url, method="POST", data=data)
    req.add_header("Content-Type", "application/json")
    if cookies:
        req.add_header("Cookie", "; ".join(f"{k}={v}" for k, v in cookies.items()))
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    try:
        resp = urllib.request.urlopen(req)
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}"), dict(e.headers or {})
    parsed = json.loads(resp.read() or b"{}")
    set_cookies = resp.headers.get_all("Set-Cookie") or []
    return resp.status, parsed, {"Set-Cookie": "\n".join(set_cookies)}


def _parse_set_cookie(raw: str) -> dict[str, str]:
    """Pull out the cookie name→value pairs from one or more Set-Cookie lines."""
    out: dict[str, str] = {}
    for line in raw.splitlines():
        if not line:
            continue
        nv = line.split(";", 1)[0]
        if "=" in nv:
            name, value = nv.split("=", 1)
            out[name.strip()] = value.strip()
    return out


def test_auth_status_before_setup(dashboard_server: tuple[str, Queue]) -> None:
    base, _ = dashboard_server
    status, body, _ = _get(f"{base}/api/auth/status")
    assert status == 200
    assert body == {"auth_enabled": True, "setup_required": True}


def test_protected_route_returns_503_before_setup(dashboard_server: tuple[str, Queue]) -> None:
    base, _ = dashboard_server
    status, body, _ = _get(f"{base}/api/stats")
    assert status == 503
    assert body == {"error": "setup_required"}


def test_setup_creates_first_admin(dashboard_server: tuple[str, Queue]) -> None:
    base, queue = dashboard_server
    status, body, _ = _post(
        f"{base}/api/auth/setup",
        {"username": "alice", "password": "hunter2-secret"},
    )
    assert status == 200
    assert body["user"]["username"] == "alice"
    assert AuthStore(queue).count_users() == 1


def test_setup_blocked_after_first_user(dashboard_server: tuple[str, Queue]) -> None:
    base, queue = dashboard_server
    AuthStore(queue).create_user("alice", "hunter2-secret")
    status, body, _ = _post(
        f"{base}/api/auth/setup",
        {"username": "mallory", "password": "hijack-attempt"},
    )
    assert status == 400
    assert "setup already complete" in body["error"]


def test_login_and_session_cookie(dashboard_server: tuple[str, Queue]) -> None:
    base, queue = dashboard_server
    AuthStore(queue).create_user("alice", "hunter2-secret")
    status, body, headers = _post(
        f"{base}/api/auth/login",
        {"username": "alice", "password": "hunter2-secret"},
    )
    assert status == 200
    assert body["user"]["username"] == "alice"
    # Token must NOT leak in the body — it lives only in the HttpOnly cookie.
    assert "token" not in body["session"]

    cookies = _parse_set_cookie(headers["Set-Cookie"])
    assert "flexiq_session" in cookies
    assert "flexiq_csrf" in cookies
    # HttpOnly must be set on the session cookie.
    assert "HttpOnly" in headers["Set-Cookie"]
    # CSRF cookie value must match what whoami says.
    sess_token = cookies["flexiq_session"]
    csrf = cookies["flexiq_csrf"]
    status, body, _ = _get(f"{base}/api/auth/whoami", cookies={"flexiq_session": sess_token})
    assert status == 200
    assert body["user"]["username"] == "alice"
    assert body["csrf_token"] == csrf


def test_login_with_wrong_password(dashboard_server: tuple[str, Queue]) -> None:
    base, queue = dashboard_server
    AuthStore(queue).create_user("alice", "hunter2-secret")
    status, body, _ = _post(
        f"{base}/api/auth/login",
        {"username": "alice", "password": "nope"},
    )
    assert status == 400
    assert body["error"] == "invalid_credentials"


def test_whoami_without_session_returns_404(dashboard_server: tuple[str, Queue]) -> None:
    base, queue = dashboard_server
    AuthStore(queue).create_user("alice", "hunter2-secret")
    status, body, _ = _get(f"{base}/api/auth/whoami")
    assert status == 401
    assert body["error"] == "not_authenticated"


def test_protected_get_requires_session(dashboard_server: tuple[str, Queue]) -> None:
    base, queue = dashboard_server
    AuthStore(queue).create_user("alice", "hunter2-secret")
    status, _, _ = _get(f"{base}/api/stats")
    assert status == 401


def test_protected_get_works_with_session(dashboard_server: tuple[str, Queue]) -> None:
    base, queue = dashboard_server
    store = AuthStore(queue)
    user = store.create_user("alice", "hunter2-secret")
    session = store.create_session(user)
    status, _, _ = _get(f"{base}/api/stats", cookies={"flexiq_session": session.token})
    assert status == 200


def test_post_requires_csrf(dashboard_server: tuple[str, Queue]) -> None:
    base, queue = dashboard_server
    store = AuthStore(queue)
    user = store.create_user("alice", "hunter2-secret")
    session = store.create_session(user)
    # POST with only the session cookie but no CSRF → 403.
    status, body, _ = _post(
        f"{base}/api/dead-letters/purge",
        {},
        cookies={"flexiq_session": session.token},
    )
    assert status == 403
    assert body["error"] == "csrf_failed"


def test_post_succeeds_with_valid_csrf(dashboard_server: tuple[str, Queue]) -> None:
    base, queue = dashboard_server
    store = AuthStore(queue)
    user = store.create_user("alice", "hunter2-secret")
    session = store.create_session(user)
    status, _, _ = _post(
        f"{base}/api/dead-letters/purge",
        {},
        cookies={
            "flexiq_session": session.token,
            "flexiq_csrf": session.csrf_token,
        },
        headers={"X-CSRF-Token": session.csrf_token},
    )
    assert status == 200


def test_post_rejected_when_csrf_mismatched(dashboard_server: tuple[str, Queue]) -> None:
    base, queue = dashboard_server
    store = AuthStore(queue)
    user = store.create_user("alice", "hunter2-secret")
    session = store.create_session(user)
    status, body, _ = _post(
        f"{base}/api/dead-letters/purge",
        {},
        cookies={
            "flexiq_session": session.token,
            "flexiq_csrf": session.csrf_token,
        },
        headers={"X-CSRF-Token": "different-value"},
    )
    assert status == 403
    assert body["error"] == "csrf_failed"


def test_logout_invalidates_session(dashboard_server: tuple[str, Queue]) -> None:
    base, queue = dashboard_server
    store = AuthStore(queue)
    user = store.create_user("alice", "hunter2-secret")
    session = store.create_session(user)
    status, _, _ = _post(
        f"{base}/api/auth/logout",
        {},
        cookies={
            "flexiq_session": session.token,
            "flexiq_csrf": session.csrf_token,
        },
        headers={"X-CSRF-Token": session.csrf_token},
    )
    assert status == 200
    assert AuthStore(queue).get_session(session.token) is None


def test_change_password_flow(dashboard_server: tuple[str, Queue]) -> None:
    base, queue = dashboard_server
    store = AuthStore(queue)
    user = store.create_user("alice", "hunter2-secret")
    session = store.create_session(user)
    status, _, _ = _post(
        f"{base}/api/auth/change-password",
        {"old_password": "hunter2-secret", "new_password": "brand-new-secure"},
        cookies={
            "flexiq_session": session.token,
            "flexiq_csrf": session.csrf_token,
        },
        headers={"X-CSRF-Token": session.csrf_token},
    )
    assert status == 200
    assert store.authenticate("alice", "brand-new-secure") is not None
    assert store.authenticate("alice", "hunter2-secret") is None


def test_health_endpoint_is_public(dashboard_server: tuple[str, Queue]) -> None:
    base, queue = dashboard_server
    AuthStore(queue).create_user("alice", "hunter2-secret")
    status, _, _ = _get(f"{base}/health")
    assert status == 200


# ── Viewer role (read-only RBAC) ───────────────────────────────────────


def test_viewer_can_read_but_not_mutate(dashboard_server: tuple[str, Queue]) -> None:
    base, queue = dashboard_server
    store = AuthStore(queue)
    viewer = store.create_user("watcher", "hunter2-secret", role="viewer")
    session = store.create_session(viewer)
    cookies = {"flexiq_session": session.token, "flexiq_csrf": session.csrf_token}

    status, _, _ = _get(f"{base}/api/stats", cookies=cookies)
    assert status == 200

    status, body, _ = _post(
        f"{base}/api/dead-letters/purge",
        {},
        cookies=cookies,
        headers={"X-CSRF-Token": session.csrf_token},
    )
    assert status == 403
    assert body["error"] == "forbidden"


def test_viewer_keeps_auth_self_service(dashboard_server: tuple[str, Queue]) -> None:
    base, queue = dashboard_server
    store = AuthStore(queue)
    viewer = store.create_user("watcher", "hunter2-secret", role="viewer")
    session = store.create_session(viewer)
    status, _, _ = _post(
        f"{base}/api/auth/change-password",
        {"old_password": "hunter2-secret", "new_password": "brand-new-secure"},
        cookies={"flexiq_session": session.token, "flexiq_csrf": session.csrf_token},
        headers={"X-CSRF-Token": session.csrf_token},
    )
    assert status == 200


# ── Probe endpoints (/readiness, /metrics) ─────────────────────────────


def test_readiness_requires_session_when_auth_enabled(
    dashboard_server: tuple[str, Queue],
) -> None:
    base, queue = dashboard_server
    store = AuthStore(queue)
    user = store.create_user("alice", "hunter2-secret")

    status, body, _ = _get(f"{base}/readiness")
    assert status == 401
    assert body["error"] == "not_authenticated"

    session = store.create_session(user)
    status, _, _ = _get(f"{base}/readiness", cookies={"flexiq_session": session.token})
    assert status == 200


def test_readiness_answers_503_when_degraded(
    open_dashboard_server: tuple[str, Queue],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A degraded probe must fail the check, or the orchestrator keeps routing here."""
    base, _ = open_dashboard_server
    status, _, _ = _get(f"{base}/readiness")
    assert status == 200

    monkeypatch.setattr(
        dashboard_module,
        "check_readiness",
        lambda _queue: {"status": "degraded", "checks": {"storage": "error: db gone"}},
    )
    status, body, _ = _get(f"{base}/readiness")
    assert status == 503
    # The body still explains which dependency failed.
    assert body["checks"]["storage"] == "error: db gone"


def test_metrics_requires_auth_when_enabled(dashboard_server: tuple[str, Queue]) -> None:
    base, _ = dashboard_server
    status, body, _ = _get(f"{base}/metrics")
    assert status == 401
    assert body["error"] == "not_authenticated"


def test_probe_accepts_metrics_bearer_when_auth_enabled(
    dashboard_server: tuple[str, Queue],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base, queue = dashboard_server
    monkeypatch.setenv("FLEXIQ_DASHBOARD_METRICS_TOKEN", "scraper-token")
    store = AuthStore(queue)
    user = store.create_user("alice", "hunter2-secret")

    status, _, _ = _get(f"{base}/readiness", headers={"Authorization": "Bearer scraper-token"})
    assert status == 200

    status, _, _ = _get(f"{base}/readiness", headers={"Authorization": "Bearer wrong"})
    assert status == 401

    # A valid session works even when the bearer token is configured.
    session = store.create_session(user)
    status, _, _ = _get(f"{base}/readiness", cookies={"flexiq_session": session.token})
    assert status == 200


# ── Security headers ───────────────────────────────────────────────────


def test_security_headers_on_every_response(open_dashboard_server: tuple[str, Queue]) -> None:
    base, _ = open_dashboard_server
    # "/" is skipped: SPA assets may be absent in CI, and the headers are
    # emitted from end_headers() so they apply to every response uniformly.
    for path in ("/health", "/api/stats"):
        resp = urllib.request.urlopen(f"{base}{path}")
        headers = resp.headers
        assert headers["X-Content-Type-Options"] == "nosniff", path
        assert headers["X-Frame-Options"] == "DENY", path
        assert headers["Referrer-Policy"] == "same-origin", path
        assert "default-src 'self'" in (headers["Content-Security-Policy"] or ""), path


# ── Auth disabled (the default) ────────────────────────────────────────


def test_auth_disabled_status(open_dashboard_server: tuple[str, Queue]) -> None:
    base, _ = open_dashboard_server
    status, body, _ = _get(f"{base}/api/auth/status")
    assert status == 200
    assert body == {"auth_enabled": False, "setup_required": False}


def test_auth_disabled_serves_api_without_session(
    open_dashboard_server: tuple[str, Queue],
) -> None:
    base, _ = open_dashboard_server
    status, _, _ = _get(f"{base}/api/stats")
    assert status == 200


def test_auth_disabled_allows_mutations_without_csrf(
    open_dashboard_server: tuple[str, Queue],
) -> None:
    base, _ = open_dashboard_server
    status, _, _ = _post(f"{base}/api/dead-letters/purge", {})
    assert status == 200


def test_auth_disabled_ignores_existing_users(
    open_dashboard_server: tuple[str, Queue],
) -> None:
    base, queue = open_dashboard_server
    AuthStore(queue).create_user("alice", "hunter2-secret")
    status, _, _ = _get(f"{base}/api/stats")
    assert status == 200


def test_auth_disabled_rejects_auth_endpoints(
    open_dashboard_server: tuple[str, Queue],
) -> None:
    base, _ = open_dashboard_server
    for method, path in [
        ("GET", "/api/auth/whoami"),
        ("GET", "/api/auth/providers"),
        ("POST", "/api/auth/login"),
        ("POST", "/api/auth/setup"),
        ("POST", "/api/auth/logout"),
    ]:
        if method == "GET":
            status, body, _ = _get(f"{base}{path}")
        else:
            status, body, _ = _post(f"{base}{path}", {})
        assert status == 404, path
        assert body == {"error": "auth_disabled"}, path


def test_auth_disabled_probes_stay_public(open_dashboard_server: tuple[str, Queue]) -> None:
    base, _ = open_dashboard_server
    status, _, _ = _get(f"{base}/readiness")
    assert status == 200


def test_auth_disabled_probe_honours_metrics_token(
    open_dashboard_server: tuple[str, Queue],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base, _ = open_dashboard_server
    monkeypatch.setenv("FLEXIQ_DASHBOARD_METRICS_TOKEN", "scraper-token")
    status, _, _ = _get(f"{base}/readiness")
    assert status == 401
    status, _, _ = _get(f"{base}/readiness", headers={"Authorization": "Bearer scraper-token"})
    assert status == 200
