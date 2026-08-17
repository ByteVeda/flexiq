"""Authentication route handlers."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from flexiq.dashboard.auth import AuthStore
from flexiq.dashboard.errors import _BadRequest, _NotFound

if TYPE_CHECKING:
    from flexiq.app import Queue
    from flexiq.dashboard.request_context import RequestContext


def _require_field(body: dict, key: str) -> str:
    value = body.get(key)
    if not isinstance(value, str) or not value:
        raise _BadRequest(f"missing or empty field '{key}'")
    return value


def _serialize_user(user: Any) -> dict[str, Any]:
    return {
        "username": user.username,
        "role": user.role,
        "created_at": user.created_at,
        "last_login_at": user.last_login_at,
    }


def _serialize_session(session: Any) -> dict[str, Any]:
    return {
        "username": session.username,
        "role": session.role,
        "expires_at": session.expires_at,
        "csrf_token": session.csrf_token,
    }


def handle_auth_status(queue: Queue, _qs: dict, *, auth_enabled: bool = True) -> dict[str, Any]:
    """Public endpoint: tells the SPA whether auth applies and setup is required.

    Returns ``{auth_enabled: bool, setup_required: bool}``. The SPA uses this
    on cold-load to decide between rendering the app directly (auth disabled),
    showing the setup page, or showing the login page. Provider listing is
    fetched separately via ``GET /api/auth/providers`` so this endpoint stays
    free of any OAuth dependency.
    """
    setup_required = auth_enabled and AuthStore(queue).count_users() == 0
    return {"auth_enabled": auth_enabled, "setup_required": setup_required}


def handle_setup(queue: Queue, body: dict) -> dict[str, Any]:
    """Create the first admin user. Only callable when zero users exist."""
    store = AuthStore(queue)
    if store.count_users() > 0:
        raise _BadRequest("setup already complete")
    username = _require_field(body, "username")
    password = _require_field(body, "password")
    try:
        user = store.create_user(username, password, role="admin")
    except ValueError as e:
        raise _BadRequest(str(e)) from None
    return {"user": _serialize_user(user)}


def handle_login(queue: Queue, body: dict) -> dict[str, Any]:
    """Verify credentials and create a session.

    Returns ``{user, session}`` on success. The caller (server) reads the
    session token from the returned object and sets it as an HttpOnly cookie.
    On failure raises ``_BadRequest`` to drive a 400 — we intentionally
    return the same generic error for unknown user / bad password to avoid
    revealing which one was wrong.
    """
    store = AuthStore(queue)
    if store.count_users() == 0:
        raise _BadRequest("setup_required")
    username = _require_field(body, "username")
    password = _require_field(body, "password")
    user = store.authenticate(username, password)
    if not user:
        raise _BadRequest("invalid_credentials")
    session = store.create_session(user)
    return {
        "user": _serialize_user(user),
        "session": _serialize_session(session) | {"token": session.token},
    }


def handle_logout(queue: Queue, ctx: RequestContext) -> dict[str, bool]:
    """Invalidate the current session. Idempotent."""
    if not ctx.session:
        return {"ok": True}
    AuthStore(queue).delete_session(ctx.session.token)
    return {"ok": True}


def handle_whoami(queue: Queue, ctx: RequestContext) -> dict[str, Any]:
    """Return the current user, or 401-equivalent if no session."""
    if not ctx.session:
        raise _NotFound("not_authenticated")
    store = AuthStore(queue)
    user = store.get_user(ctx.session.username)
    if not user:
        # Session valid but user deleted — invalidate and treat as logged out.
        store.delete_session(ctx.session.token)
        raise _NotFound("not_authenticated")
    return {
        "user": _serialize_user(user),
        "csrf_token": ctx.session.csrf_token,
        "expires_at": ctx.session.expires_at,
    }


def handle_change_password(queue: Queue, body: dict, ctx: RequestContext) -> dict[str, bool]:
    """Change the current user's password. Requires the old password."""
    if not ctx.session:
        raise _BadRequest("not_authenticated")
    old_password = _require_field(body, "old_password")
    new_password = _require_field(body, "new_password")
    store = AuthStore(queue)
    user = store.authenticate(ctx.session.username, old_password)
    if not user:
        raise _BadRequest("invalid_credentials")
    try:
        store.update_password(user.username, new_password)
    except ValueError as e:
        raise _BadRequest(str(e)) from None
    return {"ok": True}
