"""HTTP-level integration tests for the OAuth endpoints.

Spins up a real :class:`ThreadingHTTPServer` with a stubbed
:class:`OAuthFlow` so we can drive the full request → 302-redirect →
cookies path without making real provider calls.
"""

from __future__ import annotations

import contextlib
import json
import threading
import urllib.error
import urllib.request
from collections.abc import Callable, Generator
from http.server import ThreadingHTTPServer
from pathlib import Path
from typing import Any

import pytest

from taskito import Queue
from taskito.dashboard import _make_handler
from taskito.dashboard.auth import AuthStore
from taskito.dashboard.oauth.config import (
    GitHubConfig,
    GoogleConfig,
    OAuthConfig,
    OIDCConfig,
)
from taskito.dashboard.oauth.flow import OAuthFlow
from taskito.dashboard.oauth.identity import (
    AllowlistDenied,
    IdentityFetchError,
    ProviderIdentity,
)


@pytest.fixture
def queue(tmp_path: Path) -> Queue:
    return Queue(db_path=str(tmp_path / "oauth_endpoints.db"))


class _FakeProvider:
    """Programmable provider used by the integration tests."""

    def __init__(self, slot: str, *, label: str = "Test", ptype: str = "google") -> None:
        self.slot = slot
        self.label = label
        self.type = ptype
        self.identity: ProviderIdentity | None = None
        self.allow = True
        self.start_called_with: dict[str, str] | None = None

    def authorization_url(
        self,
        *,
        state: str,
        nonce: str,
        code_challenge: str,
        redirect_uri: str,
    ) -> str:
        self.start_called_with = {
            "state": state,
            "nonce": nonce,
            "code_challenge": code_challenge,
            "redirect_uri": redirect_uri,
        }
        return f"https://idp.example.com/authorize?state={state}"

    def exchange_code(
        self,
        *,
        code: str,
        code_verifier: str,
        redirect_uri: str,
        expected_nonce: str | None,
    ) -> ProviderIdentity:
        if self.identity is None:
            raise IdentityFetchError("no identity configured")
        return self.identity

    def check_allowlist(self, identity: ProviderIdentity) -> None:
        if not self.allow:
            raise AllowlistDenied("denied")


@pytest.fixture
def google_provider() -> _FakeProvider:
    return _FakeProvider("google", label="Google", ptype="google")


@pytest.fixture
def okta_provider() -> _FakeProvider:
    return _FakeProvider("okta", label="Acme SSO", ptype="oidc")


def _make_flow(
    queue: Queue,
    providers: dict[str, _FakeProvider],
    *,
    password_enabled: bool = True,
    admin_emails: tuple[str, ...] = (),
) -> OAuthFlow:
    google_cfg = GoogleConfig(client_id="gid", client_secret="gsec")
    github_cfg = GitHubConfig(client_id="hid", client_secret="hsec")
    config = OAuthConfig(
        redirect_base_url="http://127.0.0.1",
        google=google_cfg if "google" in providers else None,
        github=github_cfg if "github" in providers else None,
        oidc=tuple(
            OIDCConfig(
                slot=slot,
                client_id="x",
                client_secret="y",
                discovery_url=f"https://idp/{slot}/.well-known/openid-configuration",
            )
            for slot in providers
            if slot not in ("google", "github")
        ),
        password_auth_enabled=password_enabled,
        admin_emails=admin_emails,
    )
    return OAuthFlow(queue, config, providers=providers)  # type: ignore[arg-type]


@pytest.fixture
def server_factory(
    queue: Queue,
) -> Generator[Callable[[OAuthFlow | None], str]]:
    """Spawns dashboard servers with the requested OAuthFlow."""
    handles: list[ThreadingHTTPServer] = []

    def _factory(flow: OAuthFlow | None) -> str:
        handler = _make_handler(queue, oauth_flow=flow, auth_enabled=True)
        server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        handles.append(server)
        port = server.server_address[1]
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        return f"http://127.0.0.1:{port}"

    yield _factory

    for server in handles:
        server.shutdown()


def _get_no_redirect(
    url: str, *, cookies: dict[str, str] | None = None
) -> tuple[int, Any, dict[str, list[str]]]:
    """GET without following redirects, returning (status, body, headers)."""

    class _NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, *_a: Any, **_k: Any) -> None:
            return None

    opener = urllib.request.build_opener(_NoRedirect())
    req = urllib.request.Request(url, method="GET")
    if cookies:
        req.add_header("Cookie", "; ".join(f"{k}={v}" for k, v in cookies.items()))
    try:
        resp = opener.open(req)
        body: Any = None
        try:
            raw = resp.read()
            body = json.loads(raw) if raw else None
        except (ValueError, json.JSONDecodeError):
            body = None
        headers = {k: resp.headers.get_all(k) or [] for k in set(resp.headers.keys())}
        return resp.status, body, headers
    except urllib.error.HTTPError as e:
        body = None
        with contextlib.suppress(ValueError, json.JSONDecodeError):
            body = json.loads(e.read() or b"{}")
        headers = {k: e.headers.get_all(k) or [] for k in set(e.headers.keys())}
        return e.code, body, headers


def _parse_set_cookies(raw: list[str]) -> dict[str, str]:
    out: dict[str, str] = {}
    for line in raw:
        nv = line.split(";", 1)[0]
        if "=" in nv:
            name, value = nv.split("=", 1)
            out[name.strip()] = value.strip()
    return out


# ── /api/auth/providers ──────────────────────────────────────────────


def test_providers_endpoint_returns_empty_list_when_no_flow(
    server_factory: Any,
) -> None:
    base = server_factory(None)
    status, body, _ = _get_no_redirect(f"{base}/api/auth/providers")
    assert status == 200
    assert body == {"password_enabled": True, "providers": []}


def test_providers_endpoint_lists_configured_providers(
    server_factory: Any,
    queue: Queue,
    google_provider: _FakeProvider,
    okta_provider: _FakeProvider,
) -> None:
    flow = _make_flow(queue, {"google": google_provider, "okta": okta_provider})
    base = server_factory(flow)
    status, body, _ = _get_no_redirect(f"{base}/api/auth/providers")
    assert status == 200
    assert body == {
        "password_enabled": True,
        "providers": [
            {"slot": "google", "label": "Google", "type": "google"},
            {"slot": "okta", "label": "Acme SSO", "type": "oidc"},
        ],
    }


def test_providers_endpoint_reflects_password_disabled(
    server_factory: Any, queue: Queue, google_provider: _FakeProvider
) -> None:
    flow = _make_flow(queue, {"google": google_provider}, password_enabled=False)
    base = server_factory(flow)
    _, body, _ = _get_no_redirect(f"{base}/api/auth/providers")
    assert body["password_enabled"] is False


# ── /api/auth/oauth/start/{slot} ─────────────────────────────────────


def test_start_returns_302_to_provider(
    server_factory: Any, queue: Queue, google_provider: _FakeProvider
) -> None:
    flow = _make_flow(queue, {"google": google_provider})
    base = server_factory(flow)
    status, _, headers = _get_no_redirect(f"{base}/api/auth/oauth/start/google")
    assert status == 302
    locations = headers.get("Location") or []
    assert len(locations) == 1
    assert locations[0].startswith("https://idp.example.com/authorize?state=")
    assert google_provider.start_called_with is not None
    assert google_provider.start_called_with["redirect_uri"].endswith(
        "/api/auth/oauth/callback/google"
    )


def test_start_returns_404_for_unknown_slot(
    server_factory: Any, queue: Queue, google_provider: _FakeProvider
) -> None:
    flow = _make_flow(queue, {"google": google_provider})
    base = server_factory(flow)
    status, body, _ = _get_no_redirect(f"{base}/api/auth/oauth/start/azure")
    assert status == 404
    assert body is not None and "azure" in body.get("error", "")


def test_start_returns_404_when_oauth_not_configured(
    server_factory: Any,
) -> None:
    base = server_factory(None)
    status, body, _ = _get_no_redirect(f"{base}/api/auth/oauth/start/google")
    assert status == 404
    assert body is not None
    assert body.get("error") == "oauth_not_configured"


# ── /api/auth/oauth/callback/{slot} ──────────────────────────────────


def test_callback_creates_session_and_sets_cookies(
    server_factory: Any, queue: Queue, google_provider: _FakeProvider
) -> None:
    google_provider.identity = ProviderIdentity(
        slot="google",
        subject="118420987654321",
        email="alice@acme.com",
        email_verified=True,
        name="Alice",
    )
    flow = _make_flow(queue, {"google": google_provider})
    base = server_factory(flow)

    # First /start to mint state.
    start_status, _, headers = _get_no_redirect(
        f"{base}/api/auth/oauth/start/google?next=/dashboard"
    )
    assert start_status == 302
    location = headers["Location"][0]
    state = location.split("state=")[-1]

    cb_status, _, cb_headers = _get_no_redirect(
        f"{base}/api/auth/oauth/callback/google?code=abc&state={state}"
    )
    assert cb_status == 302
    # Redirected to the safe ``next`` URL.
    assert cb_headers["Location"] == ["/dashboard"]

    cookies = _parse_set_cookies(cb_headers.get("Set-Cookie", []))
    assert "flexiq_session" in cookies
    assert "taskito_csrf" in cookies
    assert cookies["flexiq_session"]

    # A user was created in the AuthStore with the OAuth username scheme.
    user = AuthStore(queue).get_user("google:118420987654321")
    assert user is not None
    assert user.email == "alice@acme.com"
    assert user.is_oauth


def test_callback_rejects_unsafe_next_via_fallback_root(
    server_factory: Any, queue: Queue, google_provider: _FakeProvider
) -> None:
    google_provider.identity = ProviderIdentity(
        slot="google", subject="2", email="bob@acme.com", email_verified=True
    )
    flow = _make_flow(queue, {"google": google_provider})
    base = server_factory(flow)
    _, _, headers = _get_no_redirect(
        f"{base}/api/auth/oauth/start/google?next=https://evil.com/take"
    )
    state = headers["Location"][0].split("state=")[-1]
    _, _, cb_headers = _get_no_redirect(
        f"{base}/api/auth/oauth/callback/google?code=abc&state={state}"
    )
    # Unsafe next was scrubbed to "/" before being persisted with the state.
    assert cb_headers["Location"] == ["/"]


def test_callback_replayed_state_is_rejected(
    server_factory: Any, queue: Queue, google_provider: _FakeProvider
) -> None:
    google_provider.identity = ProviderIdentity(
        slot="google", subject="3", email="c@acme.com", email_verified=True
    )
    flow = _make_flow(queue, {"google": google_provider})
    base = server_factory(flow)
    _, _, headers = _get_no_redirect(f"{base}/api/auth/oauth/start/google")
    state = headers["Location"][0].split("state=")[-1]
    # First callback succeeds.
    first_status, _, _ = _get_no_redirect(
        f"{base}/api/auth/oauth/callback/google?code=abc&state={state}"
    )
    assert first_status == 302
    # Replay redirects to login with error.
    replay_status, _, replay_headers = _get_no_redirect(
        f"{base}/api/auth/oauth/callback/google?code=abc&state={state}"
    )
    assert replay_status == 302
    assert "oauth_state_invalid" in replay_headers["Location"][0]


def test_callback_with_provider_error_redirects_to_login(
    server_factory: Any, queue: Queue, google_provider: _FakeProvider
) -> None:
    flow = _make_flow(queue, {"google": google_provider})
    base = server_factory(flow)
    status, _, headers = _get_no_redirect(
        f"{base}/api/auth/oauth/callback/google?error=access_denied"
    )
    assert status == 302
    assert "oauth_failed" in headers["Location"][0]


def test_callback_blocked_by_allowlist(
    server_factory: Any, queue: Queue, google_provider: _FakeProvider
) -> None:
    google_provider.identity = ProviderIdentity(
        slot="google", subject="4", email="eve@evil.com", email_verified=True
    )
    google_provider.allow = False
    flow = _make_flow(queue, {"google": google_provider})
    base = server_factory(flow)
    _, _, start_headers = _get_no_redirect(f"{base}/api/auth/oauth/start/google")
    state = start_headers["Location"][0].split("state=")[-1]
    status, _, headers = _get_no_redirect(
        f"{base}/api/auth/oauth/callback/google?code=abc&state={state}"
    )
    assert status == 302
    assert "oauth_denied" in headers["Location"][0]


def test_oauth_paths_bypass_setup_required_gate(
    server_factory: Any, queue: Queue, google_provider: _FakeProvider
) -> None:
    """Even before the first user exists, the OAuth flow paths must answer.

    Otherwise a fresh deployment using OAuth-only mode could never bootstrap.
    """
    flow = _make_flow(queue, {"google": google_provider})
    base = server_factory(flow)
    assert AuthStore(queue).count_users() == 0
    status, _, _ = _get_no_redirect(f"{base}/api/auth/providers")
    assert status == 200
    status, _, _ = _get_no_redirect(f"{base}/api/auth/oauth/start/google")
    assert status == 302
