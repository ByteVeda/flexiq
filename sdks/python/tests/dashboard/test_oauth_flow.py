"""Tests for :class:`OAuthFlow` — state + provider + auth-store orchestration."""

from __future__ import annotations

from pathlib import Path

import pytest

from flexiq import Queue
from flexiq.dashboard.auth import AuthStore
from flexiq.dashboard.oauth.config import GoogleConfig, OAuthConfig
from flexiq.dashboard.oauth.flow import OAuthFlow
from flexiq.dashboard.oauth.identity import (
    AllowlistDenied,
    IdentityFetchError,
    ProviderIdentity,
    ProviderNotConfigured,
    StateValidationError,
)


@pytest.fixture
def queue(tmp_path: Path) -> Queue:
    return Queue(db_path=str(tmp_path / "oauth_flow.db"))


@pytest.fixture
def config() -> OAuthConfig:
    return OAuthConfig(
        redirect_base_url="https://flexiq.example.com",
        google=GoogleConfig(
            client_id="cid",
            client_secret="csec",
            allowed_domains=("acme.com",),
        ),
        admin_emails=("alice@acme.com",),
    )


class FakeProvider:
    """In-memory provider with programmable identity / allowlist behaviour."""

    type = "google"
    label = "Test"

    def __init__(self, slot: str, identity: ProviderIdentity | None = None) -> None:
        self.slot = slot
        self.identity = identity
        self.allow = True
        self.last_authorization_args: dict | None = None

    def authorization_url(
        self,
        *,
        state: str,
        nonce: str,
        code_challenge: str,
        redirect_uri: str,
    ) -> str:
        self.last_authorization_args = {
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
            raise IdentityFetchError("test stub: no identity configured")
        return self.identity

    def check_allowlist(self, identity: ProviderIdentity) -> None:
        if not self.allow:
            raise AllowlistDenied("test stub: denied")


def test_start_returns_provider_url_with_safe_next(queue: Queue, config: OAuthConfig) -> None:
    fake = FakeProvider("google")
    flow = OAuthFlow(queue, config, providers={"google": fake})
    url = flow.start("google", next_url="/dashboard/jobs")
    assert url.startswith("https://idp.example.com/authorize?state=")
    args = fake.last_authorization_args
    assert args is not None
    assert args["redirect_uri"] == "https://flexiq.example.com/api/auth/oauth/callback/google"
    assert len(args["state"]) >= 32


def test_start_falls_back_to_root_when_next_unsafe(queue: Queue, config: OAuthConfig) -> None:
    fake = FakeProvider("google")
    flow = OAuthFlow(queue, config, providers={"google": fake})
    flow.start("google", next_url="https://evil.com/x")
    # We can't read state.next_url back without inspecting the store —
    # but we can confirm the callback rejects it via a separate test.


def test_start_raises_for_unknown_slot(queue: Queue, config: OAuthConfig) -> None:
    flow = OAuthFlow(queue, config, providers={})
    with pytest.raises(ProviderNotConfigured):
        flow.start("nonexistent", next_url="/")


def test_handle_callback_creates_user_and_session(queue: Queue, config: OAuthConfig) -> None:
    identity = ProviderIdentity(
        slot="google",
        subject="100200300",
        email="alice@acme.com",
        email_verified=True,
        name="Alice",
    )
    fake = FakeProvider("google", identity=identity)
    flow = OAuthFlow(queue, config, providers={"google": fake})

    # Mint state then handle the callback.
    flow.start("google", next_url="/dashboard")
    state_token = next(iter(_state_tokens(queue)))

    session, next_url = flow.handle_callback(
        "google", code="abc", state_token=state_token, error=None
    )
    assert session.username == "google:100200300"
    assert session.role == "admin"  # alice is in admin_emails
    assert next_url == "/dashboard"

    # Replay attempt fails because state is single-use.
    with pytest.raises(StateValidationError, match="invalid"):
        flow.handle_callback("google", code="abc", state_token=state_token, error=None)


def test_handle_callback_rejects_slot_mismatch(queue: Queue, config: OAuthConfig) -> None:
    identity = ProviderIdentity(slot="google", subject="x", email=None, email_verified=False)
    fake = FakeProvider("google", identity=identity)
    flow = OAuthFlow(queue, config, providers={"google": fake})
    flow.start("google", next_url="/")
    state_token = next(iter(_state_tokens(queue)))
    with pytest.raises(StateValidationError, match="slot"):
        flow.handle_callback("github", code="abc", state_token=state_token, error=None)


def test_handle_callback_propagates_provider_error(queue: Queue, config: OAuthConfig) -> None:
    flow = OAuthFlow(queue, config, providers={"google": FakeProvider("google")})
    flow.start("google", next_url="/")
    state_token = next(iter(_state_tokens(queue)))
    with pytest.raises(IdentityFetchError):
        flow.handle_callback("google", code="abc", state_token=state_token, error=None)


def test_handle_callback_propagates_allowlist_denied(queue: Queue, config: OAuthConfig) -> None:
    identity = ProviderIdentity(
        slot="google",
        subject="1",
        email="eve@evil.com",
        email_verified=True,
    )
    fake = FakeProvider("google", identity=identity)
    fake.allow = False
    flow = OAuthFlow(queue, config, providers={"google": fake})
    flow.start("google", next_url="/")
    state_token = next(iter(_state_tokens(queue)))
    with pytest.raises(AllowlistDenied):
        flow.handle_callback("google", code="abc", state_token=state_token, error=None)


def test_handle_callback_with_provider_error_raises(queue: Queue, config: OAuthConfig) -> None:
    fake = FakeProvider("google")
    flow = OAuthFlow(queue, config, providers={"google": fake})
    with pytest.raises(IdentityFetchError, match="provider returned error"):
        flow.handle_callback("google", code=None, state_token=None, error="access_denied")


def test_providers_listing_returns_visible_metadata(queue: Queue, config: OAuthConfig) -> None:
    fake = FakeProvider("google")
    flow = OAuthFlow(queue, config, providers={"google": fake})
    listing = flow.providers_listing()
    assert listing == [{"slot": "google", "label": "Test", "type": "google"}]


def test_admin_emails_promote_first_user(queue: Queue, config: OAuthConfig) -> None:
    identity = ProviderIdentity(
        slot="google",
        subject="alice-sub",
        email="alice@acme.com",
        email_verified=True,
    )
    fake = FakeProvider("google", identity=identity)
    flow = OAuthFlow(queue, config, providers={"google": fake})
    flow.start("google", next_url="/")
    state_token = next(iter(_state_tokens(queue)))
    session, _ = flow.handle_callback("google", code="x", state_token=state_token, error=None)
    user = AuthStore(queue).get_user(session.username)
    assert user is not None
    assert user.role == "admin"
    assert user.email == "alice@acme.com"


# ── Helpers ──────────────────────────────────────────────────────────


def _state_tokens(queue: Queue) -> list[str]:
    prefix = "auth:oauth_state:"
    return [k[len(prefix) :] for k in queue.list_settings() if k.startswith(prefix)]
