"""Unit tests for the concrete OAuth provider implementations.

These tests stub every HTTP boundary so they run without network access.
The end-to-end "real flow" test lives in ``test_oauth_endpoints.py``.
"""

from __future__ import annotations

import json
import time
from typing import Any
from urllib.parse import parse_qs, urlparse

import pytest
from joserfc import jwt as joserfc_jwt
from joserfc.jwk import RSAKey

from flexiq.dashboard.oauth.config import (
    GitHubConfig,
    GoogleConfig,
    OIDCConfig,
)
from flexiq.dashboard.oauth.identity import (
    AllowlistDenied,
    IdentityFetchError,
)
from flexiq.dashboard.oauth.providers import (
    GenericOIDCProvider,
    GitHubProvider,
    GoogleProvider,
    _audience_matches,
    _email_domain,
)

# ── HTTP stub helpers ────────────────────────────────────────────────


class StubResponse:
    """Minimal stand-in for a ``requests.Response`` object."""

    def __init__(self, *, status_code: int = 200, payload: Any = None, text: str = "") -> None:
        self.status_code = status_code
        self._payload = payload
        self.text = text or json.dumps(payload)

    def json(self) -> Any:
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class StubSession:
    """Replaces ``requests.Session`` with a programmable URL → response map."""

    def __init__(self, routes: dict[str, StubResponse]) -> None:
        self._routes = routes
        self.calls: list[tuple[str, dict[str, str]]] = []

    def get(self, url: str, *, headers: dict[str, str] | None = None, **_: Any) -> StubResponse:
        self.calls.append((url, headers or {}))
        if url in self._routes:
            return self._routes[url]
        # Wildcard fallback: match by prefix to support .../members/<login>.
        for prefix, response in self._routes.items():
            if prefix.endswith("*") and url.startswith(prefix[:-1]):
                return response
        return StubResponse(status_code=404, payload={"error": "not found"})


# ── Test fixtures ────────────────────────────────────────────────────


@pytest.fixture
def rsa_key() -> RSAKey:
    """A fresh RSA keypair used to sign + verify test ID tokens."""
    return RSAKey.generate_key(2048, parameters={"kid": "test-kid"}, private=True)


@pytest.fixture
def google_discovery() -> dict[str, str]:
    return {
        "issuer": "https://accounts.google.com",
        "authorization_endpoint": "https://accounts.google.com/o/oauth2/v2/auth",
        "token_endpoint": "https://oauth2.googleapis.com/token",
        "jwks_uri": "https://www.googleapis.com/oauth2/v3/certs",
    }


def _make_google_provider(
    *,
    allowed_domains: tuple[str, ...] = (),
    discovery: dict[str, str],
    jwks_payload: dict,
) -> GoogleProvider:
    routes = {
        "https://accounts.google.com/.well-known/openid-configuration": StubResponse(
            payload=discovery
        ),
        discovery["jwks_uri"]: StubResponse(payload=jwks_payload),
    }
    provider = GoogleProvider(
        GoogleConfig(
            client_id="test-client-id",
            client_secret="test-client-secret",
            allowed_domains=allowed_domains,
        ),
        http=StubSession(routes),
    )
    return provider


def _make_id_token(
    *,
    key: RSAKey,
    issuer: str,
    audience: str,
    subject: str,
    email: str,
    email_verified: bool,
    nonce: str | None,
    name: str | None = "Alice Example",
    extra_claims: dict[str, Any] | None = None,
) -> str:
    claims: dict[str, Any] = {
        "iss": issuer,
        "aud": audience,
        "sub": subject,
        "email": email,
        "email_verified": email_verified,
        "name": name,
        "iat": int(time.time()),
        "exp": int(time.time()) + 600,
    }
    if nonce is not None:
        claims["nonce"] = nonce
    if extra_claims:
        claims.update(extra_claims)
    header = {"alg": "RS256", "kid": key.kid}
    encoded: str = joserfc_jwt.encode(header, claims, key)
    return encoded


# ── Helpers ──────────────────────────────────────────────────────────


def test_email_domain_extracts_lowercase() -> None:
    assert _email_domain("Alice@ACME.com") == "acme.com"
    assert _email_domain(None) is None
    assert _email_domain("not-an-email") is None


def test_audience_matches_string_and_list() -> None:
    assert _audience_matches("cid", "cid")
    assert _audience_matches(["cid", "other"], "cid")
    assert not _audience_matches("other", "cid")
    assert not _audience_matches([], "cid")
    assert not _audience_matches(None, "cid")


# ── Google: authorization URL ────────────────────────────────────────


def test_google_authorization_url_includes_required_params(
    google_discovery: dict[str, str],
) -> None:
    provider = _make_google_provider(
        discovery=google_discovery,
        jwks_payload={"keys": []},
    )
    url = provider.authorization_url(
        state="STATE",
        nonce="NONCE",
        code_challenge="CHALLENGE",
        redirect_uri="https://flexiq.example.com/api/auth/oauth/callback/google",
    )
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)
    assert parsed.scheme == "https"
    assert qs["response_type"] == ["code"]
    assert qs["client_id"] == ["test-client-id"]
    assert qs["scope"] == ["openid email profile"]
    assert qs["state"] == ["STATE"]
    assert qs["nonce"] == ["NONCE"]
    assert qs["code_challenge"] == ["CHALLENGE"]
    assert qs["code_challenge_method"] == ["S256"]
    assert qs["prompt"] == ["select_account"]
    assert "hd" not in qs  # no allowed_domains configured


def test_google_authorization_url_sets_hd_hint_for_single_domain(
    google_discovery: dict[str, str],
) -> None:
    provider = _make_google_provider(
        discovery=google_discovery,
        jwks_payload={"keys": []},
        allowed_domains=("acme.com",),
    )
    url = provider.authorization_url(
        state="s", nonce="n", code_challenge="c", redirect_uri="https://x/y"
    )
    qs = parse_qs(urlparse(url).query)
    assert qs["hd"] == ["acme.com"]


def test_google_authorization_url_omits_hd_for_multi_domain(
    google_discovery: dict[str, str],
) -> None:
    provider = _make_google_provider(
        discovery=google_discovery,
        jwks_payload={"keys": []},
        allowed_domains=("acme.com", "partner.com"),
    )
    url = provider.authorization_url(
        state="s", nonce="n", code_challenge="c", redirect_uri="https://x/y"
    )
    qs = parse_qs(urlparse(url).query)
    assert "hd" not in qs  # ambiguous, do not preselect


# ── Google: exchange_code → identity ─────────────────────────────────


def test_google_exchange_code_returns_identity_for_valid_id_token(
    google_discovery: dict[str, str], rsa_key: RSAKey, monkeypatch: pytest.MonkeyPatch
) -> None:
    id_token = _make_id_token(
        key=rsa_key,
        issuer=google_discovery["issuer"],
        audience="test-client-id",
        subject="118420987654321",
        email="alice@acme.com",
        email_verified=True,
        nonce="EXPECTED_NONCE",
    )
    provider = _make_google_provider(
        discovery=google_discovery,
        jwks_payload={"keys": [rsa_key.as_dict(private=False)]},
    )
    monkeypatch.setattr(
        provider, "_fetch_token", lambda **_: {"id_token": id_token, "access_token": "AT"}
    )
    identity = provider.exchange_code(
        code="abc",
        code_verifier="verifier",
        redirect_uri="https://x",
        expected_nonce="EXPECTED_NONCE",
    )
    assert identity.slot == "google"
    assert identity.subject == "118420987654321"
    assert identity.email == "alice@acme.com"
    assert identity.email_verified is True
    assert identity.name == "Alice Example"


def test_google_exchange_code_rejects_wrong_nonce(
    google_discovery: dict[str, str], rsa_key: RSAKey, monkeypatch: pytest.MonkeyPatch
) -> None:
    id_token = _make_id_token(
        key=rsa_key,
        issuer=google_discovery["issuer"],
        audience="test-client-id",
        subject="111",
        email="x@y.com",
        email_verified=True,
        nonce="WRONG",
    )
    provider = _make_google_provider(
        discovery=google_discovery,
        jwks_payload={"keys": [rsa_key.as_dict(private=False)]},
    )
    monkeypatch.setattr(provider, "_fetch_token", lambda **_: {"id_token": id_token})
    with pytest.raises(IdentityFetchError, match="nonce mismatch"):
        provider.exchange_code(
            code="abc", code_verifier="v", redirect_uri="https://x", expected_nonce="EXPECTED"
        )


def test_google_exchange_code_rejects_wrong_audience(
    google_discovery: dict[str, str], rsa_key: RSAKey, monkeypatch: pytest.MonkeyPatch
) -> None:
    id_token = _make_id_token(
        key=rsa_key,
        issuer=google_discovery["issuer"],
        audience="DIFFERENT-CLIENT",
        subject="111",
        email="x@y.com",
        email_verified=True,
        nonce=None,
    )
    provider = _make_google_provider(
        discovery=google_discovery,
        jwks_payload={"keys": [rsa_key.as_dict(private=False)]},
    )
    monkeypatch.setattr(provider, "_fetch_token", lambda **_: {"id_token": id_token})
    with pytest.raises(IdentityFetchError, match="audience mismatch"):
        provider.exchange_code(
            code="abc", code_verifier="v", redirect_uri="https://x", expected_nonce=None
        )


def test_google_exchange_code_rejects_wrong_issuer(
    google_discovery: dict[str, str], rsa_key: RSAKey, monkeypatch: pytest.MonkeyPatch
) -> None:
    id_token = _make_id_token(
        key=rsa_key,
        issuer="https://evil.com",
        audience="test-client-id",
        subject="111",
        email="x@y.com",
        email_verified=True,
        nonce=None,
    )
    provider = _make_google_provider(
        discovery=google_discovery,
        jwks_payload={"keys": [rsa_key.as_dict(private=False)]},
    )
    monkeypatch.setattr(provider, "_fetch_token", lambda **_: {"id_token": id_token})
    with pytest.raises(IdentityFetchError, match="issuer mismatch"):
        provider.exchange_code(
            code="abc", code_verifier="v", redirect_uri="https://x", expected_nonce=None
        )


def test_google_exchange_code_rejects_missing_id_token(
    google_discovery: dict[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    provider = _make_google_provider(
        discovery=google_discovery,
        jwks_payload={"keys": []},
    )
    monkeypatch.setattr(provider, "_fetch_token", lambda **_: {"access_token": "AT"})
    with pytest.raises(IdentityFetchError, match="no id_token"):
        provider.exchange_code(
            code="abc", code_verifier="v", redirect_uri="https://x", expected_nonce=None
        )


# ── Google: allowlist ─────────────────────────────────────────────────


def test_google_check_allowlist_passes_when_no_restriction(
    google_discovery: dict[str, str],
) -> None:
    provider = _make_google_provider(discovery=google_discovery, jwks_payload={"keys": []})
    from flexiq.dashboard.oauth.identity import ProviderIdentity

    identity = ProviderIdentity(slot="google", subject="x", email="x@y.com", email_verified=True)
    # Should not raise.
    provider.check_allowlist(identity)


def test_google_check_allowlist_rejects_unverified_email(
    google_discovery: dict[str, str],
) -> None:
    from flexiq.dashboard.oauth.identity import ProviderIdentity

    provider = _make_google_provider(
        discovery=google_discovery,
        jwks_payload={"keys": []},
        allowed_domains=("acme.com",),
    )
    with pytest.raises(AllowlistDenied, match="verified email"):
        provider.check_allowlist(
            ProviderIdentity(
                slot="google",
                subject="x",
                email="user@acme.com",
                email_verified=False,
            )
        )


def test_google_check_allowlist_rejects_out_of_domain_email(
    google_discovery: dict[str, str],
) -> None:
    from flexiq.dashboard.oauth.identity import ProviderIdentity

    provider = _make_google_provider(
        discovery=google_discovery,
        jwks_payload={"keys": []},
        allowed_domains=("acme.com",),
    )
    with pytest.raises(AllowlistDenied, match="not in the allowed domains"):
        provider.check_allowlist(
            ProviderIdentity(
                slot="google",
                subject="x",
                email="user@gmail.com",
                email_verified=True,
            )
        )


def test_google_check_allowlist_accepts_listed_domain(
    google_discovery: dict[str, str],
) -> None:
    from flexiq.dashboard.oauth.identity import ProviderIdentity

    provider = _make_google_provider(
        discovery=google_discovery,
        jwks_payload={"keys": []},
        allowed_domains=("acme.com",),
    )
    provider.check_allowlist(
        ProviderIdentity(slot="google", subject="x", email="USER@Acme.COM", email_verified=True)
    )


# ── Generic OIDC ──────────────────────────────────────────────────────


def test_generic_oidc_uses_provided_discovery_url(rsa_key: RSAKey) -> None:
    discovery = {
        "issuer": "https://acme.okta.com",
        "authorization_endpoint": "https://acme.okta.com/oauth2/authorize",
        "token_endpoint": "https://acme.okta.com/oauth2/token",
        "jwks_uri": "https://acme.okta.com/oauth2/jwks",
    }
    routes = {
        "https://acme.okta.com/.well-known/openid-configuration": StubResponse(payload=discovery),
        discovery["jwks_uri"]: StubResponse(payload={"keys": [rsa_key.as_dict(private=False)]}),
    }
    provider = GenericOIDCProvider(
        OIDCConfig(
            slot="okta",
            client_id="cid",
            client_secret="csec",
            discovery_url="https://acme.okta.com/.well-known/openid-configuration",
            label="Acme SSO",
        ),
        http=StubSession(routes),
    )
    url = provider.authorization_url(
        state="s", nonce="n", code_challenge="c", redirect_uri="https://flexiq.x/cb"
    )
    assert url.startswith("https://acme.okta.com/oauth2/authorize?")
    assert provider.slot == "okta"
    assert provider.label == "Acme SSO"
    assert provider.type == "oidc"


# ── GitHub ────────────────────────────────────────────────────────────


def _gh_provider(
    *,
    allowed_orgs: tuple[str, ...] = (),
    routes: dict[str, StubResponse] | None = None,
) -> GitHubProvider:
    return GitHubProvider(
        GitHubConfig(
            client_id="gh-client",
            client_secret="gh-secret",
            allowed_orgs=allowed_orgs,
        ),
        http=StubSession(routes or {}),
    )


def test_github_authorization_url_includes_pkce_and_state() -> None:
    provider = _gh_provider()
    url = provider.authorization_url(
        state="STATE", nonce="UNUSED", code_challenge="CHL", redirect_uri="https://x/cb"
    )
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)
    assert parsed.netloc == "github.com"
    assert qs["client_id"] == ["gh-client"]
    assert qs["state"] == ["STATE"]
    assert qs["code_challenge"] == ["CHL"]
    assert qs["code_challenge_method"] == ["S256"]
    assert "nonce" not in qs  # GitHub does not implement OIDC


def test_github_authorization_url_adds_read_org_when_allowlist_configured() -> None:
    provider = _gh_provider(allowed_orgs=("acme",))
    url = provider.authorization_url(
        state="s", nonce="n", code_challenge="c", redirect_uri="https://x/cb"
    )
    qs = parse_qs(urlparse(url).query)
    assert "read:org" in qs["scope"][0]


def test_github_exchange_code_returns_verified_primary_email(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    routes = {
        "https://api.github.com/user": StubResponse(
            payload={"id": 584213, "login": "alice", "name": "Alice", "avatar_url": "https://x/y"}
        ),
        "https://api.github.com/user/emails": StubResponse(
            payload=[
                {"email": "alt@x.com", "primary": False, "verified": True},
                {"email": "alice@acme.com", "primary": True, "verified": True},
            ]
        ),
    }
    provider = _gh_provider(routes=routes)
    monkeypatch.setattr(provider, "_fetch_token", lambda **_: {"access_token": "AT"})
    identity = provider.exchange_code(
        code="abc", code_verifier="v", redirect_uri="https://x", expected_nonce=None
    )
    assert identity.slot == "github"
    assert identity.subject == "584213"
    assert identity.email == "alice@acme.com"
    assert identity.email_verified is True
    assert identity.name == "Alice"


def test_github_exchange_code_returns_none_email_when_no_verified_primary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    routes = {
        "https://api.github.com/user": StubResponse(
            payload={"id": 1, "login": "u", "name": None, "avatar_url": None}
        ),
        "https://api.github.com/user/emails": StubResponse(
            payload=[
                {"email": "claimed@x.com", "primary": True, "verified": False},
            ]
        ),
    }
    provider = _gh_provider(routes=routes)
    monkeypatch.setattr(provider, "_fetch_token", lambda **_: {"access_token": "AT"})
    identity = provider.exchange_code(
        code="abc", code_verifier="v", redirect_uri="https://x", expected_nonce=None
    )
    assert identity.email is None
    assert identity.email_verified is False


def test_github_exchange_code_enforces_org_membership(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    routes = {
        "https://api.github.com/user": StubResponse(
            payload={"id": 1, "login": "alice", "name": "A", "avatar_url": "x"}
        ),
        "https://api.github.com/user/emails": StubResponse(
            payload=[{"email": "a@x.com", "primary": True, "verified": True}]
        ),
        "https://api.github.com/orgs/acme/members/alice": StubResponse(
            status_code=204, payload=None, text=""
        ),
    }
    provider = _gh_provider(allowed_orgs=("acme",), routes=routes)
    monkeypatch.setattr(provider, "_fetch_token", lambda **_: {"access_token": "AT"})
    identity = provider.exchange_code(
        code="abc", code_verifier="v", redirect_uri="https://x", expected_nonce=None
    )
    assert identity.email == "a@x.com"


def test_github_exchange_code_rejects_non_member(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    routes = {
        "https://api.github.com/user": StubResponse(
            payload={"id": 1, "login": "eve", "name": "E", "avatar_url": "x"}
        ),
        "https://api.github.com/user/emails": StubResponse(
            payload=[{"email": "e@x.com", "primary": True, "verified": True}]
        ),
        "https://api.github.com/orgs/acme/members/eve": StubResponse(
            status_code=404, payload={"message": "Not Found"}
        ),
    }
    provider = _gh_provider(allowed_orgs=("acme",), routes=routes)
    monkeypatch.setattr(provider, "_fetch_token", lambda **_: {"access_token": "AT"})
    with pytest.raises(AllowlistDenied, match="not a member"):
        provider.exchange_code(
            code="abc", code_verifier="v", redirect_uri="https://x", expected_nonce=None
        )
