"""Concrete provider implementations: Google, GitHub, generic OIDC.

Every provider satisfies :class:`OAuthProvider`. The split between
``exchange_code`` (network IO + claim normalisation) and
``check_allowlist`` (pure-data permission check) is deliberate so tests
can drive either path in isolation.

Providers depend on :class:`HttpClient`, a structural Protocol over
the small subset of ``requests.Session`` they actually use (one ``get``
method). Production code passes a ``requests.Session``; tests pass an
in-memory stub. The Protocol keeps the provider layer framework-free
and test-friendly without runtime ``cast`` calls at either boundary.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any, Protocol
from urllib.parse import urlencode

import requests
from authlib.integrations.requests_client import OAuth2Session
from joserfc import jwt
from joserfc.errors import JoseError
from joserfc.jwk import KeySet

from flexiq.dashboard.oauth.identity import (
    AllowlistDenied,
    IdentityFetchError,
    ProviderIdentity,
)

if TYPE_CHECKING:
    from flexiq.dashboard.oauth.config import (
        GitHubConfig,
        GoogleConfig,
        OIDCConfig,
    )


class HttpResponse(Protocol):
    """Minimal response shape a provider consumes from its HTTP client."""

    status_code: int
    text: str

    def json(self) -> Any: ...

    def raise_for_status(self) -> None: ...


class HttpClient(Protocol):
    """Minimal HTTP client shape — ``requests.Session`` satisfies this."""

    def get(
        self,
        url: str,
        *,
        headers: dict[str, str] | None = ...,
        timeout: float = ...,
    ) -> HttpResponse: ...


GOOGLE_DISCOVERY_URL = "https://accounts.google.com/.well-known/openid-configuration"
GITHUB_AUTH_URL = "https://github.com/login/oauth/authorize"
GITHUB_TOKEN_URL = "https://github.com/login/oauth/access_token"
GITHUB_API_BASE = "https://api.github.com"

_HTTP_TIMEOUT = 10.0


def _email_domain(email: str | None) -> str | None:
    if not email or "@" not in email:
        return None
    return email.rsplit("@", 1)[-1].lower()


def _audience_matches(aud: Any, client_id: str) -> bool:
    if isinstance(aud, str):
        return aud == client_id
    if isinstance(aud, list):
        return client_id in aud
    return False


# ── OIDC provider (shared logic for Google + generic OIDC) ─────────────


class _OIDCProviderBase:
    """Shared OIDC machinery: discovery, JWKS caching, ID-token decoding."""

    slot: str
    label: str
    type: str
    client_id: str
    client_secret: str
    discovery_url: str
    scope: str = "openid email profile"

    def __init__(self, *, http: HttpClient | None = None) -> None:
        self._http = http or requests.Session()
        self._discovery: dict[str, Any] | None = None
        self._jwks: dict[str, Any] | None = None

    # Sub-classes override / extend `_extra_auth_params` to add hints.
    def _extra_auth_params(self) -> dict[str, str]:
        return {}

    def _get_discovery(self) -> dict[str, Any]:
        if self._discovery is None:
            resp = self._http.get(self.discovery_url, timeout=_HTTP_TIMEOUT)
            resp.raise_for_status()
            self._discovery = resp.json()
        return self._discovery

    def _get_jwks(self) -> dict[str, Any]:
        if self._jwks is None:
            resp = self._http.get(self._get_discovery()["jwks_uri"], timeout=_HTTP_TIMEOUT)
            resp.raise_for_status()
            self._jwks = resp.json()
        return self._jwks

    def authorization_url(
        self,
        *,
        state: str,
        nonce: str,
        code_challenge: str,
        redirect_uri: str,
    ) -> str:
        params: dict[str, str] = {
            "response_type": "code",
            "client_id": self.client_id,
            "redirect_uri": redirect_uri,
            "scope": self.scope,
            "state": state,
            "nonce": nonce,
            "code_challenge": code_challenge,
            "code_challenge_method": "S256",
        }
        params.update(self._extra_auth_params())
        return f"{self._get_discovery()['authorization_endpoint']}?{urlencode(params)}"

    def _fetch_token(
        self,
        *,
        code: str,
        code_verifier: str,
        redirect_uri: str,
    ) -> dict[str, Any]:
        """POST the auth code to the token endpoint. Returns the raw token dict.

        Isolated so tests can stub it without involving Authlib's HTTP stack.
        """
        client = OAuth2Session(
            client_id=self.client_id,
            client_secret=self.client_secret,
        )
        try:
            token = client.fetch_token(
                self._get_discovery()["token_endpoint"],
                code=code,
                code_verifier=code_verifier,
                redirect_uri=redirect_uri,
                grant_type="authorization_code",
            )
        except Exception as e:
            raise IdentityFetchError(f"token exchange failed: {e}") from e
        return dict(token)

    def exchange_code(
        self,
        *,
        code: str,
        code_verifier: str,
        redirect_uri: str,
        expected_nonce: str | None,
    ) -> ProviderIdentity:
        token = self._fetch_token(
            code=code, code_verifier=code_verifier, redirect_uri=redirect_uri
        )
        id_token = token.get("id_token")
        if not id_token:
            raise IdentityFetchError("no id_token in token response")

        try:
            # joserfc's KeySet.import_key_set is typed as accepting its own
            # KeySetSerialization TypedDict, but operationally it accepts any
            # standard JWKS dict. Mypy 2.x widened the stub; 1.x still
            # complains. The dual code suppresses both directions.
            key_set = KeySet.import_key_set(self._get_jwks())  # type: ignore[arg-type, unused-ignore]
            decoded = jwt.decode(id_token, key_set)
            claims = decoded.claims
        except JoseError as e:
            raise IdentityFetchError(f"id_token validation failed: {e}") from e

        issuer = self._get_discovery().get("issuer")
        if issuer and claims.get("iss") != issuer:
            raise IdentityFetchError(
                f"id_token issuer mismatch: expected {issuer!r}, got {claims.get('iss')!r}"
            )
        if not _audience_matches(claims.get("aud"), self.client_id):
            raise IdentityFetchError(f"id_token audience mismatch: {claims.get('aud')!r}")
        if expected_nonce is not None and claims.get("nonce") != expected_nonce:
            raise IdentityFetchError("id_token nonce mismatch")

        exp = claims.get("exp")
        if isinstance(exp, (int, float)) and exp < int(time.time()) - 60:
            # 60s clock skew tolerance.
            raise IdentityFetchError("id_token expired")

        sub = claims.get("sub")
        if not sub:
            raise IdentityFetchError("id_token missing 'sub' claim")

        return ProviderIdentity(
            slot=self.slot,
            subject=str(sub),
            email=claims.get("email"),
            email_verified=bool(claims.get("email_verified")),
            name=claims.get("name"),
            picture=claims.get("picture"),
        )


class GoogleProvider(_OIDCProviderBase):
    slot = "google"
    type = "google"
    discovery_url = GOOGLE_DISCOVERY_URL

    def __init__(self, config: GoogleConfig, *, http: HttpClient | None = None) -> None:
        super().__init__(http=http)
        self.config = config
        self.label = config.label
        self.client_id = config.client_id
        self.client_secret = config.client_secret

    def _extra_auth_params(self) -> dict[str, str]:
        params = {"prompt": "select_account"}
        # When exactly one domain is allowlisted, pass it as ``hd`` so Google
        # pre-selects the right account. This is a UX hint only — the real
        # enforcement happens in ``check_allowlist``.
        if len(self.config.allowed_domains) == 1:
            params["hd"] = self.config.allowed_domains[0]
        return params

    def check_allowlist(self, identity: ProviderIdentity) -> None:
        if not self.config.allowed_domains:
            return
        if not identity.email or not identity.email_verified:
            raise AllowlistDenied("verified email required for domain check")
        domain = _email_domain(identity.email)
        allowed = {d.lower() for d in self.config.allowed_domains}
        if domain not in allowed:
            raise AllowlistDenied(f"email domain {domain!r} is not in the allowed domains list")


class GenericOIDCProvider(_OIDCProviderBase):
    type = "oidc"

    def __init__(self, config: OIDCConfig, *, http: HttpClient | None = None) -> None:
        super().__init__(http=http)
        self.config = config
        self.slot = config.slot
        self.label = config.label or config.slot.title()
        self.client_id = config.client_id
        self.client_secret = config.client_secret
        self.discovery_url = config.discovery_url

    def check_allowlist(self, identity: ProviderIdentity) -> None:
        if not self.config.allowed_domains:
            return
        if not identity.email or not identity.email_verified:
            raise AllowlistDenied("verified email required for domain check")
        domain = _email_domain(identity.email)
        allowed = {d.lower() for d in self.config.allowed_domains}
        if domain not in allowed:
            raise AllowlistDenied(f"email domain {domain!r} is not in the allowed domains list")


# ── GitHub (OAuth2-only, no OIDC) ──────────────────────────────────────


class GitHubProvider:
    slot = "github"
    type = "github"
    scope = "read:user user:email"

    def __init__(self, config: GitHubConfig, *, http: HttpClient | None = None) -> None:
        self.config = config
        self.label = config.label
        self._http = http or requests.Session()

    def authorization_url(
        self,
        *,
        state: str,
        nonce: str,
        code_challenge: str,
        redirect_uri: str,
    ) -> str:
        # GitHub does not implement OIDC: ``nonce`` is unused. PKCE is honoured
        # — GitHub added support for it in 2023.
        params = {
            "client_id": self.config.client_id,
            "redirect_uri": redirect_uri,
            "scope": self.scope,
            "state": state,
            "code_challenge": code_challenge,
            "code_challenge_method": "S256",
            "allow_signup": "false",
        }
        # Request read:org scope when allowlist is configured, so the
        # membership endpoint returns reliable results.
        if self.config.allowed_orgs:
            params["scope"] = self.scope + " read:org"
        return f"{GITHUB_AUTH_URL}?{urlencode(params)}"

    def _fetch_token(
        self,
        *,
        code: str,
        code_verifier: str,
        redirect_uri: str,
    ) -> dict[str, Any]:
        client = OAuth2Session(
            client_id=self.config.client_id,
            client_secret=self.config.client_secret,
        )
        try:
            token = client.fetch_token(
                GITHUB_TOKEN_URL,
                code=code,
                code_verifier=code_verifier,
                redirect_uri=redirect_uri,
                grant_type="authorization_code",
                # GitHub returns form-encoded by default; ask for JSON.
                headers={"Accept": "application/json"},
            )
        except Exception as e:
            raise IdentityFetchError(f"token exchange failed: {e}") from e
        return dict(token)

    def _api_get(self, path: str, access_token: str) -> Any:
        resp = self._http.get(
            f"{GITHUB_API_BASE}{path}",
            headers={
                "Authorization": f"Bearer {access_token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            },
            timeout=_HTTP_TIMEOUT,
        )
        if resp.status_code >= 400 and resp.status_code != 404:
            raise IdentityFetchError(
                f"GitHub API {path} returned {resp.status_code}: {resp.text[:200]}"
            )
        return resp

    def exchange_code(
        self,
        *,
        code: str,
        code_verifier: str,
        redirect_uri: str,
        expected_nonce: str | None,
    ) -> ProviderIdentity:
        token = self._fetch_token(
            code=code, code_verifier=code_verifier, redirect_uri=redirect_uri
        )
        access_token = token.get("access_token")
        if not access_token:
            raise IdentityFetchError("no access_token in token response")

        user_resp = self._api_get("/user", access_token)
        if user_resp.status_code != 200:
            raise IdentityFetchError(f"GET /user failed: {user_resp.status_code}")
        user = user_resp.json()
        gh_id = user.get("id")
        login = user.get("login")
        if gh_id is None or not login:
            raise IdentityFetchError("GitHub /user response missing 'id' or 'login'")

        primary_email, verified = self._primary_email(access_token)

        # Org membership requires the access token, so we enforce it here
        # rather than in ``check_allowlist`` (which is a no-op for GitHub).
        # Any denial raises :class:`AllowlistDenied` straight through.
        self._verify_org_membership(access_token, str(login))

        return ProviderIdentity(
            slot=self.slot,
            subject=str(gh_id),
            email=primary_email,
            email_verified=verified,
            name=user.get("name") or user.get("login"),
            picture=user.get("avatar_url"),
        )

    def _primary_email(self, access_token: str) -> tuple[str | None, bool]:
        """Return ``(primary_verified_email_or_None, verified_flag)``.

        Falls back to ``None`` if no verified primary exists. We never trust
        an unverified email for any access decision.
        """
        resp = self._api_get("/user/emails", access_token)
        if resp.status_code != 200:
            return None, False
        for entry in resp.json():
            if entry.get("primary") and entry.get("verified"):
                return entry.get("email"), True
        return None, False

    def _verify_org_membership(self, access_token: str, login: str) -> None:
        if not self.config.allowed_orgs:
            return
        for org in self.config.allowed_orgs:
            resp = self._http.get(
                f"{GITHUB_API_BASE}/orgs/{org}/members/{login}",
                headers={
                    "Authorization": f"Bearer {access_token}",
                    "Accept": "application/vnd.github+json",
                    "X-GitHub-Api-Version": "2022-11-28",
                },
                timeout=_HTTP_TIMEOUT,
            )
            if resp.status_code == 204:
                return
            if resp.status_code not in (302, 404):
                raise IdentityFetchError(f"GitHub org membership check failed: {resp.status_code}")
        raise AllowlistDenied(
            f"user is not a member of any allowed GitHub org "
            f"({', '.join(self.config.allowed_orgs)})"
        )

    def check_allowlist(self, identity: ProviderIdentity) -> None:
        """No-op — GitHub's org check happens inside :meth:`exchange_code`.

        Required by the :class:`OAuthProvider` protocol for interface symmetry.
        """
        return
