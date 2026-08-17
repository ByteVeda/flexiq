"""Operator-facing OAuth configuration and env-var parsing.

All settings come from environment variables (or an equivalent
:class:`OAuthConfig` instance passed programmatically — used by tests).
Secrets are never stored in the dashboard settings DB.

See ``docs/content/docs/dashboard/oauth.mdx`` for the full env-var
reference.
"""

from __future__ import annotations

import os
import re
import urllib.parse
from collections.abc import Mapping
from dataclasses import dataclass, field


class OAuthConfigError(ValueError):
    """Raised when env-var configuration is invalid."""


SLOT_RE = re.compile(r"^[a-z][a-z0-9_-]{0,31}$")
RESERVED_SLOTS = frozenset({"google", "github"})

# Hostnames where http:// is accepted for ``redirect_base_url`` (dev only).
_LOCAL_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})


def _split_csv(raw: str | None) -> tuple[str, ...]:
    if not raw:
        return ()
    return tuple(part.strip() for part in raw.split(",") if part.strip())


@dataclass(frozen=True)
class GoogleConfig:
    client_id: str
    client_secret: str
    allowed_domains: tuple[str, ...] = ()

    slot: str = "google"
    label: str = "Google"
    type: str = "google"


@dataclass(frozen=True)
class GitHubConfig:
    client_id: str
    client_secret: str
    allowed_orgs: tuple[str, ...] = ()

    slot: str = "github"
    label: str = "GitHub"
    type: str = "github"


@dataclass(frozen=True)
class OIDCConfig:
    slot: str
    client_id: str
    client_secret: str
    discovery_url: str
    allowed_domains: tuple[str, ...] = ()
    label: str = ""
    type: str = "oidc"

    def __post_init__(self) -> None:
        if not SLOT_RE.match(self.slot):
            raise OAuthConfigError(f"OIDC slot {self.slot!r} must match {SLOT_RE.pattern}")
        if self.slot in RESERVED_SLOTS:
            raise OAuthConfigError(f"OIDC slot {self.slot!r} collides with built-in provider")
        if not self.discovery_url:
            raise OAuthConfigError(f"OIDC slot {self.slot!r}: discovery_url is required")


@dataclass(frozen=True)
class OAuthConfig:
    """Top-level OAuth configuration.

    ``redirect_base_url`` is the public origin the dashboard is served at —
    every callback URL is built from it (``{redirect_base_url}/api/auth/oauth/callback/{slot}``).
    OAuth is considered disabled if no provider is configured.
    """

    redirect_base_url: str
    google: GoogleConfig | None = None
    github: GitHubConfig | None = None
    oidc: tuple[OIDCConfig, ...] = ()
    password_auth_enabled: bool = True
    admin_emails: tuple[str, ...] = field(default=())

    def __post_init__(self) -> None:
        _validate_redirect_base_url(self.redirect_base_url)

    @property
    def is_enabled(self) -> bool:
        return self.google is not None or self.github is not None or bool(self.oidc)

    def providers(self) -> tuple[GoogleConfig | GitHubConfig | OIDCConfig, ...]:
        """Configured providers in display order (Google, GitHub, then OIDC slots)."""
        out: list[GoogleConfig | GitHubConfig | OIDCConfig] = []
        if self.google is not None:
            out.append(self.google)
        if self.github is not None:
            out.append(self.github)
        out.extend(self.oidc)
        return tuple(out)

    def find_provider(self, slot: str) -> GoogleConfig | GitHubConfig | OIDCConfig | None:
        for p in self.providers():
            if p.slot == slot:
                return p
        return None

    def callback_url(self, slot: str) -> str:
        return f"{self.redirect_base_url.rstrip('/')}/api/auth/oauth/callback/{slot}"


def _validate_redirect_base_url(url: str) -> None:
    if not url:
        raise OAuthConfigError("redirect_base_url must be set when OAuth is enabled")
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme not in ("http", "https"):
        raise OAuthConfigError(f"redirect_base_url must be http(s), got {parsed.scheme!r}")
    if not parsed.hostname:
        raise OAuthConfigError("redirect_base_url must include a hostname")
    if parsed.scheme == "http" and parsed.hostname not in _LOCAL_HOSTS:
        raise OAuthConfigError(
            f"redirect_base_url must use https for non-local hosts (got http://{parsed.hostname})"
        )


def from_env(environ: Mapping[str, str] | None = None) -> OAuthConfig | None:
    """Parse :class:`OAuthConfig` from the environment.

    Returns ``None`` when neither ``FLEXIQ_DASHBOARD_OAUTH_REDIRECT_BASE_URL``
    nor any provider client-id env var is set — i.e. OAuth is not configured.
    Raises :class:`OAuthConfigError` if some but not all required vars are set
    for a configured provider (fail-fast on partial configuration).
    """
    env = environ if environ is not None else os.environ

    base_url = env.get("FLEXIQ_DASHBOARD_OAUTH_REDIRECT_BASE_URL", "").strip()
    google_id = env.get("FLEXIQ_DASHBOARD_OAUTH_GOOGLE_CLIENT_ID", "").strip()
    github_id = env.get("FLEXIQ_DASHBOARD_OAUTH_GITHUB_CLIENT_ID", "").strip()
    oidc_slots_raw = env.get("FLEXIQ_DASHBOARD_OAUTH_OIDC_PROVIDERS", "").strip()

    any_provider_signal = bool(google_id or github_id or oidc_slots_raw)
    if not any_provider_signal and not base_url:
        return None
    if any_provider_signal and not base_url:
        raise OAuthConfigError(
            "FLEXIQ_DASHBOARD_OAUTH_REDIRECT_BASE_URL must be set when any "
            "OAuth provider is configured"
        )

    google = _parse_google(env) if google_id else None
    github = _parse_github(env) if github_id else None
    oidc = _parse_oidc_slots(env, oidc_slots_raw)
    password_enabled = _parse_bool(
        env.get("FLEXIQ_DASHBOARD_PASSWORD_AUTH_ENABLED", "true"), default=True
    )
    admin_emails = _split_csv(env.get("FLEXIQ_DASHBOARD_OAUTH_ADMIN_EMAILS"))

    config = OAuthConfig(
        redirect_base_url=base_url,
        google=google,
        github=github,
        oidc=oidc,
        password_auth_enabled=password_enabled,
        admin_emails=admin_emails,
    )

    if not config.is_enabled and not password_enabled:
        raise OAuthConfigError(
            "password auth disabled but no OAuth providers configured — no way to log in"
        )

    return config


def _parse_google(env: Mapping[str, str]) -> GoogleConfig:
    cid = env.get("FLEXIQ_DASHBOARD_OAUTH_GOOGLE_CLIENT_ID", "").strip()
    secret = env.get("FLEXIQ_DASHBOARD_OAUTH_GOOGLE_CLIENT_SECRET", "").strip()
    if not secret:
        raise OAuthConfigError(
            "FLEXIQ_DASHBOARD_OAUTH_GOOGLE_CLIENT_SECRET is required when google client_id is set"
        )
    return GoogleConfig(
        client_id=cid,
        client_secret=secret,
        allowed_domains=_split_csv(env.get("FLEXIQ_DASHBOARD_OAUTH_GOOGLE_ALLOWED_DOMAINS")),
    )


def _parse_github(env: Mapping[str, str]) -> GitHubConfig:
    cid = env.get("FLEXIQ_DASHBOARD_OAUTH_GITHUB_CLIENT_ID", "").strip()
    secret = env.get("FLEXIQ_DASHBOARD_OAUTH_GITHUB_CLIENT_SECRET", "").strip()
    if not secret:
        raise OAuthConfigError(
            "FLEXIQ_DASHBOARD_OAUTH_GITHUB_CLIENT_SECRET is required when github client_id is set"
        )
    return GitHubConfig(
        client_id=cid,
        client_secret=secret,
        allowed_orgs=_split_csv(env.get("FLEXIQ_DASHBOARD_OAUTH_GITHUB_ALLOWED_ORGS")),
    )


def _parse_oidc_slots(env: Mapping[str, str], slots_raw: str) -> tuple[OIDCConfig, ...]:
    slot_names = _split_csv(slots_raw)
    if not slot_names:
        return ()
    out: list[OIDCConfig] = []
    seen: set[str] = set()
    for raw_slot in slot_names:
        slot = raw_slot.lower()
        if slot in seen:
            raise OAuthConfigError(
                f"OIDC slot {slot!r} listed twice in FLEXIQ_DASHBOARD_OAUTH_OIDC_PROVIDERS"
            )
        seen.add(slot)
        out.append(_parse_oidc_slot(env, slot))
    return tuple(out)


def _parse_oidc_slot(env: Mapping[str, str], slot: str) -> OIDCConfig:
    prefix = f"FLEXIQ_DASHBOARD_OAUTH_OIDC_{slot.upper().replace('-', '_')}"
    cid = env.get(f"{prefix}_CLIENT_ID", "").strip()
    secret = env.get(f"{prefix}_CLIENT_SECRET", "").strip()
    discovery = env.get(f"{prefix}_DISCOVERY_URL", "").strip()
    if not cid or not secret or not discovery:
        raise OAuthConfigError(
            f"OIDC slot {slot!r} requires {prefix}_CLIENT_ID, _CLIENT_SECRET, and _DISCOVERY_URL"
        )
    default_label = slot.replace("-", " ").replace("_", " ").title()
    label = env.get(f"{prefix}_LABEL", "").strip() or default_label
    allowed = _split_csv(env.get(f"{prefix}_ALLOWED_DOMAINS"))
    return OIDCConfig(
        slot=slot,
        client_id=cid,
        client_secret=secret,
        discovery_url=discovery,
        allowed_domains=allowed,
        label=label,
    )


def _parse_bool(raw: str, *, default: bool) -> bool:
    lowered = raw.strip().lower()
    if lowered in ("1", "true", "yes", "on"):
        return True
    if lowered in ("0", "false", "no", "off"):
        return False
    return default
