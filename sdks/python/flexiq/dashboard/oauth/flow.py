"""End-to-end OAuth flow orchestration.

:class:`OAuthFlow` is the seam between the HTTP handler layer and the
provider implementations. It owns the registry of configured providers,
the state store, and the :class:`AuthStore` integration. Handlers call
``start()`` to mint a redirect URL and ``handle_callback()`` to land a
session.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from flexiq.dashboard.auth import AuthStore
from flexiq.dashboard.oauth.config import (
    GitHubConfig,
    GoogleConfig,
    OAuthConfig,
    OIDCConfig,
)
from flexiq.dashboard.oauth.identity import (
    IdentityFetchError,
    OAuthProvider,
    ProviderNotConfigured,
    StateValidationError,
)
from flexiq.dashboard.oauth.pkce import s256_challenge
from flexiq.dashboard.oauth.providers import (
    GenericOIDCProvider,
    GitHubProvider,
    GoogleProvider,
)
from flexiq.dashboard.oauth.state_store import OAuthStateStore
from flexiq.dashboard.url_safety import is_safe_redirect

if TYPE_CHECKING:
    from flexiq.app import Queue
    from flexiq.dashboard.auth import Session

logger = logging.getLogger("flexiq.dashboard.oauth")


def build_providers(
    config: OAuthConfig,
) -> dict[str, OAuthProvider]:
    """Instantiate one provider per configured slot, keyed by slot."""
    registry: dict[str, OAuthProvider] = {}
    for entry in config.providers():
        if isinstance(entry, GoogleConfig):
            registry[entry.slot] = GoogleProvider(entry)
        elif isinstance(entry, GitHubConfig):
            registry[entry.slot] = GitHubProvider(entry)
        elif isinstance(entry, OIDCConfig):
            registry[entry.slot] = GenericOIDCProvider(entry)
    return registry


class OAuthFlow:
    """Ties together config, providers, state store, and the auth store."""

    def __init__(
        self,
        queue: Queue,
        config: OAuthConfig,
        *,
        providers: dict[str, OAuthProvider] | None = None,
        state_store: OAuthStateStore | None = None,
    ) -> None:
        self._queue = queue
        self._config = config
        self._providers: dict[str, OAuthProvider] = (
            providers if providers is not None else build_providers(config)
        )
        self._state_store = state_store or OAuthStateStore(queue)
        if self._providers and not config.admin_emails:
            # OAuth users only ever get the viewer role without an allowlist,
            # so an OAuth-only deployment would silently have zero admins.
            logger.warning(
                "OAuth is configured without admin emails: every OAuth login "
                "gets the viewer role. Set FLEXIQ_DASHBOARD_OAUTH_ADMIN_EMAILS "
                "(or OAuthConfig.admin_emails) to grant admin access."
            )

    # ── Introspection ────────────────────────────────────────────────

    @property
    def password_auth_enabled(self) -> bool:
        return self._config.password_auth_enabled

    def has_provider(self, slot: str) -> bool:
        return slot in self._providers

    def providers_listing(self) -> list[dict[str, str]]:
        """Compact provider summary for the login UI (no secrets)."""
        return [
            {"slot": p.slot, "label": p.label, "type": p.type} for p in self._providers.values()
        ]

    # ── Flow ─────────────────────────────────────────────────────────

    def start(self, slot: str, next_url: str | None) -> str:
        """Mint a state row and return the provider's authorize URL.

        ``next_url`` is sanitised against :func:`is_safe_redirect` and falls
        back to ``"/"`` if it fails the check.
        """
        provider = self._require_provider(slot)
        safe_next = next_url if next_url and is_safe_redirect(next_url) else "/"
        state = self._state_store.create(slot=slot, next_url=safe_next)
        challenge = s256_challenge(state.code_verifier)
        return provider.authorization_url(
            state=state.state,
            nonce=state.nonce,
            code_challenge=challenge,
            redirect_uri=self._config.callback_url(slot),
        )

    def handle_callback(
        self,
        slot: str,
        *,
        code: str | None,
        state_token: str | None,
        error: str | None,
    ) -> tuple[Session, str]:
        """Exchange ``code`` for an identity and create a session.

        Returns ``(session, next_url)`` on success. Raises:

        - :class:`StateValidationError` for missing/expired/replayed state
        - :class:`IdentityFetchError` for any token / userinfo / claim issue
        - :class:`AllowlistDenied` if the identity is outside the allowlist
        """
        if error:
            raise IdentityFetchError(f"provider returned error: {error}")
        if not code or not state_token:
            raise StateValidationError("missing code or state parameter")

        row = self._state_store.consume(state_token)
        if row is None:
            raise StateValidationError("state is invalid, expired, or already used")
        if row.slot != slot:
            raise StateValidationError("state slot does not match callback slot")

        provider = self._require_provider(slot)
        identity = provider.exchange_code(
            code=code,
            code_verifier=row.code_verifier,
            redirect_uri=self._config.callback_url(slot),
            expected_nonce=row.nonce,
        )
        provider.check_allowlist(identity)

        store = AuthStore(self._queue)
        user = store.get_or_create_oauth_user(
            slot=identity.slot,
            subject=identity.subject,
            email=identity.email,
            name=identity.name,
            email_verified=identity.email_verified,
            admin_emails=self._config.admin_emails,
        )
        session = store.create_session(user)
        return session, row.next_url

    # ── Maintenance ──────────────────────────────────────────────────

    def prune_state(self) -> int:
        return self._state_store.prune_expired()

    # ── Internal ─────────────────────────────────────────────────────

    def _require_provider(self, slot: str) -> OAuthProvider:
        provider = self._providers.get(slot)
        if provider is None:
            raise ProviderNotConfigured(f"OAuth provider {slot!r} is not configured")
        return provider
