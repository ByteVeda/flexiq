"""Identity types and the provider contract.

A :class:`ProviderIdentity` is the canonical shape of "who just logged in"
that every provider must return. The :class:`OAuthProvider` protocol is
the contract every concrete provider (Google, GitHub, generic OIDC)
satisfies.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


class OAuthError(Exception):
    """Base class for any OAuth-flow error surfaced to the handler layer."""


class StateValidationError(OAuthError):
    """Raised when the callback state is missing, expired, replayed, or forged."""


class IdentityFetchError(OAuthError):
    """Raised when the provider returns an error during token / userinfo fetch."""


class AllowlistDenied(OAuthError):
    """Raised when a verified identity is rejected by a configured allowlist."""


class ProviderNotConfigured(OAuthError):
    """Raised when a request references an OAuth slot that is not registered."""


@dataclass(frozen=True)
class ProviderIdentity:
    """Normalised identity returned by every provider after a successful flow.

    ``slot`` is the registry key (``google``, ``github``, or the operator-
    chosen OIDC slot name). ``subject`` is the provider's stable unique ID
    for the user (``sub`` claim, GitHub ``id``); never the email, which
    can change. Both together form the FlexiQ username ``f"{slot}:{subject}"``.
    """

    slot: str
    subject: str
    email: str | None
    email_verified: bool
    name: str | None = None
    picture: str | None = None


class OAuthProvider(Protocol):
    """Contract every OAuth provider implementation must satisfy."""

    slot: str
    """URL-safe unique identifier used in the callback path."""

    label: str
    """Human-readable button label rendered by the dashboard."""

    type: str
    """One of ``"google"``, ``"github"``, ``"oidc"`` — chooses the icon."""

    def authorization_url(
        self,
        *,
        state: str,
        nonce: str,
        code_challenge: str,
        redirect_uri: str,
    ) -> str:
        """Build the provider-side authorize URL the browser is redirected to."""
        ...

    def exchange_code(
        self,
        *,
        code: str,
        code_verifier: str,
        redirect_uri: str,
        expected_nonce: str | None,
    ) -> ProviderIdentity:
        """Exchange the auth code for an identity, raising on any failure."""
        ...

    def check_allowlist(self, identity: ProviderIdentity) -> None:
        """Raise :class:`AllowlistDenied` if the identity is not permitted."""
        ...
