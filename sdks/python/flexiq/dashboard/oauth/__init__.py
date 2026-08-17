"""OAuth2 / OIDC support for the FlexiQ dashboard.

Adds Google, GitHub, and one-or-more generic OIDC providers (Okta, Auth0,
Keycloak, Microsoft Entra, …) alongside the existing password login. Auth
state continues to live in ``dashboard_settings``; OAuth users are stored
in the same ``auth:users`` blob as password users, with a sentinel
``password_hash`` prefix (``oauth:{slot}``) that ``verify_password``
refuses.
"""

from __future__ import annotations

from flexiq.dashboard.oauth.config import (
    GitHubConfig,
    GoogleConfig,
    OAuthConfig,
    OAuthConfigError,
    OIDCConfig,
)
from flexiq.dashboard.oauth.identity import (
    AllowlistDenied,
    IdentityFetchError,
    OAuthError,
    OAuthProvider,
    ProviderIdentity,
    ProviderNotConfigured,
    StateValidationError,
)
from flexiq.dashboard.oauth.state_store import OAuthState, OAuthStateStore

__all__ = [
    "AllowlistDenied",
    "GitHubConfig",
    "GoogleConfig",
    "IdentityFetchError",
    "OAuthConfig",
    "OAuthConfigError",
    "OAuthError",
    "OAuthProvider",
    "OAuthState",
    "OAuthStateStore",
    "OIDCConfig",
    "ProviderIdentity",
    "ProviderNotConfigured",
    "StateValidationError",
]
