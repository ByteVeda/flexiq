"""HTTP handlers for the OAuth login flow.

These handlers are not JSON-producing like the rest of ``handlers/`` —
they emit 302 redirects (and, on a successful callback, set the session
cookies). The server wires them into ``_handle_get`` directly rather
than through the generic JSON dispatcher.

The handlers themselves are network-IO-free aside from what the wrapped
:class:`OAuthFlow` does internally. They translate provider/flow
exceptions to dashboard ``_BadRequest`` / ``_NotFound`` for the server's
error machinery to pick up, and they return :class:`OAuthRedirect` —
a tiny adapter type that tells the server "emit 302 to URL, optionally
with these cookies attached".
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from flexiq.dashboard.errors import _NotFound
from flexiq.dashboard.oauth.identity import (
    AllowlistDenied,
    IdentityFetchError,
    ProviderNotConfigured,
    StateValidationError,
)

if TYPE_CHECKING:
    from flexiq.app import Queue
    from flexiq.dashboard.auth import Session
    from flexiq.dashboard.oauth.flow import OAuthFlow


@dataclass(frozen=True)
class OAuthRedirect:
    """Server adapter: emit ``302 Location: url``.

    ``session`` is set on a successful callback so the server can attach
    the same ``flexiq_session`` + ``flexiq_csrf`` cookies it sets for
    password login. On the ``/start`` redirect ``session`` is ``None``.
    """

    url: str
    session: Session | None = None
    status: int = 302


def handle_providers(queue: Queue, _qs: dict, flow: OAuthFlow | None) -> dict:
    """List configured providers + whether password auth is enabled.

    Returns ``{password_enabled: bool, providers: [{slot, label, type}]}``.
    Always callable; returns ``providers: []`` when OAuth is not configured.
    """
    if flow is None:
        return {"password_enabled": True, "providers": []}
    return {
        "password_enabled": flow.password_auth_enabled,
        "providers": flow.providers_listing(),
    }


def handle_start(
    queue: Queue,
    qs: dict[str, list[str]],
    slot: str,
    flow: OAuthFlow | None,
) -> OAuthRedirect:
    """Begin an OAuth login: mint state, return a 302 to the provider URL."""
    if flow is None:
        raise _NotFound("oauth_not_configured")
    next_values = qs.get("next") or []
    next_url = next_values[0] if next_values else None
    try:
        provider_url = flow.start(slot, next_url)
    except ProviderNotConfigured as e:
        raise _NotFound(str(e)) from None
    return OAuthRedirect(url=provider_url)


def handle_callback(
    queue: Queue,
    qs: dict[str, list[str]],
    slot: str,
    flow: OAuthFlow | None,
) -> OAuthRedirect:
    """Land an OAuth login: verify state, create a session, redirect home.

    The returned :class:`OAuthRedirect` carries the new :class:`Session`;
    the server attaches the standard ``flexiq_session`` + ``flexiq_csrf``
    cookies before sending the 302.
    """
    if flow is None:
        raise _NotFound("oauth_not_configured")

    def _first(name: str) -> str | None:
        values = qs.get(name) or []
        return values[0] if values else None

    code = _first("code")
    state_token = _first("state")
    error = _first("error")
    try:
        session, next_url = flow.handle_callback(
            slot, code=code, state_token=state_token, error=error
        )
    except ProviderNotConfigured as e:
        raise _NotFound(str(e)) from None
    except StateValidationError:
        return OAuthRedirect(url="/login?error=oauth_state_invalid")
    except IdentityFetchError:
        return OAuthRedirect(url="/login?error=oauth_failed")
    except AllowlistDenied:
        return OAuthRedirect(url="/login?error=oauth_denied")
    flow.prune_state()
    return OAuthRedirect(url=next_url, session=session)
