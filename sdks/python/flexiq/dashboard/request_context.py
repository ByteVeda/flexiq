"""Per-request authentication context for the dashboard.

The HTTP server populates a :class:`RequestContext` for every request and
hands it to the dispatcher. Handlers that need the calling user (login,
logout, whoami, etc.) accept it as a keyword argument; pure-data handlers
ignore it.
"""

from __future__ import annotations

from dataclasses import dataclass
from email.message import Message as _EmailMessage

from flexiq.dashboard.auth import Session

# Cookie name used for the session token. HttpOnly + SameSite=Strict — the
# session cookie must never be readable from JavaScript or sent on
# third-party requests.
SESSION_COOKIE = "flexiq_session"

# Cookie name for the CSRF token. NOT HttpOnly — the SPA reads it and
# echoes it in the X-CSRF-Token header on state-changing requests.
CSRF_COOKIE = "flexiq_csrf"
CSRF_HEADER = "X-CSRF-Token"


@dataclass(frozen=True)
class RequestContext:
    """Auth state attached to a single HTTP request."""

    session: Session | None
    csrf_cookie: str | None
    csrf_header: str | None

    @property
    def is_authenticated(self) -> bool:
        return self.session is not None

    @property
    def username(self) -> str | None:
        return self.session.username if self.session else None

    @property
    def role(self) -> str | None:
        return self.session.role if self.session else None

    def csrf_valid(self) -> bool:
        """Double-submit cookie check.

        For state-changing requests we require:
        - a non-empty CSRF cookie
        - an ``X-CSRF-Token`` header that equals it byte-for-byte
        - the value matches the session's stored CSRF token (defends
          against an attacker who pre-seeds the cookie)
        """
        if not self.session:
            return False
        if not self.csrf_cookie or not self.csrf_header:
            return False
        if self.csrf_cookie != self.csrf_header:
            return False
        return self.csrf_cookie == self.session.csrf_token


def parse_cookies(header: str | None) -> dict[str, str]:
    """Parse a raw ``Cookie:`` header into a ``{name: value}`` dict.

    Empty or malformed cookies are silently skipped; only the first value
    is kept for any duplicated cookie name.
    """
    if not header:
        return {}
    cookies: dict[str, str] = {}
    for part in header.split(";"):
        if "=" not in part:
            continue
        name, _, value = part.strip().partition("=")
        name = name.strip()
        value = value.strip()
        if name and name not in cookies:
            cookies[name] = value
    return cookies


def build_context(headers: _EmailMessage, session: Session | None) -> RequestContext:
    """Construct a :class:`RequestContext` from raw HTTP headers and the
    session resolved by the server. ``headers`` is the email.message-style
    ``http.client.HTTPMessage`` exposed by :class:`BaseHTTPRequestHandler`."""
    cookies = parse_cookies(headers.get("Cookie"))
    return RequestContext(
        session=session,
        csrf_cookie=cookies.get(CSRF_COOKIE),
        csrf_header=headers.get(CSRF_HEADER),
    )
