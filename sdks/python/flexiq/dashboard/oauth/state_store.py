"""Short-lived store for in-flight OAuth flows.

When the dashboard redirects a browser to a provider's ``/authorize`` URL,
we stash the ``state``, ``nonce``, PKCE ``code_verifier``, target slot,
and post-login ``next_url`` server-side, keyed by ``state``. On callback
we look the row up, validate ``state`` (single-use, time-bounded), and
delete it.

Rows live in ``dashboard_settings`` under the ``auth:oauth_state:<state>``
key namespace alongside sessions and users, so they work uniformly across
SQLite / Postgres / Redis with no new migrations.
"""

from __future__ import annotations

import json
import logging
import secrets
import time
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from flexiq.app import Queue


logger = logging.getLogger("flexiq.dashboard.oauth")

STATE_PREFIX = "auth:oauth_state:"
DEFAULT_STATE_TTL_SECONDS = 5 * 60  # 5 min — covers consent UX + reasonable network latency.

STATE_TOKEN_BYTES = 32
NONCE_BYTES = 16
CODE_VERIFIER_BYTES = 32


@dataclass(frozen=True)
class OAuthState:
    """One in-flight OAuth flow, stored server-side until callback or expiry."""

    state: str
    nonce: str
    code_verifier: str
    slot: str
    next_url: str
    created_at: int
    expires_at: int

    def is_expired(self, now: int | None = None) -> bool:
        return (now if now is not None else int(time.time())) >= self.expires_at


def generate_state() -> str:
    return secrets.token_urlsafe(STATE_TOKEN_BYTES)


def generate_nonce() -> str:
    return secrets.token_urlsafe(NONCE_BYTES)


def generate_code_verifier() -> str:
    # RFC 7636 section 4.1: high-entropy URL-safe string, 43-128 chars.
    # 32 bytes yields 43 chars base64url, comfortably above the minimum.
    return secrets.token_urlsafe(CODE_VERIFIER_BYTES)


class OAuthStateStore:
    """Create, consume (read+delete), and prune short-lived OAuth state rows."""

    def __init__(self, queue: Queue) -> None:
        self._queue = queue

    def create(
        self,
        slot: str,
        next_url: str,
        ttl_seconds: int = DEFAULT_STATE_TTL_SECONDS,
    ) -> OAuthState:
        """Mint a fresh state/nonce/verifier triple and persist it."""
        now = int(time.time())
        state = OAuthState(
            state=generate_state(),
            nonce=generate_nonce(),
            code_verifier=generate_code_verifier(),
            slot=slot,
            next_url=next_url,
            created_at=now,
            expires_at=now + ttl_seconds,
        )
        payload = {k: v for k, v in asdict(state).items() if k != "state"}
        self._queue.set_setting(
            STATE_PREFIX + state.state, json.dumps(payload, separators=(",", ":"))
        )
        return state

    def consume(self, state_token: str) -> OAuthState | None:
        """Look up ``state_token`` and atomically delete it. Returns ``None``
        if the row is missing, malformed, or expired. Single-use — the row
        is always deleted, so a replayed state never re-validates.
        """
        if not state_token:
            return None
        key = STATE_PREFIX + state_token
        raw = self._queue.get_setting(key)
        if not raw:
            return None
        # Always delete first so any subsequent request with the same state
        # sees a missing row, even if parsing fails below.
        self._queue.delete_setting(key)
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return None
        try:
            row = OAuthState(state=state_token, **data)
        except TypeError:
            return None
        if row.is_expired():
            return None
        return row

    def prune_expired(self) -> int:
        """Best-effort sweep of expired state rows. Returns count removed."""
        now = int(time.time())
        removed = 0
        for key, value in self._queue.list_settings().items():
            if not key.startswith(STATE_PREFIX):
                continue
            try:
                data = json.loads(value)
                expires_at = int(data.get("expires_at", 0))
            except (json.JSONDecodeError, TypeError, ValueError):
                continue
            if expires_at <= now:
                self._queue.delete_setting(key)
                removed += 1
        return removed
