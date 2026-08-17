"""Shared helpers for dashboard endpoint tests.

The dashboard requires a logged-in session for every API route once setup
is complete. :class:`AuthedClient` wraps the stdlib ``urllib.request`` so
tests can issue authenticated HTTP calls without repeating the cookie /
CSRF dance.
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any

from flexiq import Queue
from flexiq.dashboard.auth import AuthStore, Session


@dataclass(frozen=True)
class AuthedClient:
    """Stateless HTTP helper that attaches session + CSRF to every call."""

    base: str
    session: Session

    @property
    def _cookies(self) -> dict[str, str]:
        return {
            "flexiq_session": self.session.token,
            "flexiq_csrf": self.session.csrf_token,
        }

    def _cookie_header(self) -> str:
        return "; ".join(f"{k}={v}" for k, v in self._cookies.items())

    def get(self, path: str, *, raise_for_status: bool = True) -> Any:
        url = self.base + path
        req = urllib.request.Request(url, method="GET")
        req.add_header("Cookie", self._cookie_header())
        try:
            with urllib.request.urlopen(req) as resp:
                return json.loads(resp.read() or b"{}")
        except urllib.error.HTTPError as e:
            if raise_for_status:
                raise
            return {"status": e.code, "body": json.loads(e.read() or b"{}")}

    def post(self, path: str, body: dict | None = None) -> Any:
        return self._mutate("POST", path, body)

    def put(self, path: str, body: dict | None = None) -> Any:
        return self._mutate("PUT", path, body)

    def delete(self, path: str) -> Any:
        return self._mutate("DELETE", path, None)

    def _mutate(self, method: str, path: str, body: dict | None) -> Any:
        url = self.base + path
        data = json.dumps(body).encode() if body is not None else b""
        req = urllib.request.Request(url, method=method, data=data)
        req.add_header("Cookie", self._cookie_header())
        req.add_header("X-CSRF-Token", self.session.csrf_token)
        if body is not None:
            req.add_header("Content-Type", "application/json")
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read() or b"{}")


def seed_admin_and_session(
    queue: Queue,
    *,
    username: str = "test-admin",
    password: str = "test-pass-1234",
) -> Session:
    """Create a one-off admin and return a fresh session for it."""
    store = AuthStore(queue)
    if store.get_user(username) is None:
        store.create_user(username, password, role="admin")
    user = store.get_user(username)
    assert user is not None
    return store.create_session(user)
