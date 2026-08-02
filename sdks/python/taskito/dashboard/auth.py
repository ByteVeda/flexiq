"""Authentication primitives for the dashboard.

Users and sessions are persisted through ``Queue.set_setting`` / ``get_setting``
— the same key/value store that already backs dashboard branding and
integration settings. This avoids new database tables and keeps the auth
feature working uniformly across SQLite, Postgres, and Redis backends.

Key layout in ``dashboard_settings``:

- ``auth:users`` — JSON object ``{username: {password_hash, role, ...}}``
- ``auth:session:<token>`` — JSON object describing one active session
- ``auth:csrf_secret`` — random secret used as a HMAC key for CSRF tokens

Password hashes use PBKDF2-HMAC-SHA256 (stdlib ``hashlib``) with
600,000 iterations — the OWASP 2023+ baseline for PBKDF2. No third-party
crypto dependency is required.
"""

from __future__ import annotations

import enum
import hashlib
import hmac
import json
import logging
import os
import secrets
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, TypeVar

from taskito.dashboard.kv import update
from taskito.enums import coerce_enum

if TYPE_CHECKING:
    from taskito.app import Queue


logger = logging.getLogger("taskito.dashboard.auth")

_Outcome = TypeVar("_Outcome")

# ── Storage keys ───────────────────────────────────────────────────────

USERS_KEY = "auth:users"
SESSION_PREFIX = "auth:session:"
CSRF_SECRET_KEY = "auth:csrf_secret"

# ── Crypto parameters ──────────────────────────────────────────────────

PBKDF2_ITERATIONS = 600_000
PBKDF2_SALT_BYTES = 16
PBKDF2_HASH_BYTES = 32
SESSION_TOKEN_BYTES = 32

# ── Session lifetime ───────────────────────────────────────────────────

DEFAULT_SESSION_TTL_SECONDS = 24 * 60 * 60  # 24h

# ── Validation ─────────────────────────────────────────────────────────

USERNAME_MAX_LEN = 64
PASSWORD_MIN_LEN = 8
PASSWORD_MAX_LEN = 256


class Role(str, enum.Enum):
    """A dashboard user's access level. The wire form is stored in settings."""

    ADMIN = "admin"
    """Full read/write access to jobs, queues, and users."""

    VIEWER = "viewer"
    """Read-only access."""


# Sentinel prefix used in ``password_hash`` for OAuth-only users so
# ``verify_password`` can short-circuit-reject any password attempt.
OAUTH_PASSWORD_HASH_PREFIX = "oauth:"


# ── Password hashing ───────────────────────────────────────────────────


def hash_password(password: str) -> str:
    """Hash a password with PBKDF2-HMAC-SHA256.

    Returns a self-describing string of the form
    ``pbkdf2_sha256$<iters>$<salt_b64>$<hash_b64>`` so the verifier can
    parse out the salt and iteration count without separate columns.
    """
    salt = secrets.token_bytes(PBKDF2_SALT_BYTES)
    digest = hashlib.pbkdf2_hmac(
        "sha256", password.encode("utf-8"), salt, PBKDF2_ITERATIONS, PBKDF2_HASH_BYTES
    )
    return f"pbkdf2_sha256${PBKDF2_ITERATIONS}${salt.hex()}${digest.hex()}"


def verify_password(password: str, encoded: str) -> bool:
    """Constant-time verify a password against the encoded hash."""
    # Sentinel for OAuth-only users — they have no real password and must
    # never authenticate via the password endpoint.
    if encoded.startswith(OAUTH_PASSWORD_HASH_PREFIX):
        return False
    try:
        scheme, iters_str, salt_hex, hash_hex = encoded.split("$")
    except ValueError:
        return False
    if scheme != "pbkdf2_sha256":
        return False
    try:
        iters = int(iters_str)
        salt = bytes.fromhex(salt_hex)
        expected = bytes.fromhex(hash_hex)
    except ValueError:
        return False
    candidate = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iters, len(expected))
    return hmac.compare_digest(candidate, expected)


# ── Tokens ─────────────────────────────────────────────────────────────


def generate_session_token() -> str:
    """Cryptographically secure URL-safe session token."""
    return secrets.token_urlsafe(SESSION_TOKEN_BYTES)


# ── Data classes ───────────────────────────────────────────────────────


@dataclass(frozen=True)
class User:
    """A persisted dashboard user.

    ``email`` and ``display_name`` are populated for users created via the
    OAuth flow; for password users they are typically ``None`` until set
    by an admin.
    """

    username: str
    password_hash: str
    role: Role
    created_at: int
    last_login_at: int | None = None
    email: str | None = None
    display_name: str | None = None

    @property
    def is_oauth(self) -> bool:
        return self.password_hash.startswith(OAUTH_PASSWORD_HASH_PREFIX)


@dataclass(frozen=True)
class Session:
    """An active dashboard session."""

    token: str
    username: str
    role: Role
    created_at: int
    expires_at: int
    csrf_token: str

    def is_expired(self, now: int | None = None) -> bool:
        return (now if now is not None else int(time.time())) >= self.expires_at


# ── Validation helpers ─────────────────────────────────────────────────


def _validate_username(username: str) -> None:
    if not username:
        raise ValueError("username must not be empty")
    if len(username) > USERNAME_MAX_LEN:
        raise ValueError(f"username must be <= {USERNAME_MAX_LEN} chars")
    if not all(c.isalnum() or c in "._-" for c in username):
        raise ValueError("username may only contain letters, digits, '.', '_', or '-'")


def _validate_password(password: str) -> None:
    if len(password) < PASSWORD_MIN_LEN:
        raise ValueError(f"password must be >= {PASSWORD_MIN_LEN} chars")
    if len(password) > PASSWORD_MAX_LEN:
        raise ValueError(f"password must be <= {PASSWORD_MAX_LEN} chars")


def _validate_role(role: Role | str) -> Role:
    return coerce_enum(Role, role, param="role")


def _role_or_viewer(stored: object) -> Role:
    """Read a persisted role, failing closed on anything unrecognized."""
    try:
        return Role(stored)
    except ValueError:
        return Role.VIEWER


def _oauth_bootstrap_role(
    *,
    email: str | None,
    email_verified: bool,
    admin_emails: tuple[str, ...],
) -> Role:
    """Decide the role for a freshly-created OAuth user.

    ``admin`` requires a verified email (defence against spoofed claims)
    AND membership in the configured admin list. Everyone else — including
    the very first user — gets ``viewer``: admin access comes only from the
    allowlist, the password setup flow, or the env bootstrap, so a stray
    first OAuth login can never win admin.
    """
    if not email_verified or not email:
        return Role.VIEWER
    if email.lower() in {e.lower() for e in admin_emails}:
        return Role.ADMIN
    return Role.VIEWER


# ── Auth store ─────────────────────────────────────────────────────────


class AuthStore:
    """Read/write users and sessions through ``Queue``'s settings store."""

    def __init__(self, queue: Queue) -> None:
        self._queue = queue

    # ── Users ──────────────────────────────────────────────────────

    @staticmethod
    def _decode_users(raw: str | None) -> dict[str, dict[str, object]]:
        if not raw:
            return {}
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("auth:users entry is not valid JSON; treating as empty")
            return {}
        return data if isinstance(data, dict) else {}

    def _load_users(self) -> dict[str, dict[str, object]]:
        return self._decode_users(self._queue.get_setting(USERS_KEY))

    def _update_users(
        self, mutate: Callable[[dict[str, dict[str, object]]], _Outcome]
    ) -> _Outcome:
        """Apply ``mutate`` to the user table without losing a concurrent edit.

        All of them live in one JSON document, so an unconditional write would
        drop a user another dashboard replica had just created.
        """
        return update(self._queue, USERS_KEY, self._decode_users, mutate)

    def count_users(self) -> int:
        return len(self._load_users())

    def list_users(self) -> list[User]:
        return [self._row_to_user(name, row) for name, row in self._load_users().items()]

    def get_user(self, username: str) -> User | None:
        row = self._load_users().get(username)
        return self._row_to_user(username, row) if row else None

    def create_user(self, username: str, password: str, role: Role | str = Role.ADMIN) -> User:
        _validate_username(username)
        _validate_password(password)
        validated_role = _validate_role(role)
        # Hashed once, outside the mutation: PBKDF2 is the expensive part and a
        # retry must not redo it (nor produce a different salt each attempt).
        row: dict[str, object] = {
            "password_hash": hash_password(password),
            "role": validated_role,
            "created_at": int(time.time() * 1000),
            "last_login_at": None,
        }

        def insert(users: dict[str, dict[str, object]]) -> None:
            if username in users:
                raise ValueError(f"user '{username}' already exists")
            users[username] = row

        self._update_users(insert)
        return self._row_to_user(username, row)

    def update_password(self, username: str, new_password: str) -> None:
        _validate_password(new_password)
        password_hash = hash_password(new_password)

        def rehash(users: dict[str, dict[str, object]]) -> None:
            if username not in users:
                raise ValueError(f"user '{username}' does not exist")
            users[username]["password_hash"] = password_hash

        self._update_users(rehash)

    def delete_user(self, username: str) -> bool:
        def drop(users: dict[str, dict[str, object]]) -> bool:
            return users.pop(username, None) is not None

        return self._update_users(drop)

    def authenticate(self, username: str, password: str) -> User | None:
        """Return the user iff username+password match; updates last_login_at."""
        row = self._load_users().get(username)
        if not row:
            # Run a dummy verify against a fixed hash to keep timing constant
            # for unknown vs. known usernames.
            verify_password(password, _DUMMY_HASH)
            return None
        if not verify_password(password, str(row["password_hash"])):
            return None
        # Stamped conditionally, and only on the row as it stands now: a user
        # deleted between the read and the stamp stays deleted rather than
        # being resurrected by writing the whole document back.
        now_ms = int(time.time() * 1000)

        def stamp(users: dict[str, dict[str, object]]) -> None:
            current = users.get(username)
            if current is not None:
                current["last_login_at"] = now_ms

        self._update_users(stamp)
        row["last_login_at"] = now_ms
        return self._row_to_user(username, row)

    @staticmethod
    def _row_to_user(username: str, row: dict[str, object] | None) -> User:
        assert row is not None
        created_raw = row["created_at"]
        last_raw = row.get("last_login_at")
        email_raw = row.get("email")
        name_raw = row.get("display_name")
        return User(
            username=username,
            password_hash=str(row["password_hash"]),
            role=_role_or_viewer(row.get("role")),
            created_at=int(created_raw) if isinstance(created_raw, (int, float, str)) else 0,
            last_login_at=(int(last_raw) if isinstance(last_raw, (int, float, str)) else None),
            email=str(email_raw) if isinstance(email_raw, str) and email_raw else None,
            display_name=str(name_raw) if isinstance(name_raw, str) and name_raw else None,
        )

    # ── OAuth users ────────────────────────────────────────────────

    def get_or_create_oauth_user(
        self,
        slot: str,
        subject: str,
        email: str | None,
        name: str | None,
        email_verified: bool,
        admin_emails: tuple[str, ...] = (),
    ) -> User:
        """Look up or create the User row backing an OAuth identity.

        Username is ``f"{slot}:{subject}"``. On first sight, the role is
        assigned by :func:`_oauth_bootstrap_role`. On subsequent logins,
        the role is left alone but ``email`` / ``display_name`` are refreshed
        from the latest provider claims.
        """
        username = f"{slot}:{subject}"
        now_ms = int(time.time() * 1000)
        role = _oauth_bootstrap_role(
            email=email,
            email_verified=email_verified,
            admin_emails=admin_emails,
        )

        def upsert(users: dict[str, dict[str, object]]) -> dict[str, object]:
            existing = users.get(username)
            if existing is not None:
                if email and existing.get("email") != email:
                    existing["email"] = email
                if name and existing.get("display_name") != name:
                    existing["display_name"] = name
                existing["last_login_at"] = now_ms
                return existing
            # `role` is only consulted here, on first sight: a later login must
            # not re-derive it and undo an administrator's change.
            users[username] = {
                "password_hash": f"{OAUTH_PASSWORD_HASH_PREFIX}{slot}",
                "role": role,
                "created_at": now_ms,
                "last_login_at": now_ms,
                "email": email,
                "display_name": name,
            }
            return users[username]

        return self._row_to_user(username, self._update_users(upsert))

    # ── Sessions ───────────────────────────────────────────────────

    def create_session(
        self, user: User, ttl_seconds: int = DEFAULT_SESSION_TTL_SECONDS
    ) -> Session:
        now = int(time.time())
        token = generate_session_token()
        session = Session(
            token=token,
            username=user.username,
            role=user.role,
            created_at=now,
            expires_at=now + ttl_seconds,
            csrf_token=generate_session_token(),
        )
        self._queue.set_setting(
            SESSION_PREFIX + token,
            json.dumps(
                {k: v for k, v in asdict(session).items() if k != "token"},
                separators=(",", ":"),
            ),
        )
        return session

    def get_session(self, token: str) -> Session | None:
        if not token:
            return None
        raw = self._queue.get_setting(SESSION_PREFIX + token)
        if not raw:
            return None
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return None
        if not isinstance(data, dict):
            return None
        try:
            session = Session(token=token, **{**data, "role": _role_or_viewer(data.get("role"))})
        except TypeError:
            return None
        if session.is_expired():
            self.delete_session(token)
            return None
        return session

    def delete_session(self, token: str) -> bool:
        if not token:
            return False
        return self._queue.delete_setting(SESSION_PREFIX + token)

    def prune_expired_sessions(self) -> int:
        """Best-effort cleanup of expired session entries. Returns count removed."""
        now = int(time.time())
        removed = 0
        for key, value in self._queue.list_settings().items():
            if not key.startswith(SESSION_PREFIX):
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


# Fixed hash used to keep authentication timing constant for unknown users.
# Value computed once with a throw-away password — never used for real auth.
_DUMMY_HASH = (
    "pbkdf2_sha256$600000$"
    "00000000000000000000000000000000$"
    "0000000000000000000000000000000000000000000000000000000000000000"
)


# ── Bootstrap from environment ─────────────────────────────────────────


def bootstrap_admin_from_env(queue: Queue) -> User | None:
    """Idempotently create the first admin from environment variables.

    If ``TASKITO_DASHBOARD_ADMIN_USER`` and ``TASKITO_DASHBOARD_ADMIN_PASSWORD``
    are set AND the user does not exist yet, create it. Safe to call on every
    startup — does nothing if the user already exists.

    The password is removed from ``os.environ`` immediately after it is read so
    it cannot later be harvested via ``/proc/<pid>/environ``, ``ps``, or a
    crash reporter.
    """
    username = os.environ.get("TASKITO_DASHBOARD_ADMIN_USER")
    password = os.environ.pop("TASKITO_DASHBOARD_ADMIN_PASSWORD", None)
    if not username or not password:
        return None
    store = AuthStore(queue)
    if store.get_user(username):
        return None
    try:
        user = store.create_user(username, password, role="admin")
    except ValueError as e:
        logger.warning("Failed to bootstrap admin %r from env: %s", username, e)
        return None
    logger.info("Bootstrapped admin user %r from environment", username)
    return user
