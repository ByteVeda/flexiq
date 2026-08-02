"""Persistent webhook subscription store.

Webhook subscriptions are stored as a JSON list under the
``webhooks:subscriptions`` key in the ``dashboard_settings`` table. This
gives us cross-backend persistence (SQLite, Postgres, Redis) without
adding new tables, while keeping the data structured enough for the
dashboard CRUD UI.

Each entry is fully described by :class:`WebhookSubscription`. The
``secret`` field stores the HMAC signing secret in plaintext (the
storage backend is already trusted with everything else taskito
persists); the dashboard API NEVER returns the raw secret — only a
``has_secret`` indicator. Use :meth:`WebhookSubscriptionStore.rotate_secret`
to generate a new value and surface it once on rotation.
"""

from __future__ import annotations

import json
import logging
import secrets
import time
import uuid
from collections.abc import Callable
from dataclasses import asdict, dataclass, field, fields, replace
from typing import TYPE_CHECKING, Any, TypeVar

from taskito.dashboard.kv import update

if TYPE_CHECKING:
    from taskito.app import Queue


SUBSCRIPTIONS_KEY = "webhooks:subscriptions"
SECRET_BYTES = 32

logger = logging.getLogger("taskito.dashboard.webhooks")

_Outcome = TypeVar("_Outcome")


@dataclass(frozen=True)
class WebhookSubscription:
    """A single persisted webhook subscription."""

    id: str
    url: str
    events: list[str] = field(default_factory=list)  # empty = all
    task_filter: list[str] | None = None  # None = all tasks
    headers: dict[str, str] = field(default_factory=dict)
    secret: str | None = None
    max_retries: int = 3
    timeout_seconds: float = 10.0
    retry_backoff: float = 2.0
    enabled: bool = True
    description: str | None = None
    created_at: int = 0
    updated_at: int = 0

    def matches(self, event: str, task_name: str | None) -> bool:
        """Return True iff this subscription should fire for the event."""
        if not self.enabled:
            return False
        if self.events and event not in self.events:
            return False
        return not (self.task_filter is not None and task_name not in self.task_filter)


def _new_id() -> str:
    return uuid.uuid4().hex


def _now() -> int:
    return int(time.time() * 1000)


def generate_secret() -> str:
    """Return a fresh URL-safe webhook signing secret."""
    return secrets.token_urlsafe(SECRET_BYTES)


#: The keys :class:`WebhookSubscription` round-trips. Anything else in a stored
#: row was written by a runtime that models more than this one does.
_MODELLED_FIELDS = frozenset(f.name for f in fields(WebhookSubscription))


def _unmodelled_fields(row: dict[str, Any]) -> dict[str, Any]:
    """The keys of a stored row this runtime does not model."""
    return {key: value for key, value in row.items() if key not in _MODELLED_FIELDS}


class WebhookSubscriptionStore:
    """CRUD for webhook subscriptions backed by ``Queue``'s settings store."""

    def __init__(self, queue: Queue) -> None:
        self._queue = queue

    # ── Internal load/save ───────────────────────────────────────

    @staticmethod
    def _decode(raw: str | None) -> list[dict[str, Any]]:
        if not raw:
            return []
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("webhooks:subscriptions is not valid JSON; treating as empty")
            return []
        return data if isinstance(data, list) else []

    def _load_raw(self) -> list[dict[str, Any]]:
        return self._decode(self._queue.get_setting(SUBSCRIPTIONS_KEY))

    def _update(self, mutate: Callable[[list[dict[str, Any]]], _Outcome]) -> _Outcome:
        """Apply ``mutate`` to the subscription list without losing a concurrent edit."""
        return update(self._queue, SUBSCRIPTIONS_KEY, self._decode, mutate)

    @staticmethod
    def _row_to_subscription(row: dict[str, Any]) -> WebhookSubscription:
        return WebhookSubscription(
            id=str(row["id"]),
            url=str(row["url"]),
            events=list(row.get("events") or []),
            task_filter=(list(row["task_filter"]) if row.get("task_filter") is not None else None),
            headers=dict(row.get("headers") or {}),
            secret=row.get("secret"),
            max_retries=int(row.get("max_retries", 3)),
            timeout_seconds=float(row.get("timeout_seconds", 10.0)),
            retry_backoff=float(row.get("retry_backoff", 2.0)),
            enabled=bool(row.get("enabled", True)),
            description=row.get("description"),
            created_at=int(row.get("created_at", 0)),
            updated_at=int(row.get("updated_at", 0)),
        )

    # ── Public API ───────────────────────────────────────────────

    def list_all(self) -> list[WebhookSubscription]:
        return [self._row_to_subscription(r) for r in self._load_raw()]

    def get(self, subscription_id: str) -> WebhookSubscription | None:
        for row in self._load_raw():
            if row.get("id") == subscription_id:
                return self._row_to_subscription(row)
        return None

    def create(
        self,
        *,
        url: str,
        events: list[str] | None = None,
        task_filter: list[str] | None = None,
        headers: dict[str, str] | None = None,
        secret: str | None = None,
        max_retries: int = 3,
        timeout_seconds: float = 10.0,
        retry_backoff: float = 2.0,
        enabled: bool = True,
        description: str | None = None,
    ) -> WebhookSubscription:
        now = _now()
        sub = WebhookSubscription(
            id=_new_id(),
            url=url,
            events=list(events or []),
            task_filter=list(task_filter) if task_filter is not None else None,
            headers=dict(headers or {}),
            secret=secret,
            max_retries=max_retries,
            timeout_seconds=timeout_seconds,
            retry_backoff=retry_backoff,
            enabled=enabled,
            description=description,
            created_at=now,
            updated_at=now,
        )
        self._update(lambda rows: rows.append(asdict(sub)))
        return sub

    def update(self, subscription_id: str, **changes: Any) -> WebhookSubscription:
        """Patch a subscription. Pass only the fields you want to change.

        Raises ``KeyError`` if the subscription does not exist.
        """

        def patch_row(rows: list[dict[str, Any]]) -> WebhookSubscription:
            for idx, row in enumerate(rows):
                if row.get("id") != subscription_id:
                    continue
                existing = self._row_to_subscription(row)
                allowed = {
                    "url",
                    "events",
                    "task_filter",
                    "headers",
                    "secret",
                    "max_retries",
                    "timeout_seconds",
                    "retry_backoff",
                    "enabled",
                    "description",
                }
                patch = {k: v for k, v in changes.items() if k in allowed}
                updated = replace(existing, updated_at=_now(), **patch)
                # Fields this runtime does not model are carried over from the
                # row as it stands now: another SDK against the same backend may
                # have written keys this build has never heard of, and rebuilding
                # the row from the dataclass alone would drop them.
                rows[idx] = {**_unmodelled_fields(row), **asdict(updated)}
                return updated
            raise KeyError(subscription_id)

        return self._update(patch_row)

    def delete(self, subscription_id: str) -> bool:
        def drop_row(rows: list[dict[str, Any]]) -> bool:
            remaining = [r for r in rows if r.get("id") != subscription_id]
            if len(remaining) == len(rows):
                return False
            rows[:] = remaining
            return True

        return self._update(drop_row)

    def rotate_secret(self, subscription_id: str) -> str:
        """Generate a fresh secret for a subscription. Returns the new value."""
        secret = generate_secret()
        self.update(subscription_id, secret=secret)
        return secret
