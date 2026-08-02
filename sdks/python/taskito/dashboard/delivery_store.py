"""Persistent webhook delivery log.

Each subscription gets its own JSON list under the key
``webhooks:deliveries:{subscription_id}`` in the ``dashboard_settings``
table. The store is append-only with FIFO eviction once the per-webhook
cap is hit (default 200 entries) — enough to debug recent activity
without unbounded growth.

The structure:

    [
      {
        "id": "uuid",
        "subscription_id": "sub-uuid",
        "event": "job.completed",
        "task_name": "send_email" | null,
        "job_id": "abc123" | null,
        "payload": {...},
        "status": "pending" | "delivered" | "failed" | "dead",
        "attempts": 3,
        "response_code": 200 | null,
        "response_body": "..." | null,
        "latency_ms": 42,
        "error": "..." | null,
        "created_at": 1234567890000,
        "completed_at": 1234567890420
      },
      ...
    ]

Records are inserted in chronological order; listing reverses for newest-first.
"""

from __future__ import annotations

import enum
import json
import logging
import time
import uuid
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from typing import TYPE_CHECKING, Any

from taskito.dashboard.kv import SettingConflictError, update

if TYPE_CHECKING:
    from taskito.app import Queue


class DeliveryStatus(str, enum.Enum):
    """Terminal (or pending) state of a webhook delivery attempt.

    ``PENDING`` is a Python-only starting state — a record exists before its
    first attempt settles. Other SDKs record only settled attempts.
    """

    PENDING = "pending"
    DELIVERED = "delivered"
    FAILED = "failed"
    DEAD = "dead"


def _parse_status(value: Any) -> DeliveryStatus:
    """Read a stored status, falling back to ``PENDING`` on an unknown value."""
    try:
        return DeliveryStatus(str(value))
    except ValueError:
        return DeliveryStatus.PENDING


DELIVERY_PREFIX = "webhooks:deliveries:"
DEFAULT_MAX_PER_WEBHOOK = 200
RESPONSE_BODY_MAX_BYTES = 2048

logger = logging.getLogger("taskito.dashboard.deliveries")


@dataclass
class DeliveryRecord:
    """A single attempted webhook delivery."""

    id: str
    subscription_id: str
    event: str
    payload: dict[str, Any]
    task_name: str | None = None
    job_id: str | None = None
    status: DeliveryStatus = DeliveryStatus.PENDING
    attempts: int = 0
    response_code: int | None = None
    response_body: str | None = None
    latency_ms: int | None = None
    error: str | None = None
    created_at: int = field(default_factory=lambda: int(time.time() * 1000))
    completed_at: int | None = None

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> DeliveryRecord:
        return cls(
            id=str(row["id"]),
            subscription_id=str(row["subscription_id"]),
            event=str(row["event"]),
            payload=dict(row.get("payload") or {}),
            task_name=row.get("task_name"),
            job_id=row.get("job_id"),
            status=_parse_status(row.get("status")),
            attempts=int(row.get("attempts", 0)),
            response_code=row.get("response_code"),
            response_body=row.get("response_body"),
            latency_ms=row.get("latency_ms"),
            error=row.get("error"),
            created_at=int(row.get("created_at", 0)),
            completed_at=row.get("completed_at"),
        )


def _new_id() -> str:
    return uuid.uuid4().hex


def _now_ms() -> int:
    return int(time.time() * 1000)


def _truncate(body: str | None, *, max_bytes: int = RESPONSE_BODY_MAX_BYTES) -> str | None:
    if body is None:
        return None
    encoded = body.encode("utf-8", errors="replace")
    if len(encoded) <= max_bytes:
        return body
    return encoded[:max_bytes].decode("utf-8", errors="replace") + "…"


class DeliveryStore:
    """List/insert/update delivery records keyed by subscription id."""

    def __init__(self, queue: Queue, *, max_per_webhook: int = DEFAULT_MAX_PER_WEBHOOK) -> None:
        self._queue = queue
        self._max = max_per_webhook

    # ── Internal ────────────────────────────────────────────────

    def _key(self, subscription_id: str) -> str:
        return DELIVERY_PREFIX + subscription_id

    def _decoder(self, subscription_id: str) -> Callable[[str | None], list[dict[str, Any]]]:
        def decode(raw: str | None) -> list[dict[str, Any]]:
            if not raw:
                return []
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                logger.warning("delivery log for %s is corrupt; resetting", subscription_id)
                return []
            return data if isinstance(data, list) else []

        return decode

    def _load(self, subscription_id: str) -> list[dict[str, Any]]:
        return self._decoder(subscription_id)(self._queue.get_setting(self._key(subscription_id)))

    def _update(
        self,
        subscription_id: str,
        mutate: Callable[[list[dict[str, Any]]], None],
    ) -> None:
        """Apply ``mutate`` to one webhook's log without losing a concurrent append."""
        update(
            self._queue,
            self._key(subscription_id),
            self._decoder(subscription_id),
            mutate,
        )

    # ── Public API ─────────────────────────────────────────────

    def record_attempt(
        self,
        subscription_id: str,
        event: str,
        payload: dict[str, Any],
        *,
        status: DeliveryStatus,
        attempts: int,
        response_code: int | None = None,
        response_body: str | None = None,
        latency_ms: int | None = None,
        error: str | None = None,
        task_name: str | None = None,
        job_id: str | None = None,
    ) -> DeliveryRecord:
        """Append a delivery row and trim to the per-webhook cap."""
        now = _now_ms()
        record = DeliveryRecord(
            id=_new_id(),
            subscription_id=subscription_id,
            event=event,
            payload=payload,
            task_name=task_name,
            job_id=job_id,
            status=status,
            attempts=attempts,
            response_code=response_code,
            response_body=_truncate(response_body),
            latency_ms=latency_ms,
            error=error,
            created_at=now,
            completed_at=now if status is not DeliveryStatus.PENDING else None,
        )

        def append_and_trim(rows: list[dict[str, Any]]) -> None:
            rows.append(asdict(record))
            if len(rows) > self._max:
                del rows[: len(rows) - self._max]

        # Unlike the admin documents, this key is written once per delivery, so
        # sustained contention is plausible rather than a fault. A log row is
        # diagnostic — losing one must not fail the delivery it describes — so
        # an exhausted retry is logged and dropped rather than raised.
        try:
            self._update(subscription_id, append_and_trim)
        except SettingConflictError:
            logger.warning(
                "dropped the delivery log row for subscription %s; "
                "the log was rewritten by another writer on every attempt",
                subscription_id,
            )
        return record

    def list_for(
        self,
        subscription_id: str,
        *,
        status: DeliveryStatus | str | None = None,
        event: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[DeliveryRecord]:
        rows = list(reversed(self._load(subscription_id)))  # newest first
        if status:
            rows = [r for r in rows if r.get("status") == status]
        if event:
            rows = [r for r in rows if r.get("event") == event]
        page = rows[offset : offset + limit]
        return [DeliveryRecord.from_row(r) for r in page]

    def get(self, subscription_id: str, delivery_id: str) -> DeliveryRecord | None:
        for row in self._load(subscription_id):
            if row.get("id") == delivery_id:
                return DeliveryRecord.from_row(row)
        return None

    def delete_for(self, subscription_id: str) -> bool:
        return self._queue.delete_setting(self._key(subscription_id))

    def count_for(self, subscription_id: str) -> int:
        return len(self._load(subscription_id))
