"""Persistent task & queue runtime overrides.

Operators tune individual task or queue behaviour (rate limits, concurrency
caps, retry policy, timeouts, priority, paused state) at runtime via the
dashboard. The decorator-declared values become the *defaults* — any override
recorded here wins.

Storage layout in ``dashboard_settings``:

- ``overrides:task:<task_name>`` — JSON of overridden fields for that task
- ``overrides:queue:<queue_name>`` — JSON of overridden fields for that queue

Overrides are applied at worker startup (see
:meth:`taskito.mixins.lifecycle.QueueLifecycleMixin.start_worker`).
Changes to the store DO NOT take effect on a running worker until it is
restarted — the dashboard surfaces this so operators aren't surprised.

The contract is intentionally minimal: only the fields below can be
overridden. The store rejects anything else so a typo can't write garbage
through the dashboard.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any

from taskito.dashboard.kv import update

if TYPE_CHECKING:
    from taskito.app import Queue


TASK_PREFIX = "overrides:task:"
QUEUE_PREFIX = "overrides:queue:"

logger = logging.getLogger("taskito.dashboard.overrides")


# ── Allowed override fields ────────────────────────────────────────────


TASK_OVERRIDE_FIELDS: frozenset[str] = frozenset(
    {
        "rate_limit",
        "max_concurrent",
        "max_retries",
        "retry_backoff",
        "timeout",
        "priority",
        "paused",
    }
)

QUEUE_OVERRIDE_FIELDS: frozenset[str] = frozenset(
    {
        "rate_limit",
        "max_concurrent",
        "paused",
    }
)


# ── Data classes ───────────────────────────────────────────────────────


@dataclass(frozen=True)
class TaskOverride:
    """An operator-set override for a registered task."""

    task_name: str
    rate_limit: str | None = None
    max_concurrent: int | None = None
    max_retries: int | None = None
    retry_backoff: float | None = None
    timeout: int | None = None
    priority: int | None = None
    paused: bool = False
    updated_at: int = 0

    def as_patch(self) -> dict[str, Any]:
        """Return a dict of only the non-default fields (those the operator
        actually set). The empty/default values are NOT patched onto the
        underlying ``PyTaskConfig`` — they continue to use the decorator
        value."""
        patch: dict[str, Any] = {}
        for field in TASK_OVERRIDE_FIELDS:
            if field == "paused":
                continue  # handled separately; not a PyTaskConfig field
            value = getattr(self, field)
            if value is not None:
                patch[field] = value
        return patch


@dataclass(frozen=True)
class QueueOverride:
    """An operator-set override for a queue."""

    queue_name: str
    rate_limit: str | None = None
    max_concurrent: int | None = None
    paused: bool = False
    updated_at: int = 0


# ── Validation ─────────────────────────────────────────────────────────


def _validate_task_fields(fields: dict[str, Any]) -> None:
    unknown = set(fields) - TASK_OVERRIDE_FIELDS
    if unknown:
        raise ValueError(f"unknown task override fields: {sorted(unknown)}")
    _validate_rate_limit(fields.get("rate_limit"))
    _validate_max_concurrent(fields.get("max_concurrent"))
    _validate_int_field(fields, "max_retries", minimum=0)
    _validate_float_field(fields, "retry_backoff", minimum=0)
    _validate_int_field(fields, "timeout", minimum=1)
    _validate_int_field(fields, "priority")
    _validate_bool_field(fields, "paused")


def _validate_queue_fields(fields: dict[str, Any]) -> None:
    unknown = set(fields) - QUEUE_OVERRIDE_FIELDS
    if unknown:
        raise ValueError(f"unknown queue override fields: {sorted(unknown)}")
    _validate_rate_limit(fields.get("rate_limit"))
    _validate_max_concurrent(fields.get("max_concurrent"))
    _validate_bool_field(fields, "paused")


def _validate_rate_limit(value: Any) -> None:
    if value is None:
        return
    if not isinstance(value, str) or not value:
        raise ValueError("rate_limit must be a non-empty string like '100/m'")
    # Cheap shape check; rate-limit parsing happens in Rust.
    if "/" not in value:
        raise ValueError("rate_limit must contain a unit, e.g. '10/s', '100/m', '3600/h'")


def _validate_max_concurrent(value: Any) -> None:
    if value is None:
        return
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError("max_concurrent must be a non-negative integer")


def _validate_int_field(fields: dict[str, Any], name: str, *, minimum: int | None = None) -> None:
    value = fields.get(name)
    if value is None:
        return
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError(f"{name} must be an integer")
    if minimum is not None and value < minimum:
        raise ValueError(f"{name} must be >= {minimum}")


def _validate_float_field(
    fields: dict[str, Any], name: str, *, minimum: float | None = None
) -> None:
    value = fields.get(name)
    if value is None:
        return
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be a number")
    if minimum is not None and value < minimum:
        raise ValueError(f"{name} must be >= {minimum}")


def _validate_bool_field(fields: dict[str, Any], name: str) -> None:
    value = fields.get(name)
    if value is not None and not isinstance(value, bool):
        raise ValueError(f"{name} must be a boolean")


# ── Store ──────────────────────────────────────────────────────────────


def _now() -> int:
    return int(time.time() * 1000)


_SECONDS_ERA_THRESHOLD = 10**12


def _ensure_millis(ts: int) -> int:
    if 0 < ts < _SECONDS_ERA_THRESHOLD:
        return ts * 1000
    return ts


def _parse_json(raw: str | None) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        logger.warning("overrides entry is not valid JSON; treating as empty")
        return {}
    return data if isinstance(data, dict) else {}


class OverridesStore:
    """CRUD for per-task and per-queue runtime overrides."""

    def __init__(self, queue: Queue) -> None:
        self._queue = queue

    def _merge(
        self,
        key: str,
        fields: dict[str, Any],
        normalise: Callable[[dict[str, Any]], dict[str, Any]],
    ) -> dict[str, Any]:
        """Patch ``fields`` into the override at ``key`` without losing a concurrent edit.

        ``normalise`` reduces a stored row to the fields the override actually
        carries, so an unrecognised key never survives a round trip. A ``None``
        value clears its field rather than storing a null.
        """

        def patch(row: dict[str, Any]) -> dict[str, Any]:
            merged = normalise(row)
            for name, value in fields.items():
                if value is None:
                    merged.pop(name, None)
                else:
                    merged[name] = value
            merged["updated_at"] = _now()
            row.clear()
            row.update(merged)
            return merged

        return update(self._queue, key, _parse_json, patch)

    @staticmethod
    def _overridden_fields(row: dict[str, Any], view: Any, identity: str) -> dict[str, Any]:
        """The set fields of a decoded override, minus its identity and timestamp."""
        if not row:
            return {}
        fields = {name: value for name, value in asdict(view).items() if value is not None}
        fields.pop(identity, None)
        fields.pop("updated_at", None)
        return fields

    # ── Tasks ──────────────────────────────────────────────────

    def list_tasks(self) -> dict[str, TaskOverride]:
        """Return ``{task_name: TaskOverride}`` for every task with an override."""
        out: dict[str, TaskOverride] = {}
        for key, raw in self._queue.list_settings().items():
            if not key.startswith(TASK_PREFIX):
                continue
            task_name = key[len(TASK_PREFIX) :]
            out[task_name] = self._row_to_task(task_name, _parse_json(raw))
        return out

    def get_task(self, task_name: str) -> TaskOverride | None:
        raw = self._queue.get_setting(TASK_PREFIX + task_name)
        if not raw:
            return None
        return self._row_to_task(task_name, _parse_json(raw))

    def set_task(self, task_name: str, fields: dict[str, Any]) -> TaskOverride:
        _validate_task_fields(fields)
        if not task_name:
            raise ValueError("task_name must not be empty")
        merged = self._merge(
            TASK_PREFIX + task_name,
            fields,
            lambda row: self._overridden_fields(
                row, self._row_to_task(task_name, row), "task_name"
            ),
        )
        return self._row_to_task(task_name, merged)

    def clear_task(self, task_name: str) -> bool:
        return self._queue.delete_setting(TASK_PREFIX + task_name)

    @staticmethod
    def _row_to_task(task_name: str, row: dict[str, Any]) -> TaskOverride:
        return TaskOverride(
            task_name=task_name,
            rate_limit=row.get("rate_limit"),
            max_concurrent=row.get("max_concurrent"),
            max_retries=row.get("max_retries"),
            retry_backoff=row.get("retry_backoff"),
            timeout=row.get("timeout"),
            priority=row.get("priority"),
            paused=bool(row.get("paused", False)),
            updated_at=_ensure_millis(int(row.get("updated_at", 0))),
        )

    # ── Queues ─────────────────────────────────────────────────

    def list_queues(self) -> dict[str, QueueOverride]:
        out: dict[str, QueueOverride] = {}
        for key, raw in self._queue.list_settings().items():
            if not key.startswith(QUEUE_PREFIX):
                continue
            queue_name = key[len(QUEUE_PREFIX) :]
            out[queue_name] = self._row_to_queue(queue_name, _parse_json(raw))
        return out

    def get_queue(self, queue_name: str) -> QueueOverride | None:
        raw = self._queue.get_setting(QUEUE_PREFIX + queue_name)
        if not raw:
            return None
        return self._row_to_queue(queue_name, _parse_json(raw))

    def set_queue(self, queue_name: str, fields: dict[str, Any]) -> QueueOverride:
        _validate_queue_fields(fields)
        if not queue_name:
            raise ValueError("queue_name must not be empty")
        merged = self._merge(
            QUEUE_PREFIX + queue_name,
            fields,
            lambda row: self._overridden_fields(
                row, self._row_to_queue(queue_name, row), "queue_name"
            ),
        )
        return self._row_to_queue(queue_name, merged)

    def clear_queue(self, queue_name: str) -> bool:
        return self._queue.delete_setting(QUEUE_PREFIX + queue_name)

    @staticmethod
    def _row_to_queue(queue_name: str, row: dict[str, Any]) -> QueueOverride:
        return QueueOverride(
            queue_name=queue_name,
            rate_limit=row.get("rate_limit"),
            max_concurrent=row.get("max_concurrent"),
            paused=bool(row.get("paused", False)),
            updated_at=_ensure_millis(int(row.get("updated_at", 0))),
        )

    # ── Apply (used at worker startup) ─────────────────────────

    def apply_task_overrides(self, configs: list[Any]) -> list[str]:
        """Mutate each :class:`PyTaskConfig` in ``configs`` with any matching
        task override. Returns a list of task names that are paused (so the
        caller can skip enqueuing them).
        """
        overrides = self.list_tasks()
        paused: list[str] = []
        for config in configs:
            override = overrides.get(config.name)
            if override is None:
                continue
            for field, value in override.as_patch().items():
                if hasattr(config, field):
                    setattr(config, field, value)
            if override.paused:
                paused.append(config.name)
        return paused

    def apply_queue_overrides(
        self, queue_configs: dict[str, dict[str, Any]]
    ) -> dict[str, dict[str, Any]]:
        """Merge queue overrides into ``queue_configs``. Returns the merged
        dict (a copy)."""
        merged: dict[str, dict[str, Any]] = {k: dict(v) for k, v in queue_configs.items()}
        for queue_name, override in self.list_queues().items():
            slot = merged.setdefault(queue_name, {})
            if override.rate_limit is not None:
                slot["rate_limit"] = override.rate_limit
            if override.max_concurrent is not None:
                slot["max_concurrent"] = override.max_concurrent
            if override.paused:
                slot["paused"] = True
        return merged
