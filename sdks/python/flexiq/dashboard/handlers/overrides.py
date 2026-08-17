"""Task & queue override endpoints."""

from __future__ import annotations

from dataclasses import asdict
from typing import TYPE_CHECKING, Any

from flexiq.dashboard.errors import _BadRequest, _NotFound
from flexiq.dashboard.overrides_store import (
    QUEUE_OVERRIDE_FIELDS,
    TASK_OVERRIDE_FIELDS,
    OverridesStore,
)

if TYPE_CHECKING:
    from flexiq.app import Queue


def handle_list_tasks(queue: Queue, _qs: dict) -> list[dict[str, Any]]:
    """Return every registered task with decorator defaults + active override."""
    return queue.registered_tasks()


def handle_list_queues(queue: Queue, _qs: dict) -> list[dict[str, Any]]:
    return queue.registered_queues()


def _coerce_override_body(body: Any, allowed: frozenset[str]) -> dict[str, Any]:
    if not isinstance(body, dict):
        raise _BadRequest("body must be a JSON object")
    unknown = set(body) - allowed
    if unknown:
        raise _BadRequest(
            f"unknown override fields: {sorted(unknown)}; allowed: {sorted(allowed)}"
        )
    return body


# ── Task override endpoints ───────────────────────────────────────────


def handle_get_task_override(queue: Queue, _qs: dict, task_name: str) -> dict[str, Any]:
    override = OverridesStore(queue).get_task(task_name)
    if override is None:
        raise _NotFound(f"no override set for task '{task_name}'")
    return asdict(override)


def handle_put_task_override(queue: Queue, body: dict, task_name: str) -> dict[str, Any]:
    fields = _coerce_override_body(body, TASK_OVERRIDE_FIELDS)
    try:
        override = OverridesStore(queue).set_task(task_name, fields)
    except ValueError as e:
        raise _BadRequest(str(e)) from None
    return asdict(override)


def handle_delete_task_override(queue: Queue, task_name: str) -> dict[str, bool]:
    removed = OverridesStore(queue).clear_task(task_name)
    return {"cleared": removed}


# ── Queue override endpoints ──────────────────────────────────────────


def handle_get_queue_override(queue: Queue, _qs: dict, queue_name: str) -> dict[str, Any]:
    override = OverridesStore(queue).get_queue(queue_name)
    if override is None:
        raise _NotFound(f"no override set for queue '{queue_name}'")
    return asdict(override)


def handle_put_queue_override(queue: Queue, body: dict, queue_name: str) -> dict[str, Any]:
    fields = _coerce_override_body(body, QUEUE_OVERRIDE_FIELDS)
    try:
        override = OverridesStore(queue).set_queue(queue_name, fields)
    except ValueError as e:
        raise _BadRequest(str(e)) from None
    # Reflect "paused" immediately by touching the paused_queues store
    # (this state DOES propagate to a running worker — independent of the
    # static override consumed at worker startup).
    if "paused" in fields:
        try:
            if fields["paused"]:
                queue.pause(queue_name)
            else:
                queue.resume(queue_name)
        except Exception:  # pragma: no cover - safety net only
            pass
    return asdict(override)


def handle_delete_queue_override(queue: Queue, queue_name: str) -> dict[str, bool]:
    removed = OverridesStore(queue).clear_queue(queue_name)
    return {"cleared": removed}
