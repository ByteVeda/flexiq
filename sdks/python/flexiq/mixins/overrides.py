"""Task & queue runtime override management on :class:`flexiq.app.Queue`.

These knobs let operators tune retry policy, concurrency caps, rate
limits, timeouts, priority, and pause/resume state without touching
code. Overrides land in the dashboard settings store and apply on the
next worker startup.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from flexiq.dashboard.overrides_store import (
    OverridesStore,
    QueueOverride,
    TaskOverride,
)

if TYPE_CHECKING:
    from flexiq._flexiq import PyTaskConfig


class QueueOverridesMixin:
    """CRUD for task + queue overrides, plus a task-discovery API for the UI."""

    _task_configs: list[PyTaskConfig]
    _queue_configs: dict[str, dict[str, Any]]

    # ── Task overrides ─────────────────────────────────────────────

    def list_task_overrides(self) -> dict[str, TaskOverride]:
        """Return every persisted task override keyed by task name."""
        return OverridesStore(self).list_tasks()  # type: ignore[arg-type]

    def get_task_override(self, task_name: str) -> TaskOverride | None:
        return OverridesStore(self).get_task(task_name)  # type: ignore[arg-type]

    def set_task_override(self, task_name: str, **fields: Any) -> TaskOverride:
        """Set or update an override. Pass ``None`` for a field to clear it.

        Allowed fields: ``rate_limit``, ``max_concurrent``, ``max_retries``,
        ``retry_backoff``, ``timeout``, ``priority``, ``paused``.
        """
        return OverridesStore(self).set_task(task_name, fields)  # type: ignore[arg-type]

    def clear_task_override(self, task_name: str) -> bool:
        return OverridesStore(self).clear_task(task_name)  # type: ignore[arg-type]

    # ── Queue overrides ────────────────────────────────────────────

    def list_queue_overrides(self) -> dict[str, QueueOverride]:
        return OverridesStore(self).list_queues()  # type: ignore[arg-type]

    def get_queue_override(self, queue_name: str) -> QueueOverride | None:
        return OverridesStore(self).get_queue(queue_name)  # type: ignore[arg-type]

    def set_queue_override(self, queue_name: str, **fields: Any) -> QueueOverride:
        """Set or update a queue override. Allowed fields: ``rate_limit``,
        ``max_concurrent``, ``paused``."""
        return OverridesStore(self).set_queue(queue_name, fields)  # type: ignore[arg-type]

    def clear_queue_override(self, queue_name: str) -> bool:
        return OverridesStore(self).clear_queue(queue_name)  # type: ignore[arg-type]

    # ── Task discovery (for the dashboard) ─────────────────────────

    def registered_tasks(self) -> list[dict[str, Any]]:
        """Return every registered task with its decorator defaults and any
        active override. Each entry contains:

        - ``name``, ``queue``, ``priority``
        - ``defaults``: the decorator-declared values
        - ``override``: the override fields (or ``None`` if no override exists)
        - ``effective``: the values that will be used on the next worker start
        """
        overrides = self.list_task_overrides()
        out: list[dict[str, Any]] = []
        for config in self._task_configs:
            defaults = {
                "max_retries": config.max_retries,
                "retry_backoff": config.retry_backoff,
                "timeout": config.timeout,
                "priority": config.priority,
                "rate_limit": config.rate_limit,
                "max_concurrent": config.max_concurrent,
            }
            override = overrides.get(config.name)
            override_dict: dict[str, Any] | None
            if override is None:
                override_dict = None
                effective = dict(defaults)
                paused = False
            else:
                patch = override.as_patch()
                override_dict = dict(patch)
                if override.paused:
                    override_dict["paused"] = True
                effective = {**defaults, **patch}
                paused = override.paused
            out.append(
                {
                    "name": config.name,
                    "queue": config.queue,
                    "defaults": defaults,
                    "override": override_dict,
                    "effective": effective,
                    "paused": paused,
                }
            )
        return out

    def registered_queues(self) -> list[dict[str, Any]]:
        """Return every queue mentioned by a task config plus any
        configured-from-Python queue, with its current overrides + paused
        state."""
        queue_names: set[str] = set()
        queue_names.update(self._queue_configs.keys())
        for config in self._task_configs:
            queue_names.add(config.queue)
        overrides = self.list_queue_overrides()
        paused_set = set(
            self.paused_queues()  # type: ignore[attr-defined]
        )
        out: list[dict[str, Any]] = []
        for name in sorted(queue_names):
            base = dict(self._queue_configs.get(name, {}))
            override = overrides.get(name)
            override_dict: dict[str, Any] | None
            if override is None:
                override_dict = None
                effective = dict(base)
            else:
                patch: dict[str, Any] = {}
                if override.rate_limit is not None:
                    patch["rate_limit"] = override.rate_limit
                if override.max_concurrent is not None:
                    patch["max_concurrent"] = override.max_concurrent
                override_dict = dict(patch)
                if override.paused:
                    override_dict["paused"] = True
                effective = {**base, **patch}
            out.append(
                {
                    "name": name,
                    "defaults": base,
                    "override": override_dict,
                    "effective": effective,
                    "paused": name in paused_set or (override.paused if override else False),
                }
            )
        return out
