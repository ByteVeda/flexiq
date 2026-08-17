"""Catalog management for registered periodic (cron-scheduled) tasks."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class PeriodicInfo:
    """One registered periodic schedule.

    ``last_run`` is unset until the schedule first fires. ``last_run`` and
    ``next_run`` are Unix milliseconds. ``timezone`` is whatever the schedule
    was registered with, so it is ``None`` only when none was given — which
    means UTC, the same as an explicit ``"UTC"``.
    """

    name: str
    task_name: str
    cron_expr: str
    queue: str
    enabled: bool
    last_run: int | None
    next_run: int
    timezone: str | None


class QueuePeriodicMixin:
    """List, unschedule, pause, and resume registered periodic tasks.

    :meth:`~flexiq.Queue.periodic` declares a schedule; the declarations are
    written to storage when ``run_worker()`` starts. These methods act on what
    is already in storage, so a schedule outlives the decorator that created it
    — dropping the decorator leaves the row behind, still firing. Retire one
    with :meth:`delete_periodic`.
    """

    _inner: Any

    def list_periodic(self) -> list[PeriodicInfo]:
        """Every registered periodic task, enabled or paused."""
        return [PeriodicInfo(**row) for row in self._inner.list_periodic()]

    def delete_periodic(self, name: str) -> bool:
        """Unschedule a periodic task. False if no schedule had that name."""
        return bool(self._inner.delete_periodic(name))

    def pause_periodic(self, name: str) -> bool:
        """Stop a periodic task from firing without unscheduling it.

        False if no schedule had that name.
        """
        return bool(self._inner.pause_periodic(name))

    def resume_periodic(self, name: str) -> bool:
        """Resume a paused periodic task. False if no schedule had that name."""
        return bool(self._inner.resume_periodic(name))
