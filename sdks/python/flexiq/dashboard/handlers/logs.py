"""Log query route handlers."""

from __future__ import annotations

from typing import TYPE_CHECKING

from flexiq.context import LogLevel
from flexiq.dashboard.handlers._qs import _parse_int_qs

if TYPE_CHECKING:
    from flexiq.app import Queue


def _handle_logs(queue: Queue, qs: dict) -> list:
    task = qs.get("task", [None])[0]
    level_raw = qs.get("level", [None])[0]
    level = LogLevel(level_raw) if level_raw is not None else None
    since = _parse_int_qs(qs, "since", 3600)
    limit = _parse_int_qs(qs, "limit", 100)
    return queue.query_logs(task_name=task, level=level, since=since, limit=limit)
