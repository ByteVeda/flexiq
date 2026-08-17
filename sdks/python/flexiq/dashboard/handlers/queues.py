"""Per-queue stats route handlers."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from flexiq.app import Queue


def _handle_stats_queues(queue: Queue, qs: dict) -> dict:
    q_name = qs.get("queue", [None])[0]
    if q_name:
        return queue.stats_by_queue(q_name)
    return queue.stats_all_queues()
