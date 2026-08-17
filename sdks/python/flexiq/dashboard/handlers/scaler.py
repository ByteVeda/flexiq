"""KEDA scaler payload assembly."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from flexiq.app import Queue


logger = logging.getLogger("flexiq.dashboard")


def build_scaler_response(
    queue: Queue,
    queue_name: str | None = None,
    target_queue_depth: int = 10,
) -> dict[str, Any]:
    """Build KEDA-compatible scaler payload for a queue."""
    stats = queue.stats()
    depth = stats.get("pending", 0)
    running = stats.get("running", 0)

    worker_list = queue.workers()
    live_workers = len(worker_list)
    total_capacity = queue._workers

    response: dict[str, Any] = {
        "metricName": "flexiq_queue_depth",
        "metricValue": depth,
        "isActive": depth > 0,
        "liveWorkers": live_workers,
        "totalCapacity": total_capacity,
        "targetQueueDepth": target_queue_depth,
    }

    if total_capacity > 0:
        response["workerUtilization"] = round(running / total_capacity, 3)

    if queue_name:
        q_stats = queue.stats_by_queue(queue_name)
        response["metricValue"] = q_stats.get("pending", 0)
        response["isActive"] = q_stats.get("pending", 0) > 0
        response["metricName"] = f"flexiq_queue_depth_{queue_name}"

    try:
        all_q = queue.stats_all_queues()
        response["perQueue"] = {
            name: {"pending": s.get("pending", 0), "running": s.get("running", 0)}
            for name, s in all_q.items()
        }
    except Exception:
        logger.warning("Failed to collect per-queue stats for scaler", exc_info=True)

    return response
