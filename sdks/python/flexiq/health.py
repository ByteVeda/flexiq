"""Shared health and readiness check logic for flexiq.

Used by both the built-in dashboard and FastAPI integration to avoid duplication.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from flexiq.app import Queue


def check_health() -> dict[str, str]:
    """Basic liveness check — always returns ok."""
    return {"status": "ok"}


def check_readiness(queue: Queue) -> dict[str, Any]:
    """Readiness check — verifies storage is accessible and workers are alive."""
    checks: dict[str, Any] = {}
    all_ok = True

    # Check storage accessibility
    try:
        queue.stats()
        checks["storage"] = "ok"
    except Exception as e:
        checks["storage"] = f"error: {e}"
        all_ok = False

    # Check active workers
    try:
        workers = queue.workers()
        checks["workers"] = {"count": len(workers), "status": "ok" if workers else "none"}
    except Exception as e:
        checks["workers"] = f"error: {e}"
        all_ok = False

    # Check resource health
    try:
        resources = queue.resource_status()
        if resources:
            unhealthy = [r["name"] for r in resources if r["health"] != "healthy"]
            checks["resources"] = {
                "count": len(resources),
                "unhealthy": unhealthy,
                "status": "degraded" if unhealthy else "ok",
            }
            if unhealthy:
                all_ok = False
    except Exception as e:
        checks["resources"] = f"error: {e}"
        all_ok = False

    return {
        "status": "ready" if all_ok else "degraded",
        "checks": checks,
    }
