"""Bare-metal autoscaler for flexiq worker processes.

This package implements an in-process control loop that spawns / drains
``flexiq worker`` subprocesses to track queue depth and utilisation.
Targets users running on bare metal, Docker, or systemd (i.e. anyone
who can't use KEDA + Kubernetes).

The decision formula mirrors Kubernetes HPA::

    depth_desired = ceil(pending_jobs / target_queue_depth_per_worker)
    util_desired  = ceil(current_workers * (utilisation / target_utilisation))
    desired       = clamp(max(depth_desired, util_desired),
                          min_workers, max_workers)

Stabilisation windows protect against flapping: scale-up is fast by
default (window 0) but scale-down waits 5 minutes (matches HPA default).
A tolerance band (default 10%) suppresses single-tick noise.

Usage (programmatic)::

    from flexiq import Queue
    from flexiq.autoscale import AutoscaleConfig, serve_autoscaler

    queue = Queue()
    serve_autoscaler(queue, AutoscaleConfig(app_path="myapp:queue"))

Usage (CLI)::

    flexiq autoscale --app myapp:queue --min-workers 1 --max-workers 10
"""

from __future__ import annotations

from flexiq.autoscale.config import AutoscaleConfig
from flexiq.autoscale.controller import (
    AutoscaleController,
    ScaleDecision,
    compute_desired_workers,
    serve_autoscaler,
)
from flexiq.autoscale.process_manager import ProcessManager

__all__ = [
    "AutoscaleConfig",
    "AutoscaleController",
    "ProcessManager",
    "ScaleDecision",
    "compute_desired_workers",
    "serve_autoscaler",
]
