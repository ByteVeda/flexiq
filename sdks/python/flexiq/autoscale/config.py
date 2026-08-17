"""Configuration for the bare-metal autoscaler.

Defaults are chosen to mirror Kubernetes HPA semantics where possible:

* ``scale_down_window_sec=300`` matches HPA's
  ``--horizontal-pod-autoscaler-downscale-stabilization`` default of 5
  minutes. Scaling down too aggressively under noisy queue depth is the
  textbook autoscaler failure mode.
* ``tolerance=0.1`` matches HPA's 10% tolerance: small fluctuations
  don't churn process counts.
* ``scale_up_window_sec=0`` lets us absorb burst traffic immediately —
  the cost of an extra worker for a few seconds is much lower than the
  cost of a backlog growing while we wait.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class AutoscaleConfig:
    """Tunables for ``AutoscaleController``.

    Attributes:
        app_path: Python import path to the Queue instance — passed to
            spawned worker subprocesses as ``--app pkg.mod:queue``. Must
            be importable from the spawned process's PYTHONPATH.
        min_workers: Never scale below this count (must be >= 0).
        max_workers: Never scale above this count (must be >= min_workers
            and >= 1).
        target_queue_depth_per_worker: Target pending jobs per worker —
            the depth-signal formula divides ``pending`` by this number.
            Lower values scale up sooner; higher values absorb spikes.
        target_utilisation: Target ``running / capacity`` ratio for the
            utilisation signal. ``capacity = current_workers * threads_per_worker``.
            Must be in ``(0, 1)``.
        scale_up_window_sec: Aggregation window for scale-up decisions
            (seconds). Set to 0 for immediate scale-up. Within the window
            we take the *minimum* recent recommendation (most aggressive
            scale-up to drain the queue fast).
        scale_down_window_sec: Same idea, but for scale-down. We take the
            *maximum* recent recommendation, so a brief lull doesn't
            trigger a tear-down. Default mirrors Kubernetes HPA's 5-minute
            stabilisation window.
        tolerance: Skip scaling if the desired count is within this
            fractional delta from the current count (default 10%).
        poll_interval_sec: How often to gather metrics and tick the
            decision loop.
        drain_timeout_sec: Per-worker drain budget — passed as
            ``--drain-timeout`` to spawned workers AND used as the
            ProcessManager's SIGTERM grace period.
        threads_per_worker: Concurrency configured on each worker. The
            controller can't read this from the worker itself, so the
            operator declares it here to match the ``workers=`` value on
            ``Queue()`` (or the matching CLI flag).
    """

    app_path: str
    min_workers: int = 1
    max_workers: int = 10
    target_queue_depth_per_worker: int = 15
    target_utilisation: float = 0.75
    scale_up_window_sec: int = 0
    scale_down_window_sec: int = 300
    tolerance: float = 0.1
    poll_interval_sec: int = 5
    drain_timeout_sec: int = 30
    threads_per_worker: int = 4

    def __post_init__(self) -> None:
        if not self.app_path:
            raise ValueError("app_path is required")
        if self.min_workers < 0:
            raise ValueError(f"min_workers must be >= 0, got {self.min_workers}")
        if self.max_workers < 1:
            raise ValueError(f"max_workers must be >= 1, got {self.max_workers}")
        if self.max_workers < self.min_workers:
            raise ValueError(
                f"max_workers ({self.max_workers}) must be >= min_workers ({self.min_workers})"
            )
        if self.target_queue_depth_per_worker < 1:
            depth = self.target_queue_depth_per_worker
            raise ValueError(f"target_queue_depth_per_worker must be >= 1, got {depth}")
        if not (0.0 < self.target_utilisation < 1.0):
            raise ValueError(
                f"target_utilisation must be in (0, 1), got {self.target_utilisation}"
            )
        if self.scale_up_window_sec < 0:
            raise ValueError(f"scale_up_window_sec must be >= 0, got {self.scale_up_window_sec}")
        if self.scale_down_window_sec < 0:
            raise ValueError(
                f"scale_down_window_sec must be >= 0, got {self.scale_down_window_sec}"
            )
        if not (0.0 <= self.tolerance < 1.0):
            raise ValueError(f"tolerance must be in [0, 1), got {self.tolerance}")
        if self.poll_interval_sec < 1:
            raise ValueError(f"poll_interval_sec must be >= 1, got {self.poll_interval_sec}")
        if self.drain_timeout_sec < 1:
            raise ValueError(f"drain_timeout_sec must be >= 1, got {self.drain_timeout_sec}")
        if self.threads_per_worker < 1:
            raise ValueError(f"threads_per_worker must be >= 1, got {self.threads_per_worker}")
