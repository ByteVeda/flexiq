"""Decision loop for the bare-metal autoscaler.

The HPA-style formula and stabilisation windows live here. Process
management is delegated to ``ProcessManager``; this module is concerned
with *how many* workers we want, not with spawning them.

The pure-function ``compute_desired_workers`` is the heart of the
decision logic — it has no side effects and no subprocess interaction,
so it's exhaustively tested in isolation.
"""

from __future__ import annotations

import collections
import logging
import math
import threading
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

from flexiq.autoscale.config import AutoscaleConfig
from flexiq.autoscale.process_manager import ProcessManager, install_signal_handlers

if TYPE_CHECKING:
    from flexiq.app import Queue


logger = logging.getLogger("flexiq.autoscale")


@dataclass(frozen=True)
class ScaleDecision:
    """One tick of the decision loop, exposed for logging / tests."""

    pending: int
    running: int
    current_workers: int
    desired_workers: int
    rationale: str


def compute_desired_workers(
    *,
    pending: int,
    running: int,
    current_workers: int,
    config: AutoscaleConfig,
) -> ScaleDecision:
    """Pure HPA-style decision function.

    Two signals drive the target:

    * **depth_desired** — ``ceil(pending / target_queue_depth_per_worker)``.
      Closely tracks Kubernetes HPA's queue-depth scaler: every extra
      worker takes one ``target`` slice of pending jobs off the queue.

    * **util_desired** — ``ceil(current_workers * (utilisation /
      target_utilisation))``. The canonical HPA formula. When utilisation
      sits exactly at the target, this returns ``current_workers``.

    The maximum of the two signals is taken so the controller errs on
    the side of more capacity. Overload (running > capacity) forces an
    immediate +1 regardless of windowing.

    The tolerance band (default 10%) is applied last: if the desired
    count is within tolerance of current, return current unchanged so
    we don't churn processes for noise.
    """
    cw = max(0, current_workers)
    capacity = max(1, cw * config.threads_per_worker)

    # Depth signal: how many workers does the queue depth suggest?
    depth_desired = math.ceil(pending / config.target_queue_depth_per_worker)

    # Utilisation signal: HPA formula. When utilisation is 0 (no running jobs)
    # we treat util_desired as 0 — the depth signal and min_workers floor
    # decide. This lets a fully-idle pool scale all the way down to min.
    utilisation = running / capacity if capacity > 0 else 0.0
    if cw > 0 and utilisation > 0:
        util_desired = math.ceil(cw * (utilisation / config.target_utilisation))
    else:
        util_desired = 0

    # Overload override: jobs are running on more slots than we have
    # capacity for (worker just claimed work but its threads are saturated).
    # Bypass tolerance and bump up by one.
    overloaded = cw > 0 and running > capacity

    desired = max(depth_desired, util_desired, config.min_workers)
    if overloaded:
        desired = max(desired, cw + 1)
    desired = min(desired, config.max_workers)
    desired = max(desired, config.min_workers)

    # Tolerance band — skip churn if delta is small.
    if cw > 0 and not overloaded:
        delta_ratio = abs(desired - cw) / cw
        if delta_ratio < config.tolerance:
            desired = cw

    if desired > cw:
        rationale = f"scale-up: depth={depth_desired} util={util_desired} overload={overloaded}"
    elif desired < cw:
        rationale = f"scale-down: depth={depth_desired} util={util_desired}"
    else:
        rationale = (
            f"stable: depth={depth_desired} util={util_desired} util_ratio={utilisation:.2f}"
        )

    return ScaleDecision(
        pending=pending,
        running=running,
        current_workers=cw,
        desired_workers=desired,
        rationale=rationale,
    )


class AutoscaleController:
    """Main loop: poll metrics, decide, scale, sleep, repeat.

    Honours stabilisation windows on both scale-up and scale-down by
    buffering recent recommendations in a deque and taking the
    "least aggressive" value (max for down, min for up).
    """

    def __init__(self, queue: Queue, config: AutoscaleConfig) -> None:
        self._queue = queue
        self._config = config
        self._pm = ProcessManager(
            app_path=config.app_path,
            drain_timeout_sec=config.drain_timeout_sec,
        )
        self._stop_event = threading.Event()
        # Per-direction recommendation history, used by the stabilisation
        # windows. Each entry is (monotonic_time, desired).
        self._up_history: collections.deque[tuple[float, int]] = collections.deque()
        self._down_history: collections.deque[tuple[float, int]] = collections.deque()

    @property
    def process_manager(self) -> ProcessManager:
        return self._pm

    def tick(self) -> ScaleDecision:
        """Run one decision cycle. Returns the decision for logging / tests."""
        pending, running = self._gather_metrics()
        current = self._pm.count_live()
        # Replace any crashed workers within min/max bounds.
        crashed = self._pm.reap_dead()
        for _ in crashed:
            if self._pm.count_live() < self._config.min_workers:
                self._pm.spawn_worker()
        # Recompute current after reaping / replacing.
        current = self._pm.count_live()
        decision = compute_desired_workers(
            pending=pending,
            running=running,
            current_workers=current,
            config=self._config,
        )
        smoothed = self._apply_windows(current, decision.desired_workers)
        if smoothed != decision.desired_workers:
            decision = ScaleDecision(
                pending=decision.pending,
                running=decision.running,
                current_workers=current,
                desired_workers=smoothed,
                rationale=f"{decision.rationale} (windowed -> {smoothed})",
            )
        self._apply_decision(current, smoothed)
        return decision

    def serve_forever(self) -> None:
        """Block on the decision loop until SIGTERM / SIGINT arrives."""
        install_signal_handlers(self._stop_event.set)
        # Seed initial worker count at min_workers so we don't sit at 0
        # waiting for the first burst to be observed.
        for _ in range(self._config.min_workers):
            self._pm.spawn_worker()
        logger.info(
            "autoscale: started (min=%s max=%s target_depth=%s target_util=%s)",
            self._config.min_workers,
            self._config.max_workers,
            self._config.target_queue_depth_per_worker,
            self._config.target_utilisation,
        )
        try:
            while not self._stop_event.is_set():
                try:
                    decision = self.tick()
                    logger.info(
                        "autoscale: pending=%s running=%s workers=%s -> %s (%s)",
                        decision.pending,
                        decision.running,
                        decision.current_workers,
                        decision.desired_workers,
                        decision.rationale,
                    )
                except Exception:
                    logger.exception("autoscale: tick failed")
                self._stop_event.wait(self._config.poll_interval_sec)
        finally:
            logger.info("autoscale: draining %s workers", self._pm.count_live())
            self._pm.shutdown()

    # ── Internals ────────────────────────────────────────────────────

    def _gather_metrics(self) -> tuple[int, int]:
        try:
            stats = self._queue.stats()
            return (int(stats.get("pending", 0)), int(stats.get("running", 0)))
        except Exception:
            logger.exception("autoscale: failed to gather queue stats")
            return (0, 0)

    def _apply_windows(self, current: int, desired: int) -> int:
        """Smooth the desired count through scale-up / scale-down windows.

        Scale up: take the *minimum* recent recommendation so we
        scale up as soon as any recent tick agreed we should.
        Scale down: take the *maximum* recent recommendation so a brief
        lull doesn't tear workers down.
        """
        now = time.monotonic()
        # Evict stale entries from both histories.
        up_cutoff = now - self._config.scale_up_window_sec
        down_cutoff = now - self._config.scale_down_window_sec
        while self._up_history and self._up_history[0][0] < up_cutoff:
            self._up_history.popleft()
        while self._down_history and self._down_history[0][0] < down_cutoff:
            self._down_history.popleft()

        if desired > current:
            self._up_history.append((now, desired))
            return min(d for _, d in self._up_history)
        if desired < current:
            self._down_history.append((now, desired))
            return max(d for _, d in self._down_history)
        return current

    def _apply_decision(self, current: int, desired: int) -> None:
        if desired > current:
            for _ in range(desired - current):
                self._pm.spawn_worker()
        elif desired < current:
            pids = self._pm.live_pids()
            to_kill = pids[: current - desired]
            for pid in to_kill:
                threading.Thread(
                    target=self._pm.terminate_worker, args=(pid,), daemon=True
                ).start()


def serve_autoscaler(queue: Queue, config: AutoscaleConfig) -> None:
    """Convenience entry point — construct controller and run forever."""
    AutoscaleController(queue, config).serve_forever()
