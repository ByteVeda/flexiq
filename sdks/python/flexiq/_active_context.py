"""Private leaf module for the active-context data class.

Lives outside ``flexiq.context`` and ``flexiq.async_support.context`` so
those two modules can both import it without forming a cycle. ``context.py``
needs to call into ``async_support.context`` at runtime to resolve the
async context first; ``async_support.context`` needs the ``_ActiveContext``
type. Hosting the type here breaks the loop without relying on inline imports.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from flexiq._flexiq import WorkerSteps


class _ActiveContext:
    __slots__ = (
        "job_id",
        "namespace",
        "queue_name",
        "retry_count",
        "soft_timeout",
        "started_mono",
        "step_context",
        "step_control_raised",
        "task_name",
        "worker_steps",
    )

    def __init__(
        self,
        job_id: str,
        task_name: str,
        retry_count: int,
        queue_name: str,
        namespace: str | None = None,
        worker_steps: WorkerSteps | None = None,
    ):
        self.job_id = job_id
        self.task_name = task_name
        self.retry_count = retry_count
        self.queue_name = queue_name
        self.namespace = namespace
        # The running worker's own step handle, fenced on the claim that worker
        # won. It travels with the dispatch rather than sitting on the queue,
        # which one process may run several workers from.
        self.worker_steps = worker_steps
        self.started_mono: float | None = time.monotonic()
        self.soft_timeout: float | None = None
        # Durable steps, opened on first use: the session costs a job read and
        # a snapshot read, and most tasks never take one.
        self.step_context: Any = None
        # Set when ``ctx.step`` raises a control signal out of the task body.
        # The runner fails an attempt that returns normally with it set — the
        # second of the two layers that keep a sleep from being swallowed.
        self.step_control_raised: bool = False
