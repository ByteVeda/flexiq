"""Private leaf module for the active-context data class.

Lives outside ``flexiq.context`` and ``flexiq.async_support.context`` so
those two modules can both import it without forming a cycle. ``context.py``
needs to call into ``async_support.context`` at runtime to resolve the
async context first; ``async_support.context`` needs the ``_ActiveContext``
type. Hosting the type here breaks the loop without relying on inline imports.
"""

from __future__ import annotations

import time


class _ActiveContext:
    __slots__ = (
        "job_id",
        "queue_name",
        "retry_count",
        "soft_timeout",
        "started_mono",
        "task_name",
    )

    def __init__(
        self,
        job_id: str,
        task_name: str,
        retry_count: int,
        queue_name: str,
    ):
        self.job_id = job_id
        self.task_name = task_name
        self.retry_count = retry_count
        self.queue_name = queue_name
        self.started_mono: float | None = time.monotonic()
        self.soft_timeout: float | None = None
