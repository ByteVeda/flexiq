"""Thread-local context for the currently executing job."""

from __future__ import annotations

import enum
import json
import logging
import threading
import time
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from flexiq._active_context import _ActiveContext
from flexiq.async_support.context import get_async_context
from flexiq.exceptions import SoftTimeoutError, TaskCancelledError
from flexiq.steps import StepContext, StepSwallowedError, was_swallowed

logger = logging.getLogger("flexiq.context")


class LogLevel(str, enum.Enum):
    """Severity levels for structured task logs written via :meth:`JobContext.log`."""

    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"
    RESULT = "result"


if TYPE_CHECKING:
    from flexiq._flexiq import AttachedSteps, WorkerSteps
    from flexiq.app import Queue

_local = threading.local()
_queue_ref: Queue | None = None

# Optional in-process cancel signal. When set, ``check_cancelled()`` consults
# this hook before falling back to storage. The prefork child installs one
# that reads a local cancel set populated by the IPC reader thread; the
# default (None) preserves storage-backed behaviour for the thread pool.
_local_cancel_check: Callable[[str], bool] | None = None


def set_local_cancel_check(fn: Callable[[str], bool]) -> None:
    """Install a process-local cancel check used by ``check_cancelled()``.

    The callable receives the current ``job_id`` and returns ``True`` if the
    job has been cancelled. Installed by out-of-process workers (prefork)
    that receive cancel signals over IPC and want ``check_cancelled()`` to
    react without polling storage.
    """
    global _local_cancel_check
    _local_cancel_check = fn


def clear_local_cancel_check() -> None:
    """Remove a previously installed local cancel check."""
    global _local_cancel_check
    _local_cancel_check = None


class JobContext:
    """Provides access to the currently executing job's metadata and controls.

    Usage inside a task::

        from flexiq.context import current_job

        @queue.task()
        def process(data):
            current_job.update_progress(50)
            current_job.log("Processing started", level=LogLevel.INFO)
            current_job.check_cancelled()  # raises TaskCancelledError if cancelled
            current_job.check_timeout()    # raises SoftTimeoutError if exceeded
            ...
            current_job.update_progress(100)
    """

    @property
    def id(self) -> str:
        """The ID of the currently executing job."""
        return self._require_context().job_id

    @property
    def task_name(self) -> str:
        """The name of the currently executing task."""
        return self._require_context().task_name

    @property
    def retry_count(self) -> int:
        """How many times this job has been retried."""
        return self._require_context().retry_count

    @property
    def queue_name(self) -> str:
        """The queue this job is running on."""
        return self._require_context().queue_name

    @property
    def namespace(self) -> str | None:
        """The tenant namespace this job belongs to, if any."""
        return self._require_context().namespace

    @property
    def step(self) -> StepContext:
        """Durable inline steps for this job — see :mod:`flexiq.steps`.

        Opened lazily and held on the active context, so the session it needs
        is read once per attempt and only for tasks that actually take a step.
        """
        ctx = self._require_context()
        if ctx.step_context is None:
            ctx.step_context = StepContext(ctx, _queue_ref)
        step_context: StepContext = ctx.step_context
        return step_context

    def update_progress(self, progress: int) -> None:
        """Update the job's progress (0-100)."""
        ctx = self._require_context()
        if _queue_ref is None:
            raise RuntimeError("Queue reference not set. Cannot update progress.")
        _queue_ref._inner.update_progress(ctx.job_id, progress)

    def log(
        self,
        message: str,
        level: LogLevel = LogLevel.INFO,
        extra: dict | None = None,
    ) -> None:
        """Write a structured log entry for this job.

        Args:
            message: The log message.
            level: A :class:`LogLevel` member (e.g. ``LogLevel.INFO``).
            extra: Optional dict of structured data to attach.
        """
        ctx = self._require_context()
        if _queue_ref is None:
            raise RuntimeError("Queue reference not set. Cannot write log.")
        if extra:
            try:
                extra_str = json.dumps(extra)
            except (TypeError, ValueError):
                logger.warning("Non-serializable extra dict in task log, falling back to str()")
                extra_str = str(extra)
        else:
            extra_str = None
        _queue_ref._inner.write_task_log(
            ctx.job_id, ctx.task_name, level.value, message, extra_str
        )

    def publish(self, data: Any) -> None:
        """Publish a partial result visible to ``job.stream()`` consumers.

        Use this to stream intermediate results from long-running tasks
        (e.g. batch processing, ETL pipelines, ML training steps).

        Args:
            data: Any JSON-serializable value. Stored as a task log entry
                with level :attr:`LogLevel.RESULT` so it can be filtered from
                regular logs.
        """
        ctx = self._require_context()
        if _queue_ref is None:
            raise RuntimeError("Queue reference not set.")
        try:
            extra_str = json.dumps(data)
        except (TypeError, ValueError):
            extra_str = str(data)
        _queue_ref._inner.write_task_log(
            ctx.job_id, ctx.task_name, LogLevel.RESULT.value, "", extra_str
        )

    def check_cancelled(self) -> None:
        """Check if cancellation has been requested for this job.

        Raises:
            TaskCancelledError: If the job has been marked for cancellation.
        """
        ctx = self._require_context()
        # Fast path: a worker that received an out-of-band cancel signal
        # (e.g. the prefork child via IPC) installs a local check so we can
        # observe the cancel without a storage round-trip.
        local_check = _local_cancel_check
        if local_check is not None and local_check(ctx.job_id):
            raise TaskCancelledError(f"Job {ctx.job_id} was cancelled")
        if _queue_ref is None:
            raise RuntimeError("Queue reference not set.")
        if _queue_ref._inner.is_cancel_requested(ctx.job_id):
            raise TaskCancelledError(f"Job {ctx.job_id} was cancelled")

    def check_timeout(self) -> None:
        """Check if the soft timeout has been exceeded.

        Raises:
            SoftTimeoutError: If the soft timeout has elapsed.
        """
        ctx = self._require_context()
        if ctx.soft_timeout is not None and ctx.started_mono is not None:
            elapsed = time.monotonic() - ctx.started_mono
            if elapsed > ctx.soft_timeout:
                raise SoftTimeoutError(
                    f"Soft timeout exceeded: {elapsed:.1f}s > {ctx.soft_timeout}s"
                )

    def _check_step_control(self) -> None:
        """Fail the attempt if the body caught a step control signal and returned.

        The second of the two swallow layers. A sleep or a divergence that the
        task body catches away leaves the rest of the body running with no
        execution claim, so every side effect it has after that point happens
        again on the next attempt.

        Raises:
            StepSwallowedError: A control signal was raised and never left the body.
        """
        if was_swallowed(self._active_context()):
            raise StepSwallowedError(
                "step control flow was swallowed by the task body: a step raised to end "
                "this attempt and the task caught it. Let StepControlSignal propagate — "
                "narrow the except clause, or re-raise it."
            )

    def _finish_steps(self) -> None:
        """Close out this attempt's steps, if it took any. Never raises."""
        ctx = self._active_context()
        if ctx is not None and ctx.step_context is not None:
            ctx.step_context.finish()

    def _set_soft_timeout(self, seconds: float) -> None:
        """Set the soft timeout on the current context (called by wrapper)."""
        ctx = self._require_context()
        ctx.soft_timeout = seconds

    @staticmethod
    def _active_context() -> _ActiveContext | None:
        """The running job's context, or ``None`` outside a task.

        Contextvar first (async tasks on the native executor), thread-local
        second (sync tasks). The order matters: a pool thread's thread-local
        outlives the task that set it, so checking it first would hand an async
        task the previous sync task's job.
        """
        ctx = get_async_context()
        if ctx is not None:
            return ctx
        sync_ctx: _ActiveContext | None = getattr(_local, "context", None)
        return sync_ctx

    @staticmethod
    def _require_context() -> _ActiveContext:
        ctx = JobContext._active_context()
        if ctx is None:
            raise RuntimeError(
                "No active job context. current_job can only be used inside a running task."
            )
        return ctx


def _set_queue_ref(queue: Any) -> None:
    """Store the Queue instance at module level for use by job context."""
    global _queue_ref
    _queue_ref = queue


def _get_queue_ref() -> Any:
    """The Queue the job context resolves against, or ``None`` outside a worker."""
    return _queue_ref


def _set_context(
    job_id: str,
    task_name: str,
    retry_count: int,
    queue_name: str,
    namespace: str | None = None,
    worker_steps: WorkerSteps | AttachedSteps | None = None,
) -> None:
    """Set the thread-local job context. Called from Rust worker before each task.

    ``worker_steps`` is how this attempt writes durable steps: the dispatching
    worker's own handle, or an attached executor's channel to the scheduler that
    holds the claim. Absent where neither exists — a task under ``test_mode`` —
    and durable steps refuse there rather than run un-memoized.
    """
    _local.context = _ActiveContext(
        job_id=job_id,
        task_name=task_name,
        retry_count=retry_count,
        queue_name=queue_name,
        namespace=namespace,
        worker_steps=worker_steps,
    )


def _clear_context() -> None:
    """Clear the thread-local job context. Called from Rust worker after each task."""
    _local.context = None


current_job = JobContext()
