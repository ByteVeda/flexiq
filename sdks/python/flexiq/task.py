"""Task wrapper providing .delay() and .apply_async() methods."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from flexiq.canvas import Signature
from flexiq.interception import InterceptionReport

if TYPE_CHECKING:
    from flexiq.app import Queue
    from flexiq.debounce import Duration
    from flexiq.result import JobResult


class TaskWrapper:
    """
    Wraps a decorated function to provide task submission methods.

    Created by ``@queue.task()`` — not instantiated directly.
    """

    def __init__(
        self,
        fn: Callable,
        queue_ref: Queue,
        task_name: str,
        default_priority: int,
        default_queue: str,
        default_max_retries: int,
        default_timeout: int,
        default_expires: float | None = None,
        inject: list[str] | None = None,
    ):
        self._fn = fn
        self._queue = queue_ref
        self._task_name = task_name
        self._default_priority = default_priority
        self._default_queue = default_queue
        self._default_max_retries = default_max_retries
        self._default_timeout = default_timeout
        self._default_expires = default_expires
        self._inject = inject or []
        self._flexiq_is_async: bool = False
        self._flexiq_async_fn: Callable | None = None

    @property
    def name(self) -> str:
        """The registered task name."""
        return self._task_name

    @property
    def default_max_retries(self) -> int:
        """The task's configured ``max_retries`` (used when a workflow step
        that references this task doesn't override it)."""
        return self._default_max_retries

    @property
    def inject(self) -> list[str]:
        """Resource names this task requests via dependency injection."""
        return self._inject

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """Call the underlying function directly (synchronous, not queued)."""
        return self._fn(*args, **kwargs)

    def delay(self, *args: Any, **kwargs: Any) -> JobResult:
        """Enqueue this task for background execution.

        Shorthand for :meth:`apply_async` using the task's default options.
        """
        return self._queue.enqueue(
            task_name=self._task_name,
            args=args,
            kwargs=kwargs if kwargs else None,
            priority=self._default_priority,
            queue=self._default_queue,
            max_retries=self._default_max_retries,
            timeout=self._default_timeout,
            expires=self._default_expires,
        )

    def apply_async(
        self,
        args: tuple = (),
        kwargs: dict | None = None,
        priority: int | None = None,
        delay: float | None = None,
        queue: str | None = None,
        max_retries: int | None = None,
        timeout: int | None = None,
        unique_key: str | None = None,
        metadata: str | None = None,
        notes: dict[str, Any] | None = None,
        depends_on: str | list[str] | None = None,
        expires: float | None = None,
        result_ttl: int | None = None,
        idempotency_key: str | None = None,
        idempotent: bool | None = None,
        debounce: Duration | None = None,
        debounce_key: str | None = None,
        debounce_max_wait: Duration | None = None,
        debounce_replace_payload: bool | None = None,
    ) -> JobResult:
        """Enqueue with full control over submission options.

        Args:
            args: Positional arguments to pass to the task.
            kwargs: Keyword arguments to pass to the task.
            priority: Override default priority (higher = more urgent).
            delay: Delay in seconds before the job becomes eligible to run.
            queue: Override the default queue name.
            max_retries: Override the default max retry count.
            timeout: Override the default timeout in seconds.
            unique_key: Deduplication key (alias of ``idempotency_key``).
            metadata: Arbitrary JSON string to attach to the job.
            notes: Structured annotations dict (≤ 15 top-level fields). See
                :mod:`flexiq.notes`.
            depends_on: Job ID or list of job IDs that must complete first.
            expires: Seconds until the job expires (skipped if not started by
                then). Overrides the task's ``expires=`` default.
            result_ttl: Per-job result TTL in seconds.
            idempotency_key: Explicit dedup key. A second submission with the
                same key while the first job is pending or running returns the
                existing job's ID.
            idempotent: ``True`` forces auto-derivation of a key from the
                serialized payload; ``False`` disables it for this call (useful
                when the task is registered with ``idempotent=True`` but a
                particular submission must run again).
            debounce: Debounce window for this submission — a duration string
                (``"5m"``) or a number of seconds. Requires ``debounce_key``
                and ``debounce_max_wait``.
            debounce_key: Format template resolved against ``args``/``kwargs``,
                e.g. ``"report:{user_id}"``.
            debounce_max_wait: Ceiling on the total delay, measured from when
                the window opened. Mandatory alongside ``debounce``.
            debounce_replace_payload: Overwrite the pending job's payload with
                this call's when the submission lands on an open window.

        Passing any ``debounce*`` option makes this call define its own window;
        passing none of them uses the window from ``@queue.task(debounce=...)``,
        if the task has one. A debounced submission cannot also carry ``delay``
        — the window owns the deadline.
        """
        return self._queue.enqueue(
            task_name=self._task_name,
            args=args,
            kwargs=kwargs,
            priority=priority if priority is not None else self._default_priority,
            delay=delay,
            queue=queue if queue is not None else self._default_queue,
            max_retries=max_retries if max_retries is not None else self._default_max_retries,
            timeout=timeout if timeout is not None else self._default_timeout,
            unique_key=unique_key,
            metadata=metadata,
            notes=notes,
            depends_on=depends_on,
            expires=expires if expires is not None else self._default_expires,
            result_ttl=result_ttl,
            idempotency_key=idempotency_key,
            idempotent=idempotent,
            debounce=debounce,
            debounce_key=debounce_key,
            debounce_max_wait=debounce_max_wait,
            debounce_replace_payload=debounce_replace_payload,
        )

    def map(self, iterable: list[tuple]) -> list[JobResult]:
        """Enqueue one job per item in a single batch transaction."""
        return self._queue.enqueue_many(
            task_name=self._task_name,
            args_list=iterable,
            priority=self._default_priority,
            queue=self._default_queue,
            max_retries=self._default_max_retries,
            timeout=self._default_timeout,
            expires=self._default_expires,
        )

    def s(self, *args: Any, **kwargs: Any) -> Signature:
        """Create a mutable :class:`~flexiq.canvas.Signature`."""
        return Signature(task=self, args=args, kwargs=kwargs)

    def si(self, *args: Any, **kwargs: Any) -> Signature:
        """Create an immutable :class:`~flexiq.canvas.Signature`."""
        return Signature(task=self, args=args, kwargs=kwargs, immutable=True)

    def analyze(self, *args: Any, **kwargs: Any) -> InterceptionReport:
        """Analyze arguments without enqueuing — shows what the interceptor would do.

        Returns an :class:`~flexiq.interception.InterceptionReport` describing
        the strategy for each argument.
        """
        interceptor = self._queue._interceptor
        if interceptor is None:
            return InterceptionReport()
        return interceptor.analyze(args, kwargs)

    def __repr__(self) -> str:
        return f"<TaskWrapper '{self._task_name}'>"
