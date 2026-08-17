"""Per-item handle for items routed through the batch accumulator.

Each ``Queue.enqueue("batched_task", args=(item,))`` call returns a
``BatchedJobResult`` immediately — *before* the batch flushes. The result
blocks on :meth:`result` until two things have happened:

1. The accumulator flushes the buffer and the underlying batch job is
   enqueued (the flush event fires);
2. The underlying job reaches a terminal state.

When the batched task is configured with ``per_item_results=True``, the
worker is expected to return ``list[BatchItemResult]`` and this class
unpacks the per-item value for the caller's ``item_index``. Otherwise,
the whole batch's return value is returned (back-compat path).
"""

from __future__ import annotations

import threading
import time
from typing import TYPE_CHECKING, Any

from flexiq.batching.item_result import BatchItemResult, is_batch_item_result_list
from flexiq.exceptions import TaskFailedError

if TYPE_CHECKING:
    from flexiq.app import Queue
    from flexiq.result import JobResult


class BatchedJobResult:
    """Handle returned by ``Queue.enqueue()`` for items added to a batch.

    Until the batch flushes, ``id`` is ``None``; after flush, ``id`` returns
    the underlying batch job's ID and ``result()`` resolves to either the
    per-item value (when ``per_item_results=True``) or the whole batch
    return value (default).
    """

    __slots__ = (
        "_flush_event",
        "_jobresult",
        "_per_item_results",
        "_queue",
        "_resolve_lock",
        "item_index",
        "task_name",
    )

    def __init__(
        self,
        task_name: str,
        item_index: int,
        per_item_results: bool,
        queue: Queue,
        flush_event: threading.Event,
    ) -> None:
        self.task_name = task_name
        self.item_index = item_index
        self._per_item_results = per_item_results
        self._queue = queue
        self._flush_event = flush_event
        # Set by the accumulator after the batch flushes and the underlying
        # job is enqueued. Reading without first waiting on _flush_event is
        # undefined behaviour.
        self._jobresult: JobResult | None = None
        self._resolve_lock = threading.Lock()

    @property
    def id(self) -> str | None:
        """The underlying batch job ID, or ``None`` if the batch hasn't flushed."""
        return self._jobresult.id if self._jobresult is not None else None

    @property
    def batched(self) -> bool:
        return True

    def _resolve(self, jobresult: JobResult) -> None:
        """Called by the accumulator once the batch flushes and the job is enqueued.

        Stamps the underlying JobResult onto every pending BatchedJobResult
        for the buffer and then signals the shared flush_event so all
        ``result()`` waiters unblock.
        """
        with self._resolve_lock:
            self._jobresult = jobresult

    def _resolve_failed(self) -> None:
        """Called by the accumulator if dispatching the batch raised.

        Leaves ``_jobresult`` as ``None`` and lets the flush_event fire so
        waiters surface a clear error instead of hanging forever.
        """

    def result(self, timeout: float = 30.0) -> Any:
        """Block until the underlying batch job finishes, then return *this*
        caller's per-item value.

        Args:
            timeout: Total seconds to wait (for both the flush event and the
                underlying job). Must be positive.

        Returns:
            - When ``per_item_results=True``: the ``result`` field of the
              ``BatchItemResult`` whose ``item_index`` matches this caller.
            - Otherwise: the batch job's whole return value.

        Raises:
            TimeoutError: deadline elapsed before the batch flushed or the
                job terminalised.
            TaskFailedError: the whole batch failed, OR (with
                ``per_item_results=True``) this caller's item reported
                ``status="failure"``.
            RuntimeError: the batch dispatch itself failed (e.g. serialisation
                error) before a job was enqueued.
        """
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        deadline = time.monotonic() + timeout
        # Wait for the buffer to flush and the underlying job to be enqueued.
        flush_timeout = max(0.0, deadline - time.monotonic())
        if not self._flush_event.wait(timeout=flush_timeout):
            raise TimeoutError(f"batched task {self.task_name!r} did not flush within {timeout}s")
        if self._jobresult is None:
            raise RuntimeError(
                f"batch dispatch for task {self.task_name!r} failed before a job was enqueued"
            )
        # Delegate to the underlying job result for the actual wait.
        remaining = max(0.001, deadline - time.monotonic())
        whole = self._jobresult.result(timeout=remaining)
        if not self._per_item_results:
            return whole
        return self._extract_per_item(whole)

    def partial_failures(self, timeout: float = 30.0) -> list[BatchItemResult]:
        """Return the list of failed items in the (now-terminal) batch.

        Only meaningful for tasks configured with ``per_item_results=True``.
        Returns ``[]`` when the batch fully succeeded.

        Raises:
            TaskFailedError: the *whole* batch failed (no per-item results
                were ever returned by the task).
            RuntimeError: ``per_item_results=False`` for this task.
        """
        if not self._per_item_results:
            raise RuntimeError(f"task {self.task_name!r} does not have per_item_results enabled")
        try:
            whole = self._wait_for_whole(timeout=timeout)
        except TaskFailedError:
            raise
        if not is_batch_item_result_list(whole):
            return []
        return [item for item in whole if item.status == "failure"]

    def status(self) -> str:
        """Return the underlying batch job's status, or ``"batched"`` if not yet flushed."""
        if self._jobresult is None:
            return "batched"
        return self._jobresult.status

    # ── Internal ───────────────────────────────────────────────────────

    def _wait_for_whole(self, timeout: float) -> Any:
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        deadline = time.monotonic() + timeout
        flush_timeout = max(0.0, deadline - time.monotonic())
        if not self._flush_event.wait(timeout=flush_timeout):
            raise TimeoutError(f"batched task {self.task_name!r} did not flush within {timeout}s")
        if self._jobresult is None:
            raise RuntimeError(
                f"batch dispatch for task {self.task_name!r} failed before a job was enqueued"
            )
        remaining = max(0.001, deadline - time.monotonic())
        return self._jobresult.result(timeout=remaining)

    def _extract_per_item(self, whole: Any) -> Any:
        """Pull this caller's item out of a ``list[BatchItemResult]`` return."""
        if not is_batch_item_result_list(whole):
            raise RuntimeError(
                f"task {self.task_name!r} has per_item_results=True but returned "
                f"{type(whole).__name__} instead of list[BatchItemResult]"
            )
        for item in whole:
            if item.item_index == self.item_index:
                if item.status == "failure":
                    raise TaskFailedError(item.error or "batch item failed")
                return item.result
        raise RuntimeError(
            f"task {self.task_name!r} returned no BatchItemResult for item_index={self.item_index}"
        )

    def __repr__(self) -> str:
        return (
            f"BatchedJobResult(task_name={self.task_name!r}, "
            f"item_index={self.item_index}, id={self.id!r})"
        )
