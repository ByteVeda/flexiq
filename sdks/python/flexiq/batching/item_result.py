"""Per-item result wrapper for batched tasks.

A batched task with ``per_item_results=True`` returns ``list[BatchItemResult]``
to signal a per-item outcome. The worker treats any items with
``status="failure"`` as a partial-batch failure and retries the batch through
the standard retry policy.

This sits next to (not on top of) the per-job ``JobResult`` API:

- ``BatchedJobResult.result()`` returns the user-level value for *this*
  caller's item (or raises ``TaskFailedError`` if that item failed).
- ``BatchedJobResult.partial_failures()`` returns the full list of failed
  items for a batch that ultimately succeeded (i.e. partial-success path
  after retries exhausted).
- ``BatchPartialFailureError`` is the synthetic exception the worker raises
  when at least one item in the batch reported failure — drives the existing
  retry-or-DLQ machinery.

Unlike Celery's ``Batches`` extension (which doesn't track per-item identity
across retries), this design treats per-item retries as the user's
responsibility: the task function MUST be idempotent at the per-item level
because the framework replays the whole batch on retry. See the batching
guide for the canonical "check each item's external state before acting"
pattern.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from flexiq.exceptions import TaskFailedError

BatchItemStatus = Literal["success", "failure"]


@dataclass(frozen=True)
class BatchItemResult:
    """One entry in the ``list[BatchItemResult]`` a batched task may return.

    Attributes:
        status: ``"success"`` or ``"failure"``. The worker treats any
            ``"failure"`` entry as a partial-batch failure that triggers
            retry-or-DLQ via the standard retry policy.
        result: User-level value for this item (any picklable type). Should be
            ``None`` when ``status="failure"``.
        error: Human-readable error message for this item. Must be ``None``
            when ``status="success"`` and a non-empty string when
            ``status="failure"``.
        item_index: Position of this item in the original batch payload.
            Lets callers correlate per-item outcomes back to the inputs they
            submitted.
    """

    status: BatchItemStatus
    result: Any = None
    error: str | None = None
    item_index: int = 0

    def __post_init__(self) -> None:
        if self.status == "success" and self.error is not None:
            raise ValueError("BatchItemResult with status='success' cannot have an error")
        if self.status == "failure" and (self.error is None or not self.error.strip()):
            raise ValueError("BatchItemResult with status='failure' requires a non-empty error")
        if self.item_index < 0:
            raise ValueError(f"item_index must be >= 0, got {self.item_index}")

    @classmethod
    def success(cls, item_index: int, result: Any = None) -> BatchItemResult:
        """Convenience constructor for a successful item."""
        return cls(status="success", result=result, item_index=item_index)

    @classmethod
    def failure(cls, item_index: int, error: str) -> BatchItemResult:
        """Convenience constructor for a failed item."""
        return cls(status="failure", error=error, item_index=item_index)


class BatchPartialFailureError(TaskFailedError):
    """Raised by the worker when a batched task returns ``list[BatchItemResult]``
    with at least one failure entry.

    Subclasses ``TaskFailedError`` so the existing retry machinery handles it
    transparently: the whole batch retries according to the task's retry
    policy until success or DLQ. The ``failed_items`` attribute exposes the
    per-item failure list for middleware that wants to log / DLQ individual
    items.

    The task function MUST be idempotent at the per-item level — on retry,
    the same items are re-delivered and the function must re-check whether
    each item has already been processed.
    """

    def __init__(self, failed_items: list[BatchItemResult]) -> None:
        self.failed_items = failed_items
        count = len(failed_items)
        first_error = failed_items[0].error if failed_items else "unknown"
        super().__init__(
            f"batch partial failure: {count} item(s) failed; first error: {first_error}"
        )


class BatchResultTypeError(TypeError):
    """Raised at worker time when ``per_item_results=True`` is set on a batched
    task but the return value is not ``list[BatchItemResult]``.

    Provides a clearer error than a generic serialization or type failure so
    users immediately understand the contract.
    """


def is_batch_item_result_list(value: Any) -> bool:
    """Return True when ``value`` is a non-empty list of ``BatchItemResult``.

    Used by the worker to detect when to apply per-item-result semantics. We
    require non-empty so an empty list passes through as a regular return
    value (back-compat with tasks that happen to return ``[]``).
    """
    return (
        isinstance(value, list)
        and len(value) > 0
        and all(isinstance(x, BatchItemResult) for x in value)
    )
