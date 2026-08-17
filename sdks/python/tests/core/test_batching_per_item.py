"""Per-item result handling for batched tasks (``per_item_results=True``).

Covers the happy path, partial-failure retry routing, type contract
enforcement, and the ``BatchedJobResult.result()`` / ``partial_failures()``
API. Idempotency-with-batching is also re-asserted here for completeness.

The scheduler registers task configs at worker-boot time, BEFORE the
decorator hot path attaches per-task metadata. Tests that exercise the
terminal-failure path therefore start the worker AFTER declaring the
task — otherwise the scheduler falls back to ``RetryPolicy::default()``
(``max_retries=3``) and the failure replays three times before DLQ.
"""

from __future__ import annotations

import threading
from collections.abc import Callable, Generator
from contextlib import contextmanager
from pathlib import Path

import pytest

from flexiq import (
    BatchItemResult,
    BatchPartialFailureError,
    BatchResultTypeError,
    MaxRetriesExceededError,
    Queue,
)
from flexiq.batching import BatchConfig


@pytest.fixture
def noretry_queue(tmp_path: Path) -> Generator[Queue]:
    """Queue configured with ``default_retry=0`` so jobs DLQ on first failure."""
    db_path = str(tmp_path / "noretry.db")
    q = Queue(db_path=db_path, workers=2, default_retry=0)
    try:
        yield q
    finally:
        q.close()


@contextmanager
def started_worker(q: Queue) -> Generator[threading.Thread]:
    """Start a worker thread AFTER all tasks have been declared.

    Required for terminal-failure tests so the scheduler sees the task's
    ``max_retries=0`` policy (registration happens once at worker boot).
    """
    thread = threading.Thread(target=q.run_worker, daemon=True)
    thread.start()
    try:
        yield thread
    finally:
        q._inner.request_shutdown()
        thread.join(timeout=10)


_RegisterTaskFn = Callable[[], None]


# ── BatchItemResult dataclass ─────────────────────────────────────────────


def test_batch_item_result_success_constructor() -> None:
    item = BatchItemResult.success(item_index=3, result="ok")
    assert item.status == "success"
    assert item.result == "ok"
    assert item.error is None
    assert item.item_index == 3


def test_batch_item_result_failure_constructor() -> None:
    item = BatchItemResult.failure(item_index=0, error="boom")
    assert item.status == "failure"
    assert item.result is None
    assert item.error == "boom"


def test_batch_item_result_success_rejects_error() -> None:
    with pytest.raises(ValueError, match="cannot have an error"):
        BatchItemResult(status="success", error="should not have this")


def test_batch_item_result_failure_requires_error() -> None:
    with pytest.raises(ValueError, match="requires a non-empty error"):
        BatchItemResult(status="failure", error=None)
    with pytest.raises(ValueError, match="requires a non-empty error"):
        BatchItemResult(status="failure", error="   ")


def test_batch_item_result_rejects_negative_index() -> None:
    with pytest.raises(ValueError, match="item_index must be >= 0"):
        BatchItemResult.success(item_index=-1)


# ── Config layer ──────────────────────────────────────────────────────────


def test_batch_config_normalize_per_item_results_flag() -> None:
    cfg = BatchConfig.normalize({"per_item_results": True})
    assert cfg is not None
    assert cfg.per_item_results is True

    cfg2 = BatchConfig.normalize(True)
    assert cfg2 is not None
    assert cfg2.per_item_results is False  # default off


def test_idempotent_and_per_item_results_combination_rejected(queue: Queue) -> None:
    """idempotent=True + per_item_results=True is rejected at decoration."""
    with pytest.raises(ValueError, match="idempotent=True"):

        @queue.task(batch={"per_item_results": True}, idempotent=True, max_retries=0)
        def bad(items: list[int]) -> list[BatchItemResult]:  # pragma: no cover
            return []


# ── End-to-end happy path ────────────────────────────────────────────────


def test_per_item_results_happy_path(queue: Queue, run_worker: threading.Thread) -> None:
    """All items succeed → each BatchedJobResult.result() returns its per-item value."""
    _ = run_worker

    @queue.task(batch={"max_size": 3, "max_wait_ms": 60_000, "per_item_results": True})
    def double(items: list[int]) -> list[BatchItemResult]:
        return [BatchItemResult.success(item_index=i, result=v * 2) for i, v in enumerate(items)]

    h0 = double.delay(10)
    h1 = double.delay(20)
    h2 = double.delay(30)

    assert h0.result(timeout=15) == 20
    assert h1.result(timeout=15) == 40
    assert h2.result(timeout=15) == 60

    queue.close()


def test_per_item_results_partial_failure_routes_to_retry(
    queue: Queue, run_worker: threading.Thread
) -> None:
    """One item failure → BatchPartialFailureError → retry (max_retries=1)."""
    _ = run_worker
    attempts = 0
    attempts_lock = threading.Lock()

    @queue.task(
        batch={"max_size": 2, "max_wait_ms": 60_000, "per_item_results": True}, max_retries=1
    )
    def maybe_fail(items: list[int]) -> list[BatchItemResult]:
        nonlocal attempts
        with attempts_lock:
            attempts += 1
            current = attempts
        results = []
        for i, v in enumerate(items):
            # First attempt: item 0 fails. Second attempt: all succeed.
            if current == 1 and i == 0:
                results.append(BatchItemResult.failure(item_index=i, error="first try"))
            else:
                results.append(BatchItemResult.success(item_index=i, result=v))
        return results

    h0 = maybe_fail.delay(100)
    h1 = maybe_fail.delay(200)

    # Second attempt succeeds, so .result() returns per-item values.
    assert h0.result(timeout=15) == 100
    assert h1.result(timeout=15) == 200
    with attempts_lock:
        assert attempts == 2, f"expected 2 attempts (retry), got {attempts}"

    queue.close()


def test_per_item_results_failed_item_raises_task_failed(noretry_queue: Queue) -> None:
    """After retries exhausted, .result() on a failed item raises TaskFailedError."""

    @noretry_queue.task(
        batch={"max_size": 2, "max_wait_ms": 60_000, "per_item_results": True},
        max_retries=0,
    )
    def fail_item_0(items: list[int]) -> list[BatchItemResult]:
        return [
            BatchItemResult.failure(item_index=0, error="permanent"),
            BatchItemResult.success(item_index=1, result=items[1]),
        ]

    with started_worker(noretry_queue):
        h0 = fail_item_0.delay(7)
        h1 = fail_item_0.delay(8)

        # After max_retries=0 the failed batch lands in the DLQ — surfaces as
        # MaxRetriesExceededError whose message embeds the original per-item
        # failure string.
        with pytest.raises(MaxRetriesExceededError, match="permanent"):
            h0.result(timeout=15)
        # The non-failing item lands in the same DLQ outcome (whole-batch
        # failure semantics: one job, one retry counter).
        with pytest.raises(MaxRetriesExceededError):
            h1.result(timeout=15)


def test_per_item_results_partial_failures_returns_empty_for_success_path(
    queue: Queue, run_worker: threading.Thread
) -> None:
    """partial_failures() returns [] when the batch fully succeeded.

    Note: when the batch DLQs after partial failures, the per-item result
    list is not persisted (Rust stores only the error on the failure path),
    so partial_failures() raises MaxRetriesExceededError in that case. This
    is documented as a v1 limitation — users inspect the exception message
    to discover individual failure reasons after DLQ.
    """
    _ = run_worker

    @queue.task(batch={"max_size": 2, "max_wait_ms": 60_000, "per_item_results": True})
    def all_succeed(items: list[int]) -> list[BatchItemResult]:
        return [BatchItemResult.success(item_index=i, result=v) for i, v in enumerate(items)]

    h0 = all_succeed.delay(1)
    h1 = all_succeed.delay(2)

    # Successful batch: per_item_results yields per-item values.
    assert h0.result(timeout=15) == 1
    assert h1.result(timeout=15) == 2
    # All-success → empty partial_failures.
    assert h0.partial_failures(timeout=15) == []  # type: ignore[attr-defined]
    assert h1.partial_failures(timeout=15) == []  # type: ignore[attr-defined]

    queue.close()


def test_per_item_results_wrong_return_type_raises(noretry_queue: Queue) -> None:
    """per_item_results=True + non-list return → BatchResultTypeError on retry-exhausted DLQ."""

    @noretry_queue.task(
        batch={"max_size": 2, "max_wait_ms": 60_000, "per_item_results": True},
        max_retries=0,
    )
    def wrong_type(items: list[int]) -> int:
        return 42  # wrong: should be list[BatchItemResult]

    with started_worker(noretry_queue):
        h0 = wrong_type.delay(1)
        h1 = wrong_type.delay(2)

        # The worker raises BatchResultTypeError; max_retries=0 → DLQ;
        # JobResult surfaces MaxRetriesExceededError whose message embeds
        # the original BatchResultTypeError text.
        with pytest.raises(MaxRetriesExceededError, match="per_item_results"):
            h0.result(timeout=15)
        with pytest.raises(MaxRetriesExceededError):
            h1.result(timeout=15)


def test_per_item_results_false_passes_list_through(
    queue: Queue, run_worker: threading.Thread
) -> None:
    """Default per_item_results=False → plain list returns are stored unchanged."""
    _ = run_worker

    @queue.task(batch={"max_size": 2, "max_wait_ms": 60_000}, max_retries=0)
    def collect(items: list[int]) -> list[int]:
        return [v * 10 for v in items]

    h0 = collect.delay(1)
    h1 = collect.delay(2)

    # Without per_item_results, .result() returns the whole batch result.
    assert h0.result(timeout=15) == [10, 20]
    assert h1.result(timeout=15) == [10, 20]

    queue.close()


def test_partial_failures_raises_when_flag_not_set(queue: Queue) -> None:
    """Calling partial_failures() on a non-per-item handle is a clear error."""

    @queue.task(batch={"max_size": 1, "max_wait_ms": 60_000}, max_retries=0)
    def plain(items: list[int]) -> None:  # pragma: no cover
        pass

    handle = plain.delay(0)
    with pytest.raises(RuntimeError, match="does not have per_item_results enabled"):
        handle.partial_failures(timeout=1)  # type: ignore[attr-defined]

    queue.close()


# ── Direct exception type ────────────────────────────────────────────────


def test_batch_partial_failure_error_carries_failed_items() -> None:
    failed = [
        BatchItemResult.failure(item_index=0, error="a"),
        BatchItemResult.failure(item_index=2, error="c"),
    ]
    exc = BatchPartialFailureError(failed_items=failed)
    assert exc.failed_items == failed
    assert "2 item(s) failed" in str(exc)
    assert "first error: a" in str(exc)


def test_batch_result_type_error_is_typeerror() -> None:
    assert issubclass(BatchResultTypeError, TypeError)
