"""Tests for producer-side task batching (`@queue.task(batch=...)`)."""

from __future__ import annotations

import threading
from typing import Any

import pytest

from conftest import join_worker
from flexiq import Queue
from flexiq.batching import BatchConfig, BatchedJobResult

PollUntil = Any  # the conftest fixture's runtime type


def test_batch_config_normalize_true_uses_defaults() -> None:
    cfg = BatchConfig.normalize(True)
    assert cfg is not None
    assert cfg.max_size == 100
    assert cfg.max_wait_ms == 500


def test_batch_config_normalize_dict_overrides() -> None:
    cfg = BatchConfig.normalize({"max_size": 7, "max_wait_ms": 42})
    assert cfg is not None
    assert cfg.max_size == 7
    assert cfg.max_wait_ms == 42


def test_batch_config_normalize_none_and_false_disable() -> None:
    assert BatchConfig.normalize(None) is None
    assert BatchConfig.normalize(False) is None


def test_batch_config_rejects_invalid_input() -> None:
    with pytest.raises(TypeError):
        BatchConfig.normalize("nope")


def test_batch_config_rejects_zero_max_size() -> None:
    with pytest.raises(ValueError):
        BatchConfig(max_size=0)


def test_batch_config_rejects_zero_max_wait_ms() -> None:
    with pytest.raises(ValueError):
        BatchConfig(max_wait_ms=0)


def test_size_based_flush(queue: Queue, run_worker: threading.Thread) -> None:
    """5 items with max_size=5 → one job, fn called once with all 5."""
    _ = run_worker
    received: list[list[int]] = []
    received_lock = threading.Lock()
    done = threading.Event()

    @queue.task(batch={"max_size": 5, "max_wait_ms": 60_000}, max_retries=0)
    def collect(items: list[int]) -> None:
        with received_lock:
            received.append(items)
        done.set()

    for i in range(5):
        collect.delay(i)
    assert done.wait(timeout=10), "batch never flushed"

    queue.close()
    assert len(received) == 1
    assert sorted(received[0]) == [0, 1, 2, 3, 4]


def test_time_based_flush(queue: Queue, run_worker: threading.Thread) -> None:
    """2 items with short max_wait_ms → one job after the timer fires."""
    _ = run_worker
    received: list[list[int]] = []
    received_lock = threading.Lock()
    done = threading.Event()

    @queue.task(batch={"max_size": 100, "max_wait_ms": 100}, max_retries=0)
    def collect_time(items: list[int]) -> None:
        with received_lock:
            received.append(items)
        done.set()

    collect_time.delay(10)
    collect_time.delay(20)
    assert done.wait(timeout=5), "time-based flush never happened"

    queue.close()
    assert len(received) == 1
    assert sorted(received[0]) == [10, 20]


def test_cross_task_isolation(queue: Queue, run_worker: threading.Thread) -> None:
    """Two batched tasks accumulate independently."""
    _ = run_worker
    a_received: list[list[int]] = []
    b_received: list[list[int]] = []
    a_done = threading.Event()
    b_done = threading.Event()

    @queue.task(batch={"max_size": 3, "max_wait_ms": 60_000}, max_retries=0)
    def task_a(items: list[int]) -> None:
        a_received.append(items)
        a_done.set()

    @queue.task(batch={"max_size": 2, "max_wait_ms": 60_000}, max_retries=0)
    def task_b(items: list[int]) -> None:
        b_received.append(items)
        b_done.set()

    task_a.delay(1)
    task_b.delay(100)
    task_a.delay(2)
    task_b.delay(200)  # b reaches max_size=2 → flush
    task_a.delay(3)  # a reaches max_size=3 → flush

    assert a_done.wait(timeout=5)
    assert b_done.wait(timeout=5)

    queue.close()
    assert sorted(a_received[0]) == [1, 2, 3]
    assert sorted(b_received[0]) == [100, 200]


def test_concurrent_adds_no_loss(
    queue: Queue, run_worker: threading.Thread, poll_until: PollUntil
) -> None:
    """10 threads each enqueue 5 items; all 50 must reach the batch."""
    _ = run_worker
    received: list[int] = []
    received_lock = threading.Lock()

    @queue.task(batch={"max_size": 50, "max_wait_ms": 1_000}, max_retries=0)
    def concur(items: list[int]) -> None:
        with received_lock:
            received.extend(items)

    def producer(thread_id: int) -> None:
        for j in range(5):
            concur.delay(thread_id * 100 + j)

    threads = [threading.Thread(target=producer, args=(i,)) for i in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        join_worker(t, message="producer thread never finished enqueueing")

    poll_until(lambda: len(received) >= 50, timeout=5)
    queue.close()
    assert len(received) == 50


def test_batched_returns_batchedjobresult(queue: Queue) -> None:
    """`Queue.enqueue` returns a per-item handle for batched calls.

    Before the batch flushes, ``id`` is None and ``result(timeout=...)``
    times out (the flush event hasn't fired). After flush, ``id`` resolves
    to the underlying batch job ID.
    """

    @queue.task(batch=True, max_retries=0)
    def some_batched(items: list[int]) -> None:  # pragma: no cover
        pass

    result = some_batched.delay(42)
    assert isinstance(result, BatchedJobResult)
    assert result.id is None
    assert result.batched is True
    # No flush yet → result() blocks until timeout fires.
    with pytest.raises(TimeoutError):
        result.result(timeout=0.05)

    # Stop the accumulator without flushing (the no-op task would error if invoked).
    assert queue._batch_accumulator is not None
    queue._batch_accumulator.shutdown(flush=False)
    queue._batch_accumulator = None


def test_batched_rejects_kwargs(queue: Queue) -> None:
    @queue.task(batch=True, max_retries=0)
    def kw_batched(items: list[int]) -> None:  # pragma: no cover
        pass

    with pytest.raises(ValueError, match="does not accept kwargs"):
        kw_batched.apply_async(args=(42,), kwargs={"extra": 1})

    queue.close()


def test_batched_rejects_multiple_positional_args(queue: Queue) -> None:
    @queue.task(batch=True, max_retries=0)
    def multi(items: list[int]) -> None:  # pragma: no cover
        pass

    with pytest.raises(ValueError, match="exactly one positional"):
        multi.apply_async(args=(1, 2))

    queue.close()


def test_idempotent_and_batch_combination_rejected(queue: Queue) -> None:
    """`idempotent=True` + `batch=...` is rejected at decoration."""
    with pytest.raises(ValueError, match="incompatible with idempotent"):

        @queue.task(batch=True, idempotent=True, max_retries=0)
        def bad(items: list[int]) -> None:  # pragma: no cover
            pass


def test_close_flushes_pending_items(queue: Queue, run_worker: threading.Thread) -> None:
    """`Queue.close()` flushes whatever is left in the buffer."""
    _ = run_worker
    received: list[list[int]] = []
    received_lock = threading.Lock()
    done = threading.Event()

    @queue.task(batch={"max_size": 1000, "max_wait_ms": 60_000}, max_retries=0)
    def close_flush(items: list[int]) -> None:
        with received_lock:
            received.append(items)
        done.set()

    close_flush.delay(7)
    close_flush.delay(8)
    close_flush.delay(9)
    # No size or time trigger yet — close() must force the flush.
    queue.close()
    assert done.wait(timeout=5), "close did not flush pending batch"

    assert len(received) == 1
    assert sorted(received[0]) == [7, 8, 9]


def test_shutdown_without_flush_drops_items(queue: Queue) -> None:
    """Shutdown with ``flush=False`` discards pending items (documented loss)."""
    received: list[list[int]] = []

    @queue.task(batch={"max_size": 100, "max_wait_ms": 60_000}, max_retries=0)
    def dropped(items: list[int]) -> None:  # pragma: no cover
        received.append(items)

    dropped.delay(1)
    dropped.delay(2)

    assert queue._batch_accumulator is not None
    queue._batch_accumulator.shutdown(flush=False)
    queue._batch_accumulator = None

    assert received == []
