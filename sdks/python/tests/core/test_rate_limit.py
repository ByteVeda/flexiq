"""Tests for rate limiting."""

import threading
import time
from typing import Any

from conftest import join_worker
from flexiq import Queue

PollUntil = Any  # the conftest fixture's runtime type


def test_rate_limit_throttles(queue: Queue, poll_until: PollUntil) -> None:
    """Rate-limited tasks should be throttled.

    Both assertions below are one-sided on purpose. The limiter gates at *dispatch*,
    in the scheduler, while these timestamps are taken at *execution*, inside the task
    body — and between the two sits a first-call-biased pipeline (GIL acquisition, a
    fresh event loop per task, middleware setup). Job 1 pays those one-time costs and
    job 4 pays none, so execution lag is large for the first job and near zero for the
    last. Anything measured between two executions can therefore collapse even when
    the limiter behaved perfectly, which is what made the old
    `timestamps[-1] - timestamps[0] >= 0.5` flaky on loaded CI runners.

    Lag can only ever *delay* a job, never advance it, so each assertion is framed in
    the direction lag pushes away from failure.
    """
    timestamps: list[float] = []

    @queue.task(rate_limit="2/s")
    def rate_limited_task(n: int) -> int:
        timestamps.append(time.time())
        return n

    # Enqueue 4 tasks (at 2/s, should take ~2s)
    for i in range(4):
        rate_limited_task.delay(i)

    # Started after registration, not via the `run_worker` fixture: the worker
    # snapshots task configs at start, so a task registered afterwards would
    # dispatch with no rate-limit gate at all.
    started = time.time()
    worker_thread = threading.Thread(
        target=queue.run_worker,
        daemon=True,
    )
    worker_thread.start()

    try:
        # Wait for all tasks
        poll_until(lambda: len(timestamps) == 4, timeout=10)

        # Should have all 4 results
        assert len(timestamps) == 4

        # Upper bound: the bucket holds 2 tokens, so only 2 jobs can run before a
        # refill. Lag can only push a job out of this window, so a count above 2
        # means the gate was skipped rather than that the runner was slow.
        burst = [t for t in timestamps if t - timestamps[0] <= 0.4]
        assert len(burst) <= 2, f"expected a burst of 2, got {len(burst)} within 0.4s"

        # Lower bound, anchored to worker start rather than to the first execution.
        # Jobs 3 and 4 are denied and rescheduled a second out, and execution only
        # ever comes after admission, so lag inflates this rather than shrinking it.
        held = timestamps[-1] - started
        assert held >= 0.5, f"expected throttling across 4 runs, got {held:.3f}s"
    finally:
        # A worker left running holds this test's SQLite file open for the rest
        # of the session, which on Windows blocks the tmp_path cleanup.
        queue.shutdown()
        join_worker(worker_thread)


def start_error(queue: Queue) -> str:
    """Start a worker that is expected to refuse, and return what it said.

    ``run_worker`` blocks, so a config the core rejects surfaces as an exception
    on the worker's own thread rather than at the call site.
    """
    thread_error: list[BaseException] = []

    def _run() -> None:
        try:
            queue.run_worker()
        except BaseException as exc:
            thread_error.append(exc)

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    join_worker(thread, message="worker never exited after rejecting the rate")
    queue.shutdown()

    assert thread_error, "an invalid rate must be rejected, not ignored"
    return str(thread_error[0])


def test_rate_limit_rejects_an_unparseable_rate(queue: Queue) -> None:
    """A typo must not silently disable the limit."""

    @queue.task(rate_limit="not-a-rate")
    def bad_rate() -> None:
        pass

    assert "rate_limit" in start_error(queue)


def test_rate_limit_rejects_a_count_below_one(queue: Queue) -> None:
    """A bucket of zero tokens never refills past the one an acquire needs.

    Nothing downstream would report that: the task would simply never dispatch,
    indistinguishable from one nobody enqueued. The rate has to be refused at the
    start instead.
    """

    @queue.task(rate_limit="0/s")
    def never_runs() -> None:
        pass

    error = start_error(queue)
    assert "rate_limit" in error
    assert "0/s" in error


def test_queue_rate_limit_rejects_an_unparseable_rate(queue: Queue) -> None:
    """A queue's rate is held to the same rule as a task's.

    It used to be dropped on a parse failure, which left the queue running with no
    limit at all — the one direction a misconfigured throttle must never fail in.
    """

    @queue.task()
    def fine() -> None:
        pass

    queue.set_queue_rate_limit("default", "0/s")

    error = start_error(queue)
    assert "rate_limit" in error
    assert "queue default" in error
