"""Tests for rate limiting."""

import threading
import time
from typing import Any

from taskito import Queue

PollUntil = Any  # the conftest fixture's runtime type


def test_rate_limit_throttles(queue: Queue, poll_until: PollUntil) -> None:
    """Rate-limited tasks should be throttled."""
    # Wall clock, not monotonic. The token bucket refills against the stored
    # `last_refill` timestamp and a denied job is rescheduled on a wall-clock
    # `scheduled_at`, so the throttle is enforced entirely in wall-clock terms. A
    # runner whose clock steps forward mid-test — a fresh CI VM syncing NTP —
    # then releases the held-back jobs early in monotonic terms, collapsing the
    # measured span without the limiter ever having admitted more than it should.
    # Measuring on the clock the limiter enforces on keeps the two consistent.
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

        # The bucket holds 2 tokens, so the first two run as a burst and the rest
        # wait on a refill — that wait is the throttle.
        span = timestamps[-1] - timestamps[0]
        assert span >= 0.5, f"expected throttling across 4 runs, got {span:.3f}s"
    finally:
        # A worker left running holds this test's SQLite file open for the rest
        # of the session, which on Windows blocks the tmp_path cleanup.
        queue.shutdown()
        worker_thread.join(timeout=5)
