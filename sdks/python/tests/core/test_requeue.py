"""Tests for Queue.requeue_job (stuck Running job recovery)."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable

from flexiq import Queue


def test_requeue_pending_job_returns_false(queue: Queue) -> None:
    """Only Running jobs are requeueable — a pending job is a no-op."""

    @queue.task()
    def idle() -> None:
        pass

    job = idle.delay()
    assert queue.requeue_job(job.id) is False


def test_requeue_nonexistent_job_returns_false(queue: Queue) -> None:
    assert queue.requeue_job("nonexistent-id") is False


def test_requeue_running_job_reruns_it(
    queue: Queue,
    run_worker: threading.Thread,
    poll_until: Callable[..., None],
) -> None:
    """A stuck Running job goes back to Pending and is dispatched again."""
    runs: list[float] = []
    first_attempt_release = threading.Event()

    @queue.task(max_retries=0)
    def stuck() -> None:
        runs.append(time.monotonic())
        if len(runs) == 1:
            # Simulate a hung attempt: block until the test releases it.
            first_attempt_release.wait(timeout=30)

    job = stuck.delay()
    poll_until(lambda: len(runs) >= 1, message="first attempt never started")

    assert queue.requeue_job(job.id) is True

    poll_until(lambda: len(runs) >= 2, message="requeued job was never re-dispatched")
    status = queue.get_job(job.id)
    assert status is not None

    first_attempt_release.set()

    def job_completed() -> bool:
        refreshed = queue.get_job(job.id)
        return refreshed is not None and refreshed.status == "complete"

    poll_until(job_completed, message="requeued attempt never completed")
