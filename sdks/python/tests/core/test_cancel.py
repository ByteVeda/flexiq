"""Tests for job cancellation."""

from __future__ import annotations

import threading

from flexiq import Queue


def test_cancel_pending_job(queue: Queue) -> None:
    """A pending job can be cancelled."""

    @queue.task()
    def slow_task() -> str:
        return "done"

    job = slow_task.delay()
    assert queue.cancel_job(job.id) is True

    refreshed = queue.get_job(job.id)
    assert refreshed is not None
    assert refreshed.status == "cancelled"


def test_cancel_nonexistent_job(queue: Queue) -> None:
    """Cancelling a nonexistent job returns False."""

    @queue.task()
    def dummy() -> None:
        pass

    assert queue.cancel_job("nonexistent-id") is False


def test_cancel_completed_job(queue: Queue) -> None:
    """Cancelling a completed job returns False (only pending can be cancelled)."""

    @queue.task()
    def quick_task() -> int:
        return 42

    job = quick_task.delay()

    worker_thread = threading.Thread(target=queue.run_worker, daemon=True)
    worker_thread.start()

    job.result(timeout=10)
    assert queue.cancel_job(job.id) is False


def test_cancelled_in_stats(queue: Queue) -> None:
    """Cancelled jobs show up in stats."""

    @queue.task()
    def task_a() -> None:
        pass

    job = task_a.delay()
    queue.cancel_job(job.id)

    stats = queue.stats()
    assert stats["cancelled"] == 1
