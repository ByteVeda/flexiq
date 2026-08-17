"""Tests for progress tracking."""

from __future__ import annotations

import time

from flexiq import Queue


def test_update_progress(queue: Queue) -> None:
    """Progress can be updated and read back."""

    @queue.task()
    def slow_task() -> str:

        time.sleep(0.5)
        return "done"

    job = slow_task.delay()

    # Update progress directly via the queue API
    queue.update_progress(job.id, 50)

    refreshed = queue.get_job(job.id)
    assert refreshed is not None
    assert refreshed.progress == 50

    queue.update_progress(job.id, 100)
    refreshed = queue.get_job(job.id)
    assert refreshed is not None
    assert refreshed.progress == 100


def test_progress_starts_none(queue: Queue) -> None:
    """Progress is None by default."""

    @queue.task()
    def task_a() -> int:
        return 1

    job = task_a.delay()
    refreshed = queue.get_job(job.id)
    assert refreshed is not None
    assert refreshed.progress is None
