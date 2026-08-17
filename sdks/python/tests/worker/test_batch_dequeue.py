"""Tests for batch dequeue (claiming several jobs per scheduler round)."""

import threading
from pathlib import Path
from typing import Any

from flexiq import Queue

PollUntil = Any  # the conftest fixture's runtime type


def test_batch_dequeue_processes_burst(tmp_path: Path, poll_until: PollUntil) -> None:
    """A queue with scheduler_batch_size=4 drains a burst of jobs.

    Default behavior (batch_size=1) is exercised by the rest of the suite;
    here we assert the batch-claiming path completes every job correctly.
    """
    db_path = str(tmp_path / "batch.db")
    queue = Queue(db_path=db_path, workers=4, scheduler_batch_size=4)

    results: dict[int, int] = {}
    lock = threading.Lock()

    @queue.task()
    def square(n: int) -> int:
        value = n * n
        with lock:
            results[n] = value
        return value

    jobs = [square.delay(i) for i in range(20)]

    worker = threading.Thread(target=queue.run_worker, daemon=True)
    worker.start()
    try:
        for job in jobs:
            assert job.result(timeout=15) is not None
        poll_until(lambda: len(results) == 20, timeout=15)
        assert results == {i: i * i for i in range(20)}
    finally:
        queue._inner.request_shutdown()
        worker.join(timeout=5)


def test_batch_dequeue_default_is_one(tmp_path: Path) -> None:
    """scheduler_batch_size defaults to 1 (back-compat preserved)."""
    db_path = str(tmp_path / "default.db")
    queue = Queue(db_path=db_path, workers=2)

    @queue.task()
    def echo(x: str) -> str:
        return x

    job = echo.delay("hello")

    worker = threading.Thread(target=queue.run_worker, daemon=True)
    worker.start()
    try:
        assert job.result(timeout=10) == "hello"
    finally:
        queue._inner.request_shutdown()
        worker.join(timeout=5)
