"""Tests for worker behavior."""

import threading
import time
from pathlib import Path

import pytest

from flexiq import Queue


def test_multiple_tasks(queue: Queue) -> None:
    """Worker handles multiple different task types."""

    @queue.task()
    def task_a(x: int) -> int:
        return x * 2

    @queue.task()
    def task_b(x: int) -> int:
        return x + 10

    job_a = task_a.delay(5)
    job_b = task_b.delay(5)

    worker_thread = threading.Thread(
        target=queue.run_worker,
        daemon=True,
    )
    worker_thread.start()

    assert job_a.result(timeout=10) == 10
    assert job_b.result(timeout=10) == 15


def test_get_job(queue: Queue) -> None:
    """Can retrieve a job by ID."""

    @queue.task()
    def simple() -> int:
        return 42

    job = simple.delay()
    fetched = queue.get_job(job.id)
    assert fetched is not None
    assert fetched.id == job.id


def test_job_status_progression(queue: Queue) -> None:
    """Job status progresses from pending through complete."""

    @queue.task()
    def slow() -> str:
        time.sleep(0.5)
        return "done"

    job = slow.delay()

    # Initially pending
    fetched = queue.get_job(job.id)
    assert fetched is not None
    assert fetched.status == "pending"

    worker_thread = threading.Thread(
        target=queue.run_worker,
        daemon=True,
    )
    worker_thread.start()

    result = job.result(timeout=10)
    assert result == "done"

    # After completion
    fetched = queue.get_job(job.id)
    assert fetched is not None
    assert fetched.status == "complete"


@pytest.mark.asyncio
async def test_async_result(tmp_path: Path) -> None:
    """Async result retrieval works."""
    db_path = str(tmp_path / "test_async.db")
    queue = Queue(db_path=db_path, workers=2)

    @queue.task()
    def add(a: int, b: int) -> int:
        return a + b

    job = add.delay(10, 20)

    worker_thread = threading.Thread(
        target=queue.run_worker,
        daemon=True,
    )
    worker_thread.start()

    result = await job.aresult(timeout=10)
    assert result == 30


@pytest.mark.asyncio
async def test_async_stats(tmp_path: Path) -> None:
    """Async stats work."""
    db_path = str(tmp_path / "test_async_stats.db")
    queue = Queue(db_path=db_path, workers=2)

    @queue.task()
    def noop() -> None:
        pass

    noop.delay()

    stats = await queue.astats()
    assert stats["pending"] == 1
