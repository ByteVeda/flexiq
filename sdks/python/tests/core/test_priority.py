"""Tests for priority scheduling."""

import threading
from pathlib import Path
from typing import Any

import pytest

from flexiq import Queue


@pytest.fixture
def queue(tmp_path: Path) -> Queue:
    db_path = str(tmp_path / "test_priority.db")
    return Queue(db_path=db_path, workers=1)  # 1 worker for ordering


def test_priority_ordering(queue: Queue, poll_until: Any) -> None:
    """Higher priority jobs should be processed first."""
    results: list[str] = []

    @queue.task()
    def record_task(label: str) -> str:
        results.append(label)
        return label

    # Enqueue low priority first, then high priority. All three jobs are
    # synchronously visible after `apply_async` returns, so no pre-worker
    # delay is needed.
    record_task.apply_async(args=("low",), priority=1)
    record_task.apply_async(args=("medium",), priority=5)
    record_task.apply_async(args=("high",), priority=10)

    worker_thread = threading.Thread(
        target=queue.run_worker,
        daemon=True,
    )
    worker_thread.start()

    poll_until(
        lambda: len(results) >= 3,
        timeout=10,
        message="not all priority jobs completed",
    )

    # High priority should have been processed first
    assert len(results) == 3
    assert results[0] == "high"
    assert results[1] == "medium"
    assert results[2] == "low"
