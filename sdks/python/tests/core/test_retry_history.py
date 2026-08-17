"""Tests for retry history (job_errors tracking)."""

import threading
from pathlib import Path

import pytest

from flexiq import Queue


@pytest.fixture
def queue(tmp_path: Path) -> Queue:
    db_path = str(tmp_path / "test_retry_history.db")
    return Queue(db_path=db_path, workers=1)


def test_retry_errors_recorded(queue: Queue) -> None:
    """Failed attempts are recorded in job.errors."""
    call_count = {"n": 0}

    @queue.task(max_retries=3, retry_backoff=0.01)
    def flaky() -> str:
        call_count["n"] += 1
        if call_count["n"] <= 3:
            raise ValueError(f"attempt {call_count['n']}")
        return "ok"

    job = flaky.delay()

    worker_thread = threading.Thread(target=queue.run_worker, daemon=True)
    worker_thread.start()

    result = job.result(timeout=15)
    assert result == "ok"

    errors = job.errors
    assert len(errors) == 3
    assert errors[0]["attempt"] == 0
    assert "attempt 1" in errors[0]["error"]
    assert errors[1]["attempt"] == 1
    assert errors[2]["attempt"] == 2


def test_errors_empty_on_success(queue: Queue) -> None:
    """Successful jobs have an empty errors list."""

    @queue.task()
    def ok_task() -> int:
        return 42

    job = ok_task.delay()

    worker_thread = threading.Thread(target=queue.run_worker, daemon=True)
    worker_thread.start()

    result = job.result(timeout=10)
    assert result == 42
    assert job.errors == []
