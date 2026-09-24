"""Tests for the tri-state ``push_dispatch`` option (#961)."""

import threading
from pathlib import Path

import pytest

from conftest import join_worker
from flexiq import Queue


@pytest.mark.parametrize("push_dispatch", [None, True, False])
def test_push_dispatch_setting_processes_jobs(tmp_path: Path, push_dispatch: bool | None) -> None:
    """Every setting dispatches: ``None`` keeps the backend default (polling on
    SQLite), ``True`` wakes on enqueue, ``False`` keeps polling."""
    queue = Queue(db_path=str(tmp_path / "push.db"), workers=2, push_dispatch=push_dispatch)

    @queue.task()
    def echo(x: str) -> str:
        return x

    worker = threading.Thread(target=queue.run_worker, daemon=True)
    worker.start()
    try:
        # Enqueued after the worker starts, so the push path has a wake to take.
        job = echo.delay("hello")
        assert job.result(timeout=10) == "hello"
    finally:
        queue._inner.request_shutdown()
        join_worker(worker)
