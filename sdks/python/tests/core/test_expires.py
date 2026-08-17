"""Tests for job expiry — the task-level default and per-call override."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from pathlib import Path

from flexiq import Queue
from flexiq.middleware import TaskMiddleware


class _CaptureEnqueue(TaskMiddleware):
    """Records the options dict of every enqueue for assertions."""

    def __init__(self) -> None:
        self.options: list[dict] = []

    def on_enqueue(self, task_name: str, args: tuple, kwargs: dict, options: dict) -> None:
        self.options.append(dict(options))


def _queue_with_capture(tmp_path: Path) -> tuple[Queue, _CaptureEnqueue]:
    capture = _CaptureEnqueue()
    queue = Queue(db_path=str(tmp_path / "test.db"), middleware=[capture])
    return queue, capture


def test_task_expires_default_flows_through_delay(tmp_path: Path) -> None:
    queue, capture = _queue_with_capture(tmp_path)

    @queue.task(expires=30)
    def ephemeral() -> None:
        pass

    ephemeral.delay()
    assert capture.options[-1]["expires"] == 30


def test_apply_async_expires_overrides_task_default(tmp_path: Path) -> None:
    queue, capture = _queue_with_capture(tmp_path)

    @queue.task(expires=30)
    def ephemeral() -> None:
        pass

    ephemeral.apply_async(expires=5)
    assert capture.options[-1]["expires"] == 5

    ephemeral.apply_async()
    assert capture.options[-1]["expires"] == 30


def test_no_expires_by_default(tmp_path: Path) -> None:
    queue, capture = _queue_with_capture(tmp_path)

    @queue.task()
    def durable() -> None:
        pass

    durable.delay()
    assert capture.options[-1]["expires"] is None


def test_expired_job_never_executes(queue: Queue, poll_until: Callable[..., None]) -> None:
    """A job past its expiry window is skipped, not run."""
    ran = threading.Event()

    @queue.task(expires=0.05)
    def ephemeral() -> None:
        ran.set()

    job = ephemeral.delay()
    time.sleep(0.2)  # let the expiry window lapse before any worker exists

    worker_thread = threading.Thread(target=queue.run_worker, daemon=True)
    worker_thread.start()

    def job_expired() -> bool:
        refreshed = queue.get_job(job.id)
        return refreshed is not None and refreshed.status == "cancelled"

    poll_until(job_expired, message="expired job never archived as cancelled")
    assert not ran.is_set()
    queue.shutdown()
    worker_thread.join(timeout=5)
