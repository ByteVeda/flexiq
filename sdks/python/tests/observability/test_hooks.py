"""Tests for the hooks/middleware system."""

from __future__ import annotations

import threading
from typing import Any

from flexiq import Queue

PollUntil = Any  # the conftest fixture's runtime type


def test_before_and_after_hooks(queue: Queue) -> None:
    """before_task and after_task hooks fire around task execution."""
    events: list[tuple[Any, ...]] = []

    @queue.before_task
    def on_before(task_name: str, args: tuple, kwargs: dict) -> None:
        events.append(("before", task_name))

    @queue.after_task
    def on_after(task_name: str, args: tuple, kwargs: dict, result: Any, error: Any) -> None:
        events.append(("after", task_name, result, error))

    @queue.task()
    def add(a: int, b: int) -> int:
        return a + b

    job = add.delay(1, 2)

    worker_thread = threading.Thread(target=queue.run_worker, daemon=True)
    worker_thread.start()

    result = job.result(timeout=10)
    assert result == 3

    # Verify hooks fired
    assert any(e[0] == "before" for e in events)
    assert any(e[0] == "after" and len(e) > 2 and e[2] == 3 and e[3] is None for e in events)


def test_on_success_hook(queue: Queue) -> None:
    """on_success hook fires when task succeeds."""
    success_results: list[Any] = []

    @queue.on_success
    def on_success(task_name: str, args: tuple, kwargs: dict, result: Any) -> None:
        success_results.append(result)

    @queue.task()
    def multiply(a: int, b: int) -> int:
        return a * b

    job = multiply.delay(3, 4)

    worker_thread = threading.Thread(target=queue.run_worker, daemon=True)
    worker_thread.start()

    result = job.result(timeout=10)
    assert result == 12
    assert 12 in success_results


def test_on_failure_hook(queue: Queue, poll_until: PollUntil) -> None:
    """on_failure hook fires when task raises."""
    failure_errors: list[str] = []

    @queue.on_failure
    def on_failure(task_name: str, args: tuple, kwargs: dict, error: Exception) -> None:
        failure_errors.append(str(error))

    @queue.task(max_retries=1, retry_backoff=0.1)
    def always_fails() -> None:
        raise ValueError("boom")

    always_fails.delay()

    worker_thread = threading.Thread(target=queue.run_worker, daemon=True)
    worker_thread.start()

    poll_until(lambda: len(failure_errors) > 0, timeout=5)
    assert any("boom" in e for e in failure_errors)
