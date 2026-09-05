"""Tests for retry logic and dead letter queue."""

import threading
from typing import Any

from conftest import join_worker
from flexiq import Queue

PollUntil = Any  # the conftest fixture's runtime type


def test_failing_task_retries(queue: Queue) -> None:
    """A failing task should be retried up to max_retries times."""
    call_count = 0

    @queue.task(max_retries=3, retry_backoff=0.1)
    def flaky_task() -> str:
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise ValueError(f"attempt {call_count}")
        return "success"

    job = flaky_task.delay()

    worker_thread = threading.Thread(
        target=queue.run_worker,
        daemon=True,
    )
    worker_thread.start()

    result = job.result(timeout=30)
    assert result == "success"
    assert call_count == 3


def test_exhausted_retries_goes_to_dlq(queue: Queue, poll_until: PollUntil) -> None:
    """A task that always fails should end up in the dead letter queue."""

    @queue.task(max_retries=2, retry_backoff=0.1)
    def always_fails() -> None:
        raise RuntimeError("permanent failure")

    always_fails.delay()

    worker_thread = threading.Thread(
        target=queue.run_worker,
        daemon=True,
    )
    worker_thread.start()

    poll_until(
        lambda: len(queue.dead_letters()) >= 1,
        timeout=15,
        message="job did not reach DLQ after exhausting retries",
    )

    dead = queue.dead_letters()
    assert len(dead) >= 1
    assert dead[0]["task_name"].endswith("always_fails")


def test_retry_dead_letter(queue: Queue, poll_until: PollUntil) -> None:
    """A dead letter job can be re-enqueued."""

    @queue.task(max_retries=1, retry_backoff=0.1)
    def fail_once() -> None:
        raise RuntimeError("fail")

    fail_once.delay()

    worker_thread = threading.Thread(
        target=queue.run_worker,
        daemon=True,
    )
    worker_thread.start()

    poll_until(
        lambda: len(queue.dead_letters()) >= 1,
        timeout=10,
        message="job did not reach DLQ",
    )

    dead = queue.dead_letters()
    if dead:
        new_id = queue.retry_dead(dead[0]["id"])
        assert new_id is not None
        assert len(new_id) > 0


def test_retry_budget_dead_letters_a_retry_storm(queue: Queue, poll_until: PollUntil) -> None:
    """retry_budget caps the retry *rate* across a task's jobs, not per job.

    max_retries bounds one job; a broken dependency failing every job still
    retries without limit. Once the budget is spent, failures dead-letter.
    """
    attempts = 0

    @queue.task(max_retries=5, retry_backoff=0.01, retry_budget="2/m")
    def always_fails() -> None:
        nonlocal attempts
        attempts += 1
        raise RuntimeError("dependency down")

    for _ in range(4):
        always_fails.delay()

    thread = threading.Thread(target=queue.run_worker, daemon=True)
    thread.start()
    try:
        poll_until(
            lambda: len(queue.dead_letters(limit=10)) >= 2,
            timeout=30,
            message="budget-exhausted jobs never reached the DLQ",
        )
    finally:
        queue.shutdown()
        join_worker(thread)

    dead = queue.dead_letters(limit=10)
    budget_killed = [d for d in dead if d.get("metadata") == "retry_budget_exhausted"]
    assert budget_killed, f"no job dead-lettered by the budget; DLQ={dead}"


def test_retry_budget_rejects_an_unparseable_rate(queue: Queue) -> None:
    """A typo must not silently disable the cap."""

    @queue.task(retry_budget="not-a-rate")
    def bad_budget() -> None:
        pass

    thread_error: list[BaseException] = []

    def _run() -> None:
        try:
            queue.run_worker()
        except BaseException as exc:
            thread_error.append(exc)

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    join_worker(thread, message="worker never exited after rejecting the retry budget")
    queue.shutdown()

    assert thread_error, "an invalid retry_budget must be rejected, not ignored"
    assert "retry_budget" in str(thread_error[0])
