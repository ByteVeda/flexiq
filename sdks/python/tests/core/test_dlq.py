"""Tests for dead letter queue management."""

import threading
from typing import Any

from flexiq import Queue

PollUntil = Any  # the conftest fixture's runtime type


def _run_to_dlq(queue: Queue, poll_until: PollUntil, task_name: str = "instant_fail") -> None:
    """Register a failing task, enqueue it, and wait for it to land in DLQ."""

    @queue.task(name=task_name, max_retries=0, retry_backoff=0.1)
    def instant_fail() -> None:
        raise RuntimeError("fail")

    instant_fail.delay()

    worker = threading.Thread(target=queue.run_worker, daemon=True)
    worker.start()

    poll_until(
        lambda: len(queue.dead_letters()) >= 1,
        timeout=10,
        message="failed job did not reach DLQ",
    )


def test_dead_letters_empty(queue: Queue) -> None:
    """Empty DLQ returns empty list."""
    dead = queue.dead_letters()
    assert dead == []


def test_purge_dead(queue: Queue, poll_until: PollUntil) -> None:
    """Purging removes old dead letter entries."""
    _run_to_dlq(queue, poll_until)

    dead = queue.dead_letters()
    assert len(dead) >= 1

    purged = queue.purge_dead(older_than=0)
    assert isinstance(purged, int)


def test_delete_dead(queue: Queue, poll_until: PollUntil) -> None:
    """delete_dead discards a single entry without re-enqueueing."""
    _run_to_dlq(queue, poll_until)

    dead = queue.dead_letters()
    assert len(dead) == 1
    dead_id = dead[0]["id"]

    assert queue.delete_dead(dead_id) is True
    assert queue.dead_letters() == []


def test_delete_dead_nonexistent(queue: Queue) -> None:
    """delete_dead returns False for unknown IDs."""
    assert queue.delete_dead("nonexistent") is False


def test_dlq_retry_count_exposed(queue: Queue, poll_until: PollUntil) -> None:
    """DLQ entries expose dlq_retry_count field."""
    _run_to_dlq(queue, poll_until)

    dead = queue.dead_letters()
    assert dead[0]["dlq_retry_count"] == 0


def test_explicit_zero_max_retries_runs_once(queue: Queue, poll_until: PollUntil) -> None:
    """A per-call max_retries=0 overrides the task's default and runs at most once."""
    call_count = 0

    @queue.task(max_retries=3, retry_backoff=0.1)
    def at_most_once() -> None:
        nonlocal call_count
        call_count += 1
        raise RuntimeError("permanent failure")

    at_most_once.apply_async(max_retries=0)

    worker = threading.Thread(target=queue.run_worker, daemon=True)
    worker.start()

    poll_until(
        lambda: len(queue.dead_letters()) >= 1,
        timeout=15,
        message="max_retries=0 job did not reach the DLQ",
    )
    assert call_count == 1, "max_retries=0 must run the task exactly once"
