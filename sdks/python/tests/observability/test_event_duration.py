"""Job lifecycle events report how long the task ran."""

from __future__ import annotations

import threading
import time
from typing import Any

from flexiq import Queue
from flexiq.events import EventType

PollUntil = Any  # the conftest fixture's runtime type


def test_completed_event_reports_duration(queue: Queue, poll_until: PollUntil) -> None:
    """job.completed carries duration_ms, so a subscriber needn't time the run itself."""
    payloads: list[dict[str, Any]] = []
    queue.on_event(EventType.JOB_COMPLETED, lambda event_type, payload: payloads.append(payload))

    @queue.task()
    def slow() -> str:
        """Sleep long enough that a duration is unambiguously measurable."""
        time.sleep(0.06)
        return "ok"

    job = slow.delay()
    worker_thread = threading.Thread(target=queue.run_worker, daemon=True)
    worker_thread.start()
    assert job.result(timeout=10) == "ok"

    poll_until(lambda: len(payloads) >= 1, message="no job.completed event")
    # The task slept 60ms; allow for timer jitter.
    assert payloads[0]["duration_ms"] >= 50


def test_retry_event_reports_duration(queue: Queue, poll_until: PollUntil) -> None:
    """job.retrying carries the failed attempt's duration, measured by the worker."""
    payloads: list[dict[str, Any]] = []
    queue.on_event(EventType.JOB_RETRYING, lambda event_type, payload: payloads.append(payload))
    attempts: list[int] = []

    @queue.task(max_retries=1)
    def flaky() -> str:
        """Fail the first attempt so the outcome loop emits a retry."""
        attempts.append(1)
        if len(attempts) == 1:
            time.sleep(0.06)
            raise ValueError("boom")
        return "ok"

    job = flaky.delay()
    worker_thread = threading.Thread(target=queue.run_worker, daemon=True)
    worker_thread.start()
    assert job.result(timeout=15) == "ok"

    poll_until(lambda: len(payloads) >= 1, message="no job.retrying event")
    assert payloads[0]["duration_ms"] >= 50
