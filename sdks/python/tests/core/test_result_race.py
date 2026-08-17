"""Regression: result()/aresult() must surface terminal-failure exceptions
even when the deadline is reached on the same iteration the failure lands.

Race scenario:
1. `_poll_once()` returns ("running", None) — snapshot from before the failure
2. The job transitions to `failed`/`dead`/`cancelled` in storage
3. `time.monotonic() >= deadline` — about to give up
4. Fix: re-poll once before raising `TimeoutError` so the caller sees the
   real exception (TaskFailedError / MaxRetriesExceededError / ...).
"""

from typing import Any
from unittest.mock import patch

import pytest

from flexiq import Queue
from flexiq.exceptions import TaskFailedError


def _make_failing_poll(job_id: str) -> Any:
    """Returns a `_poll_once` stub: 1st call → running, 2nd call → TaskFailedError."""
    call_count = [0]

    def stub() -> tuple[str, Any]:
        call_count[0] += 1
        if call_count[0] == 1:
            return ("running", None)
        raise TaskFailedError(f"Job {job_id} failed: simulated late failure")

    stub.calls = call_count  # type: ignore[attr-defined]
    return stub


def test_result_surfaces_terminal_failure_at_deadline(tmp_path: Any) -> None:
    queue = Queue(db_path=str(tmp_path / "q.db"))

    @queue.task()
    def will_fail() -> None: ...

    job = will_fail.delay()
    poll = _make_failing_poll(job.id)

    with patch.object(job, "_poll_once", side_effect=poll), pytest.raises(TaskFailedError):
        job.result(timeout=0.01)

    assert poll.calls[0] == 2, "second defensive poll must run before raising TimeoutError"


async def test_aresult_surfaces_terminal_failure_at_deadline(tmp_path: Any) -> None:
    queue = Queue(db_path=str(tmp_path / "q.db"))

    @queue.task()
    async def will_fail() -> None: ...

    job = will_fail.delay()
    poll = _make_failing_poll(job.id)

    with patch.object(job, "_poll_once", side_effect=poll), pytest.raises(TaskFailedError):
        await job.aresult(timeout=0.01)

    assert poll.calls[0] == 2, "second defensive poll must run before raising TimeoutError"
