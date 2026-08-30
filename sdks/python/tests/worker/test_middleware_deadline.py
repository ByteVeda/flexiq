"""A middleware hook that outruns its budget is named, and never fatal.

``timeout`` bounds a task's handler and nothing else, so a ``before`` or
``after`` that blocks holds the attempt open past that limit. Python cannot
abandon the call — see :mod:`flexiq.hook_deadline` — so what is asserted here is
that the offender is named exactly once, that a hook inside its budget is
silent, and that the attempt still runs to its result either way.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Generator
from pathlib import Path
from typing import Any

import pytest

from flexiq import Queue
from flexiq.hook_deadline import _Entry, _HookWatchdog, hook_deadline
from flexiq.middleware import TaskMiddleware

_TIMEOUT = 15.0


def _overruns(caplog: pytest.LogCaptureFixture) -> list[str]:
    """Every overrun report captured so far, as rendered messages."""
    return [
        record.getMessage()
        for record in caplog.records
        if record.name == "flexiq" and "exceeded" in record.getMessage()
    ]


@pytest.fixture
def watchdog() -> Generator[_HookWatchdog]:
    """A watchdog of this test's own, so tests cannot report each other's hooks."""
    yield _HookWatchdog()


def test_an_overrunning_hook_is_named_once(
    caplog: pytest.LogCaptureFixture, watchdog: _HookWatchdog
) -> None:
    caplog.set_level(logging.WARNING, logger="flexiq")

    with hook_deadline(0.02, "pkg.Slow", "before", watchdog=watchdog):
        time.sleep(0.3)

    assert len(_overruns(caplog)) == 1
    assert "middleware pkg.Slow before() exceeded 20ms" in _overruns(caplog)[0]


def test_a_hook_that_returns_inside_its_budget_is_never_reported(
    caplog: pytest.LogCaptureFixture, watchdog: _HookWatchdog
) -> None:
    """Disarm is taken under the watchdog's lock.

    Two hundred arms of a budget the body cannot reach: every one is disarmed
    while the watchdog is holding it, and a watchdog that decided before it
    re-read the flag would report some of them.
    """
    caplog.set_level(logging.WARNING, logger="flexiq")

    for _ in range(200):
        with hook_deadline(0.05, "pkg.Fast", "after", watchdog=watchdog):
            pass
    # Let the watchdog wake and walk the entries it is still holding.
    time.sleep(0.2)

    assert _overruns(caplog) == []


def test_a_non_positive_budget_never_arms() -> None:
    """``0`` disables the bound outright — no entry, and no watchdog thread."""
    armed: list[_Entry] = []

    class Recording(_HookWatchdog):
        def arm(self, entry: _Entry) -> None:
            armed.append(entry)

    for disabled in (0.0, -1.0):
        with hook_deadline(disabled, "pkg.X", "before", watchdog=Recording()):
            pass

    assert armed == []


def test_the_sleep_path_says_nothing_will_reclaim_the_attempt(
    caplog: pytest.LogCaptureFixture, watchdog: _HookWatchdog
) -> None:
    """A slept attempt is already Pending and unclaimed, so no reaper sees it.

    That makes a hung ``on_sleep`` worse than a hung ``after``, and the report
    has to say so — the slot is held for as long as the hook runs.
    """
    caplog.set_level(logging.WARNING, logger="flexiq")

    with hook_deadline(0.02, "pkg.Slow", "on_sleep", slept=True, watchdog=watchdog):
        time.sleep(0.3)

    message = _overruns(caplog)[0]
    assert "on_sleep() exceeded 20ms" in message
    assert "never reaped" in message


def test_a_blocking_before_is_reported_and_the_task_still_runs(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """End to end: the lifecycle arms the budget, and an overrun is not fatal."""
    caplog.set_level(logging.WARNING, logger="flexiq")

    class Blocking(TaskMiddleware):
        name = "tests.Blocking"

        def before(self, ctx: Any) -> None:
            time.sleep(0.4)

    queue = Queue(
        db_path=str(tmp_path / "deadline.db"),
        workers=2,
        default_retry=0,
        middleware=[Blocking()],
        middleware_timeout=0.05,
    )
    try:

        @queue.task(name="bounded")
        def bounded(x: int) -> int:
            return x + 1

        job = bounded.delay(41)
        thread = threading.Thread(target=queue.run_worker, daemon=True)
        thread.start()
        try:
            assert job.result(timeout=_TIMEOUT) == 42
        finally:
            queue._inner.request_shutdown()
            thread.join(timeout=10)
            assert not thread.is_alive(), "worker did not stop within 10s"
    finally:
        queue.close()

    reported = [message for message in _overruns(caplog) if "tests.Blocking" in message]
    assert reported, "a before() over its budget was never reported"
    assert "before() exceeded 50ms" in reported[0]
