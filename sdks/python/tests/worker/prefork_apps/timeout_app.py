"""Module-level Queue + tasks used by the prefork timeout regression tests.

Prefork children import the app module independently, so the task registry
must live at a module path resolvable inside the child interpreter — that's
why this module exists as a sibling of ``test_prefork.py`` rather than being
defined inline in the test.

The DB path comes from ``TASKITO_TIMEOUT_TEST_DB`` so each test run can use a
unique tmp file while still letting the parent and child build identical
Queue instances from this same module.
"""

from __future__ import annotations

import os
import time

from taskito import Queue

queue = Queue(db_path=os.environ.get("TASKITO_TIMEOUT_TEST_DB", "/tmp/taskito-timeout.db"))


@queue.task(timeout=2, max_retries=0)
def hang() -> None:
    """Spin forever — used to trigger the watchdog's SIGKILL path."""
    while True:
        pass


@queue.task()
def quick(x: int) -> int:
    """Returns immediately — used to verify timeout=0 (no timeout) is unaffected."""
    return x * 2


@queue.task()
def echo_bytes(blob: bytes) -> bytes:
    """Echo a binary blob — frames carry payloads raw, so a payload containing
    newlines must survive intact in both directions."""
    return blob


@queue.task()
def returns_nothing() -> None:
    """Returns None — a result frame that declares no payload at all."""
    return None


@queue.task(timeout=2, max_retries=0)
def sleep_then_finish(seconds: float) -> str:
    """Sleeps for `seconds`, then finishes — used to verify the watchdog only
    fires when the deadline is actually exceeded."""
    time.sleep(seconds)
    return "done"
