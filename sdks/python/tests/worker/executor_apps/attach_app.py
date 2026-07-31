"""Module-level Queue + tasks for the executor attach tests.

The Queue must be importable both in the ``taskito executor`` process and
inside each prefork child interpreter, so it lives at module scope and takes
its DB path from the environment — the same shape ``prefork_apps`` uses.

Storage is only touched because importing a ``Queue`` opens one; the executor
itself never reads it, since everything a task needs arrives on the wire.
"""

from __future__ import annotations

import os
import time
from pathlib import Path

from taskito import Queue
from taskito.context import current_job

queue = Queue(db_path=os.environ.get("TASKITO_EXECUTOR_TEST_DB", "/tmp/taskito-executor.db"))


def _markers() -> Path | None:
    """Directory the test uses to rendezvous with a running task.

    Heartbeats are seconds apart, so they cannot tell a test that a sub-second
    job has *started*. A file can, without adding a protocol frame that exists
    only for tests.
    """
    configured = os.environ.get("TASKITO_EXECUTOR_MARKERS")
    return Path(configured) if configured else None


@queue.task(max_retries=3)
def echo(value: str) -> str:
    """Return its argument, proving the payload survived the hop."""
    return f"echo:{value}"


@queue.task(max_retries=3)
def boom() -> None:
    """Always fail, so the retry verdict can be asserted."""
    raise RuntimeError("deliberate failure")


@queue.task(max_retries=0, timeout=60)
def slow(max_iters: int = 600) -> int:
    """Announce that it started, then loop until released or cancelled.

    ``check_cancelled()`` is polled each tick so a cancel lands promptly, and
    the release file lets a test end the job deterministically instead of
    waiting out a sleep.
    """
    markers = _markers()
    if markers is not None:
        markers.joinpath(f"{current_job.id}.started").write_text("1")
    release = None if markers is None else markers / "release"

    for completed in range(max_iters):
        current_job.check_cancelled()
        if release is not None and release.exists():
            return completed
        time.sleep(0.05)
    return max_iters
