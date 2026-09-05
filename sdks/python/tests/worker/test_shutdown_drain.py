"""Worker shutdown must not depend on Python object lifetimes."""

import threading
import time
from pathlib import Path
from typing import Any

import pytest

from conftest import join_worker
from flexiq import Queue, _flexiq

PollUntil = Any  # the conftest fixture's runtime type

requires_native_async = pytest.mark.skipif(
    not hasattr(_flexiq, "PyResultSender"),
    reason="wheel built without the native-async feature",
)


@requires_native_async
def test_async_task_runs_on_native_executor(tmp_path: Path, poll_until: PollUntil) -> None:
    """Async tasks must dispatch to the executor's event loop, not a blocking thread."""
    queue = Queue(db_path=str(tmp_path / "native.db"), workers=2)
    seen: list[str] = []

    @queue.task(name="where_am_i")
    async def where_am_i() -> None:
        seen.append(threading.current_thread().name)

    where_am_i.delay()
    thread = threading.Thread(target=queue.run_worker, daemon=True)
    thread.start()
    try:
        poll_until(lambda: len(seen) == 1, timeout=15, message="task never ran")
    finally:
        queue.shutdown()
        join_worker(thread)

    assert seen == ["flexiq-async-executor"]


@requires_native_async
def test_failed_async_task_does_not_stall_shutdown(tmp_path: Path, poll_until: PollUntil) -> None:
    """A retained exception must not make shutdown wait out the drain timeout.

    A failed task's traceback reaches the executor frame that owns the async
    result sender, so a hook that keeps the exception held the result channel
    open — and a drain that waits for channel disconnect then burns the full
    30s, past a typical container's grace period.
    """
    queue = Queue(db_path=str(tmp_path / "stall.db"), workers=2)
    errors: list[BaseException] = []

    @queue.on_failure
    def collect(task_name: str, args: Any, kwargs: Any, error: BaseException) -> None:
        errors.append(error)

    @queue.task(name="boom", max_retries=0)
    async def boom() -> None:
        raise RuntimeError("kept alive by the hook")

    boom.delay()
    thread = threading.Thread(target=queue.run_worker, daemon=True)
    thread.start()
    try:
        poll_until(lambda: len(errors) == 1, timeout=15, message="failure hook never fired")
    finally:
        queue.shutdown()
        start = time.monotonic()
        join_worker(thread, message="the drain is waiting on the retained exception")
        elapsed = time.monotonic() - start

    assert elapsed < 5, (
        f"shutdown took {elapsed:.2f}s — the drain is waiting on the retained exception"
    )
