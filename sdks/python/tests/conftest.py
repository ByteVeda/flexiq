"""Shared fixtures for flexiq tests."""

import os
import sys
import threading
import time
from collections.abc import Callable, Generator
from contextlib import AbstractContextManager, contextmanager
from pathlib import Path

import pytest

from flexiq import Queue

# Public type alias used by workflow test files for the ``workflow_worker``
# fixture parameter (mypy requires annotated test parameters under
# ``disallow_untyped_defs``). Importing this from conftest keeps the type
# definition in one place.
WorkflowWorkerFactory = Callable[[], AbstractContextManager[threading.Thread]]

PollUntil = Callable[..., None]


@pytest.fixture
def poll_until() -> PollUntil:
    """Poll a predicate until it returns truthy, or fail on timeout.

    Replaces ``time.sleep(N)`` followed by an assertion in tests that wait
    for a background event (event-bus dispatch, webhook delivery, async
    executor completion). Polling shortens the typical wait while keeping
    a hard timeout for slow CI runners.

    Usage::

        poll_until(lambda: len(received) == 1)
        poll_until(lambda: counts["a"] == 1, timeout=10, message="callback never fired")
    """

    def _poll_until(
        predicate: Callable[[], bool],
        *,
        timeout: float = 5.0,
        interval: float = 0.05,
        message: str = "predicate did not become true",
    ) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(interval)
        if not predicate():
            raise AssertionError(f"{message} (timeout {timeout}s)")

    return _poll_until


@pytest.fixture
def queue(tmp_path: Path) -> Queue:
    """Create a fresh queue with a temp database."""
    db_path = str(tmp_path / "test.db")
    return Queue(db_path=db_path, workers=2)


@pytest.fixture
def run_worker(queue: Queue) -> Generator[threading.Thread]:
    """Start a worker thread for the given queue. Stops automatically at teardown."""
    thread = threading.Thread(target=queue.run_worker, daemon=True)
    thread.start()
    yield thread
    queue.shutdown()
    thread.join(timeout=5)


@pytest.fixture
def workflow_worker(queue: Queue) -> WorkflowWorkerFactory:
    """Context-manager factory that starts and stops a worker thread.

    Workflow tests typically run several short worker sessions per test
    (start, submit workflow, wait, stop — repeated). Returning a context
    manager from one fixture replaces the per-file ``_start_worker`` /
    ``_stop_worker`` helpers without changing test semantics.
    """

    @contextmanager
    def _ctx() -> Generator[threading.Thread]:
        thread = threading.Thread(target=queue.run_worker, daemon=True)
        thread.start()
        try:
            yield thread
        finally:
            queue.shutdown()
            thread.join(timeout=5)

    return _ctx


_PYTEST_EXIT_STATUS: int = 0


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    """Capture the exit status for ``pytest_unconfigure`` to act on."""
    global _PYTEST_EXIT_STATUS
    _PYTEST_EXIT_STATUS = int(exitstatus)


def pytest_unconfigure(config: pytest.Config) -> None:
    """Bypass CPython's interpreter finalization on a clean exit.

    Many tests leave PyO3-backed daemon threads (heartbeat, async executor,
    webhook delivery, distributed-lock extender) running at process end.
    During ``Py_Finalize`` those threads may try to (re)acquire the GIL after
    it has been torn down, producing ``FATAL: exception not rethrown`` and a
    SIGABRT — even though every test passed. ``os._exit`` skips finalization
    entirely after the terminal summary and junit XML are already written,
    eliminating the spurious crash.

    ``pytest_unconfigure`` fires after every other hook (terminal summary,
    junitxml plugin, etc.), so output and side effects are preserved. We
    skip the bypass on failure so pytest's normal traceback machinery still
    runs.
    """
    if _PYTEST_EXIT_STATUS == 0:
        sys.stdout.flush()
        sys.stderr.flush()
        os._exit(0)
