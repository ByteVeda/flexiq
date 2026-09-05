"""Shared fixtures for flexiq tests."""

import inspect
import os
import sys
import threading
import time
from collections.abc import Callable, Generator
from contextlib import AbstractContextManager, contextmanager
from pathlib import Path

import pytest

from flexiq import Queue, registry

# Public type alias used by workflow test files for the ``workflow_worker``
# fixture parameter (mypy requires annotated test parameters under
# ``disallow_untyped_defs``). Importing this from conftest keeps the type
# definition in one place.
WorkflowWorkerFactory = Callable[[], AbstractContextManager[threading.Thread]]

PollUntil = Callable[..., None]

# Seconds ``Queue.shutdown()`` is allowed to drain in-flight work. Read off the
# constructor rather than repeated as a literal, so this cannot drift from the
# timeout it exists to bound.
_DRAIN_TIMEOUT = int(inspect.signature(Queue.__init__).parameters["drain_timeout"].default)

# The drain, plus room to watch the thread unwind after it returns. A join
# budget is a failure deadline, not a delay: a passing run never spends it, so a
# generous number costs a green suite nothing and only changes how long a
# genuinely stuck one takes to report. Anything *under* the drain timeout fails
# runs the library still considers healthy — SQLite alone is opened with
# ``PRAGMA busy_timeout = 5000``, so one contended query inside the drain can
# burn 5s on its own. That is how a 5s budget copy-pasted across this suite
# surfaced as an unrelated-looking flake on whichever runner happened to be slow
# (#809). Raise this one number rather than a per-file literal.
WORKER_JOIN_TIMEOUT = _DRAIN_TIMEOUT + 15


def join_worker(
    thread: threading.Thread,
    *,
    timeout: float = WORKER_JOIN_TIMEOUT,
    message: str = "worker did not finish its drain",
) -> None:
    """Join a flexiq background thread that has already been asked to stop.

    Returning quietly on a still-live thread is how a shutdown regression hides:
    the thread keeps its SQLite handle open for the rest of the session, and on
    Windows that blocks the ``tmp_path`` cleanup of every later test.
    """
    thread.join(timeout=timeout)
    assert not thread.is_alive(), f"{message} (join budget {timeout}s)"


@pytest.fixture(autouse=True)
def _isolate_pending_registry() -> Generator[None]:
    """Snapshot and restore the module-global registry ``@flexiq.task`` writes to.

    It is a process global by design — that is what lets a task module register
    without importing the module that builds the ``Queue``. Left alone, a stray
    declaration from one test drains into every ``Queue`` built by the next.
    """
    snapshot = dict(registry._PENDING)
    try:
        yield
    finally:
        registry._PENDING.clear()
        registry._PENDING.update(snapshot)


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
    join_worker(thread)


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
            join_worker(thread)

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
