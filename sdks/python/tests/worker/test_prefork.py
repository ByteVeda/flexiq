"""Tests for the prefork (multi-process) worker pool."""

from __future__ import annotations

import contextlib
import importlib
import os
import sys
import threading
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from flexiq import Queue
from flexiq.context import JobContext
from flexiq.middleware import TaskMiddleware


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="prefork is rejected on Windows before app= is checked",
)
def test_prefork_requires_app_path(tmp_path: Path) -> None:
    """pool='prefork' without app= raises ValueError."""
    queue = Queue(db_path=str(tmp_path / "test.db"))

    @queue.task()
    def noop() -> None:
        pass

    with pytest.raises(ValueError, match="app= is required"):
        queue.run_worker(pool="prefork")


def test_prefork_rejected_on_windows(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """pool='prefork' on Windows fails fast with NotImplementedError."""
    monkeypatch.setattr(sys, "platform", "win32")
    queue = Queue(db_path=str(tmp_path / "test.db"))
    with pytest.raises(NotImplementedError, match="not supported on Windows"):
        queue.run_worker(pool="prefork", app="x:y")


def test_prefork_basic_execution(tmp_path: Path) -> None:
    """A task enqueued and processed by a prefork worker returns the correct result.

    NOTE: Prefork children import the app module independently, so the task name
    must resolve to the same module path in both parent and child. Tasks defined
    inside test functions can't be imported by children — use module-level tasks.
    This test is currently skipped; see test_prefork_module_level_queue for the
    working version.
    """
    pytest.skip("Tasks defined inside functions can't be imported by prefork children")


def test_prefork_thread_pool_unchanged(tmp_path: Path) -> None:
    """pool='thread' (default) still works normally."""
    q = Queue(db_path=str(tmp_path / "test.db"))

    @q.task()
    def multiply(x: int, y: int) -> int:
        return x * y

    job = multiply.delay(3, 7)

    worker = threading.Thread(target=q.run_worker, daemon=True)
    worker.start()

    result = job.result(timeout=10)
    assert result == 21

    q._inner.request_shutdown()


# ---------------------------------------------------------------------------
# Per-job timeout enforcement (issue #81)
# ---------------------------------------------------------------------------

# The prefork pool is Unix-oriented: child processes communicate over anonymous
# stdio pipes, which on Windows have different blocking semantics that make
# parent-side reader threads hang after `TerminateProcess`. Per-job timeout
# behaviour itself is identical, but the surrounding pool plumbing isn't
# Windows-ready, so these end-to-end tests are skipped there.
prefork_unix_only = pytest.mark.skipif(
    sys.platform == "win32",
    reason="prefork pool is Unix-only — child stdio pipe semantics differ on Windows",
)

PREFORK_APP_PATH = "prefork_apps.timeout_app:queue"
PREFORK_APP_DIR = str(Path(__file__).parent)


@pytest.fixture
def timeout_app(tmp_path: Path) -> Iterator[object]:
    """Set up the module-level timeout-test app with a per-test DB path.

    The Queue inside ``prefork_apps.timeout_app`` is constructed at import time
    from ``$FLEXIQ_TIMEOUT_TEST_DB``, and the prefork child re-imports the
    same module fresh in its own interpreter — so the env var must be set in
    the parent process before that import happens, and propagates to the
    child via inherited env.
    """
    db_path = str(tmp_path / "timeout.db")
    prev_db = os.environ.get("FLEXIQ_TIMEOUT_TEST_DB")
    prev_pythonpath = os.environ.get("PYTHONPATH")

    os.environ["FLEXIQ_TIMEOUT_TEST_DB"] = db_path
    # Make `prefork_apps.timeout_app` importable in both parent and (inherited)
    # child without depending on pytest's rootdir manipulation.
    os.environ["PYTHONPATH"] = (
        f"{PREFORK_APP_DIR}{os.pathsep}{prev_pythonpath}" if prev_pythonpath else PREFORK_APP_DIR
    )
    if PREFORK_APP_DIR not in sys.path:
        sys.path.insert(0, PREFORK_APP_DIR)

    # Force a fresh module import so the Queue picks up the per-test DB path.
    sys.modules.pop("prefork_apps.timeout_app", None)
    sys.modules.pop("prefork_apps", None)
    module = importlib.import_module("prefork_apps.timeout_app")

    try:
        yield module
    finally:
        with contextlib.suppress(Exception):
            module.queue._inner.request_shutdown()
        if prev_db is None:
            os.environ.pop("FLEXIQ_TIMEOUT_TEST_DB", None)
        else:
            os.environ["FLEXIQ_TIMEOUT_TEST_DB"] = prev_db
        if prev_pythonpath is None:
            os.environ.pop("PYTHONPATH", None)
        else:
            os.environ["PYTHONPATH"] = prev_pythonpath


def _start_prefork_worker(queue: Queue) -> threading.Thread:
    """Start a prefork worker for ``queue`` in a daemon thread."""
    thread = threading.Thread(
        target=queue.run_worker,
        kwargs={"pool": "prefork", "app": PREFORK_APP_PATH},
        daemon=True,
    )
    thread.start()
    return thread


def _wait_for_terminal(job: Any, timeout: float) -> str:
    """Poll a JobResult.refresh() until the status is terminal or `timeout` elapses."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        job.refresh()
        status: str = job.status
        if status in {"complete", "failed", "dead", "cancelled"}:
            return status
        time.sleep(0.1)
    job.refresh()
    final_status: str = job.status
    return final_status


@prefork_unix_only
def test_prefork_kills_hung_task(timeout_app: object, poll_until: Any) -> None:
    """A task that hangs past its `timeout=` is SIGKILLed by the watchdog and
    reported as a timeout failure within the timeout + watchdog tick budget."""
    timeouts_seen: list[str] = []

    class TimeoutSpy(TaskMiddleware):
        def on_timeout(self, ctx: JobContext) -> None:
            timeouts_seen.append(ctx.id)

    queue: Queue = timeout_app.queue  # type: ignore[attr-defined]
    queue._global_middleware.append(TimeoutSpy())

    started = time.monotonic()
    job = timeout_app.hang.delay()  # type: ignore[attr-defined]
    _start_prefork_worker(queue)

    # timeout=2s, watchdog tick=250ms → kill within ~2.25s; allow generous
    # headroom for child spawn and CI noise.
    status = _wait_for_terminal(job, timeout=15)
    elapsed = time.monotonic() - started

    assert status == "dead", f"expected 'dead', got {status!r} (error={job.error!r})"
    assert "timed out" in (job.error or "").lower()
    assert elapsed < 12, f"hung task took {elapsed:.1f}s to be killed (expected < 12s)"
    # The DB status flips to 'dead' inside `handle_result()`, but `on_timeout`
    # fires a moment later in `dispatch_outcome` on the worker thread — poll
    # rather than asserting on a single observation to avoid that race.
    poll_until(
        lambda: job.id in timeouts_seen,
        timeout=5,
        message="on_timeout middleware did not fire",
    )


@prefork_unix_only
def test_prefork_no_timeout_unaffected(timeout_app: object) -> None:
    """A task with no timeout (timeout=0) runs to completion — the watchdog
    must not kill jobs that have no deadline configured."""
    queue: Queue = timeout_app.queue  # type: ignore[attr-defined]

    job = timeout_app.quick.delay(21)  # type: ignore[attr-defined]
    _start_prefork_worker(queue)

    result = job.result(timeout=15)
    assert result == 42


@prefork_unix_only
def test_prefork_round_trips_binary_payloads(timeout_app: object) -> None:
    """Frames carry payloads as raw bytes after the header line, so a payload
    containing newlines must not desync the stream in either direction."""
    queue: Queue = timeout_app.queue  # type: ignore[attr-defined]

    blob = b'\n{"type":"success"}\n\x00\xff\n'
    job = timeout_app.echo_bytes.delay(blob)  # type: ignore[attr-defined]
    _start_prefork_worker(queue)

    assert job.result(timeout=15) == blob


@prefork_unix_only
def test_prefork_task_returning_none_completes(timeout_app: object) -> None:
    """A result frame that declares no payload still completes the job."""
    queue: Queue = timeout_app.queue  # type: ignore[attr-defined]

    job = timeout_app.returns_nothing.delay()  # type: ignore[attr-defined]
    _start_prefork_worker(queue)

    assert _wait_for_terminal(job, timeout=15) == "complete"
    assert job.result(timeout=5) is None


@prefork_unix_only
def test_prefork_finishes_before_deadline(timeout_app: object) -> None:
    """A task that completes well before its deadline returns normally — the
    watchdog only fires when the deadline is actually crossed."""
    queue: Queue = timeout_app.queue  # type: ignore[attr-defined]

    # timeout=2s, sleep 0.5s — should finish cleanly.
    job = timeout_app.sleep_then_finish.delay(0.5)  # type: ignore[attr-defined]
    _start_prefork_worker(queue)

    result = job.result(timeout=15)
    assert result == "done"


# ---------------------------------------------------------------------------
# Cooperative cancellation propagation (issue #82)
# ---------------------------------------------------------------------------

CANCEL_APP_PATH = "prefork_apps.cancel_app:queue"


@pytest.fixture
def cancel_app(tmp_path: Path) -> Iterator[object]:
    """Set up the module-level cancel-test app with a per-test DB path.

    Mirrors ``timeout_app`` — must set the env var before the import so the
    parent's Queue construction and the child's re-import see the same DB.
    """
    db_path = str(tmp_path / "cancel.db")
    prev_db = os.environ.get("FLEXIQ_CANCEL_TEST_DB")
    prev_pythonpath = os.environ.get("PYTHONPATH")

    os.environ["FLEXIQ_CANCEL_TEST_DB"] = db_path
    os.environ["PYTHONPATH"] = (
        f"{PREFORK_APP_DIR}{os.pathsep}{prev_pythonpath}" if prev_pythonpath else PREFORK_APP_DIR
    )
    if PREFORK_APP_DIR not in sys.path:
        sys.path.insert(0, PREFORK_APP_DIR)

    sys.modules.pop("prefork_apps.cancel_app", None)
    sys.modules.pop("prefork_apps", None)
    module = importlib.import_module("prefork_apps.cancel_app")

    try:
        yield module
    finally:
        with contextlib.suppress(Exception):
            module.queue._inner.request_shutdown()
        if prev_db is None:
            os.environ.pop("FLEXIQ_CANCEL_TEST_DB", None)
        else:
            os.environ["FLEXIQ_CANCEL_TEST_DB"] = prev_db
        if prev_pythonpath is None:
            os.environ.pop("PYTHONPATH", None)
        else:
            os.environ["PYTHONPATH"] = prev_pythonpath


def _start_cancel_worker(queue: Queue) -> threading.Thread:
    thread = threading.Thread(
        target=queue.run_worker,
        kwargs={"pool": "prefork", "app": CANCEL_APP_PATH},
        daemon=True,
    )
    thread.start()
    return thread


@prefork_unix_only
def test_prefork_cancel_running_job_stops_quickly(cancel_app: object, poll_until: Any) -> None:
    """``cancel_running_job`` propagates to the prefork child and stops a
    cooperative task within a small budget — the regression test for #82."""
    queue: Queue = cancel_app.queue  # type: ignore[attr-defined]

    cancels_seen: list[str] = []

    class CancelSpy(TaskMiddleware):
        def on_cancel(self, ctx: JobContext) -> None:
            cancels_seen.append(ctx.id)

    queue._global_middleware.append(CancelSpy())

    job = cancel_app.cooperative_loop.delay(600)  # type: ignore[attr-defined]
    _start_cancel_worker(queue)

    # Wait until the job is actually running on a child before cancelling.
    def _running() -> bool:
        job.refresh()
        return bool(job.status == "running")

    poll_until(_running, timeout=10, message="job never reached running state")

    assert queue.cancel_running_job(job.id) is True

    status = _wait_for_terminal(job, timeout=10)
    assert status == "cancelled", f"expected 'cancelled', got {status!r} (error={job.error!r})"
    # Same race as `on_timeout` in #154: handle_result flips the DB status to
    # 'cancelled' before dispatch_outcome fires on_cancel on a separate
    # thread, so a fast assertion can race past the middleware call. Poll
    # for the spy with a small budget instead.
    poll_until(
        lambda: job.id in cancels_seen,
        timeout=5,
        message="on_cancel middleware did not fire",
    )


@prefork_unix_only
def test_prefork_cancel_does_not_kill_child(cancel_app: object, poll_until: Any) -> None:
    """A cancel must stop the running task without killing the child — the
    next job dispatched to the same pool should still complete normally."""
    queue: Queue = cancel_app.queue  # type: ignore[attr-defined]

    long_job = cancel_app.cooperative_loop.delay(600)  # type: ignore[attr-defined]
    _start_cancel_worker(queue)

    def _running() -> bool:
        long_job.refresh()
        return bool(long_job.status == "running")

    poll_until(_running, timeout=10, message="long_job never reached running state")
    assert queue.cancel_running_job(long_job.id) is True
    status = _wait_for_terminal(long_job, timeout=10)
    assert status == "cancelled"

    follow_up = cancel_app.quick.delay(21)  # type: ignore[attr-defined]
    result = follow_up.result(timeout=15)
    assert result == 42


# ---------------------------------------------------------------------------
# Durable steps in a child process
# ---------------------------------------------------------------------------

STEPS_APP_PATH = "prefork_apps.steps_app:queue"


@pytest.fixture
def steps_app(tmp_path: Path) -> Iterator[object]:
    """Set up the module-level durable-step app with a per-test DB path.

    Same shape as ``cancel_app``: the env var must be set before the import so
    the parent's Queue and each child's re-import agree on the database.
    """
    db_path = str(tmp_path / "steps.db")
    prev_db = os.environ.get("FLEXIQ_STEPS_TEST_DB")
    prev_pythonpath = os.environ.get("PYTHONPATH")

    os.environ["FLEXIQ_STEPS_TEST_DB"] = db_path
    os.environ["PYTHONPATH"] = (
        f"{PREFORK_APP_DIR}{os.pathsep}{prev_pythonpath}" if prev_pythonpath else PREFORK_APP_DIR
    )
    if PREFORK_APP_DIR not in sys.path:
        sys.path.insert(0, PREFORK_APP_DIR)

    sys.modules.pop("prefork_apps.steps_app", None)
    sys.modules.pop("prefork_apps", None)
    module = importlib.import_module("prefork_apps.steps_app")

    try:
        yield module
    finally:
        with contextlib.suppress(Exception):
            module.queue._inner.request_shutdown()
        if prev_db is None:
            os.environ.pop("FLEXIQ_STEPS_TEST_DB", None)
        else:
            os.environ["FLEXIQ_STEPS_TEST_DB"] = prev_db
        if prev_pythonpath is None:
            os.environ.pop("PYTHONPATH", None)
        else:
            os.environ["PYTHONPATH"] = prev_pythonpath


def _start_steps_worker(queue: Queue) -> threading.Thread:
    thread = threading.Thread(
        target=queue.run_worker,
        kwargs={"pool": "prefork", "app": STEPS_APP_PATH},
        daemon=True,
    )
    thread.start()
    return thread


@prefork_unix_only
def test_prefork_child_memoizes_a_step(steps_app: object) -> None:
    """A child commits and replays steps against its own storage.

    The claim owner reaches the child through its spawn environment, never
    through the dispatch frame — the frame format also crosses a socket to an
    attached executor, and an owner an executor supplies is one it can forge.
    """
    queue: Queue = steps_app.queue  # type: ignore[attr-defined]

    job = steps_app.charge_once.delay()  # type: ignore[attr-defined]
    _start_steps_worker(queue)

    result = job.result(timeout=60)
    charge, ran = result.split("/")
    assert ran == "1", "the memoized step must not run a second time"
    assert charge.startswith(f"{job.id}:charge#0")


@prefork_unix_only
def test_prefork_child_sleeps_and_wakes(steps_app: object) -> None:
    """``step.sleep`` in a child ends the attempt and the job wakes to finish."""
    queue: Queue = steps_app.queue  # type: ignore[attr-defined]

    job = steps_app.naps.delay()  # type: ignore[attr-defined]
    _start_steps_worker(queue)

    assert job.result(timeout=60) == "awake"
    finished = queue.get_job(job.id)
    assert finished is not None
    assert finished.to_dict()["retry_count"] == 0, "a sleep costs no retry"
