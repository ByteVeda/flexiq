"""The lifecycle must behave identically whether a task is sync or async.

Every test here runs a real ``@queue.task()`` through a real worker, and is
parametrised over ``def`` vs ``async def``. That combination is the point: the
existing async tests drive ``AsyncTaskExecutor`` directly with a mocked queue, so
they cannot see whether a hook fired or a compensation context was set, and the
existing hook/saga/batch tests use a real queue but only sync tasks. Neither
shape catches a gap between the two dispatch paths.

Each test asserts on behaviour a caller was promised, not on which path ran, so
the same assertions hold before and after native async dispatch is activated.
That is also why none of them skip when the build lacks ``native-async``: the
``is_async=True`` cases then route through the blocking path, which is worth
asserting in its own right. Proving *which* path is live is a separate job, and
belongs to the canary that asserts coroutines run on the executor thread —
these tests deliberately cannot tell, and must not.

Tasks are declared before the worker starts on purpose: the scheduler registers
task policy once at worker boot, so a task declared afterwards silently falls
back to ``RetryPolicy::default()`` and ``max_retries=0`` is ignored.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Generator
from pathlib import Path
from typing import Any

import pytest

from conftest import join_worker
from flexiq import BatchItemResult, MaxRetriesExceededError, Queue
from flexiq.context import current_job
from flexiq.events import EventType
from flexiq.exceptions import SoftTimeoutError
from flexiq.middleware import TaskMiddleware
from flexiq.workflows import Workflow, WorkflowState
from flexiq.workflows.saga import current_compensation_context

# Parametrise the *task*, not the assertion: `async`ness is a property of the
# function object, so each case defines its own body under an `if is_async`.
BOTH_PATHS = pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])

_TIMEOUT = 15.0


@pytest.fixture
def noretry_queue(tmp_path: Path) -> Generator[Queue]:
    """Queue whose jobs DLQ on first failure, so failures stay observable."""
    q = Queue(db_path=str(tmp_path / "parity.db"), workers=2, default_retry=0)
    try:
        yield q
    finally:
        q.close()


def _start_worker(queue: Queue) -> threading.Thread:
    """Start a worker. Call only after every task in the test is declared."""
    thread = threading.Thread(target=queue.run_worker, daemon=True)
    thread.start()
    return thread


def _stop_worker(queue: Queue, thread: threading.Thread) -> None:
    """Shut the queue down and join the worker thread."""
    queue._inner.request_shutdown()
    join_worker(thread)


def _wait_for(predicate: Any, message: str) -> None:
    """Poll until ``predicate()`` is truthy, or fail with ``message``."""
    deadline = time.monotonic() + _TIMEOUT
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.02)
    raise AssertionError(message)


def test_re_registering_a_task_clears_its_soft_timeout(queue: Queue) -> None:
    """Replacing a task must not leave it wearing the old task's soft_timeout."""

    @queue.task(name="dup", soft_timeout=0.05)
    def first() -> None: ...

    assert queue._task_soft_timeouts["dup"] == 0.05

    @queue.task(name="dup")
    def second() -> None: ...

    assert "dup" not in queue._task_soft_timeouts


def test_setup_failure_undoes_what_setup_did(tmp_path: Path) -> None:
    """A task that dies during setup must still be torn down.

    Resources are injected and middleware prepared before the before_task hooks
    run, so a raising hook leaves them held — and the teardown that would undo
    them only runs once the body is entered, which it never is.
    """
    released: list[str] = []
    torn_down: list[tuple[type, str]] = []

    class Recorder(TaskMiddleware):
        def after(self, job: Any, result: Any, error: Any) -> None:
            torn_down.append((type(error), str(error)))

    q = Queue(
        db_path=str(tmp_path / "setup.db"), workers=2, default_retry=0, middleware=[Recorder()]
    )
    try:
        # Task scope, because its release calls `teardown` outright — a pooled
        # resource goes back to the pool, where nothing observes it.
        @q.worker_resource(name="db", scope="task", teardown=lambda conn: released.append(conn))
        def make_db() -> str:
            return "conn"

        # Raises after the resource is injected and Recorder.before() has run.
        @q.before_task
        def boom(task_name: str, args: tuple, kwargs: dict) -> None:
            raise RuntimeError("hook blew up during setup")

        @q.task(name="needs_db", max_retries=0, inject=["db"])
        def needs_db(db: Any = None) -> str:
            return f"got-{db}"

        needs_db.delay()
        thread = _start_worker(q)
        try:
            _wait_for(lambda: released, "the request-scoped resource was never released")
            _wait_for(lambda: torn_down, "middleware that ran before() never got its after()")
        finally:
            _stop_worker(q, thread)

        assert torn_down[0][0] is RuntimeError, (
            f"after() should see the error, got {torn_down[0]!r}"
        )
    finally:
        q.close()


def test_sync_task_returning_a_coroutine_is_awaited(queue: Queue) -> None:
    """A plain ``def`` may hand back a coroutine, and its value is the result.

    Not parametrised: the whole point is a task the lifecycle would not classify
    as async. The body must be awaited on what it returns, not on how it was
    declared — driving it instead would raise, since the lifecycle already runs
    on an event loop.
    """

    async def inner(x: int) -> int:
        return x * 2

    @queue.task(name="returns_coro")
    def returns_coro(x: int) -> Any:
        return inner(x)

    job = returns_coro.delay(21)
    thread = _start_worker(queue)
    try:
        assert job.result(timeout=_TIMEOUT) == 42
    finally:
        _stop_worker(queue, thread)


@BOTH_PATHS
def test_queue_hooks_fire(queue: Queue, is_async: bool) -> None:
    """before_task/on_success/after_task fire for either kind of task."""
    events: list[tuple[Any, ...]] = []

    @queue.before_task
    def on_before(task_name: str, args: tuple, kwargs: dict) -> None:
        events.append(("before", task_name))

    @queue.on_success
    def on_ok(task_name: str, args: tuple, kwargs: dict, result: Any) -> None:
        events.append(("success", task_name, result))

    @queue.after_task
    def on_after(task_name: str, args: tuple, kwargs: dict, result: Any, error: Any) -> None:
        events.append(("after", task_name, result, error))

    if is_async:

        @queue.task(name="hooked")
        async def hooked(a: int, b: int) -> int:
            return a + b

    else:

        @queue.task(name="hooked")
        def hooked(a: int, b: int) -> int:
            return a + b

    job = hooked.delay(1, 2)
    thread = _start_worker(queue)
    try:
        assert job.result(timeout=_TIMEOUT) == 3
    finally:
        _stop_worker(queue, thread)

    assert ("before", "hooked") in events
    assert ("success", "hooked", 3) in events
    assert ("after", "hooked", 3, None) in events


@BOTH_PATHS
def test_on_failure_hook_sees_the_exception(noretry_queue: Queue, is_async: bool) -> None:
    """on_failure receives the live exception for either kind of task."""
    # Record what the hook saw, not the exception itself: holding a live
    # exception here would pin its traceback — the documented condition that
    # stalls worker shutdown once dispatch goes native — and turn every failure
    # test into a slow one for no added assurance.
    failures: list[tuple[type, str]] = []

    @noretry_queue.on_failure
    def on_fail(task_name: str, args: tuple, kwargs: dict, error: BaseException) -> None:
        failures.append((type(error), str(error)))

    if is_async:

        @noretry_queue.task(name="boom", max_retries=0)
        async def boom() -> None:
            raise ValueError("expected failure")

    else:

        @noretry_queue.task(name="boom", max_retries=0)
        def boom() -> None:
            raise ValueError("expected failure")

    boom.delay()
    thread = _start_worker(noretry_queue)
    try:
        _wait_for(lambda: failures, "on_failure hook never fired")
    finally:
        _stop_worker(noretry_queue, thread)

    assert failures[0][0] is ValueError
    assert "expected failure" in failures[0][1]


@BOTH_PATHS
def test_soft_timeout_makes_check_timeout_raise(noretry_queue: Queue, is_async: bool) -> None:
    """check_timeout() raises once soft_timeout has elapsed, for either kind.

    soft_timeout reaches the task body through per-task config the lifecycle
    reads, so a body that never sees it would return "no timeout" instead.
    """
    outcomes: list[str] = []

    if is_async:

        @noretry_queue.task(name="slow", soft_timeout=0.05, max_retries=0)
        async def slow() -> str:
            time.sleep(0.2)
            try:
                current_job.check_timeout()
            except SoftTimeoutError:
                outcomes.append("raised")
                raise
            outcomes.append("no timeout")
            return "no timeout"

    else:

        @noretry_queue.task(name="slow", soft_timeout=0.05, max_retries=0)
        def slow() -> str:
            time.sleep(0.2)
            try:
                current_job.check_timeout()
            except SoftTimeoutError:
                outcomes.append("raised")
                raise
            outcomes.append("no timeout")
            return "no timeout"

    slow.delay()
    thread = _start_worker(noretry_queue)
    try:
        _wait_for(lambda: outcomes, "task body never ran")
    finally:
        _stop_worker(noretry_queue, thread)

    assert outcomes == ["raised"], "check_timeout() should raise once soft_timeout elapsed"


@BOTH_PATHS
def test_batch_per_item_failure_is_not_a_success(noretry_queue: Queue, is_async: bool) -> None:
    """A per-item failure fails the batch for either kind of task.

    Without per-item handling the task returns a list and looks like a plain
    success, so the failed item's caller would receive that list as its result
    instead of an error.
    """
    batch = {"max_size": 2, "max_wait_ms": 60_000, "per_item_results": True}

    if is_async:

        @noretry_queue.task(name="items", batch=batch, max_retries=0)
        async def items(values: list[int]) -> list[BatchItemResult]:
            return [
                BatchItemResult.failure(item_index=0, error="permanent"),
                BatchItemResult.success(item_index=1, result=values[1]),
            ]

    else:

        @noretry_queue.task(name="items", batch=batch, max_retries=0)
        def items(values: list[int]) -> list[BatchItemResult]:
            return [
                BatchItemResult.failure(item_index=0, error="permanent"),
                BatchItemResult.success(item_index=1, result=values[1]),
            ]

    thread = _start_worker(noretry_queue)
    try:
        first = items.delay(7)
        items.delay(8)
        with pytest.raises(MaxRetriesExceededError, match="permanent"):
            first.result(timeout=_TIMEOUT)
    finally:
        _stop_worker(noretry_queue, thread)


@BOTH_PATHS
def test_saga_compensation_context_is_visible(
    queue: Queue, workflow_worker: Any, is_async: bool
) -> None:
    """A compensator body can read its context for either kind of task.

    Parametrised on the *compensator*, since that is the body that reads the
    context. Its forward step is followed by a failing step to trigger it.
    """
    captured: dict[str, Any] = {}

    if is_async:

        @queue.task(name="refund", max_retries=0)
        async def refund(forward_args: tuple, forward_kwargs: dict, forward_result: Any) -> None:
            ctx = current_compensation_context()
            captured["ctx"] = ctx
            if ctx is not None:
                captured["node_name"] = ctx.workflow_node_name
                captured["forward_result"] = ctx.forward_result

    else:

        @queue.task(name="refund", max_retries=0)
        def refund(forward_args: tuple, forward_kwargs: dict, forward_result: Any) -> None:
            ctx = current_compensation_context()
            captured["ctx"] = ctx
            if ctx is not None:
                captured["node_name"] = ctx.workflow_node_name
                captured["forward_result"] = ctx.forward_result

    @queue.task(name="charge", max_retries=0, compensates=refund)
    def charge(amount: int) -> str:
        return f"charge-{amount}"

    @queue.task(name="fail_later", max_retries=0)
    def fail_later() -> None:
        raise RuntimeError("trigger compensation")

    wf = Workflow(name="parity_saga")
    wf.step("charge", charge, args=(250,))
    wf.step("fail", fail_later, after="charge")

    with workflow_worker():
        run = queue.submit_workflow(wf)
        final = run.wait(timeout=30)
        assert final.state in {WorkflowState.COMPENSATED, WorkflowState.COMPENSATION_FAILED}

    assert captured.get("ctx") is not None, "compensator saw no compensation context"
    assert captured["node_name"] == "charge"
    assert captured["forward_result"] == "charge-250"


@BOTH_PATHS
def test_lifecycle_is_logged(queue: Queue, is_async: bool, caplog: Any) -> None:
    """The received and succeeded lines are emitted for either kind of task."""
    if is_async:

        @queue.task(name="logged")
        async def logged() -> str:
            return "ok"

    else:

        @queue.task(name="logged")
        def logged() -> str:
            return "ok"

    with caplog.at_level("INFO", logger="flexiq"):
        job = logged.delay()
        thread = _start_worker(queue)
        try:
            assert job.result(timeout=_TIMEOUT) == "ok"
        finally:
            _stop_worker(queue, thread)

    assert "Task logged" in caplog.text
    assert "received" in caplog.text
    assert "succeeded" in caplog.text


@BOTH_PATHS
def test_middleware_after_receives_the_error(tmp_path: Path, is_async: bool) -> None:
    """Middleware .after() receives the exception, not None, for either kind."""
    seen: list[tuple[type, str] | None] = []
    ran = threading.Event()

    class Recorder(TaskMiddleware):
        def after(self, job: Any, result: Any, error: Any) -> None:
            # Type and message only — see the note in the on_failure test.
            seen.append(None if error is None else (type(error), str(error)))
            ran.set()

    q = Queue(db_path=str(tmp_path / "mw.db"), workers=2, default_retry=0, middleware=[Recorder()])
    try:
        if is_async:

            @q.task(name="failing", max_retries=0)
            async def failing() -> None:
                raise RuntimeError("expected failure")

        else:

            @q.task(name="failing", max_retries=0)
            def failing() -> None:
                raise RuntimeError("expected failure")

        failing.delay()
        thread = _start_worker(q)
        try:
            assert ran.wait(timeout=_TIMEOUT), "middleware after() never fired"
        finally:
            _stop_worker(q, thread)

        assert seen[0] is not None, "after() should see the error, not None"
        assert seen[0][0] is RuntimeError, f"after() should see the error, got {seen[0]!r}"
    finally:
        q.close()


@BOTH_PATHS
def test_base_exception_is_not_reported_as_completed(tmp_path: Path, is_async: bool) -> None:
    """A task killed by a BaseException must not emit JOB_COMPLETED.

    ``except Exception`` does not catch a BaseException, so without an explicit
    clause the lifecycle's cleanup sees no error and calls the job a success —
    it emits JOB_COMPLETED and hands ``error=None`` to after_task/middleware for
    a task whose body never returned. Latent on the blocking path, where only an
    exotic BaseException reaches it, but ``asyncio.CancelledError`` is a
    BaseException and is routine on the native path: every cancelled coroutine
    and every loop shutdown raises one.
    """
    completed: list[dict] = []
    after_task_errors: list[tuple[type, str] | None] = []
    ran = threading.Event()

    class Killed(BaseException):
        """Not an Exception, so `except Exception` cannot see it."""

    q = Queue(db_path=str(tmp_path / "base_exc.db"), workers=2, default_retry=0)
    try:
        q.on_event(EventType.JOB_COMPLETED, lambda _t, payload: completed.append(payload))

        @q.after_task
        def on_after(task_name: str, args: tuple, kwargs: dict, result: Any, error: Any) -> None:
            # Type and message only — see the note in the on_failure test.
            after_task_errors.append(None if error is None else (type(error), str(error)))
            ran.set()

        if is_async:

            @q.task(name="killed", max_retries=0)
            async def killed() -> None:
                raise Killed("torn down")

        else:

            @q.task(name="killed", max_retries=0)
            def killed() -> None:
                raise Killed("torn down")

        killed.delay()
        thread = _start_worker(q)
        try:
            assert ran.wait(timeout=_TIMEOUT), "after_task never fired"
        finally:
            _stop_worker(q, thread)

        assert completed == [], f"a torn-down task must not emit JOB_COMPLETED, got {completed}"
        assert after_task_errors[0] is not None, "after_task saw no error for a torn-down task"
        assert after_task_errors[0][0] is Killed, (
            f"after_task should see the BaseException, got {after_task_errors[0]!r}"
        )
    finally:
        q.close()


@BOTH_PATHS
def test_failed_task_does_not_stall_shutdown(tmp_path: Path, is_async: bool) -> None:
    """A failed task must not keep the worker from stopping promptly.

    The lifecycle holds the exception in a local while it runs the cleanup, and
    the exception's traceback points back at that frame — a cycle, so the frame
    and everything reachable from it survives until the cyclic collector runs.
    On native dispatch the async result sender is reachable from that frame, and
    the worker's drain loop only ends once the sender drops and the result
    channel disconnects. Leak it and every shutdown after a failed task waits out
    the full drain timeout instead of stopping.
    """
    q = Queue(db_path=str(tmp_path / "stall.db"), workers=2, default_retry=0)
    try:
        if is_async:

            @q.task(name="failing", max_retries=0)
            async def failing() -> None:
                raise ValueError("expected failure")

        else:

            @q.task(name="failing", max_retries=0)
            def failing() -> None:
                raise ValueError("expected failure")

        job = failing.delay()
        thread = _start_worker(q)

        # Wait for the job to reach a terminal state, so the failure is genuinely
        # behind us and the only thing left to measure is the shutdown itself.
        # `status` is the last-fetched value, so it needs a refresh to advance.
        def is_terminal() -> bool:
            job.refresh()
            return job.status in ("failed", "dead")

        _wait_for(is_terminal, "job never reached a terminal state")

        started = time.monotonic()
        q._inner.request_shutdown()
        join_worker(thread, message="worker did not stop after a failed task")
        elapsed = time.monotonic() - started

        # Prompt is ~0.2s; the leak shows up as the multi-second drain timeout.
        assert elapsed < 5, f"shutdown after a failed task took {elapsed:.1f}s — sender leaked?"
    finally:
        q.close()
