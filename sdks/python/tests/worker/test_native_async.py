"""Tests for native async task support."""

from __future__ import annotations

import asyncio
import threading
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import cloudpickle
import pytest

from flexiq import Queue, TaskCancelledError, current_job
from flexiq.async_support.context import (
    clear_async_context,
    get_async_context,
    set_async_context,
)
from flexiq.middleware import TaskMiddleware

PollUntil = Any  # the conftest fixture's runtime type


class _FakeQueue:
    """A queue stub covering every attribute the shared lifecycle reads.

    Deliberately not a bare ``MagicMock``: that invents whatever it is asked for,
    and the lifecycle branches on ``is not None`` and on truthiness, so an unset
    attribute would not raise — it would quietly take the wrong branch. An absent
    ``_task_batch_configs`` is the sharp one, since the auto-made mock reads as a
    per-item batch config and every task then fails the return-type contract.

    ``__slots__`` makes the trade explicit: a lifecycle read this class does not
    declare raises here, and so does a typo'd override, instead of turning into a
    silently wrong test.
    """

    __slots__ = (
        "_apply_dispatch_predicate",
        "_deserialize_payload",
        "_emit_event",
        "_get_middleware_chain",
        "_hooks",
        "_interceptor",
        "_max_reconstruction_timeout",
        "_middleware_timeout",
        "_proxy_metrics",
        "_proxy_registry",
        "_recipe_signing_key",
        "_resource_runtime",
        "_serialize_result",
        "_task_batch_configs",
        "_task_inject_map",
        "_task_predicates",
        "_task_retry_filters",
        "_task_soft_timeouts",
        "_test_mode_active",
        "_workflow_tracker",
    )

    def __init__(self) -> None:
        self._deserialize_payload = MagicMock(
            side_effect=lambda _name, data: cloudpickle.loads(data)
        )
        self._serialize_result = MagicMock(
            side_effect=lambda _name, result: cloudpickle.dumps(result)
        )
        self._get_middleware_chain = MagicMock(return_value=[])
        self._apply_dispatch_predicate = MagicMock()
        self._emit_event = MagicMock()
        self._interceptor = None
        self._proxy_registry = None
        self._proxy_metrics = None
        self._recipe_signing_key = None
        self._max_reconstruction_timeout = None
        self._middleware_timeout = 5.0
        self._test_mode_active = False
        self._resource_runtime = None
        self._workflow_tracker = None
        self._task_inject_map: dict[str, Any] = {}
        self._task_retry_filters: dict[str, Any] = {}
        self._task_predicates: dict[str, Any] = {}
        self._task_batch_configs: dict[str, Any] = {}
        self._task_soft_timeouts: dict[str, Any] = {}
        self._hooks: dict[str, list[Any]] = {
            "before_task": [],
            "after_task": [],
            "on_success": [],
            "on_failure": [],
        }


def _fake_queue(**overrides: Any) -> Any:
    """A :class:`_FakeQueue` with per-test overrides applied."""
    queue = _FakeQueue()
    for name, value in overrides.items():
        setattr(queue, name, value)
    return queue


# ── Async detection ──────────────────────────────────────────────


def test_async_task_detected(tmp_path: Path) -> None:
    """_flexiq_is_async is True for async functions."""
    queue = Queue(db_path=str(tmp_path / "test.db"))

    @queue.task()
    async def my_async_task() -> None:
        pass

    assert my_async_task._flexiq_is_async is True
    assert hasattr(my_async_task, "_flexiq_async_fn")


def test_sync_task_not_async(tmp_path: Path) -> None:
    """_flexiq_is_async is False for sync functions."""
    queue = Queue(db_path=str(tmp_path / "test.db"))

    @queue.task()
    def my_sync_task() -> None:
        pass

    assert my_sync_task._flexiq_is_async is False
    assert my_sync_task._flexiq_async_fn is None


# ── Async context (contextvars) ──────────────────────────────────


def test_async_context_var() -> None:
    """set/get/clear async context via contextvars."""
    token = set_async_context("job-1", "my_task", 0, "default")
    ctx = get_async_context()
    assert ctx is not None
    assert ctx.job_id == "job-1"
    assert ctx.task_name == "my_task"
    assert ctx.retry_count == 0
    assert ctx.queue_name == "default"
    clear_async_context(token)
    assert get_async_context() is None


def test_async_context_isolated_between_tasks() -> None:
    """Each async task gets its own contextvar context (no cross-contamination)."""
    results: list[str | None] = []

    async def coro(job_id: str) -> None:
        token = set_async_context(job_id, "task", 0, "q")
        await asyncio.sleep(0.01)
        ctx = get_async_context()
        results.append(ctx.job_id if ctx else None)
        clear_async_context(token)

    async def run_both() -> None:
        await asyncio.gather(coro("a"), coro("b"))

    asyncio.run(run_both())

    assert sorted(r for r in results if r is not None) == ["a", "b"]


def test_sync_context_unchanged(tmp_path: Path) -> None:
    """current_job still works via threading.local for sync tasks."""
    from flexiq.context import _clear_context, _set_context

    _set_context("sync-job", "sync_task", 2, "high")
    assert current_job.id == "sync-job"
    assert current_job.task_name == "sync_task"
    assert current_job.retry_count == 2
    assert current_job.queue_name == "high"
    _clear_context()


def test_async_context_fallback_to_sync() -> None:
    """_require_context falls back to threading.local when no async context."""
    from flexiq.context import _clear_context, _set_context

    # No async context set
    assert get_async_context() is None
    # Set sync context
    _set_context("sync-id", "t", 0, "q")
    assert current_job.id == "sync-id"
    _clear_context()


def test_async_context_preferred_over_sync() -> None:
    """When both async and sync contexts exist, async wins."""
    from flexiq.context import _clear_context, _set_context

    _set_context("sync-id", "t", 0, "q")
    token = set_async_context("async-id", "t", 0, "q")
    assert current_job.id == "async-id"
    clear_async_context(token)
    assert current_job.id == "sync-id"
    _clear_context()


# ── AsyncTaskExecutor unit tests ─────────────────────────────────


def test_async_executor_lifecycle() -> None:
    """Start/stop executor without errors."""
    from flexiq.async_support.executor import AsyncTaskExecutor

    sender = MagicMock()
    registry: dict[str, Any] = {}
    queue_ref = MagicMock()
    executor = AsyncTaskExecutor(sender, registry, queue_ref, max_concurrency=10)
    executor.start()
    assert executor._loop is not None
    assert executor._loop.is_running()
    executor.stop()


def test_stop_before_loop_runs_still_stops_the_thread() -> None:
    """stop() racing start() must not leave the loop thread running forever.

    Rebuilds start()'s state with the thread held before ``run_forever`` so the
    loop is provably not running when stop() checks it — the race window that
    ``is_running()`` misses and ``is_closed()`` does not.
    """
    from flexiq.async_support.executor import AsyncTaskExecutor

    executor = AsyncTaskExecutor(MagicMock(), {}, MagicMock(), max_concurrency=10)
    executor._loop = asyncio.new_event_loop()
    entered = threading.Event()
    checked = threading.Event()

    def held_run_forever() -> None:
        entered.wait(timeout=10)
        assert executor._loop is not None
        executor._loop.run_forever()

    class JoinSignalsCheckDone(threading.Thread):
        """stop() joins only after its loop check, so join marks the check done."""

        def join(self, timeout: float | None = None) -> None:
            checked.set()
            super().join(timeout)

    executor._thread = JoinSignalsCheckDone(target=held_run_forever, daemon=True)
    executor._thread.start()

    stopper = threading.Thread(target=executor.stop)
    stopper.start()
    # Release the thread into run_forever only once stop() has provably made
    # its loop check against a not-yet-running loop.
    assert checked.wait(timeout=10), "stop() never reached its join"
    entered.set()
    stopper.join(timeout=10)

    assert not stopper.is_alive(), "stop() never returned"
    assert not executor._thread.is_alive(), "loop thread kept running forever"
    assert executor._sender is None


def test_stop_releases_result_sender() -> None:
    """stop() must drop the sender itself — a pinned frame can keep the
    executor alive indefinitely, so the release cannot wait for GC."""
    from flexiq.async_support.executor import AsyncTaskExecutor

    executor = AsyncTaskExecutor(MagicMock(), {}, MagicMock(), max_concurrency=10)
    executor.start()
    executor.stop()
    assert executor._sender is None
    assert executor._thread is not None and not executor._thread.is_alive()


def test_async_executor_submit_and_execute(poll_until: PollUntil) -> None:
    """Basic async task produces correct result via executor."""
    from flexiq.async_support.executor import AsyncTaskExecutor

    sender = MagicMock()

    async def my_task(x: int, y: int) -> int:
        return x + y

    # Build a minimal wrapper that the executor expects
    class FakeWrapper:
        _flexiq_async_fn = staticmethod(my_task)

    registry: dict[str, Any] = {"test_mod.my_task": FakeWrapper()}

    queue_ref = _fake_queue()

    executor = AsyncTaskExecutor(sender, registry, queue_ref, max_concurrency=10)
    executor.start()

    payload = cloudpickle.dumps(((2, 3), {}))
    executor.submit_job("job-1", "test_mod.my_task", payload, 0, 3, "default")
    poll_until(lambda: sender.try_report_success.called, message="job-1 result not reported")
    executor.stop()

    sender.try_report_success.assert_called_once()
    call_args = sender.try_report_success.call_args
    assert call_args[0][0] == "job-1"
    assert call_args[0][1] == "test_mod.my_task"
    result = cloudpickle.loads(call_args[0][2])
    assert result == 5


def test_async_exception_reported(poll_until: PollUntil) -> None:
    """Exception in async task → failure result with traceback."""
    from flexiq.async_support.executor import AsyncTaskExecutor

    sender = MagicMock()

    async def failing_task() -> None:
        raise ValueError("boom")

    class FakeWrapper:
        _flexiq_async_fn = staticmethod(failing_task)

    registry: dict[str, Any] = {"mod.failing_task": FakeWrapper()}

    queue_ref = _fake_queue()

    executor = AsyncTaskExecutor(sender, registry, queue_ref, max_concurrency=10)
    executor.start()

    payload = cloudpickle.dumps(((), {}))
    executor.submit_job("job-2", "mod.failing_task", payload, 0, 3, "default")
    poll_until(lambda: sender.try_report_failure.called, message="job-2 failure not reported")
    executor.stop()

    sender.try_report_failure.assert_called_once()
    call_args = sender.try_report_failure.call_args
    assert call_args[0][0] == "job-2"
    assert "boom" in call_args[0][2]
    assert call_args[0][6] is True  # should_retry


def test_async_cancellation(poll_until: PollUntil) -> None:
    """TaskCancelledError → cancelled result."""
    from flexiq.async_support.executor import AsyncTaskExecutor

    sender = MagicMock()

    async def cancelling_task() -> None:
        raise TaskCancelledError("cancelled")

    class FakeWrapper:
        _flexiq_async_fn = staticmethod(cancelling_task)

    registry: dict[str, Any] = {"mod.cancelling_task": FakeWrapper()}

    queue_ref = _fake_queue()

    executor = AsyncTaskExecutor(sender, registry, queue_ref, max_concurrency=10)
    executor.start()

    payload = cloudpickle.dumps(((), {}))
    executor.submit_job("job-3", "mod.cancelling_task", payload, 0, 3, "default")
    poll_until(
        lambda: sender.try_report_cancelled.called, message="job-3 cancellation not reported"
    )
    executor.stop()

    sender.try_report_cancelled.assert_called_once()
    assert sender.try_report_cancelled.call_args[0][0] == "job-3"


def test_async_retry_filter(poll_until: PollUntil) -> None:
    """Failed async task respects retry_on filter."""
    from flexiq.async_support.executor import AsyncTaskExecutor

    sender = MagicMock()

    async def flaky_task() -> None:
        raise TypeError("wrong type")

    class FakeWrapper:
        _flexiq_async_fn = staticmethod(flaky_task)

    registry: dict[str, Any] = {"mod.flaky_task": FakeWrapper()}

    queue_ref = _fake_queue()
    # Only retry on ValueError, not TypeError
    queue_ref._task_retry_filters = {
        "mod.flaky_task": {"retry_on": [ValueError], "dont_retry_on": []},
    }
    queue_ref._get_middleware_chain.return_value = []
    queue_ref._proxy_metrics = None

    executor = AsyncTaskExecutor(sender, registry, queue_ref, max_concurrency=10)
    executor.start()

    payload = cloudpickle.dumps(((), {}))
    executor.submit_job("job-4", "mod.flaky_task", payload, 0, 3, "default")
    poll_until(lambda: sender.try_report_failure.called, message="job-4 failure not reported")
    executor.stop()

    sender.try_report_failure.assert_called_once()
    assert sender.try_report_failure.call_args[0][6] is False  # should_retry = False


def test_async_concurrency_limit(poll_until: PollUntil) -> None:
    """Semaphore bounds concurrent async tasks."""
    from flexiq.async_support.executor import AsyncTaskExecutor

    sender = MagicMock()
    max_concurrent = 0
    current = 0
    lock = threading.Lock()

    async def slow_task() -> None:
        nonlocal max_concurrent, current
        with lock:
            current += 1
            max_concurrent = max(max_concurrent, current)
        await asyncio.sleep(0.1)
        with lock:
            current -= 1

    class FakeWrapper:
        _flexiq_async_fn = staticmethod(slow_task)

    registry: dict[str, Any] = {"mod.slow_task": FakeWrapper()}

    queue_ref = _fake_queue()

    # Set concurrency to 2
    executor = AsyncTaskExecutor(sender, registry, queue_ref, max_concurrency=2)
    executor.start()

    payload = cloudpickle.dumps(((), {}))
    for i in range(5):
        executor.submit_job(f"job-{i}", "mod.slow_task", payload, 0, 3, "default")

    poll_until(
        lambda: sender.try_report_success.call_count >= 5,
        timeout=10,
        message="not all 5 slow_task jobs reported success",
    )
    executor.stop()

    assert max_concurrent <= 2
    assert sender.try_report_success.call_count == 5


def test_async_middleware_hooks(poll_until: PollUntil) -> None:
    """Middleware before/after called for async tasks."""
    from flexiq.async_support.executor import AsyncTaskExecutor

    before_called: list[str] = []
    after_called: list[str] = []

    class TestMiddleware(TaskMiddleware):
        def before(self, job_context: Any) -> None:
            before_called.append(job_context.id)

        def after(self, job_context: Any, result: Any, error: Any) -> None:
            after_called.append(job_context.id)

    sender = MagicMock()

    async def simple_task() -> int:
        return 42

    class FakeWrapper:
        _flexiq_async_fn = staticmethod(simple_task)

    registry: dict[str, Any] = {"mod.simple_task": FakeWrapper()}

    queue_ref = _fake_queue()
    queue_ref._get_middleware_chain.return_value = [TestMiddleware()]

    executor = AsyncTaskExecutor(sender, registry, queue_ref, max_concurrency=10)
    executor.start()

    payload = cloudpickle.dumps(((), {}))
    executor.submit_job("mw-job", "mod.simple_task", payload, 0, 3, "default")
    poll_until(lambda: "mw-job" in after_called, message="middleware after hook not called")
    executor.stop()

    assert "mw-job" in before_called
    assert "mw-job" in after_called


def test_async_task_with_injection(poll_until: PollUntil) -> None:
    """inject=["db"] works for async tasks via executor."""
    from flexiq.async_support.executor import AsyncTaskExecutor

    sender = MagicMock()

    async def db_task(db: Any = None) -> str:
        return f"got-{db}"

    class FakeWrapper:
        _flexiq_async_fn = staticmethod(db_task)

    registry: dict[str, Any] = {"mod.db_task": FakeWrapper()}

    fake_db = "fake-conn"

    # Mock resource runtime
    runtime = MagicMock()
    runtime.acquire_for_task.return_value = (fake_db, None)

    queue_ref = _fake_queue(
        _task_inject_map={"mod.db_task": ["db"]},
        _resource_runtime=runtime,
    )

    executor = AsyncTaskExecutor(sender, registry, queue_ref, max_concurrency=10)
    executor.start()

    payload = cloudpickle.dumps(((), {}))
    executor.submit_job("inj-job", "mod.db_task", payload, 0, 3, "default")
    poll_until(lambda: sender.try_report_success.called, message="inj-job result not reported")
    executor.stop()

    sender.try_report_success.assert_called_once()
    result = cloudpickle.loads(sender.try_report_success.call_args[0][2])
    assert result == "got-fake-conn"


def test_async_context_available_inside_task(poll_until: PollUntil) -> None:
    """current_job.id works inside an async task via contextvars."""
    from flexiq.async_support.executor import AsyncTaskExecutor

    sender = MagicMock()
    captured_id: list[str] = []

    async def ctx_task() -> str:
        captured_id.append(current_job.id)
        return "ok"

    class FakeWrapper:
        _flexiq_async_fn = staticmethod(ctx_task)

    registry: dict[str, Any] = {"mod.ctx_task": FakeWrapper()}

    queue_ref = _fake_queue()

    executor = AsyncTaskExecutor(sender, registry, queue_ref, max_concurrency=10)
    executor.start()

    payload = cloudpickle.dumps(((), {}))
    executor.submit_job("ctx-job", "mod.ctx_task", payload, 0, 3, "default")
    poll_until(lambda: captured_id == ["ctx-job"], message="ctx-job context not captured")
    executor.stop()

    assert captured_id == ["ctx-job"]


def test_async_concurrency_parameter(tmp_path: Path) -> None:
    """Queue accepts async_concurrency parameter."""
    queue = Queue(db_path=str(tmp_path / "test.db"), async_concurrency=50)
    assert queue._async_concurrency == 50


def test_async_concurrency_default(tmp_path: Path) -> None:
    """Default async_concurrency is 100."""
    queue = Queue(db_path=str(tmp_path / "test.db"))
    assert queue._async_concurrency == 100


def test_async_executor_honors_per_task_serializer(poll_until: PollUntil) -> None:
    """Regression: the async executor deserializes via
    ``queue._deserialize_payload`` (honoring ``@task(serializer=...)``) and
    serializes results via ``queue._serialize_result`` rather than hardcoded
    cloudpickle in either direction."""
    from flexiq.async_support.executor import AsyncTaskExecutor
    from flexiq.serializers import JsonSerializer

    sender = MagicMock()
    serializer = JsonSerializer()

    async def add(x: int, y: int) -> int:
        return x + y

    class FakeWrapper:
        _flexiq_async_fn = staticmethod(add)

    registry: dict[str, Any] = {"mod.add": FakeWrapper()}

    queue_ref = _fake_queue()
    # Route serialization through a NON-cloudpickle serializer; a hardcoded
    # cloudpickle.loads would raise on this JSON payload.
    queue_ref._deserialize_payload.side_effect = lambda _name, data: serializer.loads(data)
    queue_ref._serialize_result.side_effect = lambda _name, result: serializer.dumps(result)

    executor = AsyncTaskExecutor(sender, registry, queue_ref, max_concurrency=10)
    executor.start()

    payload = serializer.dumps(([2, 3], {}))
    executor.submit_job("ser-job", "mod.add", payload, 0, 3, "default")
    poll_until(lambda: sender.try_report_success.called, message="ser-job result not reported")
    executor.stop()

    queue_ref._deserialize_payload.assert_called_once_with("mod.add", payload)
    queue_ref._serialize_result.assert_called_once_with("mod.add", 5)
    result = serializer.loads(sender.try_report_success.call_args[0][2])
    assert result == 5


# ── Backpressure: permits, result hand-off, cancellation (S16/S17) ──


class FakePermit:
    """Stands in for the Rust PyJobPermit handle the pool hands to the executor."""

    def __init__(self) -> None:
        self.releases = 0

    def release(self) -> None:
        self.releases += 1


def _backpressure_executor(sender: Any, fn: Any, task_name: str) -> Any:
    """An executor wired to a single async `fn`, with everything else mocked out."""
    from flexiq.async_support.executor import AsyncTaskExecutor

    class FakeWrapper:
        _flexiq_async_fn = staticmethod(fn)

    queue_ref = _fake_queue()

    executor = AsyncTaskExecutor(sender, {task_name: FakeWrapper()}, queue_ref, max_concurrency=10)
    executor.start()
    return executor


def _payload() -> bytes:
    payload: bytes = cloudpickle.dumps(((), {}))
    return payload


@pytest.mark.filterwarnings("ignore::pytest.PytestUnhandledThreadExceptionWarning")
def test_base_exception_reports_failure(poll_until: PollUntil) -> None:
    """A non-cancellation BaseException must still reach a terminal report.

    KeyboardInterrupt and friends slip past `except Exception` just as
    CancelledError does, but they are not cancellations — unreported, the job
    sits Running until the reaper mislabels it a timeout.

    The interrupt is re-raised after reporting, which is the contract (swallowing
    it would break loop teardown), so it surfaces on the executor thread and
    pytest notices — expected here, hence the filter.
    """
    sender = MagicMock()
    sender.try_report_failure.return_value = True

    async def interrupted_task() -> None:
        raise KeyboardInterrupt

    executor = _backpressure_executor(sender, interrupted_task, "mod.interrupted_task")
    executor.submit_job("job-k", "mod.interrupted_task", _payload(), 0, 3, "default")
    poll_until(
        lambda: sender.try_report_failure.called,
        message="KeyboardInterrupt was not reported",
    )
    executor.stop()

    assert not sender.try_report_cancelled.called, "an interrupt is not a cancellation"
    # `stop()` only logs when its join times out, and the thread-exception warning
    # is filtered here — without this the test could go green on a leaked thread.
    assert not executor._thread.is_alive(), "the executor thread outlived stop()"


def test_cancelled_coroutine_reports_cancelled(poll_until: PollUntil) -> None:
    """asyncio.CancelledError is a BaseException, so `except Exception` misses it.

    Unreported, the job sits Running until the reaper mislabels it a timeout.
    """
    sender = MagicMock()
    sender.try_report_cancelled.return_value = True

    async def cancelled_task() -> None:
        raise asyncio.CancelledError

    executor = _backpressure_executor(sender, cancelled_task, "mod.cancelled_task")
    executor.submit_job("job-c", "mod.cancelled_task", _payload(), 0, 3, "default")
    poll_until(
        lambda: sender.try_report_cancelled.called,
        message="asyncio.CancelledError was not reported",
    )
    executor.stop()

    assert sender.try_report_cancelled.call_args[0][0] == "job-c"
    assert not sender.try_report_failure.called, "a cancellation is not a failure"


def test_no_double_report_when_report_raises(poll_until: PollUntil) -> None:
    """A raising success hand-off must not also fire a failure for the same job."""
    sender = MagicMock()
    sender.try_report_success.side_effect = RuntimeError("channel exploded")
    sender.try_report_failure.return_value = True

    async def ok_task() -> int:
        return 1

    executor = _backpressure_executor(sender, ok_task, "mod.ok_task")
    executor.submit_job("job-d", "mod.ok_task", _payload(), 0, 3, "default")
    poll_until(lambda: sender.try_report_success.called, message="success never attempted")
    executor.stop()

    assert not sender.try_report_failure.called, "one job must yield at most one result"


def test_permit_released_on_success(poll_until: PollUntil) -> None:
    sender = MagicMock()
    sender.try_report_success.return_value = True
    permit = FakePermit()

    async def ok_task() -> int:
        return 1

    executor = _backpressure_executor(sender, ok_task, "mod.ok_task")
    executor.submit_job("job-p", "mod.ok_task", _payload(), 0, 3, "default", permit)
    poll_until(lambda: permit.releases == 1, message="permit not released on success")
    executor.stop()


def test_permit_released_on_exception(poll_until: PollUntil) -> None:
    sender = MagicMock()
    sender.try_report_failure.return_value = True
    permit = FakePermit()

    async def boom_task() -> None:
        raise ValueError("boom")

    executor = _backpressure_executor(sender, boom_task, "mod.boom_task")
    executor.submit_job("job-p", "mod.boom_task", _payload(), 0, 3, "default", permit)
    poll_until(lambda: permit.releases == 1, message="permit not released on exception")
    executor.stop()


def test_permit_released_on_cancellation(poll_until: PollUntil) -> None:
    """The path that leaks a slot forever if release rides on the result report."""
    sender = MagicMock()
    sender.try_report_cancelled.return_value = True
    permit = FakePermit()

    async def cancelled_task() -> None:
        raise asyncio.CancelledError

    executor = _backpressure_executor(sender, cancelled_task, "mod.cancelled_task")
    executor.submit_job("job-p", "mod.cancelled_task", _payload(), 0, 3, "default", permit)
    poll_until(lambda: permit.releases == 1, message="permit not released on cancellation")
    executor.stop()


def test_report_retries_when_channel_full_without_stalling_the_loop(
    poll_until: PollUntil,
) -> None:
    """A full result channel must back off, not block — and never drop the result.

    `job-a` is refused until `job-b` reports, so `job-b` can only get through if the
    event loop stayed responsive while `job-a` was backing off. A blocking send would
    wedge both and time this test out.
    """
    sender = MagicMock()
    reported_b = threading.Event()

    def send(job_id: str, task_name: str, result: Any, wall_ns: int) -> bool:
        if job_id == "job-b":
            reported_b.set()
            return True
        return reported_b.is_set()

    sender.try_report_success.side_effect = send

    async def ok_task() -> int:
        return 1

    executor = _backpressure_executor(sender, ok_task, "mod.ok_task")
    executor.submit_job("job-a", "mod.ok_task", _payload(), 0, 3, "default")
    executor.submit_job("job-b", "mod.ok_task", _payload(), 0, 3, "default")

    poll_until(
        lambda: (
            sum(1 for c in sender.try_report_success.call_args_list if c[0][0] == "job-a") > 1
            and reported_b.is_set()
        ),
        message="job-a never retried, or job-b starved behind it",
    )
    executor.stop()

    a_calls = [c for c in sender.try_report_success.call_args_list if c[0][0] == "job-a"]
    assert len(a_calls) > 1, "job-a must retry rather than drop its result"


def test_native_dispatch_emits_job_completed(poll_until: PollUntil) -> None:
    """Native dispatch must emit JOB_COMPLETED itself.

    The Rust outcome loop skips Success on the grounds that the blocking task
    wrapper emits it — but native dispatch bypasses that wrapper. Without this,
    nothing downstream (notably the workflow tracker) ever learns the job
    finished, and a workflow with an async step hangs in `running` forever.
    """
    from flexiq.events import EventType

    sender = MagicMock()
    sender.try_report_success.return_value = True

    async def ok_task() -> int:
        return 1

    executor = _backpressure_executor(sender, ok_task, "mod.ok_task")
    queue_ref = executor._queue_ref
    executor.submit_job("job-e", "mod.ok_task", _payload(), 0, 3, "default")
    poll_until(lambda: queue_ref._emit_event.called, message="no lifecycle event emitted")
    executor.stop()

    event_type, payload = queue_ref._emit_event.call_args[0]
    assert event_type == EventType.JOB_COMPLETED
    assert payload["job_id"] == "job-e"
    assert payload["task_name"] == "mod.ok_task"
    assert payload["queue"] == "default"


def test_native_dispatch_emits_job_failed(poll_until: PollUntil) -> None:
    from flexiq.events import EventType

    sender = MagicMock()
    sender.try_report_failure.return_value = True

    async def boom_task() -> None:
        raise ValueError("boom")

    executor = _backpressure_executor(sender, boom_task, "mod.boom_task")
    queue_ref = executor._queue_ref
    executor.submit_job("job-f", "mod.boom_task", _payload(), 0, 3, "default")
    poll_until(lambda: queue_ref._emit_event.called, message="no lifecycle event emitted")
    executor.stop()

    event_type, payload = queue_ref._emit_event.call_args[0]
    assert event_type == EventType.JOB_FAILED
    assert payload["error"] == "boom"


def test_native_dispatch_does_not_emit_completed_on_cancel(poll_until: PollUntil) -> None:
    """Cancellation has its own Rust-side event; emitting COMPLETED would be a lie.

    Both paths run one lifecycle, so both report a cancel the same way:
    TaskCancelledError is an ordinary Exception, so it emits JOB_FAILED here and
    the outcome loop adds JOB_CANCELLED. That pairing is the blocking path's
    long-standing behaviour — what must never appear is JOB_COMPLETED.
    """
    from flexiq.events import EventType

    sender = MagicMock()
    sender.try_report_cancelled.return_value = True

    async def cancelling_task() -> None:
        raise TaskCancelledError("nope")

    executor = _backpressure_executor(sender, cancelling_task, "mod.cancelling_task")
    queue_ref = executor._queue_ref
    executor.submit_job("job-g", "mod.cancelling_task", _payload(), 0, 3, "default")
    poll_until(lambda: sender.try_report_cancelled.called, message="cancellation not reported")
    executor.stop()

    emitted = [call.args[0] for call in queue_ref._emit_event.call_args_list]
    assert EventType.JOB_COMPLETED not in emitted, "a cancelled job did not complete"
    assert EventType.JOB_FAILED in emitted, "cancel should report like the blocking path"
