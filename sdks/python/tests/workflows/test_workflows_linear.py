"""Tests for Phase 2 linear workflow execution."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from contextlib import AbstractContextManager

import pytest

from flexiq import Queue
from flexiq.events import EventType
from flexiq.workflows import NodeStatus, Workflow, WorkflowState
from flexiq.workflows.run import WorkflowTimeoutError

WorkflowWorkerFactory = Callable[[], AbstractContextManager[threading.Thread]]


def test_linear_three_step_workflow(queue: Queue, workflow_worker: WorkflowWorkerFactory) -> None:
    """A→B→C runs in order and the workflow reaches COMPLETED."""

    order: list[str] = []

    @queue.task()
    def step_a() -> str:
        order.append("a")
        return "a-done"

    @queue.task()
    def step_b() -> str:
        order.append("b")
        return "b-done"

    @queue.task()
    def step_c() -> str:
        order.append("c")
        return "c-done"

    wf = Workflow(name="linear_pipe")
    wf.step("a", step_a)
    wf.step("b", step_b, after="a")
    wf.step("c", step_c, after="b")

    with workflow_worker():
        run = queue.submit_workflow(wf)
        final = run.wait(timeout=15)

    assert final.state == WorkflowState.COMPLETED
    assert order == ["a", "b", "c"]
    assert all(n.status == NodeStatus.COMPLETED for n in final.nodes.values())
    assert set(final.nodes.keys()) == {"a", "b", "c"}


def test_workflow_step_honors_task_max_retries_zero(
    queue: Queue, workflow_worker: WorkflowWorkerFactory
) -> None:
    """Regression: a step referencing a ``max_retries=0`` task runs exactly once.

    Previously steps ignored the task's ``max_retries`` and defaulted to 3, so a
    failing step was queue-retried — racing the workflow's own failure handling
    into a double execution of the same job.
    """
    calls = {"n": 0}

    @queue.task(max_retries=0)
    def always_fails() -> None:
        calls["n"] += 1
        raise RuntimeError("boom")

    wf = Workflow(name="no_retry_step")
    wf.step("x", always_fails)

    with workflow_worker():
        run = queue.submit_workflow(wf)
        # Generous timeout: this asserts a *count*, so it must not itself flake
        # on a slow/loaded runner before the single step has finished.
        final = run.wait(timeout=30)

    assert final.state == WorkflowState.FAILED
    assert calls["n"] == 1, f"max_retries=0 step must run once, ran {calls['n']}x"


def test_workflow_with_args_and_kwargs(
    queue: Queue, workflow_worker: WorkflowWorkerFactory
) -> None:
    """Step args and kwargs round-trip through the queue serializer."""

    received: list[tuple] = []

    @queue.task()
    def collect(x: int, y: int, *, label: str) -> int:
        received.append((x, y, label))
        return x + y

    wf = Workflow(name="args_pipe")
    wf.step("first", collect, args=(2, 3), kwargs={"label": "a"})
    wf.step("second", collect, args=(10, 20), kwargs={"label": "b"}, after="first")

    with workflow_worker():
        run = queue.submit_workflow(wf)
        final = run.wait(timeout=15)

    assert final.state == WorkflowState.COMPLETED
    assert (2, 3, "a") in received
    assert (10, 20, "b") in received


def test_workflow_decorator_registration(
    queue: Queue, workflow_worker: WorkflowWorkerFactory
) -> None:
    """@queue.workflow() stores a proxy that can build and submit."""

    @queue.task()
    def noop() -> None:
        return None

    @queue.workflow("nightly")
    def build() -> Workflow:
        wf = Workflow()
        wf.step("x", noop)
        return wf

    assert "nightly" in queue._workflow_registry
    built = build.build()
    assert built.name == "nightly"
    assert built.step_names == ["x"]

    with workflow_worker():
        run = build.submit()
        final = run.wait(timeout=10)

    assert final.state == WorkflowState.COMPLETED


def test_workflow_status_before_completion(queue: Queue) -> None:
    """status() reflects non-terminal state before the workflow finishes."""

    @queue.task()
    def noop() -> None:
        return None

    wf = Workflow(name="status_check")
    wf.step("only", noop)

    run = queue.submit_workflow(wf)
    snapshot = run.status()
    # No worker running, so the run stays in RUNNING with the node PENDING
    assert snapshot.state == WorkflowState.RUNNING
    assert snapshot.nodes["only"].status == NodeStatus.PENDING
    assert snapshot.nodes["only"].job_id is not None


def test_workflow_wait_timeout(queue: Queue) -> None:
    """wait() raises WorkflowTimeoutError if the workflow doesn't finish in time."""

    @queue.task()
    def noop() -> None:
        return None

    wf = Workflow(name="timeout_test")
    wf.step("only", noop)

    run = queue.submit_workflow(wf)
    # No worker running → timeout
    with pytest.raises(WorkflowTimeoutError):
        run.wait(timeout=0.3)


def test_workflow_cancellation(queue: Queue) -> None:
    """Cancelling a workflow marks pending nodes SKIPPED and the run CANCELLED."""

    @queue.task()
    def noop() -> None:
        return None

    wf = Workflow(name="cancel_test")
    wf.step("a", noop)
    wf.step("b", noop, after="a")
    wf.step("c", noop, after="b")

    run = queue.submit_workflow(wf)
    run.cancel()

    snapshot = run.status()
    assert snapshot.state == WorkflowState.CANCELLED
    for node in snapshot.nodes.values():
        assert node.status == NodeStatus.SKIPPED


def test_workflow_failing_step(queue: Queue, workflow_worker: WorkflowWorkerFactory) -> None:
    """A failing step fails the workflow and skips downstream steps."""

    @queue.task(max_retries=0)
    def good() -> str:
        return "ok"

    @queue.task(max_retries=0)
    def boom() -> str:
        raise RuntimeError("kaboom")

    wf = Workflow(name="failing")
    wf.step("a", good)
    wf.step("b", boom, after="a")
    wf.step("c", good, after="b")

    with workflow_worker():
        run = queue.submit_workflow(wf)
        final = run.wait(timeout=15)

    assert final.state == WorkflowState.FAILED
    assert final.nodes["a"].status == NodeStatus.COMPLETED
    assert final.nodes["b"].status == NodeStatus.FAILED
    assert final.nodes["c"].status == NodeStatus.SKIPPED


def test_workflow_node_snapshot_fields(queue: Queue) -> None:
    """Each node snapshot has name, status, job_id, error."""

    @queue.task()
    def noop() -> None:
        return None

    wf = Workflow(name="snapshot_check")
    wf.step("one", noop)
    wf.step("two", noop, after="one")

    run = queue.submit_workflow(wf)
    snapshot = run.status()
    assert "one" in snapshot.nodes
    assert "two" in snapshot.nodes
    for name, node in snapshot.nodes.items():
        assert node.name == name
        assert node.status == NodeStatus.PENDING
        assert node.job_id is not None
        assert node.error is None


def test_workflow_node_status_helper(queue: Queue) -> None:
    """node_status() returns the status of a specific node."""

    @queue.task()
    def noop() -> None:
        return None

    wf = Workflow(name="helper_check")
    wf.step("one", noop)

    run = queue.submit_workflow(wf)
    assert run.node_status("one") == NodeStatus.PENDING

    with pytest.raises(KeyError):
        run.node_status("nonexistent")


def test_workflow_step_ordering_validation() -> None:
    """step() raises ValueError if after references an unknown predecessor."""
    wf = Workflow(name="invalid")

    class _FakeTask:
        _task_name = "fake"

    wf.step("a", _FakeTask())
    with pytest.raises(ValueError, match="predecessor 'missing'"):
        wf.step("b", _FakeTask(), after="missing")


def test_workflow_duplicate_step_name() -> None:
    """step() raises ValueError if the same name is added twice."""
    wf = Workflow(name="dup")

    class _FakeTask:
        _task_name = "fake"

    wf.step("a", _FakeTask())
    with pytest.raises(ValueError, match="already defined"):
        wf.step("a", _FakeTask())


def test_workflow_definition_reuse(queue: Queue) -> None:
    """Submitting the same workflow name+version reuses the definition row."""

    @queue.task()
    def noop() -> None:
        return None

    wf1 = Workflow(name="reused", version=1)
    wf1.step("only", noop)
    wf2 = Workflow(name="reused", version=1)
    wf2.step("only", noop)

    run1 = queue.submit_workflow(wf1)
    run2 = queue.submit_workflow(wf2)
    assert run1.id != run2.id
    # Definitions share an ID via name+version uniqueness
    # (verified indirectly: second submit succeeds without duplicate-key errors)


def test_workflow_emits_completed_event(
    queue: Queue, workflow_worker: WorkflowWorkerFactory
) -> None:
    """WORKFLOW_COMPLETED event fires on successful run completion."""

    @queue.task()
    def noop() -> None:
        return None

    events: list[dict] = []
    event_received = threading.Event()

    def listener(_event_type: EventType, payload: dict) -> None:
        events.append(payload)
        event_received.set()

    queue._event_bus.on(EventType.WORKFLOW_COMPLETED, listener)

    wf = Workflow(name="event_pipe")
    wf.step("x", noop)

    with workflow_worker():
        run = queue.submit_workflow(wf)
        run.wait(timeout=10)
        # Give event bus thread a moment to dispatch
        event_received.wait(timeout=5)

    assert any(e.get("run_id") == run.id for e in events)
    assert any(e.get("state") == "completed" for e in events)


def test_workflow_emits_submitted_event(queue: Queue) -> None:
    """WORKFLOW_SUBMITTED event fires at submit time, no worker needed."""

    @queue.task()
    def noop() -> None:
        return None

    events: list[dict] = []
    event_received = threading.Event()

    def listener(_event_type: EventType, payload: dict) -> None:
        events.append(payload)
        event_received.set()

    queue._event_bus.on(EventType.WORKFLOW_SUBMITTED, listener)

    wf = Workflow(name="submit_event_pipe")
    wf.step("x", noop)

    run = queue.submit_workflow(wf)
    event_received.wait(timeout=5)

    assert any(
        e.get("run_id") == run.id and e.get("workflow_name") == "submit_event_pipe" for e in events
    )


def test_workflow_run_repr(queue: Queue) -> None:
    """WorkflowRun __repr__ is informative."""

    @queue.task()
    def noop() -> None:
        return None

    wf = Workflow(name="repr_test")
    wf.step("x", noop)

    run = queue.submit_workflow(wf)
    r = repr(run)
    assert run.id in r
    assert "repr_test" in r


@pytest.mark.asyncio
async def test_workflow_async_step(queue: Queue, workflow_worker: WorkflowWorkerFactory) -> None:
    """An async @queue.task() step works inside a workflow."""

    @queue.task()
    async def async_step() -> str:
        return "async-ok"

    @queue.task()
    def sync_step() -> str:
        return "sync-ok"

    wf = Workflow(name="async_mix")
    wf.step("a", async_step)
    wf.step("b", sync_step, after="a")

    with workflow_worker():
        run = queue.submit_workflow(wf)
        # Poll instead of blocking wait to play nicely with asyncio
        deadline = time.monotonic() + 15
        final = run.status()
        while not final.state.is_terminal() and time.monotonic() < deadline:
            await _async_sleep(0.1)
            final = run.status()

    assert final.state == WorkflowState.COMPLETED


async def _async_sleep(seconds: float) -> None:
    import asyncio

    await asyncio.sleep(seconds)
