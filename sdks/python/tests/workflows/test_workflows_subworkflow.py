"""Tests for Phase 5B sub-workflows."""

from __future__ import annotations

import threading
from collections.abc import Callable
from contextlib import AbstractContextManager
from typing import Any

from flexiq import Queue
from flexiq.events import EventType
from flexiq.workflows import NodeStatus, Workflow, WorkflowState

WorkflowWorkerFactory = Callable[[], AbstractContextManager[threading.Thread]]
PollUntil = Any  # the conftest fixture's runtime type


def test_sub_workflow_executes(queue: Queue, workflow_worker: WorkflowWorkerFactory) -> None:
    """A sub-workflow step runs the child workflow, parent continues after."""

    order: list[str] = []

    @queue.task()
    def extract() -> str:
        order.append("extract")
        return "data"

    @queue.task()
    def load() -> str:
        order.append("load")
        return "loaded"

    @queue.task()
    def report() -> str:
        order.append("report")
        return "done"

    @queue.workflow("etl")
    def etl_pipeline() -> Workflow:
        wf = Workflow()
        wf.step("extract", extract)
        wf.step("load", load, after="extract")
        return wf

    wf = Workflow(name="parent")
    wf.step("etl", etl_pipeline.as_step())
    wf.step("report", report, after="etl")

    with workflow_worker():
        run = queue.submit_workflow(wf)
        final = run.wait(timeout=20)

    assert final.state == WorkflowState.COMPLETED
    assert "extract" in order
    assert "load" in order
    assert "report" in order
    assert order.index("load") < order.index("report")


def test_sub_workflow_emits_submitted_event(
    queue: Queue, workflow_worker: WorkflowWorkerFactory, poll_until: PollUntil
) -> None:
    """Child submission emits WORKFLOW_SUBMITTED carrying parent_run_id."""

    @queue.task()
    def child_step() -> str:
        return "ok"

    @queue.workflow("child_flow")
    def child_flow() -> Workflow:
        wf = Workflow()
        wf.step("c", child_step)
        return wf

    events: list[dict] = []
    queue._event_bus.on(EventType.WORKFLOW_SUBMITTED, lambda _et, payload: events.append(payload))

    wf = Workflow(name="parent_events")
    wf.step("child", child_flow.as_step())

    with workflow_worker():
        run = queue.submit_workflow(wf)
        run.wait(timeout=20)
        # Event-bus dispatch is asynchronous — the callback may lag run completion.
        poll_until(
            lambda: any(e.get("parent_run_id") == run.id for e in events),
            message="child WORKFLOW_SUBMITTED was not delivered",
        )

    child_events = [e for e in events if e.get("parent_run_id") == run.id]
    assert len(child_events) == 1
    assert child_events[0]["workflow_name"] == "child_flow"


def test_sub_workflow_failure(queue: Queue, workflow_worker: WorkflowWorkerFactory) -> None:
    """A failing sub-workflow fails the parent node."""

    @queue.task(max_retries=0)
    def fail_task() -> str:
        raise RuntimeError("sub failed")

    @queue.task()
    def ok_task() -> str:
        return "ok"

    @queue.workflow("failing_sub")
    def failing_sub() -> Workflow:
        wf = Workflow()
        wf.step("boom", fail_task)
        return wf

    wf = Workflow(name="parent_fail")
    wf.step("sub", failing_sub.as_step())
    wf.step("after", ok_task, after="sub")

    with workflow_worker():
        run = queue.submit_workflow(wf)
        final = run.wait(timeout=20)

    assert final.state == WorkflowState.FAILED
    assert final.nodes["sub"].status == NodeStatus.FAILED
    assert final.nodes["after"].status == NodeStatus.SKIPPED


def test_sub_workflow_compile_failure_marks_parent_failed(
    queue: Queue, workflow_worker: WorkflowWorkerFactory
) -> None:
    """Regression: a factory that raises during `build()` must not leave the
    parent node Skipped forever — it must be marked Failed so the outer run
    can finalize. Before the fix, the tracker called `skip_workflow_node`
    on the parent before attempting compile, and a compile failure left the
    node Skipped permanently."""

    @queue.task()
    def downstream() -> str:
        return "should not run"

    @queue.workflow("broken_sub")
    def broken_sub() -> Workflow:
        raise RuntimeError("factory blew up")

    wf = Workflow(name="parent_compile_fail")
    wf.step("sub", broken_sub.as_step())
    wf.step("after", downstream, after="sub")

    with workflow_worker():
        run = queue.submit_workflow(wf)
        final = run.wait(timeout=15)

    assert final.state == WorkflowState.FAILED, (
        f"outer run must finalize as FAILED, got {final.state}"
    )
    assert final.nodes["sub"].status == NodeStatus.FAILED, (
        f"sub-workflow parent must be FAILED (was {final.nodes['sub'].status}) — "
        "the old bug left it SKIPPED"
    )
    assert final.nodes["after"].status == NodeStatus.SKIPPED


def test_cancel_parent_cascades(
    queue: Queue, workflow_worker: WorkflowWorkerFactory, poll_until: PollUntil
) -> None:
    """Cancelling a parent workflow cancels the child sub-workflow too."""

    import time

    @queue.task()
    def slow_task() -> str:
        # Intentional pacing — keeps the child running so the cancel cascades
        # mid-flight rather than after natural completion.
        time.sleep(30)
        return "slow"

    @queue.workflow("slow_sub")
    def slow_sub() -> Workflow:
        wf = Workflow()
        wf.step("slow", slow_task)
        return wf

    wf = Workflow(name="parent_cancel")
    wf.step("sub", slow_sub.as_step())

    with workflow_worker():
        run = queue.submit_workflow(wf)
        poll_until(
            lambda: run.node_status("sub") == NodeStatus.RUNNING,
            timeout=10,
            message="sub-workflow did not reach RUNNING",
        )
        run.cancel()
        snapshot = run.status()

    assert snapshot.state == WorkflowState.CANCELLED


def test_parallel_sub_workflows(queue: Queue, workflow_worker: WorkflowWorkerFactory) -> None:
    """Two sub-workflows can run concurrently."""

    order: list[str] = []

    @queue.task()
    def task_a() -> str:
        order.append("a")
        return "a"

    @queue.task()
    def task_b() -> str:
        order.append("b")
        return "b"

    @queue.task()
    def reconcile() -> str:
        order.append("reconcile")
        return "done"

    @queue.workflow("sub_a")
    def sub_a() -> Workflow:
        wf = Workflow()
        wf.step("a", task_a)
        return wf

    @queue.workflow("sub_b")
    def sub_b() -> Workflow:
        wf = Workflow()
        wf.step("b", task_b)
        return wf

    wf = Workflow(name="parallel_parent")
    wf.step("sa", sub_a.as_step())
    wf.step("sb", sub_b.as_step())
    wf.step("reconcile", reconcile, after=["sa", "sb"])

    with workflow_worker():
        run = queue.submit_workflow(wf)
        final = run.wait(timeout=20)

    assert final.state == WorkflowState.COMPLETED
    assert "a" in order
    assert "b" in order
    assert "reconcile" in order
