"""End-to-end saga tests for on_failure="continue" workflows.

Continue mode lets the workflow run to completion despite individual node
failures. When ``compensate_on_continue=True``, the framework still runs
compensators for completed nodes after the run terminalises with a partial
failure (new ``CompletedWithFailures`` state).
"""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING, Any

from flexiq import EventType, Queue
from flexiq.workflows import Workflow, WorkflowState

if TYPE_CHECKING:
    pass


WorkflowWorkerFactory = Any


# ── Shared helpers ────────────────────────────────────────────────────────


def _wait_for_state(run: Any, states: set[WorkflowState], timeout: float = 30) -> WorkflowState:
    final = run.wait(timeout=timeout)
    state: WorkflowState = final.state
    assert state in states, f"expected one of {states}, got {state}"
    return state


# ── Default-off behaviour (regression for pre-0.14 semantics) ────────────


def test_continue_mode_without_flag_does_not_compensate(
    queue: Queue, workflow_worker: WorkflowWorkerFactory
) -> None:
    """compensate_on_continue=False (default) — no compensation, run ends Failed."""
    order_lock = threading.Lock()
    call_order: list[str] = []

    @queue.task(max_retries=0)
    def comp_a(args: tuple, kwargs: dict, result: object) -> None:
        with order_lock:
            call_order.append("comp_a")

    @queue.task(max_retries=0, compensates=comp_a)
    def step_a() -> None:
        with order_lock:
            call_order.append("a")

    @queue.task(max_retries=0)
    def step_b_failing() -> None:
        with order_lock:
            call_order.append("b")
        raise RuntimeError("nope")

    wf = Workflow(name="continue_no_comp", on_failure="continue")
    wf.step("a", step_a)
    wf.step("b", step_b_failing, after="a")

    with workflow_worker():
        run = queue.submit_workflow(wf)
        state = _wait_for_state(run, {WorkflowState.FAILED, WorkflowState.COMPLETED})

    assert state == WorkflowState.FAILED
    assert "a" in call_order
    assert "b" in call_order
    assert "comp_a" not in call_order, "compensator must not run when flag is off"


# ── Opt-in behaviour ──────────────────────────────────────────────────────


def test_continue_mode_with_flag_compensates_completed_nodes(
    queue: Queue, workflow_worker: WorkflowWorkerFactory
) -> None:
    """compensate_on_continue=True + one failed + one completed → run ends Compensated.

    The single completed node 'a' is compensated; the failed node 'b' is not
    (it never succeeded, so there's nothing to undo).
    """
    order_lock = threading.Lock()
    call_order: list[str] = []

    @queue.task(max_retries=0)
    def comp_a(args: tuple, kwargs: dict, result: object) -> None:
        with order_lock:
            call_order.append("comp_a")

    @queue.task(max_retries=0)
    def comp_b(args: tuple, kwargs: dict, result: object) -> None:
        with order_lock:
            call_order.append("comp_b")

    @queue.task(max_retries=0, compensates=comp_a)
    def step_a() -> None:
        with order_lock:
            call_order.append("a")

    @queue.task(max_retries=0, compensates=comp_b)
    def step_b_failing() -> None:
        with order_lock:
            call_order.append("b")
        raise RuntimeError("nope")

    wf = Workflow(name="continue_with_comp", on_failure="continue", compensate_on_continue=True)
    wf.step("a", step_a)
    wf.step("b", step_b_failing, after="a")

    with workflow_worker():
        run = queue.submit_workflow(wf)
        state = _wait_for_state(
            run, {WorkflowState.COMPENSATED, WorkflowState.COMPENSATION_FAILED}
        )

    assert state == WorkflowState.COMPENSATED
    assert "a" in call_order
    assert "b" in call_order
    assert "comp_a" in call_order, "completed node must be compensated"
    assert "comp_b" not in call_order, "failed node has nothing to compensate"


def test_continue_mode_with_flag_all_succeed_does_not_compensate(
    queue: Queue, workflow_worker: WorkflowWorkerFactory
) -> None:
    """compensate_on_continue=True + zero failures → run ends Completed, no comp."""
    order_lock = threading.Lock()
    call_order: list[str] = []

    @queue.task(max_retries=0)
    def comp_a(args: tuple, kwargs: dict, result: object) -> None:
        with order_lock:
            call_order.append("comp_a")

    @queue.task(max_retries=0, compensates=comp_a)
    def step_a() -> None:
        with order_lock:
            call_order.append("a")

    @queue.task(max_retries=0)
    def step_b() -> None:
        with order_lock:
            call_order.append("b")

    wf = Workflow(name="continue_all_ok", on_failure="continue", compensate_on_continue=True)
    wf.step("a", step_a)
    wf.step("b", step_b, after="a")

    with workflow_worker():
        run = queue.submit_workflow(wf)
        state = _wait_for_state(run, {WorkflowState.COMPLETED})

    assert state == WorkflowState.COMPLETED
    assert call_order == ["a", "b"]


def test_fail_fast_flag_is_noop(queue: Queue, workflow_worker: WorkflowWorkerFactory) -> None:
    """compensate_on_continue=True is silently ignored when on_failure=fail_fast.

    Fail-fast already triggers compensation on the first failure — the flag
    must not cause double-compensation or other surprises.
    """
    comp_call_count = 0
    lock = threading.Lock()

    @queue.task(max_retries=0)
    def comp_a(args: tuple, kwargs: dict, result: object) -> None:
        nonlocal comp_call_count
        with lock:
            comp_call_count += 1

    @queue.task(max_retries=0, compensates=comp_a)
    def step_a() -> None:
        pass

    @queue.task(max_retries=0)
    def step_b_failing() -> None:
        raise RuntimeError("nope")

    # on_failure defaults to fail_fast.
    wf = Workflow(name="ff_with_flag", compensate_on_continue=True)
    wf.step("a", step_a)
    wf.step("b", step_b_failing, after="a")

    with workflow_worker():
        run = queue.submit_workflow(wf)
        state = _wait_for_state(
            run, {WorkflowState.COMPENSATED, WorkflowState.COMPENSATION_FAILED}
        )

    assert state == WorkflowState.COMPENSATED
    assert comp_call_count == 1, f"compensator must fire exactly once, got {comp_call_count}"


# ── Event signalling ──────────────────────────────────────────────────────


def test_completed_with_failures_event_fires_once(
    queue: Queue, workflow_worker: WorkflowWorkerFactory
) -> None:
    """WORKFLOW_COMPLETED_WITH_FAILURES is emitted exactly once on the transition."""
    fire_count = 0
    fire_lock = threading.Lock()

    def on_completed_with_failures(_event_type: EventType, _payload: dict) -> None:
        nonlocal fire_count
        with fire_lock:
            fire_count += 1

    queue._event_bus.on(EventType.WORKFLOW_COMPLETED_WITH_FAILURES, on_completed_with_failures)

    @queue.task(max_retries=0)
    def step_a() -> None:
        pass

    @queue.task(max_retries=0)
    def step_b_failing() -> None:
        raise RuntimeError("nope")

    # Use a parallel layout (no edges) so the failure of b can't cascade-skip
    # a, and continue mode lets both nodes terminalise — yielding the partial
    # failure that triggers the event.
    wf = Workflow(name="event_partial", on_failure="continue", compensate_on_continue=True)
    wf.step("a", step_a)
    wf.step("b", step_b_failing)

    with workflow_worker():
        run = queue.submit_workflow(wf)
        # No compensator registered, so we expect Completed-with-failures →
        # compensation no-op → terminal.
        run.wait(timeout=30)

    assert fire_count == 1, f"event must fire exactly once, got {fire_count}"
