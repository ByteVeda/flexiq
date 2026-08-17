"""End-to-end saga compensation tests.

Each test submits a workflow with at least one ``@queue.task(compensates=...)``
step, deliberately fails a later step, and asserts that the framework runs
the compensators in reverse topological order and the run terminates in
``COMPENSATED`` / ``COMPENSATION_FAILED``.
"""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING, Any

from flexiq import Queue
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


# ── Tests ─────────────────────────────────────────────────────────────────


def test_linear_saga_compensates_in_reverse_order(
    queue: Queue, workflow_worker: WorkflowWorkerFactory
) -> None:
    """3 steps: A → B → C. C raises → A and B should compensate in reverse."""
    order_lock = threading.Lock()
    call_order: list[str] = []

    @queue.task(max_retries=0)
    def compensate_a(args: tuple, kwargs: dict, result: object) -> None:
        with order_lock:
            call_order.append("comp_a")

    @queue.task(max_retries=0)
    def compensate_b(args: tuple, kwargs: dict, result: object) -> None:
        with order_lock:
            call_order.append("comp_b")

    @queue.task(max_retries=0, compensates=compensate_a)
    def step_a() -> str:
        with order_lock:
            call_order.append("a")
        return "a-ok"

    @queue.task(max_retries=0, compensates=compensate_b)
    def step_b() -> str:
        with order_lock:
            call_order.append("b")
        return "b-ok"

    @queue.task(max_retries=0)
    def step_c_failing() -> None:
        with order_lock:
            call_order.append("c")
        raise RuntimeError("kaboom")

    wf = Workflow(name="saga_linear")
    wf.step("a", step_a)
    wf.step("b", step_b, after="a")
    wf.step("c", step_c_failing, after="b")

    with workflow_worker():
        run = queue.submit_workflow(wf)
        state = _wait_for_state(
            run, {WorkflowState.COMPENSATED, WorkflowState.COMPENSATION_FAILED}
        )

    assert state == WorkflowState.COMPENSATED, (
        f"compensations should all succeed; got {state} (order={call_order})"
    )
    # Forward order: a, b, c. Compensation in reverse: comp_b, comp_a.
    fwd = [e for e in call_order if e in {"a", "b", "c"}]
    comp = [e for e in call_order if e.startswith("comp_")]
    assert fwd == ["a", "b", "c"], call_order
    assert comp == ["comp_b", "comp_a"], call_order


def test_steps_without_compensator_are_skipped(
    queue: Queue, workflow_worker: WorkflowWorkerFactory
) -> None:
    """A has compensator; B doesn't. C fails. Only A compensates."""
    order_lock = threading.Lock()
    call_order: list[str] = []

    @queue.task(max_retries=0)
    def comp_a(args: tuple, kwargs: dict, result: object) -> None:
        with order_lock:
            call_order.append("comp_a")

    @queue.task(max_retries=0, compensates=comp_a)
    def a() -> None:
        with order_lock:
            call_order.append("a")

    @queue.task(max_retries=0)
    def b_no_comp() -> None:
        with order_lock:
            call_order.append("b")

    @queue.task(max_retries=0)
    def c_failing() -> None:
        raise RuntimeError("boom")

    wf = Workflow(name="saga_partial_comp")
    wf.step("a", a)
    wf.step("b", b_no_comp, after="a")
    wf.step("c", c_failing, after="b")

    with workflow_worker():
        run = queue.submit_workflow(wf)
        state = _wait_for_state(
            run, {WorkflowState.COMPENSATED, WorkflowState.COMPENSATION_FAILED}
        )

    assert state == WorkflowState.COMPENSATED
    assert "comp_a" in call_order
    assert "a" in call_order
    assert "b" in call_order


def test_failing_compensator_ends_run_compensation_failed(
    queue: Queue, workflow_worker: WorkflowWorkerFactory
) -> None:
    """If a compensator itself fails (retries exhausted), run ends in
    COMPENSATION_FAILED and downstream compensations are NOT dispatched."""

    @queue.task(max_retries=0)
    def comp_a_failing(args: tuple, kwargs: dict, result: object) -> None:
        raise RuntimeError("refund api down")

    @queue.task(max_retries=0, compensates=comp_a_failing)
    def a() -> None:
        pass

    @queue.task(max_retries=0)
    def b_failing() -> None:
        raise RuntimeError("forward failure")

    wf = Workflow(name="saga_failing_comp")
    wf.step("a", a)
    wf.step("b", b_failing, after="a")

    with workflow_worker():
        run = queue.submit_workflow(wf)
        state = _wait_for_state(run, {WorkflowState.COMPENSATION_FAILED})

    assert state == WorkflowState.COMPENSATION_FAILED


def test_on_failure_continue_does_not_trigger_saga(
    queue: Queue, workflow_worker: WorkflowWorkerFactory
) -> None:
    """``on_failure='continue'`` does not start compensation even if a
    step has a registered compensator — saga is fail-fast only in v1."""
    compensator_called = threading.Event()

    @queue.task(max_retries=0)
    def comp_a(args: tuple, kwargs: dict, result: object) -> None:
        compensator_called.set()

    @queue.task(max_retries=0, compensates=comp_a)
    def a() -> None:
        pass

    @queue.task(max_retries=0)
    def b_failing() -> None:
        raise RuntimeError("boom")

    wf = Workflow(name="saga_no_continue", on_failure="continue")
    wf.step("a", a)
    wf.step("b", b_failing, after="a")

    with workflow_worker():
        run = queue.submit_workflow(wf)
        state = _wait_for_state(run, {WorkflowState.COMPLETED, WorkflowState.FAILED})

    # In ``continue`` mode the run can settle as either COMPLETED (a
    # succeeded, b's failure is allowed) or FAILED, depending on
    # tracker semantics. Either way, the compensator must NOT run.
    del state
    assert not compensator_called.is_set(), "compensator must NOT run when on_failure='continue'"
