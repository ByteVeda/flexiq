"""Sub-workflow saga propagation.

When a parent workflow's compensation reaches a node that *was* a
sub-workflow, and the sub-workflow has its own registered compensators,
the parent's saga propagates compensation into the child run. The child
saga runs its compensators in reverse topological order; the parent node
is marked compensated/failed based on the child's terminal outcome; the
parent's wave advances.
"""

from __future__ import annotations

import threading
from typing import Any

from flexiq import Queue
from flexiq.workflows import Workflow, WorkflowState

WorkflowWorkerFactory = Any


def _wait_for_state(run: Any, states: set[WorkflowState], timeout: float = 30) -> WorkflowState:
    final = run.wait(timeout=timeout)
    state: WorkflowState = final.state
    assert state in states, f"expected one of {states}, got {state}"
    return state


def test_child_saga_propagates_when_parent_node_is_sub_workflow(
    queue: Queue, workflow_worker: WorkflowWorkerFactory
) -> None:
    """Parent workflow has a sub-workflow node with no explicit
    ``compensates=``. The sub-workflow itself has a saga (its steps
    register compensators). When a later parent step fails, the
    sub-workflow's compensators must run in reverse order."""
    order_lock = threading.Lock()
    call_order: list[str] = []

    # Sub-workflow with its own saga ----------------------------------------
    @queue.task(max_retries=0)
    def comp_pack(forward_args: tuple, forward_kwargs: dict, forward_result: Any) -> None:
        with order_lock:
            call_order.append("comp_pack")

    @queue.task(max_retries=0)
    def comp_label(forward_args: tuple, forward_kwargs: dict, forward_result: Any) -> None:
        with order_lock:
            call_order.append("comp_label")

    @queue.task(max_retries=0, compensates=comp_pack)
    def pack() -> str:
        with order_lock:
            call_order.append("pack")
        return "packed"

    @queue.task(max_retries=0, compensates=comp_label)
    def label() -> str:
        with order_lock:
            call_order.append("label")
        return "labeled"

    @queue.workflow("ship_subwf")
    def ship_subwf() -> Workflow:
        wf = Workflow()
        wf.step("pack", pack)
        wf.step("label", label, after="pack")
        return wf

    # Parent workflow -------------------------------------------------------
    @queue.task(max_retries=0)
    def fail_later() -> None:
        with order_lock:
            call_order.append("fail")
        raise RuntimeError("kaboom in parent")

    parent_wf = Workflow(name="parent_with_subwf_saga")
    parent_wf.step("ship", ship_subwf.as_step())
    parent_wf.step("notify", fail_later, after="ship")

    with workflow_worker():
        run = queue.submit_workflow(parent_wf)
        _wait_for_state(
            run,
            {WorkflowState.COMPENSATED, WorkflowState.COMPENSATION_FAILED},
            timeout=30,
        )

    fwd = [e for e in call_order if e in {"pack", "label", "fail"}]
    comp = [e for e in call_order if e.startswith("comp_")]
    assert fwd == ["pack", "label", "fail"], call_order
    # Compensation order inside the child sub-workflow is reverse-topological:
    # label compensates before pack.
    assert comp == ["comp_label", "comp_pack"], call_order


def test_explicit_compensator_on_subworkflow_node_overrides_propagation(
    queue: Queue, workflow_worker: WorkflowWorkerFactory
) -> None:
    """If the parent step has its own ``compensates=``, propagation does
    not happen — the parent's explicit compensator runs instead."""
    order_lock = threading.Lock()
    call_order: list[str] = []

    @queue.task(max_retries=0)
    def comp_inner(forward_args: tuple, forward_kwargs: dict, forward_result: Any) -> None:
        with order_lock:
            call_order.append("comp_inner")

    @queue.task(max_retries=0)
    def comp_ship_explicit(forward_args: tuple, forward_kwargs: dict, forward_result: Any) -> None:
        with order_lock:
            call_order.append("comp_ship_explicit")

    @queue.task(max_retries=0, compensates=comp_inner)
    def inner_step() -> None:
        with order_lock:
            call_order.append("inner")

    @queue.workflow("ship_inner")
    def ship_inner() -> Workflow:
        wf = Workflow()
        wf.step("inner", inner_step)
        return wf

    @queue.task(max_retries=0)
    def boom() -> None:
        raise RuntimeError("parent boom")

    parent_wf = Workflow(name="parent_explicit_comp")
    parent_wf.step("ship", ship_inner.as_step(), compensates=comp_ship_explicit)
    parent_wf.step("notify", boom, after="ship")

    with workflow_worker():
        run = queue.submit_workflow(parent_wf)
        _wait_for_state(
            run,
            {WorkflowState.COMPENSATED, WorkflowState.COMPENSATION_FAILED},
            timeout=30,
        )

    # Explicit parent-step compensator wins: ``comp_ship_explicit`` runs,
    # the child's ``comp_inner`` does NOT (would only run via propagation).
    assert "comp_ship_explicit" in call_order
    assert "comp_inner" not in call_order


def test_subworkflow_compensation_failure_marks_parent_failed(
    queue: Queue, workflow_worker: WorkflowWorkerFactory
) -> None:
    """When the propagated child saga itself fails (one of its
    compensators errors), the parent node ends compensation_failed and so
    does the parent run."""

    @queue.task(max_retries=0)
    def comp_inner_failing(forward_args: tuple, forward_kwargs: dict, forward_result: Any) -> None:
        raise RuntimeError("inner comp failed")

    @queue.task(max_retries=0, compensates=comp_inner_failing)
    def inner_step() -> None:
        pass

    @queue.workflow("ship_failing_inner")
    def ship_failing_inner() -> Workflow:
        wf = Workflow()
        wf.step("inner", inner_step)
        return wf

    @queue.task(max_retries=0)
    def boom() -> None:
        raise RuntimeError("parent boom")

    parent_wf = Workflow(name="parent_propagated_failure")
    parent_wf.step("ship", ship_failing_inner.as_step())
    parent_wf.step("notify", boom, after="ship")

    with workflow_worker():
        run = queue.submit_workflow(parent_wf)
        final = _wait_for_state(
            run,
            {WorkflowState.COMPENSATED, WorkflowState.COMPENSATION_FAILED},
            timeout=30,
        )

    assert final == WorkflowState.COMPENSATION_FAILED
