"""Tests for the saga compensator contract: forward args / kwargs / result
and ``current_compensation_context()`` inside a compensator body."""

from __future__ import annotations

import threading
from typing import Any

from flexiq import Queue
from flexiq.workflows import Workflow, WorkflowState
from flexiq.workflows.saga import CompensationContext, current_compensation_context

WorkflowWorkerFactory = Any


def _wait_for_state(run: Any, states: set[WorkflowState], timeout: float = 30) -> WorkflowState:
    final = run.wait(timeout=timeout)
    state: WorkflowState = final.state
    assert state in states, f"expected one of {states}, got {state}"
    return state


def test_static_node_compensator_receives_forward_args(
    queue: Queue, workflow_worker: WorkflowWorkerFactory
) -> None:
    """Compensator for a static (non-deferred) step receives the original
    args/kwargs that the forward step was invoked with."""

    captured: dict[str, Any] = {}
    captured_lock = threading.Lock()

    @queue.task(max_retries=0)
    def refund(forward_args: tuple, forward_kwargs: dict, forward_result: Any) -> None:
        with captured_lock:
            captured["args"] = forward_args
            captured["kwargs"] = forward_kwargs
            captured["result"] = forward_result

    @queue.task(max_retries=0, compensates=refund)
    def charge(amount: int, customer_id: str) -> str:
        return f"charge:{amount}:{customer_id}"

    @queue.task(max_retries=0)
    def fail_later() -> None:
        raise RuntimeError("boom")

    wf = Workflow(name="saga_static_args")
    wf.step("charge", charge, args=(100, "cust-42"))
    wf.step("fail", fail_later, after="charge")

    with workflow_worker():
        run = queue.submit_workflow(wf)
        _wait_for_state(run, {WorkflowState.COMPENSATED, WorkflowState.COMPENSATION_FAILED})

    assert captured.get("args") == (100, "cust-42")
    assert captured.get("kwargs") == {}
    assert captured.get("result") == "charge:100:cust-42"


def test_static_node_compensator_receives_kwargs(
    queue: Queue, workflow_worker: WorkflowWorkerFactory
) -> None:
    """kwargs from the forward step survive through to the compensator."""

    captured: dict[str, Any] = {}

    @queue.task(max_retries=0)
    def refund(forward_args: tuple, forward_kwargs: dict, forward_result: Any) -> None:
        captured["kwargs"] = forward_kwargs
        captured["result"] = forward_result

    @queue.task(max_retries=0, compensates=refund)
    def reserve(*, sku: str, qty: int) -> dict[str, Any]:
        return {"reservation_id": "r-1", "sku": sku, "qty": qty}

    @queue.task(max_retries=0)
    def fail_later() -> None:
        raise RuntimeError("nope")

    wf = Workflow(name="saga_static_kwargs")
    wf.step("reserve", reserve, kwargs={"sku": "ABC", "qty": 3})
    wf.step("fail", fail_later, after="reserve")

    with workflow_worker():
        run = queue.submit_workflow(wf)
        _wait_for_state(run, {WorkflowState.COMPENSATED, WorkflowState.COMPENSATION_FAILED})

    assert captured.get("kwargs") == {"sku": "ABC", "qty": 3}
    assert captured.get("result") == {"reservation_id": "r-1", "sku": "ABC", "qty": 3}


def test_current_compensation_context_inside_compensator(
    queue: Queue, workflow_worker: WorkflowWorkerFactory
) -> None:
    """``current_compensation_context()`` returns a populated context when
    called from inside a compensator body."""

    captured: dict[str, Any] = {}

    @queue.task(max_retries=0)
    def refund(forward_args: tuple, forward_kwargs: dict, forward_result: Any) -> None:
        ctx = current_compensation_context()
        assert ctx is not None
        assert isinstance(ctx, CompensationContext)
        captured["run_id"] = ctx.workflow_run_id
        captured["node_name"] = ctx.workflow_node_name
        captured["forward_args"] = ctx.forward_args
        captured["forward_kwargs"] = ctx.forward_kwargs
        captured["forward_result"] = ctx.forward_result
        captured["forward_job_id"] = ctx.forward_job_id

    @queue.task(max_retries=0, compensates=refund)
    def charge(amount: int) -> str:
        return f"charge-{amount}"

    @queue.task(max_retries=0)
    def fail_later() -> None:
        raise RuntimeError("boom")

    wf = Workflow(name="saga_ctx_test")
    wf.step("charge", charge, args=(250,))
    wf.step("fail", fail_later, after="charge")

    with workflow_worker():
        run = queue.submit_workflow(wf)
        _wait_for_state(run, {WorkflowState.COMPENSATED, WorkflowState.COMPENSATION_FAILED})
        run_id = run.id

    assert captured.get("run_id") == run_id
    assert captured.get("node_name") == "charge"
    assert captured.get("forward_args") == (250,)
    assert captured.get("forward_kwargs") == {}
    assert captured.get("forward_result") == "charge-250"
    assert isinstance(captured.get("forward_job_id"), str)


def test_current_compensation_context_returns_none_outside_compensator(
    queue: Queue, run_worker: threading.Thread
) -> None:
    """Outside a compensator body (regular task) the context is ``None``."""
    _ = run_worker
    captured: dict[str, Any] = {"ctx": "unset"}

    @queue.task(max_retries=0)
    def regular() -> None:
        captured["ctx"] = current_compensation_context()

    job = regular.delay()
    job.result(timeout=10)

    assert captured["ctx"] is None
