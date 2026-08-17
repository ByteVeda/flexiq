"""Tests for Phase 4 conditional execution and error handling."""

from __future__ import annotations

import threading
from collections.abc import Callable
from contextlib import AbstractContextManager

from flexiq import Queue
from flexiq.workflows import NodeStatus, Workflow, WorkflowContext, WorkflowState

WorkflowWorkerFactory = Callable[[], AbstractContextManager[threading.Thread]]


def test_on_failure_step_runs(queue: Queue, workflow_worker: WorkflowWorkerFactory) -> None:
    """A step with condition='on_failure' runs when predecessor fails."""

    @queue.task(max_retries=0)
    def fail_task() -> str:
        raise RuntimeError("boom")

    collected: list[str] = []

    @queue.task()
    def cleanup() -> str:
        collected.append("cleanup ran")
        return "cleaned"

    wf = Workflow(name="on_failure_runs")
    wf.step("a", fail_task)
    wf.step("b", cleanup, after="a", condition="on_failure")

    with workflow_worker():
        run = queue.submit_workflow(wf)
        final = run.wait(timeout=20)

    assert final.nodes["a"].status == NodeStatus.FAILED
    assert final.nodes["b"].status == NodeStatus.COMPLETED
    assert collected == ["cleanup ran"]


def test_on_failure_step_skipped_on_success(
    queue: Queue, workflow_worker: WorkflowWorkerFactory
) -> None:
    """A step with condition='on_failure' is SKIPPED when predecessor succeeds."""

    @queue.task()
    def ok_task() -> str:
        return "ok"

    @queue.task()
    def rollback() -> str:
        return "should not run"

    wf = Workflow(name="on_failure_skipped")
    wf.step("a", ok_task)
    wf.step("b", rollback, after="a", condition="on_failure")

    with workflow_worker():
        run = queue.submit_workflow(wf)
        final = run.wait(timeout=20)

    assert final.nodes["a"].status == NodeStatus.COMPLETED
    assert final.nodes["b"].status == NodeStatus.SKIPPED


def test_always_step_runs_on_success(queue: Queue, workflow_worker: WorkflowWorkerFactory) -> None:
    """A step with condition='always' runs when predecessor succeeds."""

    collected: list[str] = []

    @queue.task()
    def ok_task() -> str:
        return "ok"

    @queue.task()
    def always_task() -> str:
        collected.append("always ran")
        return "done"

    wf = Workflow(name="always_on_success")
    wf.step("a", ok_task)
    wf.step("b", always_task, after="a", condition="always")

    with workflow_worker():
        run = queue.submit_workflow(wf)
        final = run.wait(timeout=20)

    assert final.nodes["b"].status == NodeStatus.COMPLETED
    assert collected == ["always ran"]


def test_always_step_runs_on_failure(queue: Queue, workflow_worker: WorkflowWorkerFactory) -> None:
    """A step with condition='always' runs even when predecessor fails."""

    collected: list[str] = []

    @queue.task(max_retries=0)
    def fail_task() -> str:
        raise RuntimeError("boom")

    @queue.task()
    def always_task() -> str:
        collected.append("always ran")
        return "done"

    wf = Workflow(name="always_on_failure")
    wf.step("a", fail_task)
    wf.step("b", always_task, after="a", condition="always")

    with workflow_worker():
        run = queue.submit_workflow(wf)
        final = run.wait(timeout=20)

    assert final.nodes["a"].status == NodeStatus.FAILED
    assert final.nodes["b"].status == NodeStatus.COMPLETED
    assert collected == ["always ran"]


def test_on_success_default(queue: Queue, workflow_worker: WorkflowWorkerFactory) -> None:
    """Default condition (on_success) skips the step when predecessor fails."""

    @queue.task(max_retries=0)
    def fail_task() -> str:
        raise RuntimeError("boom")

    @queue.task()
    def next_task() -> str:
        return "should not run"

    wf = Workflow(name="on_success_default")
    wf.step("a", fail_task)
    wf.step("b", next_task, after="a")

    with workflow_worker():
        run = queue.submit_workflow(wf)
        final = run.wait(timeout=20)

    assert final.state == WorkflowState.FAILED
    assert final.nodes["a"].status == NodeStatus.FAILED
    assert final.nodes["b"].status == NodeStatus.SKIPPED


def test_continue_mode_independent_branches(
    queue: Queue, workflow_worker: WorkflowWorkerFactory
) -> None:
    """on_failure='continue' lets independent branches keep running."""

    order: list[str] = []

    @queue.task(max_retries=0)
    def fail_task() -> str:
        order.append("fail")
        raise RuntimeError("boom")

    @queue.task()
    def ok_task() -> str:
        order.append("ok")
        return "ok"

    @queue.task()
    def after_fail() -> str:
        order.append("after_fail")
        return "nope"

    @queue.task()
    def after_ok() -> str:
        order.append("after_ok")
        return "yes"

    # Diamond: root → {fail_branch, ok_branch} → {after_fail, after_ok}
    wf = Workflow(name="continue_branches", on_failure="continue")
    wf.step("root", ok_task)
    wf.step("fail_branch", fail_task, after="root")
    wf.step("ok_branch", ok_task, after="root")
    wf.step("after_fail", after_fail, after="fail_branch")
    wf.step("after_ok", after_ok, after="ok_branch")

    with workflow_worker():
        run = queue.submit_workflow(wf)
        final = run.wait(timeout=20)

    # fail_branch failed → after_fail skipped (condition=on_success, pred failed)
    # ok_branch succeeded → after_ok ran
    assert final.state == WorkflowState.FAILED  # overall: has failures
    assert final.nodes["fail_branch"].status == NodeStatus.FAILED
    assert final.nodes["ok_branch"].status == NodeStatus.COMPLETED
    assert final.nodes["after_fail"].status == NodeStatus.SKIPPED
    assert final.nodes["after_ok"].status == NodeStatus.COMPLETED
    assert "after_ok" in order


def test_continue_mode_skips_downstream(
    queue: Queue, workflow_worker: WorkflowWorkerFactory
) -> None:
    """In continue mode, failure skips on_success downstream in the chain."""

    @queue.task(max_retries=0)
    def fail_task() -> str:
        raise RuntimeError("boom")

    @queue.task()
    def ok_task() -> str:
        return "ok"

    wf = Workflow(name="continue_chain", on_failure="continue")
    wf.step("a", ok_task)
    wf.step("b", fail_task, after="a")
    wf.step("c", ok_task, after="b")
    wf.step("d", ok_task, after="c")

    with workflow_worker():
        run = queue.submit_workflow(wf)
        final = run.wait(timeout=20)

    assert final.state == WorkflowState.FAILED
    assert final.nodes["a"].status == NodeStatus.COMPLETED
    assert final.nodes["b"].status == NodeStatus.FAILED
    assert final.nodes["c"].status == NodeStatus.SKIPPED
    assert final.nodes["d"].status == NodeStatus.SKIPPED


def test_callable_condition_true(queue: Queue, workflow_worker: WorkflowWorkerFactory) -> None:
    """A callable condition that returns True lets the step run."""

    @queue.task()
    def ok_task() -> str:
        return "ok"

    collected: list[str] = []

    @queue.task()
    def guarded() -> str:
        collected.append("ran")
        return "done"

    wf = Workflow(name="callable_true")
    wf.step("a", ok_task)
    wf.step("b", guarded, after="a", condition=lambda ctx: True)

    with workflow_worker():
        run = queue.submit_workflow(wf)
        final = run.wait(timeout=20)

    assert final.nodes["b"].status == NodeStatus.COMPLETED
    assert collected == ["ran"]


def test_callable_condition_false(queue: Queue, workflow_worker: WorkflowWorkerFactory) -> None:
    """A callable condition that returns False skips the step."""

    @queue.task()
    def ok_task() -> str:
        return "ok"

    @queue.task()
    def guarded() -> str:
        return "should not run"

    wf = Workflow(name="callable_false")
    wf.step("a", ok_task)
    wf.step("b", guarded, after="a", condition=lambda ctx: False)

    with workflow_worker():
        run = queue.submit_workflow(wf)
        final = run.wait(timeout=20)

    assert final.nodes["b"].status == NodeStatus.SKIPPED


def test_callable_accesses_results(queue: Queue, workflow_worker: WorkflowWorkerFactory) -> None:
    """A callable condition can access predecessor results via ctx.results."""

    @queue.task()
    def score_task() -> dict:
        return {"score": 0.98}

    collected: list[str] = []

    @queue.task()
    def deploy() -> str:
        collected.append("deployed")
        return "ok"

    @queue.task()
    def skip_deploy() -> str:
        collected.append("should not deploy")
        return "skip"

    def high_score(ctx: WorkflowContext) -> bool:
        return bool(ctx.results.get("validate", {}).get("score", 0) > 0.95)

    wf = Workflow(name="callable_results")
    wf.step("validate", score_task)
    wf.step("deploy", deploy, after="validate", condition=high_score)

    with workflow_worker():
        run = queue.submit_workflow(wf)
        final = run.wait(timeout=20)

    assert final.nodes["deploy"].status == NodeStatus.COMPLETED
    assert collected == ["deployed"]


def test_fail_fast_backward_compat(queue: Queue, workflow_worker: WorkflowWorkerFactory) -> None:
    """Phase 2 regression: fail_fast (default) cascades all pending nodes."""

    @queue.task(max_retries=0)
    def fail_task() -> str:
        raise RuntimeError("boom")

    @queue.task()
    def ok_task() -> str:
        return "ok"

    wf = Workflow(name="fail_fast_compat")
    wf.step("a", fail_task)
    wf.step("b", ok_task, after="a")
    wf.step("c", ok_task, after="b")

    with workflow_worker():
        run = queue.submit_workflow(wf)
        final = run.wait(timeout=15)

    assert final.state == WorkflowState.FAILED
    assert final.nodes["a"].status == NodeStatus.FAILED
    assert final.nodes["b"].status == NodeStatus.SKIPPED
    assert final.nodes["c"].status == NodeStatus.SKIPPED


def test_skip_propagation_respects_always(
    queue: Queue, workflow_worker: WorkflowWorkerFactory
) -> None:
    """A→B→C: A fails, B(on_success) skipped, C(always) still runs."""

    @queue.task(max_retries=0)
    def fail_task() -> str:
        raise RuntimeError("boom")

    @queue.task()
    def ok_task() -> str:
        return "ok"

    collected: list[str] = []

    @queue.task()
    def always_task() -> str:
        collected.append("always ran")
        return "done"

    wf = Workflow(name="skip_propagation")
    wf.step("a", fail_task)
    wf.step("b", ok_task, after="a")
    wf.step("c", always_task, after="b", condition="always")

    with workflow_worker():
        run = queue.submit_workflow(wf)
        final = run.wait(timeout=20)

    assert final.nodes["a"].status == NodeStatus.FAILED
    assert final.nodes["b"].status == NodeStatus.SKIPPED
    assert final.nodes["c"].status == NodeStatus.COMPLETED
    assert collected == ["always ran"]


def test_fan_out_with_on_failure_downstream(
    queue: Queue, workflow_worker: WorkflowWorkerFactory
) -> None:
    """Fan-out child fails, downstream on_failure step runs."""

    @queue.task()
    def source() -> list[int]:
        return [1, 2]

    @queue.task(max_retries=0)
    def process(x: int) -> int:
        if x == 2:
            raise RuntimeError("boom")
        return x * 10

    @queue.task()
    def aggregate(results: list[int]) -> str:
        return "agg"

    collected: list[str] = []

    @queue.task()
    def on_error() -> str:
        collected.append("error handled")
        return "handled"

    wf = Workflow(name="fan_out_on_failure")
    wf.step("fetch", source)
    wf.step("process", process, after="fetch", fan_out="each")
    wf.step("collect", aggregate, after="process", fan_in="all")
    wf.step("handle_error", on_error, after="process", condition="on_failure")

    with workflow_worker():
        run = queue.submit_workflow(wf)
        final = run.wait(timeout=20)

    assert final.nodes["process"].status == NodeStatus.FAILED
    assert final.nodes["collect"].status == NodeStatus.SKIPPED
    assert final.nodes["handle_error"].status == NodeStatus.COMPLETED
    assert collected == ["error handled"]


def test_static_successor_skipped_in_managed_run(
    queue: Queue, workflow_worker: WorkflowWorkerFactory
) -> None:
    """A plain (static) successor of a failed step is skipped in a managed run."""

    @queue.task(max_retries=0)
    def risky() -> str:
        raise RuntimeError("boom")

    ran: list[str] = []

    @queue.task()
    def follow_up() -> str:
        ran.append("follow_up")
        return "next"

    @queue.task()
    def recover() -> str:
        ran.append("recover")
        return "recovered"

    wf = Workflow(name="managed_static_cascade")
    wf.step("a", risky)
    wf.step("b", follow_up, after="a")
    wf.step("c", recover, after="a", condition="on_failure")

    with workflow_worker():
        run = queue.submit_workflow(wf)
        final = run.wait(timeout=20)

    assert final.state == WorkflowState.FAILED
    assert final.nodes["a"].status == NodeStatus.FAILED
    assert final.nodes["b"].status == NodeStatus.SKIPPED
    assert final.nodes["c"].status == NodeStatus.COMPLETED
    assert ran == ["recover"]
