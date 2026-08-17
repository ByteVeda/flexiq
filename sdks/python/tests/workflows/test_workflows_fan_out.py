"""Tests for Phase 3 fan-out / fan-in workflow execution."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from contextlib import AbstractContextManager
from typing import Any

import pytest

from flexiq import Queue
from flexiq.workflows import NodeStatus, Workflow, WorkflowState

WorkflowWorkerFactory = Callable[[], AbstractContextManager[threading.Thread]]
PollUntil = Any  # the conftest fixture's runtime type


def test_fan_out_each(queue: Queue, workflow_worker: WorkflowWorkerFactory) -> None:
    """fan_out='each' splits a list into N parallel jobs, fan_in='all' collects."""

    @queue.task()
    def source() -> list[int]:
        return [10, 20, 30]

    @queue.task()
    def double(x: int) -> int:
        return x * 2

    collected: list[Any] = []

    @queue.task()
    def aggregate(results: list[int]) -> str:
        collected.extend(results)
        return "done"

    wf = Workflow(name="fan_out_each")
    wf.step("fetch", source)
    wf.step("process", double, after="fetch", fan_out="each")
    wf.step("collect", aggregate, after="process", fan_in="all")

    with workflow_worker():
        run = queue.submit_workflow(wf)
        final = run.wait(timeout=20)

    assert final.state == WorkflowState.COMPLETED
    assert sorted(collected) == [20, 40, 60]


def test_fan_out_empty_list(queue: Queue, workflow_worker: WorkflowWorkerFactory) -> None:
    """Fan-out over an empty list → fan-in receives []."""

    @queue.task()
    def source() -> list:
        return []

    @queue.task()
    def process(x: int) -> int:
        return x * 2

    collected: list[Any] = []

    @queue.task()
    def aggregate(results: list) -> str:
        collected.extend(results)
        return "empty"

    wf = Workflow(name="fan_out_empty")
    wf.step("fetch", source)
    wf.step("process", process, after="fetch", fan_out="each")
    wf.step("collect", aggregate, after="process", fan_in="all")

    with workflow_worker():
        run = queue.submit_workflow(wf)
        final = run.wait(timeout=20)

    assert final.state == WorkflowState.COMPLETED
    assert collected == []


def test_fan_out_single_item(queue: Queue, workflow_worker: WorkflowWorkerFactory) -> None:
    """Fan-out with a single-element list → 1 child → fan-in gets [result]."""

    @queue.task()
    def source() -> list[int]:
        return [42]

    @queue.task()
    def add_one(x: int) -> int:
        return x + 1

    collected: list[Any] = []

    @queue.task()
    def aggregate(results: list[int]) -> str:
        collected.extend(results)
        return "single"

    wf = Workflow(name="fan_out_single")
    wf.step("fetch", source)
    wf.step("process", add_one, after="fetch", fan_out="each")
    wf.step("collect", aggregate, after="process", fan_in="all")

    with workflow_worker():
        run = queue.submit_workflow(wf)
        final = run.wait(timeout=20)

    assert final.state == WorkflowState.COMPLETED
    assert collected == [43]


def test_fan_out_with_downstream(queue: Queue, workflow_worker: WorkflowWorkerFactory) -> None:
    """Full pipeline: source → fan_out → fan_in → downstream static step."""

    order: list[str] = []

    @queue.task()
    def source() -> list[str]:
        order.append("source")
        return ["a", "b"]

    @queue.task()
    def process(item: str) -> str:
        order.append(f"process:{item}")
        return item.upper()

    @queue.task()
    def aggregate(results: list[str]) -> str:
        order.append("aggregate")
        return ",".join(sorted(results))

    @queue.task()
    def report() -> str:
        order.append("report")
        return "finished"

    wf = Workflow(name="downstream_pipe")
    wf.step("fetch", source)
    wf.step("process", process, after="fetch", fan_out="each")
    wf.step("agg", aggregate, after="process", fan_in="all")
    wf.step("report", report, after="agg")

    with workflow_worker():
        run = queue.submit_workflow(wf)
        final = run.wait(timeout=20)

    assert final.state == WorkflowState.COMPLETED
    assert "source" in order
    assert "aggregate" in order
    assert "report" in order
    # Source runs first, report runs last
    assert order.index("source") < order.index("aggregate")
    assert order.index("aggregate") < order.index("report")


def test_fan_out_child_failure(queue: Queue, workflow_worker: WorkflowWorkerFactory) -> None:
    """A failing fan-out child triggers fail-fast, workflow fails."""

    @queue.task()
    def source() -> list[int]:
        return [1, 2, 3]

    @queue.task(max_retries=0)
    def maybe_fail(x: int) -> int:
        if x == 2:
            raise RuntimeError("boom on 2")
        return x * 10

    @queue.task()
    def aggregate(results: list[int]) -> str:
        return "should not run"

    wf = Workflow(name="fan_out_fail")
    wf.step("fetch", source)
    wf.step("process", maybe_fail, after="fetch", fan_out="each")
    wf.step("collect", aggregate, after="process", fan_in="all")

    with workflow_worker():
        run = queue.submit_workflow(wf)
        final = run.wait(timeout=20)

    assert final.state == WorkflowState.FAILED
    # The aggregate node should be skipped
    assert final.nodes["collect"].status == NodeStatus.SKIPPED


def test_fan_out_source_failure(queue: Queue, workflow_worker: WorkflowWorkerFactory) -> None:
    """If the fan-out source step fails, all deferred nodes are SKIPPED."""

    @queue.task(max_retries=0)
    def bad_source() -> list[int]:
        raise RuntimeError("source failed")

    @queue.task()
    def process(x: int) -> int:
        return x * 2

    @queue.task()
    def aggregate(results: list[int]) -> str:
        return "nope"

    wf = Workflow(name="source_fail")
    wf.step("fetch", bad_source)
    wf.step("process", process, after="fetch", fan_out="each")
    wf.step("collect", aggregate, after="process", fan_in="all")

    with workflow_worker():
        run = queue.submit_workflow(wf)
        final = run.wait(timeout=20)

    assert final.state == WorkflowState.FAILED
    assert final.nodes["fetch"].status == NodeStatus.FAILED
    assert final.nodes["process"].status == NodeStatus.SKIPPED
    assert final.nodes["collect"].status == NodeStatus.SKIPPED


def test_fan_out_cancellation(
    queue: Queue, workflow_worker: WorkflowWorkerFactory, poll_until: PollUntil
) -> None:
    """Cancelling a workflow mid-fan-out skips pending children."""

    @queue.task()
    def source() -> list[int]:
        return [1, 2, 3]

    @queue.task()
    def slow_process(x: int) -> int:
        # Intentional pacing — the test needs the children to be running so the
        # cancel happens mid-fan-out.
        time.sleep(10)
        return x

    @queue.task()
    def aggregate(results: list[int]) -> str:
        return "nope"

    wf = Workflow(name="cancel_fan_out")
    wf.step("fetch", source)
    wf.step("process", slow_process, after="fetch", fan_out="each")
    wf.step("collect", aggregate, after="process", fan_in="all")

    with workflow_worker():
        run = queue.submit_workflow(wf)
        poll_until(
            lambda: run.node_status("fetch") == NodeStatus.COMPLETED,
            timeout=10,
            message="source did not complete; fan-out never expanded",
        )
        run.cancel()
        snapshot = run.status()

    assert snapshot.state == WorkflowState.CANCELLED


def test_fan_out_status_shows_children(
    queue: Queue, workflow_worker: WorkflowWorkerFactory
) -> None:
    """status() returns child node snapshots like process[0], process[1]."""

    @queue.task()
    def source() -> list[int]:
        return [1, 2, 3]

    @queue.task()
    def process(x: int) -> int:
        return x

    @queue.task()
    def aggregate(results: list[int]) -> str:
        return "done"

    wf = Workflow(name="show_children")
    wf.step("fetch", source)
    wf.step("process", process, after="fetch", fan_out="each")
    wf.step("collect", aggregate, after="process", fan_in="all")

    with workflow_worker():
        run = queue.submit_workflow(wf)
        final = run.wait(timeout=20)

    assert final.state == WorkflowState.COMPLETED
    # Children should appear in the node map
    assert "process[0]" in final.nodes
    assert "process[1]" in final.nodes
    assert "process[2]" in final.nodes
    for i in range(3):
        assert final.nodes[f"process[{i}]"].status == NodeStatus.COMPLETED
        assert final.nodes[f"process[{i}]"].job_id is not None


def test_fan_out_preserves_result_order(
    queue: Queue, workflow_worker: WorkflowWorkerFactory
) -> None:
    """Fan-in results maintain the order of child indices."""

    @queue.task()
    def source() -> list[str]:
        return ["x", "y", "z"]

    @queue.task()
    def identity(item: str) -> str:
        return item

    collected: list[Any] = []

    @queue.task()
    def aggregate(results: list[str]) -> str:
        collected.extend(results)
        return "ok"

    wf = Workflow(name="order_check")
    wf.step("fetch", source)
    wf.step("process", identity, after="fetch", fan_out="each")
    wf.step("collect", aggregate, after="process", fan_in="all")

    with workflow_worker():
        run = queue.submit_workflow(wf)
        final = run.wait(timeout=20)

    assert final.state == WorkflowState.COMPLETED
    # Results should be in child index order (same as input order)
    assert collected == ["x", "y", "z"]


def test_step_name_bracket_validation() -> None:
    """Step names containing '[' raise ValueError."""
    wf = Workflow(name="bad_name")

    class _FakeTask:
        _task_name = "fake"

    with pytest.raises(ValueError, match="must not contain"):
        wf.step("bad[0]", _FakeTask())


def test_linear_workflow_still_works(queue: Queue, workflow_worker: WorkflowWorkerFactory) -> None:
    """Phase 2 regression: a linear workflow without fan-out still works."""

    order: list[str] = []

    @queue.task()
    def step_a() -> str:
        order.append("a")
        return "a"

    @queue.task()
    def step_b() -> str:
        order.append("b")
        return "b"

    @queue.task()
    def step_c() -> str:
        order.append("c")
        return "c"

    wf = Workflow(name="linear_regression")
    wf.step("a", step_a)
    wf.step("b", step_b, after="a")
    wf.step("c", step_c, after="b")

    with workflow_worker():
        run = queue.submit_workflow(wf)
        final = run.wait(timeout=15)

    assert final.state == WorkflowState.COMPLETED
    assert order == ["a", "b", "c"]


def test_fan_out_sibling_successor_runs(
    queue: Queue, workflow_worker: WorkflowWorkerFactory
) -> None:
    """A fan-out parent's non-fan-in successor runs alongside the fan-in."""

    @queue.task()
    def source() -> list[int]:
        return [1, 2]

    @queue.task()
    def double(x: int) -> int:
        return x * 2

    collected: list[int] = []

    @queue.task()
    def aggregate(results: list[int]) -> str:
        collected.extend(results)
        return "done"

    ran: list[str] = []

    @queue.task()
    def notify() -> str:
        ran.append("notify")
        return "sent"

    wf = Workflow(name="fan_out_sibling")
    wf.step("fetch", source)
    wf.step("process", double, after="fetch", fan_out="each")
    wf.step("collect", aggregate, after="process", fan_in="all")
    wf.step("notify", notify, after="process", condition="always")

    with workflow_worker():
        run = queue.submit_workflow(wf)
        final = run.wait(timeout=20)

    assert final.state == WorkflowState.COMPLETED
    assert sorted(collected) == [2, 4]
    assert ran == ["notify"]
    assert final.nodes["notify"].status == NodeStatus.COMPLETED
