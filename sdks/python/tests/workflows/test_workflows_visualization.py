"""Tests for Phase 8 workflow visualization."""

from __future__ import annotations

import threading
from collections.abc import Callable
from contextlib import AbstractContextManager

from flexiq import Queue
from flexiq.workflows import Workflow

WorkflowWorkerFactory = Callable[[], AbstractContextManager[threading.Thread]]


class _FakeTask:
    _task_name = "fake"


def test_mermaid_linear() -> None:
    """Linear DAG renders correct Mermaid graph."""
    wf = Workflow(name="linear")
    wf.step("a", _FakeTask())
    wf.step("b", _FakeTask(), after="a")
    wf.step("c", _FakeTask(), after="b")

    output = wf.visualize("mermaid")
    assert "graph LR" in output
    assert "a[a]" in output
    assert "b[b]" in output
    assert "c[c]" in output
    assert "a --> b" in output
    assert "b --> c" in output


def test_mermaid_diamond() -> None:
    """Diamond DAG with parallel nodes."""
    wf = Workflow(name="diamond")
    wf.step("a", _FakeTask())
    wf.step("b", _FakeTask(), after="a")
    wf.step("c", _FakeTask(), after="a")
    wf.step("d", _FakeTask(), after=["b", "c"])

    output = wf.visualize("mermaid")
    assert "a --> b" in output
    assert "a --> c" in output
    assert "b --> d" in output
    assert "c --> d" in output


def test_mermaid_with_status() -> None:
    """Mermaid output with status colors."""
    from flexiq.workflows.visualization import render_mermaid

    output = render_mermaid(
        nodes=["a", "b", "c"],
        edges=[("a", "b"), ("b", "c")],
        statuses={"a": "completed", "b": "failed", "c": "pending"},
    )
    assert "style a fill:#90EE90" in output  # green
    assert "style b fill:#FFB6C1" in output  # red
    assert "style c fill:#D3D3D3" in output  # gray


def test_dot_linear() -> None:
    """DOT format output for linear DAG."""
    wf = Workflow(name="linear")
    wf.step("a", _FakeTask())
    wf.step("b", _FakeTask(), after="a")

    output = wf.visualize("dot")
    assert "digraph workflow" in output
    assert "rankdir=LR" in output
    assert "a -> b" in output


def test_visualize_live_run(queue: Queue, workflow_worker: WorkflowWorkerFactory) -> None:
    """WorkflowRun.visualize() shows live statuses."""

    @queue.task()
    def ok_task() -> str:
        return "ok"

    wf = Workflow(name="viz_live")
    wf.step("a", ok_task)
    wf.step("b", ok_task, after="a")

    with workflow_worker():
        run = queue.submit_workflow(wf)
        final = run.wait(timeout=15)
        output = run.visualize("mermaid")

    assert final.state.value == "completed"
    assert "graph LR" in output
    assert "a --> b" in output
    # Both nodes should have completed status styling
    assert "#90EE90" in output  # green for completed
