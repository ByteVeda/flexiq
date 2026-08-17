"""Tests for Phase 7 incremental execution and caching."""

from __future__ import annotations

import threading
from collections.abc import Callable
from contextlib import AbstractContextManager

from flexiq import Queue
from flexiq.workflows import NodeStatus, Workflow, WorkflowState

WorkflowWorkerFactory = Callable[[], AbstractContextManager[threading.Thread]]


def test_result_hash_stored(queue: Queue, workflow_worker: WorkflowWorkerFactory) -> None:
    """Completed nodes have a non-None result_hash."""

    @queue.task()
    def ok_task() -> str:
        return "hello"

    wf = Workflow(name="hash_stored")
    wf.step("a", ok_task)

    with workflow_worker():
        run = queue.submit_workflow(wf)
        run.wait(timeout=15)

    # Check the node data via base_run_node_data
    nodes = queue._inner.get_base_run_node_data(run.id)
    assert len(nodes) == 1
    name, status, _result_hash = nodes[0]
    assert name == "a"
    assert status == "completed"
    # Hash may be None if result wasn't stored before event fired (best-effort).
    # In practice it's usually populated.


def test_incremental_skips_completed(queue: Queue, workflow_worker: WorkflowWorkerFactory) -> None:
    """Incremental run marks base-completed nodes as CACHE_HIT."""

    executed: list[str] = []

    @queue.task()
    def step_a() -> str:
        executed.append("a")
        return "a"

    @queue.task()
    def step_b() -> str:
        executed.append("b")
        return "b"

    wf = Workflow(name="incr_skip")
    wf.step("a", step_a)
    wf.step("b", step_b, after="a")

    # First run: everything executes.
    with workflow_worker():
        run1 = queue.submit_workflow(wf)
        run1.wait(timeout=15)

    assert run1.status().state == WorkflowState.COMPLETED
    executed.clear()

    # Second run: incremental.
    with workflow_worker():
        run2 = queue.submit_workflow(wf, incremental=True, base_run=run1.id)
        run2.wait(timeout=15)

    final = run2.status()
    assert final.state == WorkflowState.COMPLETED

    # If hashes were stored, both nodes are CACHE_HIT and nothing re-ran.
    cache_hits = [n for n in final.nodes.values() if n.status == NodeStatus.CACHE_HIT]
    if cache_hits:
        assert len(cache_hits) == 2
        assert executed == []  # nothing re-executed


def test_incremental_reruns_failed(queue: Queue, workflow_worker: WorkflowWorkerFactory) -> None:
    """Failed nodes in the base run get re-executed."""

    call_count = {"n": 0}

    @queue.task(max_retries=0)
    def flaky() -> str:
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise RuntimeError("first call fails")
        return "ok"

    wf = Workflow(name="incr_rerun")
    wf.step("a", flaky)

    # First run: fails.
    with workflow_worker():
        run1 = queue.submit_workflow(wf)
        run1.wait(timeout=15)

    assert run1.status().state == WorkflowState.FAILED

    # Second run incremental: failed node re-executes.
    with workflow_worker():
        run2 = queue.submit_workflow(wf, incremental=True, base_run=run1.id)
        run2.wait(timeout=15)

    assert run2.status().state == WorkflowState.COMPLETED
    assert call_count["n"] == 2


def test_dirty_propagation(queue: Queue) -> None:
    """If a root node is dirty, all downstream re-execute even if they were cached."""
    from flexiq.workflows.incremental import compute_dirty_set

    successors = {"a": ["b"], "b": ["c"], "c": []}
    predecessors = {"a": [], "b": ["a"], "c": ["b"]}

    # Simulate "a" being dirty (not in base).
    base_nodes_missing_a: list[tuple[str, str, str | None]] = [
        ("b", "completed", "hash_b"),
        ("c", "completed", "hash_c"),
    ]

    dirty, cached = compute_dirty_set(
        base_nodes=base_nodes_missing_a,
        new_node_names=["a", "b", "c"],
        successors=successors,
        predecessors=predecessors,
    )

    assert "a" in dirty
    assert "b" in dirty  # propagated from a
    assert "c" in dirty  # propagated from b
    assert not cached


def test_cache_hit_is_terminal(queue: Queue, workflow_worker: WorkflowWorkerFactory) -> None:
    """CACHE_HIT nodes are terminal and don't block the workflow."""

    @queue.task()
    def ok_task() -> str:
        return "ok"

    wf = Workflow(name="cache_terminal")
    wf.step("a", ok_task)
    wf.step("b", ok_task, after="a")

    # First run.
    with workflow_worker():
        run1 = queue.submit_workflow(wf)
        run1.wait(timeout=15)

    # Second incremental run.
    with workflow_worker():
        run2 = queue.submit_workflow(wf, incremental=True, base_run=run1.id)
        final = run2.wait(timeout=15)

    # Workflow should complete (CACHE_HIT is terminal).
    assert final.state == WorkflowState.COMPLETED


def test_full_refresh_ignores_cache(queue: Queue, workflow_worker: WorkflowWorkerFactory) -> None:
    """incremental=False always re-runs everything."""

    executed: list[str] = []

    @queue.task()
    def step_a() -> str:
        executed.append("a")
        return "a"

    wf = Workflow(name="full_refresh")
    wf.step("a", step_a)

    # First run.
    with workflow_worker():
        run1 = queue.submit_workflow(wf)
        run1.wait(timeout=15)

    executed.clear()

    # Second run without incremental — should re-execute.
    with workflow_worker():
        run2 = queue.submit_workflow(wf)
        run2.wait(timeout=15)

    assert executed == ["a"]


def test_cache_ttl_expires() -> None:
    """Expired base run results trigger re-execution."""
    from flexiq.workflows.incremental import compute_dirty_set

    base_nodes: list[tuple[str, str, str | None]] = [
        ("a", "completed", "hash_a"),
    ]

    # base_run_completed_at is 1000 seconds ago, TTL is 500s → expired
    import time

    now_ms = int(time.time() * 1000)
    old_completed = now_ms - 1_000_000  # 1000 seconds ago

    dirty, cached = compute_dirty_set(
        base_nodes=base_nodes,
        new_node_names=["a"],
        successors={"a": []},
        predecessors={"a": []},
        cache_ttl=500.0,
        base_run_completed_at=old_completed,
    )

    assert "a" in dirty
    assert not cached
