"""Pure DAG helpers and node-action implementations.

Functions here are leaf helpers — they accept the tracker as the first
argument so they can read state and call back into tracker methods, but
they never import other tracker submodules. The tracker class wires them
together in its orchestrator methods.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

from flexiq.events import EventType
from flexiq.workflows.context import WorkflowContext
from flexiq.workflows.types import NodeStatus, WorkflowCondition, WorkflowState

if TYPE_CHECKING:
    from flexiq.workflows.tracker.tracker import WorkflowTracker
    from flexiq.workflows.tracker.types import _RunConfig


logger = logging.getLogger("flexiq.workflows")

# A predecessor gates its successors once it reaches one of these node statuses.
_PREDECESSOR_TERMINAL = frozenset(
    {NodeStatus.COMPLETED.value, NodeStatus.FAILED.value, NodeStatus.SKIPPED.value}
)


def build_dag_maps(
    dag_bytes: bytes | list[int],
) -> tuple[dict[str, list[str]], dict[str, list[str]]]:
    """Parse DAG JSON to build successor and predecessor maps."""
    raw = bytes(dag_bytes) if isinstance(dag_bytes, list) else dag_bytes
    dag_json = json.loads(raw)
    successors: dict[str, list[str]] = {}
    predecessors: dict[str, list[str]] = {}
    for node in dag_json.get("nodes", []):
        name = node["name"]
        successors.setdefault(name, [])
        predecessors.setdefault(name, [])
    for edge in dag_json.get("edges", []):
        successors.setdefault(edge["from"], []).append(edge["to"])
        predecessors.setdefault(edge["to"], []).append(edge["from"])
    return successors, predecessors


def int_or(value: Any, default: int) -> int:
    return value if value is not None else default


def final_state_to_event(state: str) -> EventType | None:
    if state == WorkflowState.COMPLETED.value:
        return EventType.WORKFLOW_COMPLETED
    if state == WorkflowState.COMPLETED_WITH_FAILURES.value:
        return EventType.WORKFLOW_COMPLETED_WITH_FAILURES
    if state == WorkflowState.FAILED.value:
        return EventType.WORKFLOW_FAILED
    if state == WorkflowState.CANCELLED.value:
        return EventType.WORKFLOW_CANCELLED
    return None


def all_predecessors_terminal(
    tracker: WorkflowTracker, run_id: str, node_name: str, config: _RunConfig
) -> bool:
    """Check whether all predecessors have a terminal status."""
    raw = tracker._queue._inner.get_workflow_run_status(run_id)
    node_statuses = raw.node_statuses()
    for pred in config.predecessors.get(node_name, []):
        info = node_statuses.get(pred)
        if info is None:
            return False
        status = info["status"]
        if status not in _PREDECESSOR_TERMINAL:
            return False
    return True


def get_predecessor_statuses(
    tracker: WorkflowTracker, run_id: str, node_name: str, config: _RunConfig
) -> dict[str, str]:
    """Return ``{pred_name: status_str}`` for all predecessors."""
    raw = tracker._queue._inner.get_workflow_run_status(run_id)
    node_statuses = raw.node_statuses()
    result: dict[str, str] = {}
    for pred in config.predecessors.get(node_name, []):
        info = node_statuses.get(pred)
        result[pred] = info["status"] if info else "pending"
    return result


def build_workflow_context(
    tracker: WorkflowTracker, run_id: str, config: _RunConfig
) -> WorkflowContext:
    """Build a :class:`WorkflowContext` from the current run state."""
    raw = tracker._queue._inner.get_workflow_run_status(run_id)
    node_statuses = raw.node_statuses()

    results: dict[str, Any] = {}
    statuses: dict[str, str] = {}
    failure_count = 0
    success_count = 0

    for name, info in node_statuses.items():
        status = info["status"]
        statuses[name] = status
        if status == NodeStatus.COMPLETED.value:
            success_count += 1
            jid = info.get("job_id")
            if jid:
                results[name] = tracker._fetch_result(jid)
        elif status == NodeStatus.FAILED.value:
            failure_count += 1

    return WorkflowContext(
        run_id=run_id,
        results=results,
        statuses=statuses,
        params=None,
        failure_count=failure_count,
        success_count=success_count,
    )


def should_execute(
    tracker: WorkflowTracker, run_id: str, node_name: str, config: _RunConfig
) -> bool:
    """Decide whether a deferred node should execute based on its condition."""
    callable_cond = config.callable_conditions.get(node_name)
    if callable_cond is not None:
        ctx = build_workflow_context(tracker, run_id, config)
        try:
            return bool(callable_cond(ctx))
        except Exception:
            logger.exception("callable condition failed for %s", node_name)
            return False

    meta = config.step_metadata.get(node_name, {})
    condition = meta.get("condition")
    pred_statuses = get_predecessor_statuses(tracker, run_id, node_name, config)

    if condition is None or condition == WorkflowCondition.ON_SUCCESS.value:
        return all(s == NodeStatus.COMPLETED.value for s in pred_statuses.values())
    if condition == WorkflowCondition.ON_FAILURE.value:
        return any(s == NodeStatus.FAILED.value for s in pred_statuses.values())
    return bool(condition == WorkflowCondition.ALWAYS.value)


def skip_and_propagate(
    tracker: WorkflowTracker, run_id: str, node_name: str, config: _RunConfig
) -> None:
    """Mark a node as SKIPPED and recursively evaluate its successors."""
    try:
        tracker._queue._inner.skip_workflow_node(run_id, node_name)
    except (RuntimeError, ValueError):
        logger.exception("skip_workflow_node failed for %s", node_name)
        return
    config.deferred_nodes.discard(node_name)
    tracker._evaluate_successors(run_id, node_name, config)


def create_deferred_job_for_node(
    tracker: WorkflowTracker, run_id: str, node_name: str, config: _RunConfig
) -> None:
    """Create a job for a deferred node and record the mapping."""
    payload = config.deferred_payloads.get(node_name)
    if payload is None:  # pragma: no cover
        logger.error("no cached payload for deferred node %s", node_name)
        return
    meta = config.step_metadata.get(node_name, {})
    task_name = meta["task_name"]
    queue_name = meta.get("queue") or "default"
    max_retries = int_or(meta.get("max_retries"), 3)
    timeout_ms = int_or(meta.get("timeout_ms"), 300_000)
    priority = int_or(meta.get("priority"), 0)

    try:
        job_id = tracker._queue._inner.create_deferred_job(
            run_id,
            node_name,
            payload,
            task_name,
            queue_name,
            max_retries,
            timeout_ms,
            priority,
        )
    except (RuntimeError, ValueError):
        logger.exception("create_deferred_job failed for %s", node_name)
        return
    with tracker._state_lock:
        tracker._job_to_run[job_id] = run_id
    config.deferred_nodes.discard(node_name)
