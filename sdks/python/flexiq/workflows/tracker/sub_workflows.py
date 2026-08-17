"""Sub-workflow submission and parent-node promotion."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from flexiq.events import EventType

if TYPE_CHECKING:
    from flexiq.workflows.tracker.tracker import WorkflowTracker
    from flexiq.workflows.tracker.types import _RunConfig


logger = logging.getLogger("flexiq.workflows")


def submit_sub_workflow(
    tracker: WorkflowTracker, run_id: str, node_name: str, config: _RunConfig
) -> None:
    """Submit a child workflow and transition the parent node to Running.

    The parent node is only promoted to ``Running`` after the child has
    successfully compiled *and* been submitted. On any failure during
    compile/submit, the parent is marked Failed so the run can finalize
    instead of hanging in an indeterminate state.
    """
    ref = config.sub_workflow_refs.get(node_name)
    if ref is None:  # pragma: no cover
        return

    try:
        child_wf = ref.proxy.build(**ref.params)
        (
            dag_bytes,
            meta_json,
            payloads,
            deferred,
            callables,
            on_failure,
            gates,
            sub_refs,
        ) = child_wf._compile(tracker._queue)

        handle = tracker._queue._inner.submit_workflow(
            child_wf.name,
            child_wf.version,
            dag_bytes,
            meta_json,
            payloads,
            "default",
            None,
            deferred if deferred else None,
            run_id,  # parent_run_id
            node_name,  # parent_node_name
        )
    except Exception as exc:
        logger.exception("submit sub-workflow failed for %s", node_name)
        # Mark the parent node Failed so the outer run can finalize rather
        # than hanging. This is the central fix for the old bug where a
        # compile failure left the node permanently Skipped.
        try:
            tracker._queue._inner.fail_workflow_node(
                run_id, node_name, f"sub-workflow submit failed: {exc}"
            )
        except (RuntimeError, ValueError):
            logger.exception("failed to mark sub-workflow parent %s as failed", node_name)
        config.deferred_nodes.discard(node_name)
        tracker._evaluate_successors(run_id, node_name, config)
        return

    # Child compiled and submitted successfully — now promote the parent.
    child_run_id = handle.run_id
    try:
        tracker._queue._emit_event(
            EventType.WORKFLOW_SUBMITTED,
            {
                "run_id": child_run_id,
                "workflow_name": handle.name,
                "parent_run_id": run_id,
            },
        )
    except Exception:
        logger.exception("failed to emit WORKFLOW_SUBMITTED")
    with tracker._state_lock:
        tracker._child_to_parent[child_run_id] = (run_id, node_name)
    try:
        tracker._queue._inner.set_workflow_node_running(run_id, node_name)
    except (RuntimeError, ValueError):
        logger.exception(
            "set_workflow_node_running failed for sub-workflow parent %s",
            node_name,
        )

    # Register child with tracker if it has deferred nodes OR if it has
    # registered compensators (so the saga orchestrator can propagate into
    # it on parent failure).
    child_compensation_map = getattr(child_wf, "_compiled_compensation_map", {}) or {}
    needs_child_tracker = (
        bool(deferred)
        or bool(callables)
        or bool(gates)
        or bool(sub_refs)
        or on_failure != "fail_fast"
        or bool(child_compensation_map)
    )
    if needs_child_tracker:
        child_payloads = {n: payloads[n] for n in deferred if n in payloads}
        tracker.register_run(
            child_run_id,
            meta_json,
            dag_bytes,
            deferred,
            child_payloads,
            on_failure=on_failure,
            callable_conditions=callables,
            gate_configs=gates,
            sub_workflow_refs=sub_refs,
            compensation_map=child_compensation_map,
            steps=getattr(child_wf, "_steps", None),
        )

    config.deferred_nodes.discard(node_name)
