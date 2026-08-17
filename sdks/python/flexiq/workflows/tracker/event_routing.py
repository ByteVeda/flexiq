"""Event routing helpers for WorkflowTracker."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from flexiq.events import EventType
from flexiq.workflows.tracker import fan_out

if TYPE_CHECKING:
    from flexiq.workflows.tracker.tracker import WorkflowTracker

logger = logging.getLogger("flexiq.workflows")


def handle_saga_terminal(
    tracker: WorkflowTracker, _event_type: EventType, payload: dict[str, Any]
) -> None:
    """Release waiters and clean up when a saga reaches a terminal state."""
    run_id = payload.get("workflow_run_id")
    if not run_id:
        return
    tracker._release_waiters(run_id)
    tracker._cleanup_run(run_id)


def handle_job_result(
    tracker: WorkflowTracker,
    payload: dict[str, Any],
    *,
    succeeded: bool,
    error: str | None,
) -> None:
    """Route a terminal job event to the appropriate workflow handler."""
    job_id = payload.get("job_id")
    if not job_id:
        return

    # Compensation jobs flow through a different code path — the saga
    # orchestrator owns their lifecycle, not the normal tracker.
    if tracker._saga.is_compensation_job(job_id):
        tracker._saga.on_compensation_completed(job_id=job_id, succeeded=succeeded, error=error)
        return

    # Determine if this job belongs to a managed run.
    with tracker._state_lock:
        run_id = tracker._job_to_run.get(job_id)
        config = tracker._run_configs.get(run_id) if run_id else None
    skip_cascade = config is not None

    # Compute result hash for successful completions.
    rh: str | None = None
    if succeeded:
        rh = tracker._compute_result_hash(job_id)

    try:
        result = tracker._queue._inner.mark_workflow_node_result(
            job_id, succeeded, error, skip_cascade, rh
        )
    except (RuntimeError, ValueError) as exc:
        logger.exception("mark_workflow_node_result failed for job %s", job_id)
        # Notify any waiters so they don't block forever on a silent failure.
        if run_id is not None:
            tracker._emit_terminal(run_id, "failed", str(exc))
            tracker._cleanup_run(run_id)
        return

    if result is None:
        return

    run_id, node_name, terminal_state = result

    # The initial lookup used _job_to_run which may not have this
    # job yet (register_run populates _run_configs before the
    # slower _job_to_run database read).  Re-fetch now that Rust
    # gave us the authoritative run_id.
    if config is None:
        with tracker._state_lock:
            config = tracker._run_configs.get(run_id)

    # Track partial failure occurrence for continue-mode saga finalization.
    # This is the only place we observe individual job-failure events in
    # the tracker — we record it on the in-memory config so that
    # _try_finalize / the terminal-handler below can decide whether to
    # transition the run to CompletedWithFailures instead of Failed.
    if not succeeded and config is not None and config.on_failure == "continue":
        with tracker._state_lock:
            config.partial_failure_occurred = True

    if terminal_state is not None:
        # Continue-mode partial-failure override: when at least one node
        # succeeded AND at least one failed AND the user opted into
        # compensation via compensate_on_continue=True, transition the
        # run to CompletedWithFailures (instead of the default Failed)
        # and hand off to the saga orchestrator. The CompletedWithFailures
        # state is itself a valid pre-compensation state — see
        # WorkflowState::can_transition_to in Rust.
        override_to_completed_with_failures = (
            terminal_state == "failed"
            and config is not None
            and config.on_failure == "continue"
            and config.compensate_on_continue
            and config.partial_failure_occurred
        )
        if override_to_completed_with_failures:
            try:
                tracker._queue._inner.set_workflow_run_completed_with_failures(run_id)
            except (RuntimeError, ValueError):
                logger.exception("set_workflow_run_completed_with_failures failed for %s", run_id)
            else:
                terminal_state = "completed_with_failures"

        # Hand off to the saga orchestrator when:
        #   - on_failure="fail_fast" and the run failed, OR
        #   - on_failure="continue" with compensate_on_continue=True and
        #     the run finalised with at least one failed node.
        # The orchestrator decides eligibility internally (checks for
        # explicit compensation_map or sub-workflow propagation) and
        # emits its own terminal event when compensation finishes.
        if config is not None and (
            (terminal_state == "failed" and config.on_failure == "fail_fast")
            or (terminal_state == "completed_with_failures" and config.compensate_on_continue)
        ):
            steps = tracker._run_steps.get(run_id, {})
            started = tracker._saga.start_compensation(run_id, config, steps)
            if started:
                # The saga owns the run from here. Don't clean up yet —
                # the orchestrator will call back when it finishes.
                return

        tracker._emit_terminal(run_id, terminal_state, error)
        tracker._cleanup_run(run_id)
        return

    if config is None:
        return  # Static workflow — Rust cascade handled everything.

    # Fan-out child handling.
    if "[" in node_name:
        if succeeded:
            fan_out.handle_fan_out_child(tracker, run_id, node_name, config)
        else:
            fan_out.handle_fan_out_child_failure(tracker, run_id, node_name, config)
        return

    # Fan-out expansion trigger (only on success).
    if succeeded:
        fan_out.maybe_trigger_fan_out(tracker, run_id, node_name, job_id, config)

    # Evaluate successors with conditions.
    tracker._evaluate_successors(run_id, node_name, config)


def handle_child_workflow_terminal(
    tracker: WorkflowTracker, _event_type: EventType, payload: dict[str, Any]
) -> None:
    """Handle child workflow completion by updating parent node.

    The ``_child_to_parent`` link is *not* popped here — it stays in
    place until the parent run finalises (or until the parent's saga
    orchestrator has had a chance to propagate compensation into the
    child). The parent's ``_cleanup_run`` sweeps stale links.
    """
    child_run_id = payload.get("run_id")
    if not child_run_id:
        return
    with tracker._state_lock:
        parent_info = tracker._child_to_parent.get(child_run_id)
    if parent_info is None:
        return  # Not a sub-workflow child.

    parent_run_id, parent_node_name = parent_info
    state = payload.get("state", "")
    succeeded = state == "completed"

    try:
        tracker._queue._inner.resolve_workflow_gate(
            parent_run_id,
            parent_node_name,
            succeeded,
            payload.get("error") if not succeeded else None,
        )
    except (RuntimeError, ValueError):
        logger.exception(
            "failed to update parent node %s for child %s",
            parent_node_name,
            child_run_id,
        )
        return

    with tracker._state_lock:
        config = tracker._run_configs.get(parent_run_id)
    if config is not None:
        tracker._evaluate_successors(parent_run_id, parent_node_name, config)
        tracker._try_finalize(parent_run_id)
