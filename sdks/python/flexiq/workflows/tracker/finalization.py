"""Run finalization: terminal emission, state cleanup, and saga hand-off."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from flexiq.workflows.tracker import dag

if TYPE_CHECKING:
    from flexiq.workflows.tracker.tracker import WorkflowTracker

logger = logging.getLogger("flexiq.workflows")


def emit_terminal(
    tracker: WorkflowTracker, run_id: str, terminal_state: str, error: str | None
) -> None:
    """Emit the workflow-level terminal event and release waiters."""
    workflow_event = dag.final_state_to_event(terminal_state)
    if workflow_event is not None:
        try:
            tracker._queue._emit_event(
                workflow_event,
                {"run_id": run_id, "state": terminal_state, "error": error},
            )
        except Exception:
            logger.exception("failed to emit %s", workflow_event)
    tracker._release_waiters(run_id)


def cleanup_run(tracker: WorkflowTracker, run_id: str) -> None:
    """Drop all tracker state tied to ``run_id`` and cancel any live timers.

    When the run is a sub-workflow child whose parent is still being
    tracked, ``_run_configs`` and ``_run_steps`` are *retained* so the
    parent's saga orchestrator can propagate compensation into the
    child later. The parent's own cleanup sweeps these entries when it
    finalises (see ``stale_child_ids`` below).
    """
    with tracker._state_lock:
        parent_still_alive = False
        parent_link = tracker._child_to_parent.get(run_id)
        if parent_link is not None:
            parent_run_id = parent_link[0]
            parent_still_alive = parent_run_id in tracker._run_configs

        if parent_still_alive:
            # Keep config + steps so the parent can propagate compensation.
            # Drop transient state (job->run map entries, gate timers) so
            # the worker doesn't keep routing fan-out / gate events to a
            # finished child.
            tracker._job_to_run = {
                jid: rid for jid, rid in tracker._job_to_run.items() if rid != run_id
            }
            stale_timer_keys = [k for k in tracker._gate_timers if k[0] == run_id]
            for key in stale_timer_keys:
                timer = tracker._gate_timers.pop(key, None)
                if timer is not None:
                    timer.cancel()
            return

        tracker._run_configs.pop(run_id, None)
        tracker._run_steps.pop(run_id, None)
        tracker._job_to_run = {
            jid: rid for jid, rid in tracker._job_to_run.items() if rid != run_id
        }
        stale_timer_keys = [k for k in tracker._gate_timers if k[0] == run_id]
        for key in stale_timer_keys:
            timer = tracker._gate_timers.pop(key, None)
            if timer is not None:
                timer.cancel()
        stale_child_ids = [
            cid for cid, (prid, _) in tracker._child_to_parent.items() if prid == run_id
        ]
        for cid in stale_child_ids:
            tracker._child_to_parent.pop(cid, None)
            # Children whose cleanup we deferred (because the parent was
            # still alive) get swept here on parent finalization.
            tracker._run_configs.pop(cid, None)
            tracker._run_steps.pop(cid, None)


def try_finalize(tracker: WorkflowTracker, run_id: str) -> None:
    """If all nodes are terminal, finalize the run and emit the event."""
    try:
        terminal_state = tracker._queue._inner.finalize_run_if_terminal(run_id)
    except (RuntimeError, ValueError):
        logger.exception("finalize_run_if_terminal failed for %s", run_id)
        return
    if terminal_state is None:
        return

    with tracker._state_lock:
        config = tracker._run_configs.get(run_id)

    # Continue-mode partial-failure override + compensation hand-off.
    # Mirrors the logic in handle_job_result() so fan-out and sub-workflow
    # paths also surface CompletedWithFailures correctly.
    if (
        terminal_state == "failed"
        and config is not None
        and config.on_failure == "continue"
        and config.compensate_on_continue
        and config.partial_failure_occurred
    ):
        try:
            tracker._queue._inner.set_workflow_run_completed_with_failures(run_id)
        except (RuntimeError, ValueError):
            logger.exception("set_workflow_run_completed_with_failures failed for %s", run_id)
        else:
            terminal_state = "completed_with_failures"

    if config is not None and (
        (terminal_state == "failed" and config.on_failure == "fail_fast")
        or (terminal_state == "completed_with_failures" and config.compensate_on_continue)
    ):
        steps = tracker._run_steps.get(run_id, {})
        started = tracker._saga.start_compensation(run_id, config, steps)
        if started:
            return

    emit_terminal(tracker, run_id, terminal_state, None)
    cleanup_run(tracker, run_id)
