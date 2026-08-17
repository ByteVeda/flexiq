"""Fan-out expansion and fan-in collection."""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any

from flexiq.workflows.fan_out import apply_fan_out, build_child_payload, build_fan_in_payload
from flexiq.workflows.tracker.dag import int_or

if TYPE_CHECKING:
    from flexiq.workflows.tracker.tracker import WorkflowTracker
    from flexiq.workflows.tracker.types import _RunConfig


logger = logging.getLogger("flexiq.workflows")


def maybe_trigger_fan_out(
    tracker: WorkflowTracker,
    run_id: str,
    source_node: str,
    source_job_id: str,
    config: _RunConfig,
) -> None:
    """If a completed node's successor has ``fan_out``, expand it."""
    for successor in config.successors.get(source_node, []):
        meta = config.step_metadata.get(successor)
        if meta is None or meta.get("fan_out") is None:
            continue
        expand_fan_out(tracker, run_id, source_job_id, successor, meta, config)


def expand_fan_out(
    tracker: WorkflowTracker,
    run_id: str,
    source_job_id: str,
    fan_out_node: str,
    meta: dict[str, Any],
    config: _RunConfig,
) -> None:
    """Fetch the source result, split, and create child nodes + jobs."""
    result_bytes: bytes | None = None
    for _ in range(50):
        py_job = tracker._queue._inner.get_job(source_job_id)
        if py_job is not None:
            result_bytes = py_job.result_bytes
            if result_bytes is not None:
                break
        time.sleep(0.1)

    if result_bytes is None:
        source_result: Any = None
    else:
        source_result = tracker._queue._serializer.loads(result_bytes)

    strategy = meta["fan_out"]
    items = apply_fan_out(strategy, source_result)

    task_name = meta["task_name"]
    serializer = tracker._queue._get_serializer(task_name)
    child_names = [f"{fan_out_node}[{i}]" for i in range(len(items))]
    child_payloads = [
        tracker._queue._apply_task_codecs(task_name, build_child_payload(item, serializer))
        for item in items
    ]

    queue_name = meta.get("queue") or "default"
    max_retries = int_or(meta.get("max_retries"), 3)
    timeout_ms = int_or(meta.get("timeout_ms"), 300_000)
    priority = int_or(meta.get("priority"), 0)

    try:
        child_job_ids = tracker._queue._inner.expand_fan_out(
            run_id,
            fan_out_node,
            child_names,
            child_payloads,
            task_name,
            queue_name,
            max_retries,
            timeout_ms,
            priority,
        )
    except (RuntimeError, ValueError):
        logger.exception("expand_fan_out failed for %s in run %s", fan_out_node, run_id)
        return
    with tracker._state_lock:
        for jid in child_job_ids:
            tracker._job_to_run[jid] = run_id

    # Empty fan-out: parent is immediately COMPLETED with 0 children.
    if not child_names:
        for successor in config.successors.get(fan_out_node, []):
            succ_meta = config.step_metadata.get(successor)
            if succ_meta is not None and succ_meta.get("fan_in") is not None:
                create_fan_in_job(tracker, run_id, successor, succ_meta, [], config)
                break
        # _evaluate_successors never creates fan-in nodes, so no duplicate.
        tracker._evaluate_successors(run_id, fan_out_node, config)


def handle_fan_out_child(
    tracker: WorkflowTracker, run_id: str, child_name: str, config: _RunConfig
) -> None:
    """Check whether all siblings are done → trigger fan-in."""
    parent_name = child_name.split("[")[0]
    try:
        completion = tracker._queue._inner.check_fan_out_completion(run_id, parent_name)
    except (RuntimeError, ValueError):
        logger.exception("check_fan_out_completion failed for %s", parent_name)
        return

    if completion is None:
        return

    all_succeeded, child_job_ids = completion
    if not all_succeeded:
        # Parent marked FAILED. Evaluate successors (on_failure may trigger).
        tracker._evaluate_successors(run_id, parent_name, config)
        tracker._try_finalize(run_id)
        return

    # Trigger fan-in, if any.
    for successor in config.successors.get(parent_name, []):
        meta = config.step_metadata.get(successor)
        if meta is not None and meta.get("fan_in") is not None:
            create_fan_in_job(tracker, run_id, successor, meta, child_job_ids, config)
            break

    # _evaluate_successors never creates fan-in nodes, so no duplicate.
    tracker._evaluate_successors(run_id, parent_name, config)


def handle_fan_out_child_failure(
    tracker: WorkflowTracker, run_id: str, child_name: str, config: _RunConfig
) -> None:
    """Handle a failed fan-out child."""
    parent_name = child_name.split("[")[0]
    try:
        completion = tracker._queue._inner.check_fan_out_completion(run_id, parent_name)
    except (RuntimeError, ValueError):
        logger.exception("check_fan_out_completion failed for %s", parent_name)
        return

    if completion is None:
        return

    # Parent is marked FAILED. Evaluate successors for condition-based logic.
    tracker._evaluate_successors(run_id, parent_name, config)
    tracker._try_finalize(run_id)


def create_fan_in_job(
    tracker: WorkflowTracker,
    run_id: str,
    fan_in_node: str,
    meta: dict[str, Any],
    child_job_ids: list[str],
    config: _RunConfig,
) -> None:
    """Collect children results and create the fan-in job."""
    results: list[Any] = []
    for job_id in child_job_ids:
        results.append(tracker._fetch_result(job_id))

    task_name = meta["task_name"]
    serializer = tracker._queue._get_serializer(task_name)
    payload = tracker._queue._apply_task_codecs(
        task_name, build_fan_in_payload(results, serializer)
    )

    queue_name = meta.get("queue") or "default"
    max_retries = int_or(meta.get("max_retries"), 3)
    timeout_ms = int_or(meta.get("timeout_ms"), 300_000)
    priority = int_or(meta.get("priority"), 0)

    try:
        job_id = tracker._queue._inner.create_deferred_job(
            run_id,
            fan_in_node,
            payload,
            task_name,
            queue_name,
            max_retries,
            timeout_ms,
            priority,
        )
    except (RuntimeError, ValueError):
        logger.exception("create_deferred_job failed for fan-in %s", fan_in_node)
        return
    with tracker._state_lock:
        tracker._job_to_run[job_id] = run_id
