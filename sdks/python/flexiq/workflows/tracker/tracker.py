"""Workflow completion tracker — main class and orchestration.

Subscribes to terminal job events (``JOB_COMPLETED``, ``JOB_FAILED``,
``JOB_DEAD``, ``JOB_CANCELLED``) and forwards workflow-related ones to the
Rust ``mark_workflow_node_result`` entry point. When a run reaches a terminal
state, emits a workflow-level event and releases any threads blocked on
``WorkflowRun.wait``.

For workflows that contain fan-out / fan-in steps, conditions, or
``on_failure="continue"``, the tracker orchestrates dynamic job creation,
condition evaluation, and selective skip propagation. Concern-specific
helpers live in ``dag``, ``gates``, ``sub_workflows``, ``fan_out``,
``event_routing``, and ``finalization`` and are invoked from this
orchestrator.
"""

from __future__ import annotations

import hashlib
import json
import logging
import threading
import time
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from flexiq.events import EventType
from flexiq.workflows.saga import SagaOrchestrator
from flexiq.workflows.tracker import dag, event_routing, finalization, gates, sub_workflows
from flexiq.workflows.tracker.types import _RunConfig

if TYPE_CHECKING:
    from flexiq.app import Queue


logger = logging.getLogger("flexiq.workflows")


class WorkflowTracker:
    """Bridges flexiq job events to workflow run state transitions."""

    def __init__(self, queue: Queue):
        self._queue = queue
        self._waiters_lock = threading.Lock()
        self._waiters: dict[str, list[threading.Event]] = {}
        self._event_bus = queue._event_bus
        # `_state_lock` guards every read and write of `_run_configs`,
        # `_job_to_run`, `_child_to_parent`, and `_gate_timers`. These dicts
        # are accessed from worker threads (event bus), timer threads
        # (gate timeouts), and user threads (approve_gate/reject_gate).
        # A single lock is simple and adequate; tracker operations are short.
        self._state_lock = threading.RLock()
        self._run_configs: dict[str, _RunConfig] = {}
        self._job_to_run: dict[str, str] = {}
        # Steps per run, captured at register-run time. Needed at saga start
        # to compute reverse topological waves of the compensable nodes.
        self._run_steps: dict[str, dict[str, Any]] = {}
        self._gate_timers: dict[tuple[str, str], threading.Timer] = {}
        self._child_to_parent: dict[str, tuple[str, str]] = {}
        # Saga orchestrator — driven by the failure path in `_handle`.
        self._saga = SagaOrchestrator(queue)
        self._install_listeners()

    def _install_listeners(self) -> None:
        self._event_bus.on(EventType.JOB_COMPLETED, self._on_success)
        # React only to a node's *terminal* failure. JOB_FAILED fires
        # synchronously in the worker thread on every exception — before the
        # scheduler has decided retry-vs-dead — so subscribing to it too would
        # run the node/saga handler a second time for the one terminal failure
        # (JOB_FAILED then JOB_DEAD), flapping the run state. A retriable
        # failure surfaces as JOB_RETRYING and is correctly ignored until the
        # job is finally dead-lettered.
        self._event_bus.on(EventType.JOB_DEAD, self._on_failure)
        self._event_bus.on(EventType.JOB_CANCELLED, self._on_cancelled)
        # Sub-workflow completion events.
        self._event_bus.on(EventType.WORKFLOW_COMPLETED, self._on_child_workflow_terminal)
        self._event_bus.on(EventType.WORKFLOW_FAILED, self._on_child_workflow_terminal)
        self._event_bus.on(EventType.WORKFLOW_CANCELLED, self._on_child_workflow_terminal)
        # Saga terminal events — release waiters when compensation finishes.
        self._event_bus.on(EventType.WORKFLOW_COMPENSATED, self._on_saga_terminal)
        self._event_bus.on(EventType.WORKFLOW_COMPENSATION_FAILED, self._on_saga_terminal)

    def _on_saga_terminal(self, _event_type: EventType, payload: dict[str, Any]) -> None:
        event_routing.handle_saga_terminal(self, _event_type, payload)

    # ── Wait management ────────────────────────────────────────────

    def register_wait(self, run_id: str) -> threading.Event:
        """Return an event that will be set when ``run_id`` reaches a terminal state."""
        event = threading.Event()
        with self._waiters_lock:
            self._waiters.setdefault(run_id, []).append(event)
        return event

    def unregister_wait(self, run_id: str, event: threading.Event) -> None:
        with self._waiters_lock:
            entries = self._waiters.get(run_id)
            if not entries:
                return
            try:
                entries.remove(event)
            except ValueError:
                return
            if not entries:
                self._waiters.pop(run_id, None)

    def _release_waiters(self, run_id: str) -> None:
        with self._waiters_lock:
            entries = self._waiters.pop(run_id, [])
        for event in entries:
            event.set()

    # ── Dynamic run registration ───────────────────────────────────

    def register_run(
        self,
        run_id: str,
        step_metadata_json: str,
        dag_bytes: bytes | list[int],
        deferred_nodes: list[str],
        deferred_payloads: dict[str, bytes],
        on_failure: str = "fail_fast",
        callable_conditions: dict[str, Callable[..., bool]] | None = None,
        gate_configs: dict[str, Any] | None = None,
        sub_workflow_refs: dict[str, Any] | None = None,
        compensation_map: dict[str, str] | None = None,
        steps: dict[str, Any] | None = None,
        compensate_on_continue: bool = False,
    ) -> None:
        """Cache configuration for a tracker-managed workflow run."""
        meta: dict[str, dict[str, Any]] = json.loads(step_metadata_json)
        successors, predecessors = dag.build_dag_maps(dag_bytes)
        config = _RunConfig(
            step_metadata=meta,
            successors=successors,
            predecessors=predecessors,
            deferred_nodes=set(deferred_nodes),
            deferred_payloads=deferred_payloads,
            on_failure=on_failure,
            callable_conditions=callable_conditions or {},
            gate_configs=gate_configs or {},
            sub_workflow_refs=sub_workflow_refs or {},
            compensation_map=compensation_map or {},
            compensate_on_continue=compensate_on_continue,
        )
        with self._state_lock:
            self._run_configs[run_id] = config
            if steps is not None:
                self._run_steps[run_id] = steps

        # Populate job→run mapping for initial nodes.
        try:
            raw = self._queue._inner.get_workflow_run_status(run_id)
        except (RuntimeError, ValueError):
            logger.exception("failed to read workflow run status for %s", run_id)
        else:
            with self._state_lock:
                for _name, info in raw.node_statuses().items():
                    jid = info.get("job_id")
                    if jid:
                        self._job_to_run[jid] = run_id

        # Evaluate root deferred nodes (those with no predecessors).
        self._evaluate_root_deferred(run_id, config)

    def _evaluate_root_deferred(self, run_id: str, config: _RunConfig) -> None:
        """Immediately evaluate deferred root nodes (no predecessors)."""
        for node_name in list(config.deferred_nodes):
            preds = config.predecessors.get(node_name, [])
            if preds:
                continue  # Not a root node.
            meta = config.step_metadata.get(node_name)
            if meta is None:
                continue
            if node_name in config.gate_configs:
                gates.enter_gate(self, run_id, node_name, config)
            elif node_name in config.sub_workflow_refs:
                sub_workflows.submit_sub_workflow(self, run_id, node_name, config)
            elif meta.get("fan_out") is None and meta.get("fan_in") is None:
                dag.create_deferred_job_for_node(self, run_id, node_name, config)

    # ── Event handlers ─────────────────────────────────────────────

    def _on_success(self, _event_type: EventType, payload: dict[str, Any]) -> None:
        event_routing.handle_job_result(self, payload, succeeded=True, error=None)

    def _on_failure(self, _event_type: EventType, payload: dict[str, Any]) -> None:
        event_routing.handle_job_result(self, payload, succeeded=False, error=payload.get("error"))

    def _on_cancelled(self, _event_type: EventType, payload: dict[str, Any]) -> None:
        event_routing.handle_job_result(self, payload, succeeded=False, error="cancelled")

    # ── Successor evaluation ───────────────────────────────────────

    def _evaluate_successors(self, run_id: str, completed_node: str, config: _RunConfig) -> None:
        """Evaluate and create/skip successor nodes."""
        for successor in config.successors.get(completed_node, []):
            meta = config.step_metadata.get(successor)
            if meta is None:
                continue

            if not dag.all_predecessors_terminal(self, run_id, successor, config):
                continue

            if successor not in config.deferred_nodes:
                # The core's fail-fast cascade is suppressed on managed runs, so
                # a static successor of a failed predecessor is skipped here.
                if not dag.should_execute(self, run_id, successor, config):
                    dag.skip_and_propagate(self, run_id, successor, config)
                continue

            # Fan-out/fan-in nodes: only skip them here (expansion is
            # handled by maybe_trigger_fan_out / handle_fan_out_child).
            if meta.get("fan_out") is not None or meta.get("fan_in") is not None:
                if not dag.should_execute(self, run_id, successor, config):
                    dag.skip_and_propagate(self, run_id, successor, config)
                continue

            if not dag.should_execute(self, run_id, successor, config):
                dag.skip_and_propagate(self, run_id, successor, config)
                continue

            # Gate nodes pause for approval instead of creating a job.
            if successor in config.gate_configs:
                gates.enter_gate(self, run_id, successor, config)
            elif successor in config.sub_workflow_refs:
                sub_workflows.submit_sub_workflow(self, run_id, successor, config)
            else:
                dag.create_deferred_job_for_node(self, run_id, successor, config)

        # After evaluating successors, check if the run is now terminal.
        self._try_finalize(run_id)

    # ── Terminal state ─────────────────────────────────────────────

    def _emit_terminal(self, run_id: str, terminal_state: str, error: str | None) -> None:
        finalization.emit_terminal(self, run_id, terminal_state, error)

    def _cleanup_run(self, run_id: str) -> None:
        finalization.cleanup_run(self, run_id)

    def _try_finalize(self, run_id: str) -> None:
        finalization.try_finalize(self, run_id)

    # ── Gate resolution (public API) ───────────────────────────────

    def resolve_gate(
        self,
        run_id: str,
        node_name: str,
        *,
        approved: bool,
        error: str | None = None,
    ) -> None:
        """Approve or reject a gate, resuming the workflow."""
        with self._state_lock:
            timer = self._gate_timers.pop((run_id, node_name), None)
            config = self._run_configs.get(run_id)
        if timer is not None:
            timer.cancel()

        try:
            self._queue._inner.resolve_workflow_gate(run_id, node_name, approved, error)
        except (RuntimeError, ValueError):
            logger.exception("resolve_workflow_gate failed for %s", node_name)
            return

        if config is not None:
            self._evaluate_successors(run_id, node_name, config)
            self._try_finalize(run_id)

    # ── Sub-workflow listener ──────────────────────────────────────

    def _on_child_workflow_terminal(self, _event_type: EventType, payload: dict[str, Any]) -> None:
        event_routing.handle_child_workflow_terminal(self, _event_type, payload)

    # ── Result helpers ─────────────────────────────────────────────

    def _fetch_result(self, job_id: str) -> Any:
        """Fetch and deserialize a job's result, with polling for DB write lag."""
        for _ in range(50):
            py_job = self._queue._inner.get_job(job_id)
            if py_job is not None:
                rb = py_job.result_bytes
                if rb is not None:
                    return self._queue._serializer.loads(rb)
            time.sleep(0.1)
        return None

    def _compute_result_hash(self, job_id: str) -> str | None:
        """Compute SHA-256 of a completed job's result bytes.

        Best-effort: if the result isn't stored yet (event fires before
        DB write), returns ``None``. The hash is only used for incremental
        caching, not correctness.
        """
        py_job = self._queue._inner.get_job(job_id)
        if py_job is not None:
            rb = py_job.result_bytes
            if rb is not None:
                return hashlib.sha256(rb).hexdigest()
        return None
