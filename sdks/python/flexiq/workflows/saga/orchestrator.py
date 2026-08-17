"""Reverse-order saga orchestrator.

When a workflow run with ``on_failure="fail_fast"`` and a non-empty
:attr:`_RunConfig.compensation_map` enters terminal failure, the tracker
hands control to this module. The orchestrator:

1. Transitions the run state to ``Compensating`` and emits
   ``WORKFLOW_COMPENSATING``.
2. Collects nodes that completed successfully and have a registered
   compensator. ``Pending``/``Ready``/``Failed``/``Skipped`` nodes are
   excluded — only ``Completed`` / ``CacheHit`` are compensable.
3. Groups the compensable nodes into reverse topological levels
   (leaves first, roots last) and dispatches each level as a parallel
   wave.
4. On each compensation completion, marks the node ``Compensated`` /
   ``CompensationFailed`` and advances when the wave is fully drained.
5. Finalizes the run to ``Compensated`` (every wave succeeded) or
   ``CompensationFailed`` (one or more compensations failed —
   subsequent waves are NOT dispatched, since compensation is
   best-effort and downstream undo may depend on upstream undo).

Idempotency: each compensation job is enqueued with
``idempotency_key=f"compensation:{run_id}:{node_name}"``. The first call
creates the job; a duplicate (e.g. after a tracker restart) hits the
existing partial-index dedup in all 3 storage backends and is a no-op.

Compensator contract: the framework calls
``compensator(forward_args, forward_kwargs, forward_result)``. The args
and kwargs come from the deferred-payload cache when the forward node
was deferred, otherwise from the forward job's stored payload looked up
through ``PyQueue.get_job``. ``forward_result`` is deserialized from
the forward job's stored result when present. Compensators that need
extra metadata (run id, node name, forward job id) can call
:func:`flexiq.workflows.saga.current_compensation_context`.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from typing import TYPE_CHECKING, Any

from flexiq.events import EventType
from flexiq.workflows.analysis import topological_levels
from flexiq.workflows.saga.context import CompensationContext
from flexiq.workflows.types import NodeStatus

if TYPE_CHECKING:
    from flexiq.app import Queue
    from flexiq.workflows.builder import _Step
    from flexiq.workflows.tracker.types import _RunConfig

logger = logging.getLogger("flexiq.workflows.saga")

# Cap nested saga propagation. A parent workflow compensating a sub-workflow
# node that itself triggers nested compensation, and so on, must not recurse
# indefinitely. Ten levels is far beyond any reasonable real workflow.
MAX_SAGA_DEPTH = 10


class SagaDepthExceededError(RuntimeError):
    """Raised when nested saga propagation exceeds :data:`MAX_SAGA_DEPTH`.

    Hitting this almost always means a workflow graph has accidental sub-
    workflow recursion. Surface it loudly rather than silently truncating
    compensation.
    """


class SagaOrchestrator:
    """Per-Queue saga state machine."""

    def __init__(self, queue: Queue) -> None:
        self._queue = queue
        self._lock = threading.RLock()
        self._run_waves: dict[str, list[list[str]]] = {}
        self._run_inflight: dict[str, set[str]] = {}
        self._run_any_failed: dict[str, bool] = {}
        # Reverse map: compensation job id → (run_id, node_name) so the
        # tracker can route completion events back into the orchestrator.
        self._comp_job_to_node: dict[str, tuple[str, str]] = {}
        # In-process map: compensation job id → CompensationContext.
        # Populated when a compensator is enqueued; consumed by the task
        # wrapper before the compensator function runs so that
        # ``current_compensation_context()`` returns the forward-execution
        # metadata. Popped on task completion. This is in-memory only —
        # a worker restart between enqueue and execute loses the context,
        # which is consistent with the rest of the saga's in-memory state.
        self._comp_contexts: dict[str, CompensationContext] = {}
        # Saga-depth bookkeeping for nested propagation. Keyed by run_id;
        # populated at ``start_compensation`` time, decremented at finalize.
        # When a parent run propagates compensation into a child sub-workflow
        # the child inherits ``parent_depth + 1`` — raising
        # :class:`SagaDepthExceededError` past :data:`MAX_SAGA_DEPTH`.
        self._run_depth: dict[str, int] = {}
        # Mapping: child_run_id → (parent_run_id, parent_node_name) for the
        # ongoing nested-saga propagations the orchestrator is waiting on.
        # When the child saga finalises, the parent node is marked
        # compensated/failed based on the child's outcome and the parent's
        # wave advances.
        self._pending_parent_after_child: dict[str, tuple[str, str]] = {}

    # ── Public API used by tracker ──────────────────────────────────────

    def is_compensation_job(self, job_id: str) -> bool:
        """Whether ``job_id`` is one we dispatched for compensation."""
        with self._lock:
            return job_id in self._comp_job_to_node

    def take_compensation_context(self, job_id: str) -> CompensationContext | None:
        """Pop and return the :class:`CompensationContext` for ``job_id``.

        Called by the task wrapper before invoking a compensator's body so
        ``current_compensation_context()`` returns the forward metadata for
        the duration of the call. Returns ``None`` for non-compensation
        jobs (or compensation jobs from a different process — the context
        is in-memory only).
        """
        with self._lock:
            return self._comp_contexts.pop(job_id, None)

    def start_compensation(
        self,
        run_id: str,
        run_config: _RunConfig,
        steps: dict[str, _Step],
        depth: int = 0,
    ) -> bool:
        """Begin reverse-order compensation for ``run_id``.

        Returns ``True`` if compensation was started, ``False`` if the
        run is not eligible (no compensable completed nodes, or already
        compensating).

        ``depth`` is the nested-saga propagation depth. Each parent →
        child propagation increments it; callers from the tracker pass
        ``0``. Exceeding :data:`MAX_SAGA_DEPTH` raises
        :class:`SagaDepthExceededError`.
        """
        if depth > MAX_SAGA_DEPTH:
            raise SagaDepthExceededError(
                f"saga propagation depth exceeded {MAX_SAGA_DEPTH} for run {run_id}"
            )

        with self._lock:
            if run_id in self._run_waves:
                return False

            # A run is saga-eligible when it has *any* form of compensation:
            # an explicit ``compensation_map`` entry, or a sub-workflow node
            # whose child run carries its own saga (propagation).
            has_explicit = bool(run_config.compensation_map)
            has_subwf_propagation = self._has_propagatable_subworkflows(run_id, run_config)
            if not (has_explicit or has_subwf_propagation):
                return False

            # Transition to COMPENSATING immediately after the cheap
            # eligibility check. The storage call in _fetch_compensable_nodes
            # can be slow (especially SQLite on Windows under contention), and
            # wait()'s poll loop would see the transient FAILED state as
            # terminal if we delay this transition.
            self._mark_run_compensating(run_id)

            self._queue._emit_event(
                EventType.WORKFLOW_COMPENSATING,
                {"workflow_run_id": run_id},
            )

            completed = self._fetch_compensable_nodes(run_id, run_config)
            if not completed:
                self._finalize_locked(run_id, succeeded=True)
                return True

            waves = _reverse_topo_waves(steps, restrict_to=completed)
            if not waves:
                self._finalize_locked(run_id, succeeded=True)
                return True

            self._run_waves[run_id] = waves
            self._run_inflight[run_id] = set()
            self._run_any_failed[run_id] = False
            self._run_depth[run_id] = depth

        self._dispatch_next_wave(run_id, run_config)
        return True

    def on_compensation_completed(
        self,
        job_id: str,
        succeeded: bool,
        error: str | None,
    ) -> None:
        """Tracker callback for a completed compensation job.

        Updates per-node bookkeeping. When the in-flight set for the run
        empties, advances to the next wave or finalizes the run.
        """
        run_id: str | None = None
        node_name: str | None = None
        wave_complete = False
        with self._lock:
            mapping = self._comp_job_to_node.pop(job_id, None)
            if mapping is None:
                return
            run_id, node_name = mapping

            inflight = self._run_inflight.get(run_id)
            if inflight is None:
                return
            inflight.discard(node_name)
            if not succeeded:
                self._run_any_failed[run_id] = True

            self._record_node_outcome(run_id, node_name, succeeded, error)
            wave_complete = not inflight

        run_config = self._lookup_run_config(run_id)
        if wave_complete:
            self._advance_or_finalize(run_id, run_config)

    # ── Internal ────────────────────────────────────────────────────────

    def _lookup_run_config(self, run_id: str) -> _RunConfig | None:
        tracker = getattr(self._queue, "_workflow_tracker", None)
        if tracker is None:
            return None
        configs: dict[str, _RunConfig] = getattr(tracker, "_run_configs", {})
        return configs.get(run_id)

    def _fetch_compensable_nodes(
        self,
        run_id: str,
        run_config: _RunConfig,
    ) -> set[str]:
        """Return the set of completed nodes that should run during compensation.

        A node is compensable when its current storage status is
        ``"completed"`` or ``"cache_hit"`` AND either:

        - it has an explicit compensator in ``run_config.compensation_map``, or
        - it is a sub-workflow node whose child run has its own saga
          (propagation candidate).
        """
        try:
            node_data = self._queue._inner.get_base_run_node_data(run_id)
        except Exception:
            logger.exception("saga: failed to fetch node data for run %s", run_id)
            return set()

        compensation_map = run_config.compensation_map
        sub_refs = run_config.sub_workflow_refs

        compensable: set[str] = set()
        for entry in node_data:
            try:
                name = entry[0]
                status = entry[1]
            except (IndexError, TypeError):
                continue
            if status not in (NodeStatus.COMPLETED.value, NodeStatus.CACHE_HIT.value):
                continue
            if name in compensation_map:
                compensable.add(name)
                continue
            # Implicit sub-workflow propagation: include only if a child
            # exists *and* has its own saga.
            if name in sub_refs and self._child_has_saga(run_id, name):
                compensable.add(name)
        return compensable

    def _has_propagatable_subworkflows(self, run_id: str, run_config: _RunConfig) -> bool:
        """Are any of this run's sub-workflow children eligible for saga
        propagation? Cheap check used to decide whether to start a saga at
        all when the run has no explicit ``compensation_map`` entries."""
        for node_name in run_config.sub_workflow_refs:
            if self._child_has_saga(run_id, node_name):
                return True
        return False

    def _child_has_saga(self, parent_run_id: str, parent_node_name: str) -> bool:
        """Whether any tracked child run of ``(parent_run_id, parent_node_name)``
        has a non-empty ``compensation_map``."""
        tracker = getattr(self._queue, "_workflow_tracker", None)
        if tracker is None:
            return False
        with getattr(tracker, "_state_lock", threading.RLock()):
            for cid, (prid, pnn) in list(getattr(tracker, "_child_to_parent", {}).items()):
                if prid != parent_run_id or pnn != parent_node_name:
                    continue
                cfg = getattr(tracker, "_run_configs", {}).get(cid)
                if cfg is not None and cfg.compensation_map:
                    return True
        return False

    def _advance_or_finalize(self, run_id: str, run_config: _RunConfig | None) -> None:
        finalized: tuple[bool, str | None] | None = None
        with self._lock:
            waves = self._run_waves.get(run_id)
            any_failed = self._run_any_failed.get(run_id, False)

            if any_failed:
                # First failure → terminate compensation, do not dispatch more.
                if waves:
                    self._run_waves[run_id] = []
                self._finalize_locked(run_id, succeeded=False)
                finalized = (False, f"sub-workflow {run_id} compensation failed")
            elif not waves:
                self._finalize_locked(run_id, succeeded=True)
                finalized = (True, None)

        # If this run was a child saga whose parent is waiting on it, hand
        # the outcome back to the parent so it can complete its node and
        # advance. Run outside the lock — the callback re-enters
        # ``_handle_inline_no_op`` which acquires it.
        if finalized is not None:
            with self._lock:
                pending = run_id in self._pending_parent_after_child
            if pending:
                succeeded, err = finalized
                self._on_child_saga_terminal(run_id, succeeded, err)
            return

        if run_config is not None:
            self._dispatch_next_wave(run_id, run_config)

    def _dispatch_next_wave(self, run_id: str, run_config: _RunConfig) -> None:
        with self._lock:
            waves = self._run_waves.get(run_id)
            if not waves:
                return
            wave = waves.pop(0)
            self._run_inflight[run_id] = set(wave)

        for node_name in wave:
            self._dispatch_single(run_id, node_name, run_config)

    def _dispatch_single(
        self,
        run_id: str,
        node_name: str,
        run_config: _RunConfig,
    ) -> None:
        comp_task_name = run_config.compensation_map.get(node_name)

        # Sub-workflow propagation: if the parent has NO explicit compensator
        # for this node but the node *was* a sub-workflow whose child run has
        # its own saga, propagate compensation into the child. The parent
        # node is held in the inflight set until the child finalises; on the
        # child's terminal event the parent node is marked accordingly and
        # the parent's wave advances.
        if (
            comp_task_name is None
            and node_name in run_config.sub_workflow_refs
            and self._propagate_to_child_saga(run_id, node_name)
        ):
            return

        if comp_task_name is None:
            logger.warning(
                "saga: no compensator registered for %s/%s — skipping",
                run_id,
                node_name,
            )
            self._handle_inline_no_op(run_id, node_name, succeeded=True, error=None)
            return

        forward_args, forward_kwargs, forward_result, forward_job_id = self._load_forward_payload(
            run_id, node_name, run_config
        )

        idempotency_key = f"compensation:{run_id}:{node_name}"
        metadata_blob = json.dumps(
            {
                "workflow_run_id": run_id,
                "workflow_node_name": node_name,
                "forward_job_id": forward_job_id,
                "_kind": "compensation",
            }
        )

        # SQLite serializes writes; the result handler that just failed
        # the forward job may still be flushing its own writes when we
        # land here from the event bus. Retry briefly so a transient
        # ``database is locked`` doesn't bubble out as a saga failure.
        job = None
        last_exc: Exception | None = None
        for attempt in range(5):
            try:
                job = self._queue.enqueue(
                    comp_task_name,
                    args=(forward_args, forward_kwargs, forward_result),
                    kwargs=None,
                    idempotency_key=idempotency_key,
                    metadata=metadata_blob,
                )
                last_exc = None
                break
            except Exception as exc:
                last_exc = exc
                if "database is locked" not in str(exc):
                    break
                time.sleep(0.05 * (attempt + 1))
        if last_exc is not None or job is None:
            logger.exception(
                "saga: enqueue of compensation %s for %s/%s failed",
                comp_task_name,
                run_id,
                node_name,
            )
            self._handle_inline_no_op(
                run_id,
                node_name,
                succeeded=False,
                error=f"failed to enqueue compensator: {last_exc}",
            )
            return

        job_id = getattr(job, "id", None)
        if job_id is None:
            # Batched or no real job id — treat as immediate success.
            self._handle_inline_no_op(run_id, node_name, succeeded=True, error=None)
            return

        now_ms = int(time.time() * 1000)
        self._safe_call(
            "set_workflow_node_compensation_job",
            run_id,
            node_name,
            job_id,
            now_ms,
        )

        comp_ctx = CompensationContext(
            workflow_run_id=run_id,
            workflow_node_name=node_name,
            forward_job_id=forward_job_id,
            forward_args=forward_args,
            forward_kwargs=forward_kwargs,
            forward_result=forward_result,
        )
        with self._lock:
            self._comp_job_to_node[job_id] = (run_id, node_name)
            self._comp_contexts[job_id] = comp_ctx

        self._queue._emit_event(
            EventType.NODE_COMPENSATING,
            {
                "workflow_run_id": run_id,
                "workflow_node_name": node_name,
                "compensation_job_id": job_id,
                "compensation_task": comp_task_name,
            },
        )

    def _propagate_to_child_saga(self, parent_run_id: str, parent_node_name: str) -> bool:
        """Trigger compensation on a sub-workflow child run.

        Returns ``True`` if propagation started (parent node now waits on
        the child's terminal event), ``False`` if there's no eligible child
        to propagate to (the caller should fall back to the no-op path).
        """
        tracker = getattr(self._queue, "_workflow_tracker", None)
        if tracker is None:
            return False

        child_runs: list[tuple[str, _RunConfig]] = []
        # ``_child_to_parent`` maps child_run_id → (parent_run_id, parent_node_name).
        # Reverse-scan to find children of this specific parent node.
        with getattr(tracker, "_state_lock", threading.RLock()):
            for cid, (prid, pnn) in list(getattr(tracker, "_child_to_parent", {}).items()):
                if prid != parent_run_id or pnn != parent_node_name:
                    continue
                cfg = getattr(tracker, "_run_configs", {}).get(cid)
                if cfg is None or not cfg.compensation_map:
                    continue
                child_runs.append((cid, cfg))

        if not child_runs:
            return False

        # Single child per parent sub-workflow node is the common shape. If
        # multiple children exist (parallel sub-workflows under the same
        # node) we propagate to all of them and complete the parent only
        # after every child terminates.
        parent_depth = self._run_depth.get(parent_run_id, 0)
        propagated_any = False
        for child_run_id, child_cfg in child_runs:
            child_steps = getattr(tracker, "_run_steps", {}).get(child_run_id, {})
            try:
                started = self.start_compensation(
                    child_run_id,
                    child_cfg,
                    child_steps,
                    depth=parent_depth + 1,
                )
            except SagaDepthExceededError:
                logger.exception(
                    "saga: depth guard tripped propagating from %s/%s to %s",
                    parent_run_id,
                    parent_node_name,
                    child_run_id,
                )
                return False

            if started:
                with self._lock:
                    self._pending_parent_after_child[child_run_id] = (
                        parent_run_id,
                        parent_node_name,
                    )
                    # If start_compensation finalized immediately (vacuously
                    # compensated — no nodes to undo), the child saga already
                    # completed but _pending_parent_after_child was set after
                    # the fact. Detect and resolve the parent node inline.
                    already_done = (
                        child_run_id not in self._run_waves
                        and child_run_id not in self._run_inflight
                    )
                if already_done:
                    self._on_child_saga_terminal(child_run_id, True, None)
                propagated_any = True

        return propagated_any

    def _on_child_saga_terminal(
        self, child_run_id: str, child_succeeded: bool, child_error: str | None
    ) -> None:
        """Callback fired by :meth:`_finalize_locked` for a child saga whose
        parent run is waiting for it. Marks the parent node compensated
        (success) or failed (failure) and advances the parent's wave."""
        with self._lock:
            mapping = self._pending_parent_after_child.pop(child_run_id, None)
        if mapping is None:
            return
        parent_run_id, parent_node_name = mapping
        self._handle_inline_no_op(
            parent_run_id,
            parent_node_name,
            succeeded=child_succeeded,
            error=child_error,
        )

    def _handle_inline_no_op(
        self,
        run_id: str,
        node_name: str,
        *,
        succeeded: bool,
        error: str | None,
    ) -> None:
        """Treat a node as immediately completed without a real comp job.

        Used when no compensator is registered, or the enqueue failed
        synchronously. Updates state and lets the wave advance.
        """
        wave_complete = False
        with self._lock:
            inflight = self._run_inflight.get(run_id)
            if inflight is None:
                return
            inflight.discard(node_name)
            if not succeeded:
                self._run_any_failed[run_id] = True

            self._record_node_outcome(run_id, node_name, succeeded, error)
            wave_complete = not inflight

        run_config = self._lookup_run_config(run_id)
        if wave_complete:
            self._advance_or_finalize(run_id, run_config)

    def _record_node_outcome(
        self,
        run_id: str,
        node_name: str,
        succeeded: bool,
        error: str | None,
    ) -> None:
        now_ms = int(time.time() * 1000)
        if succeeded:
            self._safe_call("set_workflow_node_compensated", run_id, node_name, now_ms)
            event_type = EventType.NODE_COMPENSATED
        else:
            self._safe_call(
                "set_workflow_node_compensation_failed",
                run_id,
                node_name,
                error or "compensation failed",
                now_ms,
            )
            event_type = EventType.NODE_COMPENSATION_FAILED

        self._queue._emit_event(
            event_type,
            {
                "workflow_run_id": run_id,
                "workflow_node_name": node_name,
                "error": error,
            },
        )

    def _mark_run_compensating(self, run_id: str) -> None:
        """Move the run to ``Compensating`` in storage.

        Retries briefly on transient SQLite "database is locked" errors
        so that contention from a concurrent result-handler write does
        not silently leave the run in ``Failed``.
        """
        for attempt in range(5):
            try:
                self._queue._inner.set_workflow_run_compensating(run_id)
                return
            except Exception as exc:
                if "database is locked" not in str(exc) or attempt == 4:
                    logger.exception("saga: failed to mark run %s compensating", run_id)
                    return
                time.sleep(0.05 * (attempt + 1))

    def _finalize_locked(self, run_id: str, *, succeeded: bool) -> None:
        """Cleanly finalize a saga. Must be called with ``self._lock``."""
        self._run_waves.pop(run_id, None)
        self._run_inflight.pop(run_id, None)
        self._run_depth.pop(run_id, None)
        any_failed = self._run_any_failed.pop(run_id, False)
        # Drop any straggling job→node mappings and their stashed contexts.
        stale_jids = [jid for jid, (rid, _nn) in self._comp_job_to_node.items() if rid == run_id]
        for jid in stale_jids:
            self._comp_job_to_node.pop(jid, None)
            self._comp_contexts.pop(jid, None)

        now_ms = int(time.time() * 1000)
        try:
            if succeeded:
                self._queue._inner.set_workflow_run_compensated(run_id, now_ms)
            else:
                self._queue._inner.set_workflow_run_compensation_failed(
                    run_id, now_ms, "one or more compensations failed"
                )
        except Exception:
            logger.exception(
                "saga: failed to finalize run %s as %s",
                run_id,
                "compensated" if succeeded else "compensation_failed",
            )

        event_type = (
            EventType.WORKFLOW_COMPENSATED if succeeded else EventType.WORKFLOW_COMPENSATION_FAILED
        )
        self._queue._emit_event(
            event_type,
            {"workflow_run_id": run_id, "any_failed": any_failed},
        )

        # ``_advance_or_finalize`` handles sub-workflow saga propagation
        # (calling ``_on_child_saga_terminal`` *after* this method returns
        # and the orchestrator lock has been released).

    def _safe_call(self, method_name: str, *args: Any) -> None:
        """Invoke a storage method on the Rust binding, logging on failure."""
        try:
            method = getattr(self._queue._inner, method_name)
        except AttributeError:
            logger.warning("saga: binding %s is not available", method_name)
            return
        try:
            method(*args)
        except Exception:
            logger.exception("saga: %s(%s) failed", method_name, args)

    def _load_forward_payload(
        self,
        run_id: str,
        node_name: str,
        run_config: _RunConfig,
    ) -> tuple[tuple, dict[str, Any], Any, str | None]:
        """Reconstruct the forward task's (args, kwargs, result, job_id).

        Sources, in priority order:

        - **Deferred-payload cache** (``run_config.deferred_payloads``) — the
          tracker serialized the forward args/kwargs at submit time. This is
          the authoritative source for fan-out children whose payloads are
          constructed at runtime; it covers conditional, fan-out, fan-in,
          gate, and sub-workflow nodes.
        - **Storage round-trip** — for static nodes (and as a fall-back for
          missing/corrupt deferred entries) we look up the forward job via
          its ID from the workflow-run status, then deserialize the job's
          stored payload + result.

        Missing or undeserialisable pieces degrade to ``()`` / ``{}`` /
        ``None`` rather than aborting compensation. The compensator's
        function still runs; it just won't see the forward state for that
        node.
        """
        step_meta = run_config.step_metadata.get(node_name, {})
        task_name = step_meta.get("task_name")

        forward_args: tuple = ()
        forward_kwargs: dict[str, Any] = {}
        forward_result: Any = None
        forward_job_id: str | None = None

        # Deferred-cache path (covers fan-out children, conditional, gate, etc.).
        raw_deferred = run_config.deferred_payloads.get(node_name)
        deferred_decoded = False
        if raw_deferred is not None and task_name is not None:
            try:
                d_args, d_kwargs = self._queue._deserialize_payload(task_name, raw_deferred)
                forward_args = tuple(d_args)
                forward_kwargs = dict(d_kwargs)
                deferred_decoded = True
            except Exception:
                logger.exception(
                    "saga: failed to deserialize deferred payload for %s/%s — "
                    "will fall back to storage lookup",
                    run_id,
                    node_name,
                )

        # Find the forward job id from the run's node statuses. Needed for
        # static-node payload/result recovery and for CompensationContext.
        try:
            status = self._queue._inner.get_workflow_run_status(run_id)
            statuses = status.node_statuses()
        except Exception:
            logger.exception("saga: failed to fetch run status for %s", run_id)
            statuses = {}
        info = statuses.get(node_name) or {}
        forward_job_id = info.get("job_id") if isinstance(info, dict) else None

        if forward_job_id is None or task_name is None:
            return forward_args, forward_kwargs, forward_result, forward_job_id

        # Fetch the forward job for payload / result recovery.
        try:
            forward_job = self._queue._inner.get_job(forward_job_id)
        except Exception:
            logger.exception("saga: get_job(%s) failed for compensator setup", forward_job_id)
            return forward_args, forward_kwargs, forward_result, forward_job_id

        if forward_job is None:
            return forward_args, forward_kwargs, forward_result, forward_job_id

        # Static-node payload: only deserialize if the deferred cache didn't
        # already give us the args/kwargs.
        if not deferred_decoded:
            try:
                s_args, s_kwargs = self._queue._deserialize_payload(
                    task_name, forward_job.payload_bytes
                )
                forward_args = tuple(s_args)
                forward_kwargs = dict(s_kwargs)
            except Exception:
                logger.exception(
                    "saga: failed to deserialize stored payload for %s/%s "
                    "(job %s) — compensator will receive empty args",
                    run_id,
                    node_name,
                    forward_job_id,
                )

        # Forward result: always deserialize when present.
        result_bytes = forward_job.result_bytes
        if result_bytes:
            try:
                forward_result = self._queue._serializer.loads(result_bytes)
            except Exception:
                logger.exception(
                    "saga: failed to deserialize forward result for %s/%s — "
                    "compensator will see forward_result=None",
                    run_id,
                    node_name,
                )

        return forward_args, forward_kwargs, forward_result, forward_job_id


def _reverse_topo_waves(steps: dict[str, _Step], restrict_to: set[str]) -> list[list[str]]:
    """Compute reverse-topological waves of the steps in ``restrict_to``.

    Each forward-topological level becomes one compensation wave,
    iterated in reverse. Nodes not in ``restrict_to`` are dropped.
    Empty waves are omitted.
    """
    forward_levels = topological_levels(steps)
    waves: list[list[str]] = []
    for level in reversed(forward_levels):
        wave = [n for n in level if n in restrict_to]
        if wave:
            waves.append(wave)
    return waves


__all__ = ["SagaOrchestrator"]
