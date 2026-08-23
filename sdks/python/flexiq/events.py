"""Job event system for flexiq."""

from __future__ import annotations

import enum
import logging
from collections import defaultdict
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any

logger = logging.getLogger("flexiq.events")


class EventType(enum.Enum):
    """The cross-SDK event taxonomy, as job and worker lifecycle events.

    Nearly every member is emitted by this runtime; the few reserved
    contract entries other SDKs emit are noted per member, and exist so a
    webhook subscription stays portable across SDKs.
    """

    JOB_ENQUEUED = "job.enqueued"
    JOB_COMPLETED = "job.completed"
    JOB_FAILED = "job.failed"
    JOB_RETRYING = "job.retrying"
    JOB_DEAD = "job.dead"
    JOB_CANCELLED = "job.cancelled"
    # An attempt ended in a durable step sleep; the job is pending until its
    # deadline. Reserved: part of the cross-SDK contract, and emitted once the
    # task context carries a step API.
    JOB_SLEEPING = "job.sleeping"
    WORKER_STARTED = "worker.started"
    WORKER_STOPPED = "worker.stopped"
    WORKER_ONLINE = "worker.online"
    WORKER_OFFLINE = "worker.offline"
    WORKER_UNHEALTHY = "worker.unhealthy"
    QUEUE_PAUSED = "queue.paused"
    QUEUE_RESUMED = "queue.resumed"
    WORKFLOW_SUBMITTED = "workflow.submitted"
    WORKFLOW_COMPLETED = "workflow.completed"
    # on_failure="continue" run reached terminal with mixed success/failure
    # (at least one node completed and at least one node failed).
    WORKFLOW_COMPLETED_WITH_FAILURES = "workflow.completed_with_failures"
    WORKFLOW_FAILED = "workflow.failed"
    WORKFLOW_CANCELLED = "workflow.cancelled"
    WORKFLOW_GATE_REACHED = "workflow.gate_reached"
    # Saga lifecycle (set when @task(compensates=...) is in play).
    WORKFLOW_COMPENSATING = "workflow.compensating"
    WORKFLOW_COMPENSATED = "workflow.compensated"
    WORKFLOW_COMPENSATION_FAILED = "workflow.compensation_failed"
    NODE_COMPENSATING = "workflow.node_compensating"
    NODE_COMPENSATED = "workflow.node_compensated"
    NODE_COMPENSATION_FAILED = "workflow.node_compensation_failed"
    PREDICATE_DEFERRED = "predicate.deferred"
    # A dispatch-time predicate cancelling an already-enqueued job.
    PREDICATE_CANCELLED = "predicate.cancelled"
    PREDICATE_REJECTED = "predicate.rejected"
    # Reserved: an enqueue dropped without raising. Here a ``Cancel`` at
    # enqueue raises ``PredicateRejectedError`` and emits PREDICATE_REJECTED
    # instead, so nothing emits this.
    PREDICATE_SKIPPED = "predicate.skipped"


class EventBus:
    """In-process event bus for job lifecycle events.

    Callbacks are dispatched asynchronously in a thread pool to avoid
    blocking the worker.
    """

    def __init__(self, max_workers: int = 4):
        self._listeners: dict[EventType, list[Callable]] = defaultdict(list)
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="flexiq-events"
        )

    def on(self, event_type: EventType, callback: Callable[..., Any]) -> None:
        """Register a callback for the given event type."""
        self._listeners[event_type].append(callback)

    def emit(self, event_type: EventType, payload: dict[str, Any]) -> None:
        """Emit an event, dispatching to all registered callbacks."""
        for callback in self._listeners.get(event_type, []):
            self._executor.submit(self._safe_call, callback, event_type, payload)

    @staticmethod
    def _safe_call(callback: Callable, event_type: EventType, payload: dict[str, Any]) -> None:
        try:
            callback(event_type, payload)
        except Exception:
            logger.exception("Event callback error for %s", event_type.value)
