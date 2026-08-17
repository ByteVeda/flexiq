"""Type definitions for workflow state and node snapshots."""

from __future__ import annotations

import enum
from dataclasses import dataclass, field


class GateAction(str, enum.Enum):
    """What an approval gate does when its timeout elapses."""

    APPROVE = "approve"
    """Treat the gate as approved — the node completes and successors run."""

    REJECT = "reject"
    """Treat the gate as rejected — the node fails."""


class FanStrategy(str, enum.Enum):
    """How a step fans out over, or back in from, its predecessor's result."""

    EACH = "each"
    """Fan-out: one child job per item of the predecessor's result list."""

    ALL = "all"
    """Fan-in: collect every fan-out child's result into one list."""


class DiagramFormat(str, enum.Enum):
    """Render target for :meth:`Workflow.visualize`."""

    MERMAID = "mermaid"
    DOT = "dot"


class WorkflowCondition(str, enum.Enum):
    """When a step runs, based on its predecessors' outcomes."""

    ON_SUCCESS = "on_success"
    """Every predecessor completed (the default)."""

    ON_FAILURE = "on_failure"
    """At least one predecessor failed — an error-handler branch."""

    ALWAYS = "always"
    """Regardless of predecessor outcomes, once they settle."""


class WorkflowState(str, enum.Enum):
    """Terminal and intermediate states of a workflow run."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    # on_failure="continue" run reached terminal with at least one failed node
    # and at least one completed node.
    COMPLETED_WITH_FAILURES = "completed_with_failures"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PAUSED = "paused"
    # Saga states — used when @task(compensates=...) is in play.
    COMPENSATING = "compensating"
    COMPENSATED = "compensated"
    COMPENSATION_FAILED = "compensation_failed"

    def is_terminal(self) -> bool:
        return self in (
            WorkflowState.COMPLETED,
            WorkflowState.COMPLETED_WITH_FAILURES,
            WorkflowState.FAILED,
            WorkflowState.CANCELLED,
            WorkflowState.COMPENSATED,
            WorkflowState.COMPENSATION_FAILED,
        )


class NodeStatus(str, enum.Enum):
    """Status of a single workflow node (step)."""

    PENDING = "pending"
    READY = "ready"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    WAITING_APPROVAL = "waiting_approval"
    CACHE_HIT = "cache_hit"
    # Saga states.
    COMPENSATING = "compensating"
    COMPENSATED = "compensated"
    COMPENSATION_FAILED = "compensation_failed"

    def is_terminal(self) -> bool:
        return self in (
            NodeStatus.COMPLETED,
            NodeStatus.FAILED,
            NodeStatus.SKIPPED,
            NodeStatus.CACHE_HIT,
            NodeStatus.COMPENSATED,
            NodeStatus.COMPENSATION_FAILED,
        )


@dataclass
class NodeSnapshot:
    """Snapshot of a single workflow node's execution state."""

    name: str
    status: NodeStatus
    job_id: str | None = None
    error: str | None = None


@dataclass
class WorkflowStatus:
    """Snapshot of a workflow run's overall state plus per-node details."""

    run_id: str
    state: WorkflowState
    started_at: int | None = None
    completed_at: int | None = None
    error: str | None = None
    nodes: dict[str, NodeSnapshot] = field(default_factory=dict)
