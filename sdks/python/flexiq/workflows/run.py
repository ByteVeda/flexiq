"""Python-side workflow run handle.

Wraps a Rust ``PyWorkflowHandle`` with high-level status/wait/cancel
operations. ``wait()`` uses a threading.Event registered with the workflow
tracker for O(1) resolution when the run reaches a terminal state, with a
polling safety net.
"""

from __future__ import annotations

import threading
import time
from typing import TYPE_CHECKING

from flexiq.enums import coerce_enum

from .types import DiagramFormat, NodeSnapshot, NodeStatus, WorkflowState, WorkflowStatus
from .visualization import nodes_and_edges_from_dag_bytes, render_dot, render_mermaid

if TYPE_CHECKING:
    from flexiq.app import Queue


class WorkflowTimeoutError(TimeoutError):
    """Raised when ``WorkflowRun.wait`` times out before the run finishes."""


class WorkflowRun:
    """Handle for a submitted workflow run."""

    def __init__(self, queue: Queue, run_id: str, name: str):
        self._queue = queue
        self.id = run_id
        self.name = name

    def status(self) -> WorkflowStatus:
        """Fetch the current state snapshot of this workflow run."""
        raw = self._queue._inner.get_workflow_run_status(self.id)
        return _raw_to_status(raw)

    def node_status(self, node_name: str) -> NodeStatus:
        """Shortcut for ``status().nodes[node_name].status``."""
        snapshot = self.status()
        node = snapshot.nodes.get(node_name)
        if node is None:
            raise KeyError(f"node '{node_name}' not found in workflow run {self.id}")
        return node.status

    def wait(self, timeout: float | None = None, poll_interval: float = 0.1) -> WorkflowStatus:
        """Block until this workflow run reaches a terminal state.

        Args:
            timeout: Max seconds to wait. ``None`` = wait forever.
            poll_interval: How often to re-check status as a safety net against
                missed events.

        Returns:
            The terminal :class:`WorkflowStatus`.

        Raises:
            WorkflowTimeoutError: If the workflow did not finish within ``timeout``.
        """
        tracker = getattr(self._queue, "_workflow_tracker", None)
        event: threading.Event | None = None
        if tracker is not None:
            event = tracker.register_wait(self.id)

        try:
            deadline = None if timeout is None else time.monotonic() + timeout
            while True:
                snapshot = self.status()
                if snapshot.state.is_terminal():
                    # FAILED and COMPLETED_WITH_FAILURES can transition to
                    # COMPENSATING when a saga is about to start.  Between
                    # mark_workflow_node_result (sets FAILED in Rust) and
                    # start_compensation (sets COMPENSATING from Python)
                    # there is a window where the poll sees the transient
                    # terminal state.  When the tracker owns an event for
                    # this run, defer to the event — it is set only after
                    # the saga decision is made — instead of returning the
                    # possibly-transient state.
                    if (
                        snapshot.state
                        in (WorkflowState.FAILED, WorkflowState.COMPLETED_WITH_FAILURES)
                        and event is not None
                        and not event.is_set()
                    ):
                        grace = poll_interval
                        if deadline is not None:
                            grace = min(grace, max(0.0, deadline - time.monotonic()))
                        if grace > 0:
                            event.wait(timeout=grace)
                            continue
                    return snapshot

                remaining: float
                if deadline is None:
                    remaining = poll_interval
                else:
                    remaining = max(0.0, deadline - time.monotonic())
                    if remaining == 0.0:
                        raise WorkflowTimeoutError(
                            f"workflow run {self.id} did not complete within {timeout}s"
                        )
                    remaining = min(poll_interval, remaining)

                if event is not None:
                    if event.wait(timeout=remaining):
                        return self.status()
                else:
                    time.sleep(remaining)
        finally:
            if tracker is not None and event is not None:
                tracker.unregister_wait(self.id, event)

    def cancel(self) -> None:
        """Cancel any pending steps and mark the run as cancelled."""
        self._queue._inner.cancel_workflow_run(self.id)

    def visualize(self, fmt: DiagramFormat | str = DiagramFormat.MERMAID) -> str:
        """Render the workflow DAG with live node statuses.

        Args:
            fmt: Output format — a :class:`DiagramFormat` or its string. An
                unknown one raises rather than silently falling back to Mermaid.
        """
        dag_bytes = self._queue._inner.get_workflow_definition_dag(self.id)
        nodes, edges = nodes_and_edges_from_dag_bytes(dag_bytes)

        snapshot = self.status()
        statuses: dict[str, str] = {}
        for name, node in snapshot.nodes.items():
            statuses[name] = node.status.value

        if coerce_enum(DiagramFormat, fmt, param="fmt") is DiagramFormat.DOT:
            return render_dot(nodes, edges, statuses)
        return render_mermaid(nodes, edges, statuses)

    def __repr__(self) -> str:
        return f"WorkflowRun(id={self.id!r}, name={self.name!r})"


def _raw_to_status(raw: object) -> WorkflowStatus:
    """Convert a ``PyWorkflowRunStatus`` into the high-level dataclass."""
    node_dict = raw.node_statuses()  # type: ignore[attr-defined]
    nodes: dict[str, NodeSnapshot] = {}
    for node_name, entry in node_dict.items():
        status_str = entry.get("status", "pending")
        try:
            node_status = NodeStatus(status_str)
        except ValueError:
            node_status = NodeStatus.PENDING
        nodes[node_name] = NodeSnapshot(
            name=node_name,
            status=node_status,
            job_id=entry.get("job_id"),
            error=entry.get("error"),
        )

    state_str: str = raw.state  # type: ignore[attr-defined]
    try:
        state = WorkflowState(state_str)
    except ValueError:
        state = WorkflowState.PENDING

    return WorkflowStatus(
        run_id=raw.run_id,  # type: ignore[attr-defined]
        state=state,
        started_at=raw.started_at,  # type: ignore[attr-defined]
        completed_at=raw.completed_at,  # type: ignore[attr-defined]
        error=raw.error,  # type: ignore[attr-defined]
        nodes=nodes,
    )
