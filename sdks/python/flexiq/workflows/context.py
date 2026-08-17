"""Workflow context passed to callable conditions."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class WorkflowContext:
    """Runtime context available to callable condition functions.

    Example::

        wf.step("deploy", deploy_task, after="validate",
                condition=lambda ctx: ctx.results["validate"]["score"] > 0.95)
    """

    run_id: str
    results: dict[str, Any] = field(default_factory=dict)
    """Deserialized return values of completed predecessor nodes."""

    statuses: dict[str, str] = field(default_factory=dict)
    """Status strings for all terminal nodes (completed/failed/skipped)."""

    params: dict[str, Any] | None = None
    """Workflow-level parameters (from ``submit_workflow``)."""

    failure_count: int = 0
    """Number of nodes with status ``"failed"``."""

    success_count: int = 0
    """Number of nodes with status ``"completed"``."""
