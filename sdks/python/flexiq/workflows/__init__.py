"""DAG-based workflow support for flexiq.

This package is only functional when the native extension was built with the
``workflows`` feature. If the feature is not compiled in, importing this
package raises a :class:`RuntimeError` the first time any public API is used.
"""

from __future__ import annotations

from .builder import GateConfig, Workflow, WorkflowProxy
from .context import WorkflowContext
from .run import WorkflowRun
from .types import (
    DiagramFormat,
    FanStrategy,
    GateAction,
    NodeSnapshot,
    NodeStatus,
    WorkflowCondition,
    WorkflowState,
    WorkflowStatus,
)

__all__ = [
    "DiagramFormat",
    "FanStrategy",
    "GateAction",
    "GateConfig",
    "NodeSnapshot",
    "NodeStatus",
    "Workflow",
    "WorkflowCondition",
    "WorkflowContext",
    "WorkflowProxy",
    "WorkflowRun",
    "WorkflowState",
    "WorkflowStatus",
]
