"""Context made available to compensation tasks at execution time.

A compensation task is invoked with three positional args:
``(forward_args: tuple, forward_kwargs: dict, forward_result: Any)``.
For everything else — the forward job's id, the workflow run id, the node
name — the framework also stashes a :class:`CompensationContext` in a
context variable so the compensator can introspect its environment without
needing extra parameters.
"""

from __future__ import annotations

import contextvars
from dataclasses import dataclass
from typing import Any

_compensation_ctx: contextvars.ContextVar[CompensationContext | None] = contextvars.ContextVar(
    "flexiq_compensation_context", default=None
)


@dataclass(frozen=True)
class CompensationContext:
    """Metadata about the forward execution a compensation task is undoing.

    Attributes:
        workflow_run_id: ID of the workflow run that triggered compensation.
        workflow_node_name: Name of the node being compensated.
        forward_job_id: ID of the original (forward-execution) job.
        forward_args: Positional args the forward task was called with.
        forward_kwargs: Keyword args the forward task was called with.
        forward_result: The forward task's return value (``None`` if it
            returned ``None`` or if the result was no longer in storage).
    """

    workflow_run_id: str
    workflow_node_name: str
    forward_job_id: str | None
    forward_args: tuple
    forward_kwargs: dict[str, Any]
    forward_result: Any


def _set_compensation_context(ctx: CompensationContext | None) -> Any:
    """Push a new compensation context, returning a token for resetting."""
    return _compensation_ctx.set(ctx)


def _reset_compensation_context(token: Any) -> None:
    _compensation_ctx.reset(token)


def current_compensation_context() -> CompensationContext | None:
    """Return the currently-running compensation's context, or ``None``.

    Returns ``None`` when called outside of a compensation task body — use
    :func:`flexiq.context.current_job` for normal job context.
    """
    return _compensation_ctx.get()
