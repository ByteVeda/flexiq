"""The second swallow layer: a flag the runner checks after the body returns.

Making the control signals descend from :class:`BaseException` stops a bare
``except Exception``. It cannot stop ``except BaseException`` or a bare
``except:``, and a task that catches a sleep and carries on runs the rest of
itself with no execution claim — every side effect after that point happens
again on wake.

So ``ctx.step`` latches the context before it raises, and the runner fails the
attempt if the body returns normally with the latch set. Language-independent,
and the only defence that works in a language where ``catch`` catches
everything.
"""

from __future__ import annotations

from flexiq._active_context import _ActiveContext


def latch(ctx: _ActiveContext) -> None:
    """Record that a step control signal is being raised out of the body."""
    ctx.step_control_raised = True


def was_swallowed(ctx: _ActiveContext | None) -> bool:
    """Whether a control signal was raised and the body returned anyway."""
    return ctx is not None and ctx.step_control_raised


__all__ = ["latch", "was_swallowed"]
