"""The second swallow layer: the signal the runner checks after the body returns.

Making the control signals descend from :class:`BaseException` stops a bare
``except Exception``. It cannot stop ``except BaseException`` or a bare
``except:``, and a task that catches a sleep and carries on runs the rest of
itself with no execution claim — every side effect after that point happens
again on wake.

So ``ctx.step`` latches the context before it raises, and the runner refuses to
report what the body did afterwards. Language-independent, and the only defence
that works in a language where ``catch`` catches everything.

What the runner reports depends on *which* signal was swallowed, which is why
the signal itself is latched rather than a flag:

* A swallowed **sleep** already ended the attempt durably — ``sleep_job``
  committed the row, revoked the claim and moved the job to ``Pending`` at its
  deadline, all before the signal was raised. The attempt slept; reporting a
  failure for it reports on a claim it no longer holds, and the scheduler cannot
  tell that failure apart from the woken attempt's own (a sleep leaves
  ``retry_count`` alone, so the wake reuses the same ``(owner, attempt)``). It
  lands on the live attempt's dispatch record and dead-letters a job that is
  sleeping correctly — #890. So the sleep is re-raised and reported as a sleep.
* A swallowed **failure** signal — a divergence, a cap, a refusal — leaves the
  attempt holding its claim, and nothing downstream would question the value the
  body goes on to return. That is where the latch has to bite, and it does.

The first signal wins. A sleep releases the claim, so a step called after one
was swallowed raises in its turn; latching that would lose the fact that the
attempt is already asleep.
"""

from __future__ import annotations

from flexiq._active_context import _ActiveContext
from flexiq.steps.errors import StepControlSignal, StepSleepSignal


def latch(ctx: _ActiveContext, signal: StepControlSignal) -> None:
    """Record the step control signal being raised out of the body."""
    if ctx.step_control_signal is None:
        ctx.step_control_signal = signal


def was_swallowed(ctx: _ActiveContext | None) -> bool:
    """Whether a control signal was raised and the body returned anyway."""
    return ctx is not None and ctx.step_control_signal is not None


def latched_sleep(ctx: _ActiveContext | None) -> StepSleepSignal | None:
    """The sleep this attempt committed, if it committed one.

    Set from the moment ``step.sleep`` raises, so a body that caught the signal
    cannot hide that the attempt is over.
    """
    signal = ctx.step_control_signal if ctx is not None else None
    return signal if isinstance(signal, StepSleepSignal) else None


__all__ = ["latch", "latched_sleep", "was_swallowed"]
