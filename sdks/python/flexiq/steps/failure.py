"""Whether a failed attempt should be retried, when a step is what failed.

The core classifies a step failure once (``classify_step_failure``) and the
binding stamps the answer on the exception. Every worker path asks here before
it consults the task's ``retry_on`` / ``dont_retry_on`` filters: those filters
express an opinion about the *task's* exceptions, and they have nothing useful
to say about a divergence or an unreachable step store.
"""

from __future__ import annotations

#: Attribute the native binding stamps on every step exception. Named rather
#: than duck-typed on the class so an exception raised by a future step surface
#: participates without this module having to know about it.
SHOULD_RETRY_ATTR = "flexiq_should_retry"


def step_retry_decision(exc: BaseException) -> bool | None:
    """The core's retry decision for ``exc``, or ``None`` if it is not a step failure."""
    decision = getattr(exc, SHOULD_RETRY_ATTR, None)
    return decision if isinstance(decision, bool) else None


__all__ = ["SHOULD_RETRY_ATTR", "step_retry_decision"]
