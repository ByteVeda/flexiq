"""Sentinel return types for predicate evaluation."""

from __future__ import annotations

import enum
from dataclasses import dataclass


class PredicateAction(str, enum.Enum):
    """What a task does when its predicate returns a plain ``False``.

    The richer :class:`Defer` / :class:`Cancel` sentinels say it per-evaluation;
    this is the per-task default for the bare boolean.
    """

    DEFER = "defer"
    """Re-enqueue the job after ``default_defer_seconds``."""

    CANCEL = "cancel"
    """Skip the job terminally."""


@dataclass(frozen=True, slots=True)
class Defer:
    """Sentinel returned by a predicate to defer the job.

    At enqueue time, ``seconds`` is added to the caller's delay before the
    job is saved. At worker-dispatch time, the current job is cancelled and
    a fresh job is re-enqueued with ``seconds`` of delay (preserving task
    name, queue, arguments, and dispatch metadata).
    """

    seconds: float

    def __post_init__(self) -> None:
        if self.seconds < 0:
            raise ValueError(f"Defer seconds must be >= 0, got {self.seconds}")


@dataclass(frozen=True, slots=True)
class Cancel:
    """Sentinel returned by a predicate to terminally skip the job.

    At enqueue time, no row is created. At worker-dispatch time, the job is
    cancelled and ``PREDICATE_CANCELLED`` is emitted. ``reason`` is included
    in the emitted event payload.
    """

    reason: str = ""


PredicateOutcome = bool | Defer | Cancel
"""Anything a :meth:`Predicate.evaluate` is allowed to return."""
