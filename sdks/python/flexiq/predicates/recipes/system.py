"""Observational predicates over live queue state.

These recipes **read** state the Rust scheduler maintains. They are not
primary enforcement: hard backpressure belongs in
``@queue.task(max_concurrent=...)``, ``rate_limit=...``, or
``circuit_breaker=...`` — all enforced atomically in the Rust poller.

``queue_paused`` is kept as a *defensive* composable: e.g.
``~queue_paused() & is_business_hours()`` lets a middleware skip a
gauge update when the queue is administratively paused. It does not
replace ``Queue.pause()`` / ``Queue.resume()``, which are the
authoritative pause mechanism.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, ClassVar

from flexiq.predicates.core import Predicate

if TYPE_CHECKING:
    from flexiq.predicates.context import PredicateContext


@dataclass(frozen=True)
class QueuePaused(Predicate):
    """True when the given queue is currently paused.

    Reads ``list_paused_queues()`` from storage. Typically used
    inverted: ``~queue_paused()``.
    """

    OP: ClassVar[str | None] = "queue_paused"

    queue: str | None = None

    def evaluate(self, ctx: PredicateContext) -> bool:
        return ctx.queue_paused(self.queue)


def queue_paused(queue: str | None = None) -> Predicate:
    """True when ``queue`` (defaults to the job's own queue) is paused."""
    return QueuePaused(queue=queue)
