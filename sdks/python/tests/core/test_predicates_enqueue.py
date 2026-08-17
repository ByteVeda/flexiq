"""Enqueue-time predicate gating tests."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from flexiq.app import Queue
from flexiq.exceptions import PredicateRejectedError
from flexiq.predicates import (
    Cancel,
    Defer,
    Predicate,
    PredicateContext,
)

PollUntil = Any  # the conftest fixture's runtime type


@dataclass(frozen=True)
class _Const(Predicate):
    value: Any

    def evaluate(self, ctx: PredicateContext) -> Any:
        return self.value


def test_true_predicate_lets_job_through(queue: Queue) -> None:
    @queue.task(predicate=_Const(True))
    def t() -> str:
        return "ran"

    job = t.delay()
    assert job.id


def test_false_predicate_defers_by_default(queue: Queue) -> None:
    @queue.task(predicate=_Const(False), default_defer_seconds=120.0)
    def t() -> str:
        return "ran"

    job = t.delay()
    # Job is enqueued with a future scheduled_at — we cannot directly read
    # the delay from the JobResult, but we can confirm the job exists
    # and is pending (not cancelled).
    job.refresh()
    assert job.status == "pending"


def test_false_predicate_cancel_action_raises(queue: Queue) -> None:
    @queue.task(predicate=_Const(False), on_false="cancel")
    def t() -> str:
        return "ran"

    with pytest.raises(PredicateRejectedError) as excinfo:
        t.delay()
    assert excinfo.value.task_name.endswith("t")


def test_cancel_outcome_always_raises(queue: Queue) -> None:
    @queue.task(predicate=_Const(Cancel(reason="bad tenant")))
    def t() -> str:
        return "ran"

    with pytest.raises(PredicateRejectedError) as excinfo:
        t.delay()
    assert excinfo.value.reason == "bad tenant"


def test_defer_outcome_adds_to_caller_delay(queue: Queue) -> None:
    @queue.task(predicate=_Const(Defer(seconds=300.0)))
    def t() -> str:
        return "ran"

    job = t.delay()
    job.refresh()
    assert job.status == "pending"


def test_on_false_rejects_invalid_value(queue: Queue) -> None:
    with pytest.raises(ValueError):

        @queue.task(predicate=_Const(False), on_false="invalid")
        def t() -> None: ...


def test_default_defer_seconds_must_be_non_negative(queue: Queue) -> None:
    with pytest.raises(ValueError):

        @queue.task(predicate=_Const(False), default_defer_seconds=-1.0)
        def t() -> None: ...


def test_predicate_extras_reach_context(queue: Queue) -> None:
    seen: dict[str, object] = {}

    class _Probe(Predicate):
        def evaluate(self, ctx: PredicateContext) -> bool:
            seen.update(ctx.extras)
            return True

    @queue.task(predicate=_Probe(), predicate_extras={"tenant": "acme"})
    def t() -> str:
        return "ran"

    t.delay()
    assert seen == {"tenant": "acme"}


def test_plain_callable_is_accepted_as_predicate(queue: Queue) -> None:
    @queue.task(predicate=lambda ctx: ctx.task_name.endswith("t"))
    def t() -> str:
        return "ran"

    job = t.delay()
    assert job.id


def test_enqueue_many_cancels_entire_batch(queue: Queue) -> None:
    @queue.task(predicate=_Const(Cancel(reason="nope")))
    def t(x: int) -> int:
        return x

    with pytest.raises(PredicateRejectedError):
        queue.enqueue_many(t.name, [(1,), (2,), (3,)])


def test_enqueue_many_with_true_predicate(queue: Queue) -> None:
    @queue.task(predicate=_Const(True))
    def t(x: int) -> int:
        return x

    results = queue.enqueue_many(t.name, [(1,), (2,), (3,)])
    assert len(results) == 3


def test_metrics_record_outcomes(queue: Queue) -> None:
    @queue.task(predicate=_Const(True))
    def allowed() -> int:
        return 1

    @queue.task(predicate=_Const(Defer(seconds=10.0)))
    def deferred() -> int:
        return 1

    allowed.delay()
    allowed.delay()
    deferred.delay()
    snap = queue._predicate_metrics.snapshot()
    assert snap["allowed"] == 2
    assert snap["deferred"] == 1


def test_defer_emits_predicate_deferred_event(queue: Queue, poll_until: PollUntil) -> None:
    from flexiq.events import EventType

    received: list[dict] = []
    queue._event_bus.on(EventType.PREDICATE_DEFERRED, lambda _e, p: received.append(p))

    @queue.task(predicate=_Const(Defer(seconds=99.0)))
    def t() -> str:
        return "ran"

    t.delay()
    # Event bus dispatches in a thread pool — give it a moment.
    poll_until(lambda: len(received) >= 1, timeout=2)
    assert received[0]["defer_seconds"] == 99.0
    assert received[0]["phase"] == "enqueue"


def test_cancel_emits_predicate_rejected_event(queue: Queue, poll_until: PollUntil) -> None:
    from flexiq.events import EventType

    received: list[dict] = []
    queue._event_bus.on(EventType.PREDICATE_REJECTED, lambda _e, p: received.append(p))

    @queue.task(predicate=_Const(Cancel(reason="no good")))
    def t() -> str:
        return "ran"

    with pytest.raises(PredicateRejectedError):
        t.delay()

    poll_until(lambda: len(received) >= 1, timeout=2)
    assert received[0]["reason"] == "no good"


def test_predicate_stats_counts_outcomes(queue: Queue) -> None:
    """queue.predicate_stats() exposes the outcome counters publicly."""

    @queue.task(predicate=_Const(True))
    def allowed_task() -> str:
        return "ok"

    @queue.task(predicate=_Const(False), name="denied_task")
    def denied_task() -> str:
        return "ok"

    @queue.task(predicate=_Const(Defer(seconds=60.0)), name="deferred_task")
    def deferred_task() -> str:
        return "ok"

    @queue.task(predicate=_Const(Cancel(reason="nope")), name="cancelled_task")
    def cancelled_task() -> str:
        return "ok"

    allowed_task.delay()
    denied_task.delay()  # bare False counts as denied (then defers by default)
    deferred_task.delay()
    with pytest.raises(PredicateRejectedError):
        cancelled_task.delay()

    stats = queue.predicate_stats()
    assert stats["allowed"] == 1
    assert stats["denied"] == 1
    assert stats["deferred"] == 1
    assert stats["cancelled"] == 1
    assert stats["errors"] == 0
