"""Queue.list_predicates() and Queue.register_predicate() tests."""

from __future__ import annotations

from typing import Any

import pytest

from flexiq.app import Queue
from flexiq.predicates import (
    Predicate,
    PredicateContext,
    PredicateValidationError,
    is_business_hours,
    payload_matches,
    queue_paused,
)


def test_list_predicates_returns_serialized_dicts(queue: Queue) -> None:
    @queue.task(predicate=is_business_hours() & ~queue_paused())
    def gated() -> int:
        return 1

    @queue.task()
    def ungated() -> int:
        return 1

    snap = queue.list_predicates()
    assert gated.name in snap
    assert ungated.name not in snap

    blob = snap[gated.name]
    assert blob is not None
    assert blob["op"] == "and"

    # Round-trip: from_dict produces an equivalent predicate.
    restored = Predicate.from_dict(blob)
    assert restored.to_dict() == blob


def test_predicate_for_returns_none_for_unregistered_task(queue: Queue) -> None:
    @queue.task()
    def t() -> int:
        return 1

    assert queue.predicate_for(t.name) is None


def test_list_predicates_includes_none_for_bare_callable(queue: Queue) -> None:
    @queue.task(predicate=lambda ctx: True)
    def t() -> int:
        return 1

    assert queue.list_predicates()[t.name] is None


def test_register_predicate_decorator(queue: Queue) -> None:
    @queue.register_predicate("min_priority")
    class MinPriority(Predicate):
        def __init__(self, threshold: int = 0) -> None:
            self.threshold = threshold

        def evaluate(self, ctx: PredicateContext) -> bool:
            return ctx.priority >= self.threshold

        def to_dict(self) -> dict[str, Any]:
            return {"op": "min_priority", "threshold": self.threshold}

        @classmethod
        def _from_kwargs(cls, kwargs: dict[str, Any]) -> Predicate:
            return cls(**kwargs)

    # Class is now in the registry; from_dict resolves it.
    p = Predicate.from_dict({"op": "min_priority", "threshold": 5})
    assert isinstance(p, MinPriority)
    assert p.threshold == 5


def test_register_predicate_rejects_non_predicate(queue: Queue) -> None:
    with pytest.raises(PredicateValidationError):

        @queue.register_predicate("bad")
        class Bad:
            pass


def test_register_predicate_rejects_op_mismatch(queue: Queue) -> None:
    with pytest.raises(PredicateValidationError):

        @queue.register_predicate("decorator_name")
        class Mismatched(Predicate):
            OP = "different_name"

            def evaluate(self, ctx: PredicateContext) -> bool:
                return True


def test_payload_matches_registered_serializes(queue: Queue) -> None:
    @queue.task(predicate=payload_matches("kwargs.tenant", "acme"))
    def t(tenant: str = "") -> int:
        return 1

    blob = queue.predicate_for(t.name)
    assert blob is not None
    assert blob["op"] == "payload_matches"
    assert blob["path"] == "kwargs.tenant"
    assert blob["expected"] == "acme"
