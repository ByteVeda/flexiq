"""Core predicate algebra and evaluation tests."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from flexiq.predicates import (
    Cancel,
    Defer,
    Predicate,
    PredicateContext,
    PredicateMetrics,
    coerce_predicate,
    evaluate_predicate,
)


@dataclass(frozen=True)
class _Const(Predicate):
    value: Any

    def evaluate(self, ctx: PredicateContext) -> Any:
        return self.value


def _ctx() -> PredicateContext:
    return PredicateContext(task_name="t", queue="default")


# -- Outcome sentinels -------------------------------------------------------


def test_defer_rejects_negative_seconds() -> None:
    with pytest.raises(ValueError):
        Defer(seconds=-1)


def test_defer_and_cancel_are_frozen() -> None:
    d = Defer(seconds=10)
    c = Cancel(reason="x")
    with pytest.raises(AttributeError):
        d.seconds = 1  # type: ignore[misc]
    with pytest.raises(AttributeError):
        c.reason = "y"  # type: ignore[misc]


# -- Composition operators ---------------------------------------------------


def test_and_short_circuits_on_false() -> None:
    calls: list[str] = []

    @dataclass(frozen=True)
    class _Track(Predicate):
        label: str
        value: bool

        def evaluate(self, ctx: PredicateContext) -> bool:
            calls.append(self.label)
            return self.value

    combined = _Track(label="left", value=False) & _Track(label="right", value=True)
    assert evaluate_predicate(combined, _ctx()) is False
    assert calls == ["left"]


def test_or_short_circuits_on_true() -> None:
    calls: list[str] = []

    @dataclass(frozen=True)
    class _Track(Predicate):
        label: str
        value: bool

        def evaluate(self, ctx: PredicateContext) -> bool:
            calls.append(self.label)
            return self.value

    combined = _Track(label="left", value=True) | _Track(label="right", value=False)
    assert evaluate_predicate(combined, _ctx()) is True
    assert calls == ["left"]


def test_not_inverts_boolean() -> None:
    assert evaluate_predicate(~_Const(True), _ctx()) is False
    assert evaluate_predicate(~_Const(False), _ctx()) is True


def test_not_passes_defer_and_cancel_through() -> None:
    d = Defer(seconds=30)
    c = Cancel(reason="bad")
    assert evaluate_predicate(~_Const(d), _ctx()) == d
    assert evaluate_predicate(~_Const(c), _ctx()) == c


def test_and_propagates_defer_from_left() -> None:
    d = Defer(seconds=30)
    combined = _Const(d) & _Const(True)
    assert evaluate_predicate(combined, _ctx()) == d


def test_and_passes_through_when_left_true() -> None:
    d = Defer(seconds=30)
    combined = _Const(True) & _Const(d)
    assert evaluate_predicate(combined, _ctx()) == d


def test_or_prefers_cancel_when_both_deny() -> None:
    d = Defer(seconds=5)
    c = Cancel(reason="permanent")
    combined = _Const(d) | _Const(c)
    assert evaluate_predicate(combined, _ctx()) == c


def test_or_prefers_defer_over_false() -> None:
    d = Defer(seconds=5)
    combined = _Const(False) | _Const(d)
    assert evaluate_predicate(combined, _ctx()) == d


# -- repr stability ---------------------------------------------------------


def test_repr_uses_operator_syntax() -> None:
    expr = _Const(True) & ~_Const(False) | _Const(False)
    text = repr(expr)
    assert "&" in text
    assert "|" in text
    assert "!" in text  # DSL uses ! for negation


# -- Fail-closed -----------------------------------------------------------


def test_evaluate_returns_false_on_exception() -> None:
    @dataclass(frozen=True)
    class _Boom(Predicate):
        def evaluate(self, ctx: PredicateContext) -> bool:
            raise RuntimeError("intentional")

    metrics = PredicateMetrics()
    assert evaluate_predicate(_Boom(), _ctx(), metrics=metrics) is False
    snap = metrics.snapshot()
    assert snap["errors"] == 1
    assert snap["denied"] == 1


def test_metrics_count_outcomes() -> None:
    metrics = PredicateMetrics()
    evaluate_predicate(_Const(True), _ctx(), metrics=metrics)
    evaluate_predicate(_Const(False), _ctx(), metrics=metrics)
    evaluate_predicate(_Const(Defer(seconds=1)), _ctx(), metrics=metrics)
    evaluate_predicate(_Const(Cancel(reason="x")), _ctx(), metrics=metrics)
    snap = metrics.snapshot()
    assert snap == {
        "allowed": 1,
        "denied": 1,
        "deferred": 1,
        "cancelled": 1,
        "errors": 0,
    }


def test_metrics_reset() -> None:
    metrics = PredicateMetrics()
    evaluate_predicate(_Const(True), _ctx(), metrics=metrics)
    metrics.reset()
    assert metrics.snapshot()["allowed"] == 0


# -- coerce_predicate -------------------------------------------------------


def test_coerce_passes_through_predicate() -> None:
    p = _Const(True)
    assert coerce_predicate(p) is p


def test_coerce_wraps_callable_with_context() -> None:
    def fn(ctx: PredicateContext) -> bool:
        return ctx.task_name == "t"

    wrapped = coerce_predicate(fn)
    assert wrapped is not None
    assert evaluate_predicate(wrapped, _ctx()) is True


def test_coerce_wraps_str_callable_for_backcompat() -> None:
    def fn(name: str) -> bool:
        return name == "t"

    wrapped = coerce_predicate(fn, str_callable=True)
    assert wrapped is not None
    assert evaluate_predicate(wrapped, _ctx()) is True


def test_coerce_none_returns_none() -> None:
    assert coerce_predicate(None) is None


def test_coerce_rejects_non_callable() -> None:
    with pytest.raises(TypeError):
        coerce_predicate(42)  # type: ignore[arg-type]


# -- Async predicate is awaited ---------------------------------------------


def test_async_predicate_is_run_to_completion() -> None:
    class _Async(Predicate):
        async def evaluate(self, ctx: PredicateContext) -> bool:
            return True

    assert evaluate_predicate(_Async(), _ctx()) is True


def test_async_predicate_can_return_defer() -> None:
    d = Defer(seconds=42)

    class _Async(Predicate):
        async def evaluate(self, ctx: PredicateContext) -> Defer:
            return d

    assert evaluate_predicate(_Async(), _ctx()) == d
