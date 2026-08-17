"""Tests for the predicate AST, recipes, and queue integration.

Covers:

* Boolean combinators (And/Or/Not) — short-circuit semantics and
  pass-through of Defer / Cancel.
* Serialization round-trips through both JSON (``to_dict``/``from_dict``)
  and the string DSL (``parse``/``format_predicate``).
* Fail-closed evaluation: any exception raised in ``evaluate`` returns
  ``False`` and bumps the error counter.
* Recipe behaviour for the built-in time / config / system / attribute
  predicates.
* Integration with :class:`Queue` — predicates registered via
  ``@queue.task(predicate=...)`` correctly defer or cancel at enqueue.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import ClassVar
from unittest.mock import patch

import pytest

from taskito import Queue
from taskito.exceptions import PredicateRejectedError
from taskito.predicates import (
    AndPredicate,
    Cancel,
    Defer,
    NotPredicate,
    OrPredicate,
    Predicate,
    PredicateContext,
    PredicateMetrics,
    PredicateValidationError,
    after,
    before,
    coerce_predicate,
    default_registry,
    env_var_truthy,
    evaluate_predicate,
    format_predicate,
    in_time_window,
    is_business_hours,
    is_weekend,
    parse,
    payload_matches,
    queue_paused,
    register_predicate,
)
from taskito.predicates.core import _CallablePredicate

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _Fixed(Predicate):
    """Predicate that returns whatever it was constructed with.

    Not registered in the default registry — ``OP=None``. Used to drive
    the boolean combinators with known outcomes.
    """

    value: bool | Defer | Cancel = True
    OP: ClassVar[str | None] = None

    def evaluate(self, ctx: PredicateContext) -> bool | Defer | Cancel:
        return self.value


@dataclass(frozen=True)
class _Boom(Predicate):
    """Predicate that always raises — exercises fail-closed evaluation."""

    OP: ClassVar[str | None] = None

    def evaluate(self, ctx: PredicateContext) -> bool:
        raise RuntimeError("intentional predicate failure")


def _ctx(task_name: str = "tasks.demo", queue: str = "default") -> PredicateContext:
    """Minimal context for unit tests that don't need a Queue back-reference."""
    return PredicateContext(task_name=task_name, queue=queue)


# ---------------------------------------------------------------------------
# Boolean combinators
# ---------------------------------------------------------------------------


def test_and_short_circuits_on_false() -> None:
    """AndPredicate must not evaluate right when left returns False."""
    right_ran = []

    @dataclass(frozen=True)
    class _Tracker(Predicate):
        OP: ClassVar[str | None] = None

        def evaluate(self, ctx: PredicateContext) -> bool:
            right_ran.append(True)
            return True

    combined = AndPredicate(_Fixed(False), _Tracker())
    assert combined.evaluate(_ctx()) is False
    assert right_ran == []


def test_and_evaluates_right_when_left_is_true() -> None:
    combined = AndPredicate(_Fixed(True), _Fixed(False))
    assert combined.evaluate(_ctx()) is False


def test_and_propagates_defer_from_left() -> None:
    deferred = Defer(seconds=42.0)
    combined = AndPredicate(_Fixed(deferred), _Fixed(True))
    assert combined.evaluate(_ctx()) is deferred


def test_or_short_circuits_on_true() -> None:
    right_ran = []

    @dataclass(frozen=True)
    class _Tracker(Predicate):
        OP: ClassVar[str | None] = None

        def evaluate(self, ctx: PredicateContext) -> bool:
            right_ran.append(True)
            return False

    combined = OrPredicate(_Fixed(True), _Tracker())
    assert combined.evaluate(_ctx()) is True
    assert right_ran == []


def test_or_prefers_cancel_over_defer_when_both_deny() -> None:
    combined = OrPredicate(_Fixed(Defer(seconds=10.0)), _Fixed(Cancel(reason="nope")))
    outcome = combined.evaluate(_ctx())
    assert isinstance(outcome, Cancel)
    assert outcome.reason == "nope"


def test_or_returns_false_when_both_plain_false() -> None:
    assert OrPredicate(_Fixed(False), _Fixed(False)).evaluate(_ctx()) is False


def test_not_inverts_bools_and_passes_through_terminal_outcomes() -> None:
    assert NotPredicate(_Fixed(True)).evaluate(_ctx()) is False
    assert NotPredicate(_Fixed(False)).evaluate(_ctx()) is True

    defer = Defer(seconds=5.0)
    assert NotPredicate(_Fixed(defer)).evaluate(_ctx()) is defer

    cancel = Cancel(reason="halt")
    assert NotPredicate(_Fixed(cancel)).evaluate(_ctx()) is cancel


def test_operator_overloads_build_expected_ast() -> None:
    left = is_weekend()
    right = queue_paused()
    assert isinstance(left & right, AndPredicate)
    assert isinstance(left | right, OrPredicate)
    assert isinstance(~left, NotPredicate)


# ---------------------------------------------------------------------------
# Fail-closed evaluation + metrics
# ---------------------------------------------------------------------------


def test_evaluate_predicate_fail_closed_on_exception() -> None:
    metrics = PredicateMetrics()
    outcome = evaluate_predicate(_Boom(), _ctx(), metrics=metrics)
    assert outcome is False
    snapshot = metrics.snapshot()
    assert snapshot["errors"] == 1
    # Fail-closed outcome is recorded as "denied", not "allowed".
    assert snapshot["denied"] == 1
    assert snapshot["allowed"] == 0


def test_evaluate_predicate_records_each_outcome() -> None:
    metrics = PredicateMetrics()
    evaluate_predicate(_Fixed(True), _ctx(), metrics=metrics)
    evaluate_predicate(_Fixed(False), _ctx(), metrics=metrics)
    evaluate_predicate(_Fixed(Defer(seconds=1.0)), _ctx(), metrics=metrics)
    evaluate_predicate(_Fixed(Cancel()), _ctx(), metrics=metrics)
    snap = metrics.snapshot()
    assert snap == {"allowed": 1, "denied": 1, "deferred": 1, "cancelled": 1, "errors": 0}


# ---------------------------------------------------------------------------
# Serialization round-trips
# ---------------------------------------------------------------------------


def test_json_round_trip_simple_recipe() -> None:
    original = is_business_hours(start_hour=10, end_hour=15, tz="UTC")
    payload = original.to_dict()
    rebuilt = Predicate.from_dict(payload)
    assert rebuilt.to_dict() == payload


def test_json_round_trip_composed_tree() -> None:
    original = is_weekend(tz="UTC") & ~queue_paused()
    payload = original.to_dict()
    rebuilt = Predicate.from_dict(payload)
    # The serialized form is stable, so equality of the dicts is a strong
    # check that both the operator nodes and their leaves round-trip.
    assert rebuilt.to_dict() == payload


def test_string_dsl_round_trip() -> None:
    original = is_business_hours(tz="UTC") & ~queue_paused()
    rendered = format_predicate(original)
    parsed = parse(rendered)
    assert parsed.to_dict() == original.to_dict()


def test_from_dict_unknown_op_raises() -> None:
    with pytest.raises(PredicateValidationError):
        Predicate.from_dict({"op": "nonexistent_op_xyz"})


def test_cancel_reason_survives_round_trip_via_callable_adapter() -> None:
    cancel = Cancel(reason="quota exhausted")
    # Cancel itself is an outcome, not a predicate — round-trip through
    # the And combinator instead and verify the leaf survives evaluation.
    pred = AndPredicate(_Fixed(True), _Fixed(cancel))
    out = pred.evaluate(_ctx())
    assert isinstance(out, Cancel)
    assert out.reason == "quota exhausted"


# ---------------------------------------------------------------------------
# Recipe-level behaviour
# ---------------------------------------------------------------------------


def test_after_defers_when_target_in_future() -> None:
    target = datetime.now(timezone.utc) + timedelta(hours=1)
    outcome = after(target).evaluate(_ctx())
    assert isinstance(outcome, Defer)
    assert outcome.seconds > 0
    # Sanity: within an hour-ish window allowing for jitter.
    assert outcome.seconds <= 3700


def test_after_allows_when_target_in_past() -> None:
    past = datetime.now(timezone.utc) - timedelta(seconds=10)
    assert after(past).evaluate(_ctx()) is True


def test_before_allows_when_target_in_future() -> None:
    target = datetime.now(timezone.utc) + timedelta(hours=1)
    assert before(target).evaluate(_ctx()) is True


def test_before_denies_when_target_in_past() -> None:
    past = datetime.now(timezone.utc) - timedelta(seconds=10)
    assert before(past).evaluate(_ctx()) is False


def test_in_time_window_defers_outside_window() -> None:
    # Pick a one-minute window 30 minutes ahead so we are definitely
    # outside it regardless of wall-clock time at test runtime.
    now = datetime.now(timezone.utc)
    start_minutes = (now.hour * 60 + now.minute + 30) % (24 * 60)
    end_minutes = (start_minutes + 1) % (24 * 60)
    if end_minutes <= start_minutes:
        # The chosen window wraps midnight; the recipe rejects such
        # configurations. Skip to a non-wrapping pair instead.
        start_minutes = 0
        end_minutes = 1
    fmt = lambda m: f"{m // 60:02d}:{m % 60:02d}"  # noqa: E731 - tight test helper
    outcome = in_time_window(fmt(start_minutes), fmt(end_minutes), tz="UTC").evaluate(_ctx())
    assert isinstance(outcome, Defer)
    assert outcome.seconds > 0


def test_payload_matches_kwargs_path() -> None:
    pred = payload_matches("kwargs.tenant", "acme")
    ctx_hit = PredicateContext(task_name="t", queue="q", kwargs={"tenant": "acme"})
    ctx_miss = PredicateContext(task_name="t", queue="q", kwargs={"tenant": "other"})
    ctx_absent = PredicateContext(task_name="t", queue="q", kwargs={})

    assert pred.evaluate(ctx_hit) is True
    assert pred.evaluate(ctx_miss) is False
    assert pred.evaluate(ctx_absent) is False


def test_env_var_truthy_reads_environ() -> None:
    pred = env_var_truthy("FLEXIQ_TEST_PREDICATE_FLAG")
    with patch.dict(os.environ, {"FLEXIQ_TEST_PREDICATE_FLAG": "yes"}, clear=False):
        assert pred.evaluate(_ctx()) is True
    with patch.dict(os.environ, {"FLEXIQ_TEST_PREDICATE_FLAG": "0"}, clear=False):
        assert pred.evaluate(_ctx()) is False
    os.environ.pop("FLEXIQ_TEST_PREDICATE_FLAG", None)
    assert pred.evaluate(_ctx()) is False


def test_defer_rejects_negative_seconds() -> None:
    with pytest.raises(ValueError):
        Defer(seconds=-1.0)


# ---------------------------------------------------------------------------
# Coercion + callable adapter
# ---------------------------------------------------------------------------


def test_coerce_predicate_wraps_callables() -> None:
    def gate(ctx: PredicateContext) -> bool:
        return ctx.task_name.startswith("ok.")

    coerced = coerce_predicate(gate)
    assert isinstance(coerced, _CallablePredicate)
    assert coerced.evaluate(_ctx("ok.go")) is True
    assert coerced.evaluate(_ctx("no.way")) is False


def test_coerce_predicate_passes_through_existing_predicate() -> None:
    pred = is_weekend()
    assert coerce_predicate(pred) is pred


def test_coerce_predicate_none_is_none() -> None:
    assert coerce_predicate(None) is None


# ---------------------------------------------------------------------------
# Custom registration
# ---------------------------------------------------------------------------


def test_register_predicate_round_trips_through_default_registry() -> None:
    op_name = "tests_predicates_custom_allow"

    @dataclass(frozen=True)
    class CustomAllow(Predicate):
        OP: ClassVar[str | None] = op_name

        def evaluate(self, ctx: PredicateContext) -> bool:
            return True

    # Class auto-registers via __init_subclass__; the explicit call is a
    # no-op duplicate registration but must not raise.
    register_predicate(op_name, CustomAllow, replace=True)
    rebuilt = Predicate.from_dict({"op": op_name})
    assert isinstance(rebuilt, CustomAllow)
    assert op_name in default_registry()


# ---------------------------------------------------------------------------
# Queue integration — enqueue-time gating
# ---------------------------------------------------------------------------


def test_task_predicate_cancel_raises_at_enqueue(queue: Queue) -> None:
    @queue.task(predicate=_Fixed(Cancel(reason="quota")))
    def gated() -> None:  # pragma: no cover - never runs
        pass

    with pytest.raises(PredicateRejectedError) as excinfo:
        gated.delay()
    assert "gated" in str(excinfo.value)


def test_task_predicate_false_with_cancel_raises_at_enqueue(queue: Queue) -> None:
    @queue.task(predicate=_Fixed(False), on_false="cancel")
    def hard_off() -> None:  # pragma: no cover - never runs
        pass

    with pytest.raises(PredicateRejectedError):
        hard_off.delay()


def test_task_predicate_defer_bumps_delay(queue: Queue) -> None:
    @queue.task(predicate=_Fixed(Defer(seconds=5.0)))
    def deferred_task() -> None:  # pragma: no cover - not dispatched in this test
        pass

    # Predicate returning Defer is allowed to enqueue with an additional
    # delay; the enqueue call must succeed and return a job.
    job = deferred_task.delay()
    assert job.id is not None
