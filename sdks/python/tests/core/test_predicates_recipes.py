"""Built-in predicate recipe tests (v2)."""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from unittest import mock

import pytest

from flexiq.predicates import (
    Defer,
    Predicate,
    PredicateContext,
    PredicateValidationError,
    after,
    before,
    env_var_truthy,
    feature_flag,
    in_time_window,
    in_timezone,
    is_business_hours,
    is_weekend,
    payload_matches,
    queue_paused,
    register_feature_flag_provider,
)


def _ctx(**overrides: object) -> PredicateContext:
    defaults: dict[str, object] = {"task_name": "t", "queue": "default"}
    defaults.update(overrides)
    return PredicateContext(**defaults)  # type: ignore[arg-type]


# -- Time recipes -----------------------------------------------------------


def test_after_allows_when_past_target() -> None:
    target = datetime.now(timezone.utc) - timedelta(hours=1)
    assert after(target).evaluate(_ctx()) is True


def test_after_defers_when_before_target() -> None:
    target = datetime.now(timezone.utc) + timedelta(hours=1)
    outcome = after(target).evaluate(_ctx())
    assert isinstance(outcome, Defer)
    assert 3000 < outcome.seconds < 3700


def test_before_allows_when_strictly_earlier() -> None:
    target = datetime.now(timezone.utc) + timedelta(hours=1)
    assert before(target).evaluate(_ctx()) is True


def test_before_denies_when_past_target() -> None:
    target = datetime.now(timezone.utc) - timedelta(hours=1)
    assert before(target).evaluate(_ctx()) is False


def test_in_time_window_parses_hh_mm() -> None:
    assert in_time_window("09:00", "17:00") is not None


def test_in_time_window_rejects_inverted_range() -> None:
    with pytest.raises(PredicateValidationError):
        in_time_window("17:00", "09:00")


def test_in_time_window_rejects_bad_format() -> None:
    with pytest.raises(PredicateValidationError):
        in_time_window("nine", "five")


def test_in_time_window_returns_defer_when_outside() -> None:
    pred = in_time_window("09:00", "17:00")
    fake_now = datetime(2026, 5, 11, 8, 0, tzinfo=timezone.utc)
    ctx = _ctx()
    with mock.patch.object(ctx, "now", return_value=fake_now):
        outcome = pred.evaluate(ctx)
    assert isinstance(outcome, Defer)
    assert outcome.seconds == 3600.0


def test_is_business_hours_returns_defer_on_weekend() -> None:
    pred = is_business_hours(tz=None)
    sat = datetime(2026, 5, 9, 12, 0, tzinfo=timezone.utc)
    ctx = _ctx()
    with mock.patch.object(ctx, "now", return_value=sat):
        outcome = pred.evaluate(ctx)
    assert isinstance(outcome, Defer)
    assert outcome.seconds > 0


def test_is_business_hours_allows_during_window() -> None:
    pred = is_business_hours(tz=None)
    weekday_noon = datetime(2026, 5, 11, 12, 0, tzinfo=timezone.utc)
    ctx = _ctx()
    with mock.patch.object(ctx, "now", return_value=weekday_noon):
        assert pred.evaluate(ctx) is True


def test_is_business_hours_rejects_invalid_window() -> None:
    with pytest.raises(PredicateValidationError):
        is_business_hours(start_hour=20, end_hour=10)


def test_is_weekend_uses_utc_by_default() -> None:
    pred = is_weekend()
    sat = datetime(2026, 5, 9, 12, 0, tzinfo=timezone.utc)
    weekday = datetime(2026, 5, 11, 12, 0, tzinfo=timezone.utc)
    ctx = _ctx()
    with mock.patch.object(ctx, "now", return_value=sat):
        assert pred.evaluate(ctx) is True
    with mock.patch.object(ctx, "now", return_value=weekday):
        assert pred.evaluate(ctx) is False


def test_in_timezone_validates_at_construction() -> None:
    in_timezone("UTC")
    with pytest.raises(PredicateValidationError):
        in_timezone("Mars/Olympus_Mons")


# -- Payload recipe ---------------------------------------------------------


def test_payload_matches_kwargs() -> None:
    pred = payload_matches("kwargs.tenant", "acme")
    assert pred.evaluate(_ctx(kwargs={"tenant": "acme"})) is True
    assert pred.evaluate(_ctx(kwargs={"tenant": "other"})) is False
    assert pred.evaluate(_ctx(kwargs={})) is False


def test_payload_matches_args_by_index() -> None:
    pred = payload_matches("args.0", "x")
    assert pred.evaluate(_ctx(args=("x", "y"))) is True
    assert pred.evaluate(_ctx(args=("y",))) is False


def test_payload_matches_nested_dict() -> None:
    pred = payload_matches("kwargs.config.region", "us-east")
    assert pred.evaluate(_ctx(kwargs={"config": {"region": "us-east"}})) is True
    assert pred.evaluate(_ctx(kwargs={"config": {"region": "eu-west"}})) is False


def test_payload_matches_rejects_empty_path() -> None:
    with pytest.raises(PredicateValidationError):
        payload_matches("", "x")


# -- Defensive system recipe ------------------------------------------------


def test_queue_paused_uses_context_helper() -> None:
    pred = queue_paused()
    ctx = _ctx()
    with mock.patch.object(ctx, "queue_paused", return_value=True):
        assert pred.evaluate(ctx) is True
    with mock.patch.object(ctx, "queue_paused", return_value=False):
        assert pred.evaluate(ctx) is False


# -- Config recipes ---------------------------------------------------------


def test_env_var_truthy_reads_env() -> None:
    pred = env_var_truthy("MY_FLAG")
    with mock.patch.dict(os.environ, {"MY_FLAG": "1"}):
        assert pred.evaluate(_ctx()) is True
    with mock.patch.dict(os.environ, {"MY_FLAG": "no"}):
        assert pred.evaluate(_ctx()) is False
    with mock.patch.dict(os.environ, {}, clear=True):
        assert pred.evaluate(_ctx()) is False


def test_env_var_truthy_rejects_empty_name() -> None:
    with pytest.raises(PredicateValidationError):
        env_var_truthy("")


def test_feature_flag_default_provider_reads_ff_prefix() -> None:
    pred = feature_flag("billing")
    with mock.patch.dict(os.environ, {"FF_BILLING": "true"}):
        assert pred.evaluate(_ctx()) is True
    with mock.patch.dict(os.environ, {}, clear=True):
        assert pred.evaluate(_ctx()) is False


def test_feature_flag_named_provider_roundtrips() -> None:
    class _Stub:
        def is_enabled(self, name: str, ctx: PredicateContext) -> bool:
            return name == "yes"

    register_feature_flag_provider("stub", _Stub())
    pred = feature_flag("yes", provider="stub")
    assert pred.evaluate(_ctx()) is True

    # JSON round-trip preserves the provider name.
    snap = pred.to_dict()
    assert snap == {"op": "feature_flag", "flag": "yes", "provider": "stub"}
    restored = Predicate.from_dict(snap)
    assert restored.evaluate(_ctx()) is True


def test_feature_flag_unknown_provider_raises_on_resolve() -> None:
    with pytest.raises(PredicateValidationError):
        feature_flag("x", provider="never-registered")


def test_feature_flag_rejects_empty_name() -> None:
    with pytest.raises(PredicateValidationError):
        feature_flag("")
