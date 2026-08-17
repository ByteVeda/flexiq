"""JSON serialization and string-parser round-trip tests for the predicate DSL."""

from __future__ import annotations

import pytest

from flexiq.predicates import (
    Predicate,
    PredicateValidationError,
    after,
    before,
    env_var_truthy,
    feature_flag,
    format_predicate,
    in_time_window,
    is_business_hours,
    is_weekend,
    parse,
    payload_matches,
    queue_paused,
)

# Every builtin recipe with sample args. ``format(parse(x)) == x`` is
# the contract these tests defend.
_RECIPES: list[Predicate] = [
    is_business_hours(),
    is_business_hours(start_hour=10, end_hour=18, weekdays_only=False),
    is_weekend(),
    in_time_window("09:00", "17:00"),
    after("2026-05-11T09:00:00+00:00"),
    before("2026-12-31T00:00:00+00:00"),
    queue_paused(),
    queue_paused(queue="bulk"),
    payload_matches("kwargs.tenant", "acme"),
    payload_matches("args.0", 42),
    env_var_truthy("MY_FLAG"),
    feature_flag("billing"),
]


@pytest.mark.parametrize("p", _RECIPES, ids=lambda p: p.OP)
def test_recipe_json_round_trip(p: Predicate) -> None:
    blob = p.to_dict()
    restored = Predicate.from_dict(blob)
    assert restored.to_dict() == blob


@pytest.mark.parametrize("p", _RECIPES, ids=lambda p: p.OP)
def test_recipe_string_round_trip(p: Predicate) -> None:
    s = format_predicate(p)
    restored = parse(s)
    assert format_predicate(restored) == s
    assert restored.to_dict() == p.to_dict()


def test_composition_round_trips_through_json() -> None:
    p = is_business_hours() & ~queue_paused() | payload_matches(
        path="kwargs.tenant", expected="acme"
    )
    snap = p.to_dict()
    assert snap["op"] == "or"
    restored = Predicate.from_dict(snap)
    assert restored.to_dict() == snap


def test_composition_round_trips_through_string() -> None:
    p = is_business_hours() & ~queue_paused() | payload_matches(
        path="kwargs.tenant", expected="acme"
    )
    s = format_predicate(p)
    assert "&" in s and "|" in s and "!" in s
    restored = parse(s)
    assert restored.to_dict() == p.to_dict()


def test_parse_supports_keyword_aliases() -> None:
    # "and" / "or" / "not" tokens should be equivalent to & / | / !
    p1 = parse("is_business_hours() and not queue_paused()")
    p2 = parse("is_business_hours() & !queue_paused()")
    assert p1.to_dict() == p2.to_dict()


def test_parse_supports_parenthesised_groups() -> None:
    p = parse(
        '(is_business_hours() | payload_matches(path="kwargs.tenant", expected="acme")) '
        "& !queue_paused()"
    )
    snap = p.to_dict()
    assert snap["op"] == "and"
    assert snap["args"][0]["op"] == "or"


def test_parse_unknown_op_raises() -> None:
    with pytest.raises(PredicateValidationError):
        parse("not_a_real_op()")


def test_parse_malformed_string_raises() -> None:
    with pytest.raises(PredicateValidationError):
        parse("queue_paused(")


def test_parse_rejects_unknown_kwarg() -> None:
    with pytest.raises(PredicateValidationError):
        parse('queue_paused(unknown_field="x")')


def test_from_dict_unknown_op_raises() -> None:
    with pytest.raises(PredicateValidationError):
        Predicate.from_dict({"op": "no_such_op"})


def test_from_dict_missing_op_raises() -> None:
    with pytest.raises(PredicateValidationError):
        Predicate.from_dict({"no": "op"})


def test_from_dict_non_dict_raises() -> None:
    with pytest.raises(PredicateValidationError):
        Predicate.from_dict("not a dict")  # type: ignore[arg-type]


def test_callable_predicate_cannot_serialize() -> None:
    from flexiq.predicates import coerce_predicate

    p = coerce_predicate(lambda ctx: True)
    assert p is not None
    with pytest.raises(PredicateValidationError):
        p.to_dict()


def test_literal_types_round_trip_through_string() -> None:
    # int, float, bool, str, null, list — all parseable.
    inputs = [
        'payload_matches(path="x", expected=1)',
        'payload_matches(path="x", expected=1.5)',
        'payload_matches(path="x", expected=true)',
        'payload_matches(path="x", expected=null)',
        'payload_matches(path="x", expected="hi")',
        'payload_matches(path="x", expected=[1, 2, 3])',
    ]
    for s in inputs:
        node = parse(s)
        # Round-trip JSON keeps the value as-is.
        restored = Predicate.from_dict(node.to_dict())
        assert restored.to_dict() == node.to_dict()
