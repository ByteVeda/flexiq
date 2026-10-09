"""The metadata merge rule for W3C trace context, pinned to the core's vectors."""

from __future__ import annotations

import json

import pytest

from flexiq.trace_context import merge_trace_carrier, trace_carrier

PARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
CARRIER = {"traceparent": PARENT, "tracestate": "a=1"}


def test_absent_or_empty_metadata_becomes_the_carrier() -> None:
    expected = f'{{"traceparent":"{PARENT}","tracestate":"a=1"}}'
    assert merge_trace_carrier(None, CARRIER) == expected
    assert merge_trace_carrier(" ", CARRIER) == expected


def test_an_object_gains_the_keys_with_its_own_bytes_kept() -> None:
    carrier = {"traceparent": PARENT}
    assert (
        merge_trace_carrier('  { "user" : 1 }', carrier)
        == f'  {{"traceparent":"{PARENT}", "user" : 1 }}'
    )
    assert merge_trace_carrier("{ }", carrier) == f'{{"traceparent":"{PARENT}" }}'


@pytest.mark.parametrize("own", ['{"traceparent":"mine"}', '{"tracestate":"mine=1"}'])
def test_a_callers_own_context_wins_whole(own: str) -> None:
    assert merge_trace_carrier(own, CARRIER) == own


@pytest.mark.parametrize(
    "other", ["[1,2]", '"text"', "42", "not json", "{broken", '{"a":NaN}', '{"a":1} junk']
)
def test_metadata_that_is_not_an_object_is_left_alone(other: str) -> None:
    assert merge_trace_carrier(other, CARRIER) == other


def test_a_key_the_caller_already_set_is_never_overwritten() -> None:
    merged = merge_trace_carrier('{"baggage":"mine"}', {**CARRIER, "baggage": "theirs"})
    assert merged is not None
    assert json.loads(merged) == {**CARRIER, "baggage": "mine"}


def test_an_empty_carrier_changes_nothing() -> None:
    assert merge_trace_carrier(None, {}) is None
    assert merge_trace_carrier("plain", {}) == "plain"


def test_values_are_json_escaped() -> None:
    merged = merge_trace_carrier(None, {"tracestate": 'k="v"'})
    assert merged is not None
    assert json.loads(merged) == {"tracestate": 'k="v"'}


def test_the_carrier_is_the_objects_string_members() -> None:
    assert trace_carrier(f'{{"traceparent":"{PARENT}","n":1}}') == {"traceparent": PARENT}
    for metadata in (None, "", "[1]", "not json"):
        assert trace_carrier(metadata) == {}
