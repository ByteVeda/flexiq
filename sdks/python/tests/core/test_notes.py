"""Structured notes attached to jobs.

Exercises both the standalone validator (``flexiq.notes``) and the
end-to-end round trip through ``Queue.enqueue`` /
``Queue.enqueue_many`` → storage → ``JobResult.notes``.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from flexiq import MAX_NOTE_FIELDS, NotesValidationError, Queue
from flexiq.notes import (
    MAX_NOTE_BYTES,
    MAX_NOTE_DEPTH,
    MAX_NOTE_KEY_LENGTH,
    MAX_NOTE_VALUE_LENGTH,
    validate_and_encode_notes,
)

# ── Pure validator -----------------------------------------------------------


def test_none_passes_through_unchanged() -> None:
    assert validate_and_encode_notes(None) is None


def test_empty_dict_encodes_as_object() -> None:
    assert validate_and_encode_notes({}) == "{}"


def test_canonical_encoding_sorts_keys() -> None:
    encoded = validate_and_encode_notes({"b": 1, "a": 2})
    assert encoded == '{"a":2,"b":1}'


def test_non_dict_rejected() -> None:
    with pytest.raises(NotesValidationError, match="notes must be a dict"):
        validate_and_encode_notes(["nope"])  # type: ignore[arg-type]


def test_too_many_fields_rejected() -> None:
    too_many = {f"k{i}": i for i in range(MAX_NOTE_FIELDS + 1)}
    with pytest.raises(NotesValidationError, match=f"more than {MAX_NOTE_FIELDS} fields"):
        validate_and_encode_notes(too_many)


def test_exactly_fifteen_fields_accepted() -> None:
    notes = {f"k{i}": i for i in range(MAX_NOTE_FIELDS)}
    encoded = validate_and_encode_notes(notes)
    assert encoded is not None


def test_non_string_key_rejected() -> None:
    with pytest.raises(NotesValidationError, match="note keys must be strings"):
        validate_and_encode_notes({1: "value"})  # type: ignore[dict-item]


def test_empty_key_rejected() -> None:
    with pytest.raises(NotesValidationError, match="may not be empty"):
        validate_and_encode_notes({"": "value"})


def test_oversized_key_rejected() -> None:
    long_key = "k" * (MAX_NOTE_KEY_LENGTH + 1)
    with pytest.raises(NotesValidationError, match="exceeds limit of 64"):
        validate_and_encode_notes({long_key: "value"})


def test_oversized_string_value_rejected() -> None:
    long_value = "x" * (MAX_NOTE_VALUE_LENGTH + 1)
    with pytest.raises(NotesValidationError, match=f"exceeds limit of {MAX_NOTE_VALUE_LENGTH}"):
        validate_and_encode_notes({"k": long_value})


def test_nested_value_within_depth_allowed() -> None:
    assert MAX_NOTE_DEPTH >= 3
    encoded = validate_and_encode_notes({"k": {"inner": {"deepest": "ok"}}})
    assert encoded is not None
    assert '"deepest":"ok"' in encoded


def test_nesting_beyond_depth_rejected() -> None:
    # Build a structure whose depth exceeds the cap. Top-level key counts as
    # depth=1, so MAX_NOTE_DEPTH + 1 nested dicts trips the check.
    value: object = "leaf"
    for _ in range(MAX_NOTE_DEPTH + 1):
        value = {"nested": value}
    with pytest.raises(NotesValidationError, match="max nesting depth"):
        validate_and_encode_notes({"k": value})


def test_unsupported_value_type_rejected() -> None:
    with pytest.raises(NotesValidationError, match="unsupported type"):
        validate_and_encode_notes({"k": object()})


def test_total_size_cap_enforced() -> None:
    # Pack a single string value just over the byte cap so the per-field
    # limit doesn't trip first.
    chunk = "x" * MAX_NOTE_VALUE_LENGTH
    fields = (MAX_NOTE_BYTES // MAX_NOTE_VALUE_LENGTH) + 1
    if fields > MAX_NOTE_FIELDS:
        pytest.skip("Per-field cap reaches byte cap before field cap")
    huge = {f"k{i}": chunk for i in range(fields)}
    with pytest.raises(NotesValidationError, match="exceeds limit of"):
        validate_and_encode_notes(huge)


def test_primitives_accepted() -> None:
    encoded = validate_and_encode_notes({"s": "x", "i": 1, "f": 1.5, "b": True, "n": None})
    assert encoded == '{"b":true,"f":1.5,"i":1,"n":null,"s":"x"}'


@pytest.mark.parametrize("bad", [float("inf"), float("-inf"), float("nan")])
def test_non_finite_floats_rejected(bad: float) -> None:
    # NaN/Infinity serialize to invalid JSON tokens that break strict readers
    # (the dashboard JSON.parse, Rust serde_json) — reject at the boundary.
    with pytest.raises(NotesValidationError, match="must be a finite number"):
        validate_and_encode_notes({"x": bad})


def test_non_finite_float_rejected_when_nested() -> None:
    with pytest.raises(NotesValidationError, match="must be a finite number"):
        validate_and_encode_notes({"outer": {"inner": [1, float("nan")]}})


# ── End-to-end through Queue.enqueue ----------------------------------------


def test_enqueue_round_trip(queue: Queue) -> None:
    @queue.task()
    def noop() -> str:
        return "ok"

    job = noop.apply_async(notes={"customer_id": "cus_abc", "tier": "gold"})
    assert job.notes == {"customer_id": "cus_abc", "tier": "gold"}


def test_to_dict_emits_notes_as_json_string(queue: Queue) -> None:
    # The dashboard API contract is ``notes: string | null`` (the client
    # JSON.parse-s it). to_dict() must emit the canonical string, not the
    # parsed dict the ``notes`` property returns for Python callers.
    @queue.task()
    def noop() -> str:
        return "ok"

    job = noop.apply_async(notes={"customer_id": "cus_abc", "tier": "gold"})
    raw = job.to_dict()["notes"]
    assert isinstance(raw, str)
    assert json.loads(raw) == {"customer_id": "cus_abc", "tier": "gold"}

    plain = noop.apply_async()
    assert plain.to_dict()["notes"] is None


def test_enqueue_without_notes_returns_none(queue: Queue) -> None:
    @queue.task()
    def noop() -> str:
        return "ok"

    job = noop.apply_async()
    assert job.notes is None


def test_enqueue_rejects_oversized_notes(queue: Queue) -> None:
    @queue.task()
    def noop() -> str:
        return "ok"

    with pytest.raises(NotesValidationError):
        noop.apply_async(notes={f"k{i}": i for i in range(MAX_NOTE_FIELDS + 1)})


def test_enqueue_many_uniform_notes(queue: Queue) -> None:
    @queue.task()
    def noop(x: int) -> int:
        return x

    jobs = queue.enqueue_many(
        task_name=noop._task_name,
        args_list=[(i,) for i in range(3)],
        notes={"batch": "ETL-2026-05-14"},
    )
    for job in jobs:
        assert job.notes == {"batch": "ETL-2026-05-14"}


def test_enqueue_many_per_job_notes_override_uniform(queue: Queue) -> None:
    @queue.task()
    def noop(x: int) -> int:
        return x

    per_job: list[dict[str, Any] | None] = [{"row": str(i)} for i in range(3)]
    jobs = queue.enqueue_many(
        task_name=noop._task_name,
        args_list=[(i,) for i in range(3)],
        notes={"batch": "fallback"},
        notes_list=per_job,
    )
    for i, job in enumerate(jobs):
        assert job.notes == {"row": str(i)}


def test_enqueue_many_rejects_invalid_in_one_row(queue: Queue) -> None:
    @queue.task()
    def noop(x: int) -> int:
        return x

    with pytest.raises(NotesValidationError):
        queue.enqueue_many(
            task_name=noop._task_name,
            args_list=[(0,), (1,)],
            notes_list=[{"ok": True}, ["bad"]],  # type: ignore[list-item]
        )
