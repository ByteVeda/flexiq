"""Validation + encoding for the structured ``notes`` field on jobs.

Notes are a small, bounded, user-readable annotation dict attached to a
job at enqueue time. Storage represents them as a JSON-encoded string in
the ``jobs.notes`` column across SQLite, Postgres, and Redis.

Validation is performed in Python (the single boundary that all enqueue
paths funnel through) so the Rust/PyO3 layer can stay schema-agnostic:
it accepts an already-encoded string and stores it verbatim.

Contract:

* At most :data:`MAX_NOTE_FIELDS` top-level keys.
* Each key is a non-empty ``str`` no longer than
  :data:`MAX_NOTE_KEY_LENGTH` characters.
* Nested values may be JSON-serializable primitives, lists, or dicts up
  to :data:`MAX_NOTE_DEPTH` levels of nesting.
* String leaves are capped at :data:`MAX_NOTE_VALUE_LENGTH` characters.
* The final encoded JSON document fits in :data:`MAX_NOTE_BYTES` bytes.

Limits are chosen to be tight enough that the dashboard can render the
full note set as a fixed-size key/value table without truncation.
"""

from __future__ import annotations

import json
import math
from typing import Any

from flexiq.exceptions import NotesValidationError

__all__ = [
    "MAX_NOTE_BYTES",
    "MAX_NOTE_DEPTH",
    "MAX_NOTE_FIELDS",
    "MAX_NOTE_KEY_LENGTH",
    "MAX_NOTE_VALUE_LENGTH",
    "validate_and_encode_notes",
]

MAX_NOTE_FIELDS: int = 15
MAX_NOTE_KEY_LENGTH: int = 64
MAX_NOTE_VALUE_LENGTH: int = 500
MAX_NOTE_DEPTH: int = 3
MAX_NOTE_BYTES: int = 4096

# Internal: JSON primitives accepted as leaf values.
_PRIMITIVE_TYPES = (str, int, float, bool, type(None))


def validate_and_encode_notes(notes: dict[str, Any] | None) -> str | None:
    """Validate ``notes`` and return its canonical JSON encoding.

    Returns ``None`` if ``notes`` is ``None`` (the absence of notes is
    distinct from an empty dict, which is also accepted and encoded as
    ``"{}"``).

    Raises:
        NotesValidationError: if any rule in the module docstring is
            violated. The exception message names the offending key or
            constraint so callers can surface it to end users.
    """
    if notes is None:
        return None
    if not isinstance(notes, dict):
        raise NotesValidationError(f"notes must be a dict, got {type(notes).__name__}")
    if len(notes) > MAX_NOTE_FIELDS:
        raise NotesValidationError(
            f"notes may not have more than {MAX_NOTE_FIELDS} fields, got {len(notes)}"
        )

    for key, value in notes.items():
        _validate_key(key)
        _validate_value(key, value, depth=1)

    # allow_nan=False is a belt-and-suspenders guard: _validate_value already
    # rejects non-finite floats, but this ensures the encoder can never emit
    # invalid JSON tokens even if a new code path slips one through.
    encoded = json.dumps(
        notes, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False
    )
    # For the common all-ASCII payload, byte length == char length, so skip the
    # throwaway UTF-8 encode just to measure size.
    encoded_bytes = len(encoded) if encoded.isascii() else len(encoded.encode("utf-8"))
    if encoded_bytes > MAX_NOTE_BYTES:
        raise NotesValidationError(
            f"encoded notes are {encoded_bytes} bytes, exceeds limit of {MAX_NOTE_BYTES} bytes"
        )
    return encoded


def _validate_key(key: object) -> None:
    if not isinstance(key, str):
        raise NotesValidationError(f"note keys must be strings, got {type(key).__name__}")
    if not key:
        raise NotesValidationError("note keys may not be empty strings")
    if len(key) > MAX_NOTE_KEY_LENGTH:
        raise NotesValidationError(
            f"note key {key!r} is {len(key)} characters, exceeds limit of {MAX_NOTE_KEY_LENGTH}"
        )


def _validate_value(path: str, value: Any, *, depth: int) -> None:
    if depth > MAX_NOTE_DEPTH:
        raise NotesValidationError(
            f"note value at {path!r} exceeds max nesting depth of {MAX_NOTE_DEPTH}"
        )
    if isinstance(value, str):
        if len(value) > MAX_NOTE_VALUE_LENGTH:
            raise NotesValidationError(
                f"note value at {path!r} is {len(value)} characters, exceeds "
                f"limit of {MAX_NOTE_VALUE_LENGTH}"
            )
        return
    if isinstance(value, float) and not math.isfinite(value):
        # NaN/Infinity serialize to bare ``NaN``/``Infinity`` tokens that are
        # invalid JSON — any strict reader (the dashboard's JS JSON.parse,
        # Rust serde_json) rejects them, breaking the whole API response.
        raise NotesValidationError(
            f"note value at {path!r} must be a finite number, got {value!r}"
        )
    if isinstance(value, (bool, int, float)) or value is None:
        return
    if isinstance(value, list):
        for i, item in enumerate(value):
            _validate_value(f"{path}[{i}]", item, depth=depth + 1)
        return
    if isinstance(value, dict):
        for k, v in value.items():
            _validate_key(k)
            _validate_value(f"{path}.{k}", v, depth=depth + 1)
        return
    raise NotesValidationError(
        f"note value at {path!r} has unsupported type {type(value).__name__}; "
        "must be JSON-serializable (str, int, float, bool, None, list, dict)"
    )
