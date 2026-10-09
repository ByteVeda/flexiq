"""W3C trace context carried in a job's metadata (cross-SDK contract).

The carrier is the metadata JSON object itself: ``traceparent`` and
``tracestate`` at its top level, so any OpenTelemetry propagator reads and
writes it as a plain text map. Every SDK merges by the same rule:

- absent or empty metadata becomes an object holding just the carrier;
- an object gains the carrier keys unless it already names ``traceparent`` or
  ``tracestate`` — the caller's own context wins whole — and never loses or
  overwrites a key of its own;
- anything else (an array, a string, text that is not JSON) is left untouched.

An object's own bytes are kept: the keys are spliced in after its opening
brace instead of the document being re-serialised.
"""

from __future__ import annotations

import json
from collections.abc import Mapping

TRACEPARENT = "traceparent"
TRACESTATE = "tracestate"


def _reject_constant(name: str) -> object:
    # Strict JSON, as the other SDKs parse it: ``NaN`` and ``Infinity`` are not.
    raise ValueError(f"{name} is not JSON")


def _parse_object(metadata: str) -> dict[str, object] | None:
    try:
        value = json.loads(metadata, parse_constant=_reject_constant)
    except ValueError:
        return None
    return value if isinstance(value, dict) else None


def merge_trace_carrier(metadata: str | None, carrier: Mapping[str, str]) -> str | None:
    """``metadata`` with ``carrier``'s keys added, by the rule above."""
    if not carrier:
        return metadata
    if metadata is None or not metadata.strip():
        return json.dumps(dict(carrier), separators=(",", ":"))
    existing = _parse_object(metadata)
    if existing is None or TRACEPARENT in existing or TRACESTATE in existing:
        return metadata
    entries = ",".join(
        f"{json.dumps(key)}:{json.dumps(value)}"
        for key, value in carrier.items()
        if key not in existing
    )
    if not entries:
        return metadata
    # A parsed object's first non-whitespace character is its ``{``.
    brace = len(metadata) - len(metadata.lstrip()) + 1
    separator = "," if existing else ""
    return f"{metadata[:brace]}{entries}{separator}{metadata[brace:]}"


def trace_carrier(metadata: str | None) -> dict[str, str]:
    """The string members of ``metadata``'s object, for a propagator to read.

    Empty for metadata that is absent or not a JSON object: such a job starts a
    trace of its own rather than failing.
    """
    if not metadata:
        return {}
    existing = _parse_object(metadata)
    if existing is None:
        return {}
    return {key: value for key, value in existing.items() if isinstance(value, str)}
