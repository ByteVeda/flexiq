"""Debounce configuration for task registration and enqueue.

A debounce collapses a burst of enqueues that share a key into a single run by
sliding the pending job's deadline forward, capped so a caller who never stops
enqueuing cannot starve it. The storage layer does the collapsing; this module
owns the shell-facing contract — parsing the two durations, validating that the
knobs arrive as a set, and resolving the key template against a call's
arguments.
"""

from __future__ import annotations

import inspect
import math
import re
from dataclasses import dataclass
from typing import Any

# Accepts "500ms", "5s", "2.5m", "1h", "7d". A bare number is rejected on
# purpose: "5" reads as seconds to one person and milliseconds to the next, and
# the numeric form (``debounce=5``) already says seconds unambiguously.
_DURATION_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*(ms|s|m|h|d)\s*$", re.IGNORECASE)

_UNIT_MS = {"ms": 1, "s": 1_000, "m": 60_000, "h": 3_600_000, "d": 86_400_000}

# Storage measures the window as an i64 of milliseconds, so anything past this
# has no representation to travel in. Caught here rather than at the PyO3
# boundary, where it surfaces as ``OverflowError`` instead of the ``ValueError``
# every other duration mistake raises.
_MAX_DURATION_MS = 2**63 - 1

# What a duration may be written as, in either the decorator or a single call.
Duration = str | float | int


def parse_duration_ms(value: Duration, *, param: str) -> int:
    """Parse a duration into whole milliseconds.

    ``str`` carries its unit (``"5m"``); a number is seconds, matching every
    other time-valued argument on the Python surface (``delay``, ``expires``,
    ``timeout``). Rounds to the nearest millisecond — storage measures the
    window in integer milliseconds, so a sub-millisecond fraction has nowhere
    to go.

    Raises:
        TypeError: ``value`` is neither a string nor a real number.
        ValueError: the string has no recognized unit, or the duration is not
            finite, is not strictly positive, or exceeds what storage can hold.
    """
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        raise TypeError(
            f"{param} must be a duration string like '5m' or a number of seconds, "
            f"got {type(value).__name__}"
        )

    if isinstance(value, str):
        match = _DURATION_RE.match(value)
        if match is None:
            raise ValueError(
                f"{param}={value!r} is not a duration — expected a number followed by "
                "one of ms/s/m/h/d, e.g. '500ms', '5m', '2h'"
            )
        amount, unit = match.groups()
        scaled = float(amount) * _UNIT_MS[unit.lower()]
    else:
        scaled = float(value) * 1000

    # ``round`` raises OverflowError on an infinity and ValueError on a NaN, so
    # both are screened here to keep one error type across every bad duration.
    # An infinity can arrive directly (``debounce=float("inf")``) or by
    # overflowing the multiply above.
    if not math.isfinite(scaled):
        raise ValueError(f"{param} must be a finite duration, got {value!r}")

    millis = round(scaled)
    if millis <= 0:
        raise ValueError(f"{param} must be a positive duration, got {value!r}")
    if millis > _MAX_DURATION_MS:
        raise ValueError(
            f"{param}={value!r} is longer than the {_MAX_DURATION_MS}ms ceiling storage "
            "can represent"
        )
    return millis


@dataclass(frozen=True)
class DebounceConfig:
    """A resolved debounce window, ready to hand to storage."""

    #: Format template resolved against the call's arguments, e.g.
    #: ``"report:{user_id}"``. Never resolved at registration time — the
    #: arguments only exist per call.
    key_template: str
    #: How far ahead of *now* each enqueue pushes the run.
    window_ms: int
    #: Ceiling on the total delay, measured from when the window opened.
    max_wait_ms: int
    #: Overwrite the pending job's payload with the newest call's.
    replace_payload: bool


def normalize_debounce(
    debounce: Duration | None,
    debounce_key: str | None,
    debounce_max_wait: Duration | None,
    debounce_replace_payload: bool = False,
) -> DebounceConfig | None:
    """Fold the four debounce knobs into a config, or ``None`` when unset.

    The knobs are a set, not four independent options, and every incomplete
    combination is refused rather than half-applied:

    * ``debounce`` without ``debounce_max_wait`` is an unbounded debounce — a
      caller holding the button down starves the job forever.
    * ``debounce`` without ``debounce_key`` would debounce every job of the
      task against every other. The key is meant to be payload-derived
      (``"report:{user_id}"``); a deliberately global window is still spelled
      out as a literal key.
    * A ``debounce_key`` or ``debounce_max_wait`` with no window is dead
      configuration that reads as if debouncing were on.

    Raises:
        ValueError: on any of the above, or when ``debounce_max_wait`` is
            shorter than ``debounce`` (which would cap the very first insert,
            silently making the window meaningless).
    """
    if debounce is None:
        if debounce_key is not None:
            raise ValueError("debounce_key requires debounce=... (the window length)")
        if debounce_max_wait is not None:
            raise ValueError("debounce_max_wait requires debounce=... (the window length)")
        if debounce_replace_payload:
            raise ValueError("debounce_replace_payload requires debounce=... (the window length)")
        return None

    if debounce_max_wait is None:
        raise ValueError(
            "debounce=... requires debounce_max_wait=... — an unbounded debounce "
            "starves the job while enqueues keep arriving"
        )
    if debounce_key is None:
        raise ValueError(
            "debounce=... requires debounce_key=... — a key derived from the call's "
            'arguments, e.g. debounce_key="report:{user_id}"'
        )
    if not debounce_key:
        raise ValueError("debounce_key must not be empty")

    window_ms = parse_duration_ms(debounce, param="debounce")
    max_wait_ms = parse_duration_ms(debounce_max_wait, param="debounce_max_wait")
    if max_wait_ms < window_ms:
        raise ValueError(
            f"debounce_max_wait ({max_wait_ms}ms) must be at least as long as "
            f"debounce ({window_ms}ms), or the window never gets to slide"
        )

    return DebounceConfig(
        key_template=debounce_key,
        window_ms=window_ms,
        max_wait_ms=max_wait_ms,
        replace_payload=debounce_replace_payload,
    )


def resolve_debounce_key(
    template: str,
    task_name: str,
    signature: inspect.Signature | None,
    args: tuple,
    kwargs: dict[str, Any],
) -> str:
    """Resolve ``template`` against one call's arguments.

    Parameters are addressable by name (``{user_id}``) whether they were passed
    positionally or by keyword, and by position (``{0}``) as a fallback for
    callers that reach ``enqueue`` with no registered signature to bind
    against.

    An unresolvable placeholder raises rather than degrading to a literal or a
    global key: silently debouncing every user's report against every other
    user's is a data bug that would only surface as mysteriously missing runs.

    Raises:
        ValueError: the template names something the call does not provide, or
            resolves to the empty string.
    """
    named: dict[str, Any] = dict(kwargs)
    if signature is not None:
        try:
            bound = signature.bind_partial(*args, **kwargs)
        except TypeError as exc:
            raise ValueError(
                f"debounce_key {template!r} for task {task_name!r} cannot be resolved: "
                f"the call does not match the task signature ({exc})"
            ) from exc
        bound.apply_defaults()
        named = dict(bound.arguments)
        # ``bound.arguments`` nests a ``**kwargs`` parameter under its own name,
        # so a task declared ``def f(**kw)`` called with ``user_id=7`` would
        # otherwise hide ``user_id`` one level down and fail to resolve.
        for name, param in signature.parameters.items():
            if param.kind is inspect.Parameter.VAR_KEYWORD:
                named.update(named.pop(name, {}))
            elif param.kind is inspect.Parameter.VAR_POSITIONAL:
                named.pop(name, None)

    try:
        key = template.format(*args, **named)
    except (KeyError, IndexError, AttributeError) as exc:
        # KeyError stringifies to the bare field name; the other two carry a
        # sentence, so only the first is quoted as a placeholder.
        missing = f"{exc.args[0]!r}" if isinstance(exc, KeyError) else str(exc)
        available = ", ".join(sorted(named)) or "<none>"
        raise ValueError(
            f"debounce_key {template!r} for task {task_name!r} references {missing}, "
            f"which this call does not provide (available: {available})"
        ) from exc

    if not key:
        raise ValueError(
            f"debounce_key {template!r} for task {task_name!r} resolved to an empty key"
        )
    return key
