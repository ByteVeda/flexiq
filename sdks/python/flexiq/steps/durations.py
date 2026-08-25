"""How long a ``ctx.step.sleep`` sleeps, and until when.

Reuses the duration grammar the debounce windows already established
(``"500ms"``, ``"5m"``, ``"2h"``; a bare number is seconds) so the SDK has one
answer to "how do I write a duration", and adds the two forms a sleep
specifically invites: a :class:`~datetime.timedelta`, and an absolute
:class:`~datetime.datetime` for ``sleep_until``.
"""

from __future__ import annotations

import datetime as _dt

from flexiq.debounce import parse_duration_ms

#: What a sleep duration may be written as.
SleepDuration = str | float | int | _dt.timedelta

#: What a sleep deadline may be written as.
SleepDeadline = _dt.datetime | float | int


def sleep_duration_ms(value: SleepDuration, *, param: str = "duration") -> int:
    """Parse a sleep duration into whole milliseconds."""
    if isinstance(value, _dt.timedelta):
        value = value.total_seconds()
    return parse_duration_ms(value, param=param)


def sleep_deadline_ms(value: SleepDeadline, *, param: str = "when") -> int:
    """Parse an absolute wake instant into Unix milliseconds.

    A naive :class:`~datetime.datetime` is read as local time, matching
    :meth:`datetime.datetime.timestamp`. Pass an aware one when the deadline
    means something outside this process.
    """
    if isinstance(value, _dt.datetime):
        return round(value.timestamp() * 1000)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(
            f"{param} must be a datetime or a Unix timestamp in seconds, "
            f"got {type(value).__name__}"
        )
    millis = round(float(value) * 1000)
    if millis <= 0:
        raise ValueError(f"{param} must be a positive Unix timestamp, got {value!r}")
    return millis


__all__ = ["SleepDeadline", "SleepDuration", "sleep_deadline_ms", "sleep_duration_ms"]
