"""Template filters for the flexiq Django admin views.

FlexiQ stores every timestamp as Unix **milliseconds** (the dashboard API
contract), so the stock Django ``|date`` filter — which expects seconds or a
``datetime`` — cannot format them directly. ``ms_datetime`` bridges that gap.
"""

from __future__ import annotations

from datetime import datetime, timezone

try:
    from django import template
except ImportError as e:  # pragma: no cover - import guard mirrors sibling modules
    raise ImportError(
        "Django integration requires 'django'. Install with: pip install flexiq[django]"
    ) from e

register = template.Library()


@register.filter
def ms_datetime(value: object) -> str:
    """Render a Unix-millisecond timestamp as a human-readable UTC string.

    Returns an em dash for ``None`` or values that aren't a finite number, so
    templates never show ``None`` or raise on malformed data.
    """
    if value is None:
        return "—"
    try:
        seconds = float(value) / 1000.0  # type: ignore[arg-type]
        moment = datetime.fromtimestamp(seconds, tz=timezone.utc)
    except (TypeError, ValueError, OverflowError, OSError):
        return "—"
    return moment.strftime("%Y-%m-%d %H:%M:%S UTC")
