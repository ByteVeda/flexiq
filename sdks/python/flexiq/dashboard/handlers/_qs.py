"""Query-string parsing helpers shared by dashboard handlers."""

from __future__ import annotations

from flexiq.dashboard.errors import _BadRequest


def _parse_int_qs(qs: dict, key: str, default: int) -> int:
    """Parse a non-negative integer from query string, raising ``_BadRequest``."""
    try:
        val = int(qs.get(key, [str(default)])[0])
    except (ValueError, IndexError):
        raise _BadRequest(f"{key} must be an integer") from None
    if val < 0:
        raise _BadRequest(f"{key} must be non-negative")
    return val
