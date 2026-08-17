"""How strictly the interceptor treats an argument it cannot serialize."""

from __future__ import annotations

import enum


class InterceptionMode(str, enum.Enum):
    """Disposition for arguments the interceptor classifies as unsafe."""

    STRICT = "strict"
    """Reject the enqueue."""

    LENIENT = "lenient"
    """Warn and drop the argument."""

    OFF = "off"
    """Interception disabled (the default)."""
