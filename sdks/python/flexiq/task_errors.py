"""Canonical structured task-error encoding (cross-SDK contract).

A task failure is stored as one JSON object — ``{"errtype", "message",
"traceback"}`` — so any FlexiQ SDK (and the dashboard) can show the exception
class and message without parsing language-specific traceback text. Strings
that don't parse as that object are legacy/system errors and pass through
verbatim; see ``BINDING_CONTRACT.md`` "Task errors".
"""

from __future__ import annotations

import json
import traceback as traceback_module
from dataclasses import dataclass
from types import TracebackType


@dataclass(frozen=True)
class TaskError:
    """A decoded structured task error."""

    errtype: str
    message: str
    traceback: list[str]
    raw: str

    def summary(self) -> str:
        """One-line ``errtype: message`` form used anywhere errors are listed."""
        return f"{self.errtype}: {self.message}" if self.message else self.errtype


def encode_task_error(exc: BaseException) -> str:
    """Encode a raised exception as the canonical cross-SDK JSON string."""
    return encode_from_parts(type(exc), exc, exc.__traceback__)


def encode_from_parts(
    exc_type: type[BaseException],
    exc: BaseException,
    tb: TracebackType | None,
) -> str:
    """Encode from explicit ``(type, value, traceback)`` parts.

    Separate entry point because the Rust worker paths hold the three parts
    individually (PyO3 ``PyErr``) rather than a single raised exception.
    """
    frames = "".join(traceback_module.format_exception(exc_type, exc, tb)).splitlines()
    return json.dumps(
        {"errtype": exc_type.__qualname__, "message": str(exc), "traceback": frames},
        separators=(",", ":"),
    )


def decode_task_error(raw: str | None) -> TaskError | None:
    """Decode a stored error string, or None for legacy/system plain strings."""
    if not raw or not raw.startswith("{"):
        return None
    try:
        decoded = json.loads(raw)
    except ValueError:
        return None
    if not isinstance(decoded, dict) or not isinstance(decoded.get("message"), str):
        return None
    tb = decoded.get("traceback")
    return TaskError(
        errtype=str(decoded.get("errtype") or "Error"),
        message=decoded["message"],
        traceback=[str(line) for line in tb] if isinstance(tb, list) else [],
        raw=raw,
    )
