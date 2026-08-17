"""Internal exception sentinels raised by route handlers."""

from __future__ import annotations


class _BadRequest(Exception):
    """Raised by route handlers to signal a 400 response."""

    def __init__(self, message: str) -> None:
        self.message = message


class _NotFound(Exception):
    """Raised by route handlers to signal a 404 response."""

    def __init__(self, message: str) -> None:
        self.message = message
