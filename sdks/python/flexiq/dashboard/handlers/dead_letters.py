"""Dead-letter route handlers."""

from __future__ import annotations

from typing import TYPE_CHECKING

from flexiq.dashboard.handlers._qs import _parse_int_qs

if TYPE_CHECKING:
    from flexiq.app import Queue


def _handle_dead_letters(queue: Queue, qs: dict) -> list:
    limit = _parse_int_qs(qs, "limit", 20)
    offset = _parse_int_qs(qs, "offset", 0)
    return queue.dead_letters(limit=limit, offset=offset)
