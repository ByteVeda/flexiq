"""The job body, and the one thing every handler does with it.

Imported by the Celery, Dramatiq, RQ and FlexiQ worker entrypoints so that the
work being measured is identical: decode a dict, subtract a timestamp, append a
line. Anything heavier would be measuring the handler; anything lighter would
not exercise the payload at all.
"""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any

from harness.latency import SINK_ENV, LatencySink

_sink: LatencySink | None = None


def sink() -> LatencySink:
    """The process-local sink, opened on first use.

    Lazily, because a Celery prefork child inherits this module rather than
    importing it, and a file descriptor opened in the parent is shared state
    the child should not have to reason about — ``O_APPEND`` makes either safe,
    but one fd per process keeps the ownership obvious.
    """
    global _sink
    if _sink is None:
        _sink = LatencySink(Path(os.environ[SINK_ENV]))
    return _sink


def now_ms() -> float:
    return time.time() * 1000


def build(index: int, pad: str) -> dict[str, Any]:
    """One job's payload. ``t`` is stamped at submission, never re-stamped."""
    return {"i": index, "t": now_ms(), "pad": pad}


def handle(payload: dict[str, Any]) -> None:
    """The measured task body, in every Python runtime."""
    completed = now_ms()
    sink().record(int(payload["i"]), completed - float(payload["t"]), completed)
