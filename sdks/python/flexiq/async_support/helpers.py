"""Helpers for running potentially-async callables from sync contexts."""

from __future__ import annotations

import asyncio
from typing import Any


def run_maybe_async(result: Any) -> Any:
    """If *result* is a coroutine, run it to completion and return the value.

    Safe to call from any thread that does **not** already have a running
    event loop (worker threads, main thread, daemon threads).

    Raises:
        RuntimeError: If called from a thread that already has a running
            event loop. Use the async API (``a*`` methods, ``await`` the
            coroutine directly, or run in a separate thread) instead.
    """
    if not asyncio.iscoroutine(result):
        return result

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(result)

    raise RuntimeError(
        "Cannot run an async resource factory or callable from a thread that "
        "already has a running event loop. Use the corresponding async API "
        "method (e.g. `aresult()`, `aenqueue()`), `await` the coroutine "
        "directly, or invoke the sync API from a worker thread."
    )
