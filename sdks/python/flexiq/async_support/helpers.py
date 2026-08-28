"""Helpers for running potentially-async callables from sync contexts."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any, TypeVar

_T = TypeVar("_T")


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


async def run_off_loop(fn: Callable[[], _T]) -> _T:
    """Run a blocking call on a worker thread, keeping the event loop free.

    For the calls an async task body makes that are synchronous underneath — a
    durable step's commit is one, and on an attached executor it is a network
    round trip rather than a local write. Blocking the loop for it would stall
    every other coroutine the worker is running, including the ones whose
    answers this one is waiting behind.

    The default executor, as the rest of the async surface uses.
    """
    return await asyncio.get_running_loop().run_in_executor(None, fn)
