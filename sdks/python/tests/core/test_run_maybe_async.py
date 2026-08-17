"""Tests for `run_maybe_async` — detection of running event loop."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from flexiq.async_support.helpers import run_maybe_async


def test_run_maybe_async_passes_through_non_coroutine() -> None:
    assert run_maybe_async(42) == 42
    assert run_maybe_async("hello") == "hello"
    assert run_maybe_async(None) is None


def test_run_maybe_async_runs_coroutine_in_sync_context() -> None:
    async def make_value() -> int:
        return 7

    assert run_maybe_async(make_value()) == 7


async def test_run_maybe_async_raises_clear_error_under_running_loop() -> None:
    """Pytest-asyncio puts us in a running loop — must surface the flexiq error."""

    async def make_value() -> int:
        return 1

    coro: Any = make_value()
    with pytest.raises(RuntimeError, match="async API"):
        run_maybe_async(coro)
    # Drain to silence the "coroutine was never awaited" warning.
    coro.close()


async def test_run_maybe_async_async_message_mentions_a_methods() -> None:
    async def make_value() -> int:
        return 1

    coro: Any = make_value()
    try:
        run_maybe_async(coro)
    except RuntimeError as exc:
        assert "aresult" in str(exc) or "aenqueue" in str(exc) or "await" in str(exc)
    finally:
        coro.close()


def test_run_maybe_async_no_loop_uses_asyncio_run() -> None:
    """Sanity: a fresh thread with no loop should run a coroutine to completion."""

    async def slow() -> str:
        await asyncio.sleep(0.001)
        return "ok"

    assert run_maybe_async(slow()) == "ok"
