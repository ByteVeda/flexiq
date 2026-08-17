"""Async-safe job context using contextvars (works on event loop threads)."""

from __future__ import annotations

import contextvars

from flexiq._active_context import _ActiveContext

_context_var: contextvars.ContextVar[_ActiveContext | None] = contextvars.ContextVar(
    "_flexiq_async_context", default=None
)


def set_async_context(
    job_id: str,
    task_name: str,
    retry_count: int,
    queue_name: str,
) -> contextvars.Token[_ActiveContext | None]:
    """Set job context via contextvar (for async tasks). Returns token for cleanup."""
    ctx = _ActiveContext(job_id, task_name, retry_count, queue_name)
    return _context_var.set(ctx)


def clear_async_context(token: contextvars.Token[_ActiveContext | None]) -> None:
    """Clear async context using the saved token."""
    _context_var.reset(token)


def get_async_context() -> _ActiveContext | None:
    """Get the current async job context, if any."""
    return _context_var.get()
