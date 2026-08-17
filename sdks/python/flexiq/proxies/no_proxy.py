"""NoProxy wrapper — tells the interceptor to skip this argument."""

from __future__ import annotations

from typing import Any


class NoProxy:
    """Wrapper that tells the interceptor to skip proxy handling for a value.

    Usage::

        from flexiq import NoProxy

        # This file handle will be passed through as-is
        my_task.delay(NoProxy(some_file_handle))
    """

    __slots__ = ("value",)

    def __init__(self, value: Any) -> None:
        self.value = value
