"""ProxyRegistry — maps handler names to ProxyHandler instances."""

from __future__ import annotations

from typing import Any

from flexiq.proxies.handler import ProxyHandler


class ProxyRegistry:
    """Container for proxy handlers, keyed by name."""

    def __init__(self) -> None:
        self._handlers: dict[str, ProxyHandler] = {}

    def register(self, handler: ProxyHandler) -> None:
        """Register a proxy handler."""
        self._handlers[handler.name] = handler

    def get(self, name: str) -> ProxyHandler | None:
        """Look up a handler by name."""
        return self._handlers.get(name)

    def find_handler(self, obj: Any) -> ProxyHandler | None:
        """Find a handler that can proxy this object (calls detect())."""
        for handler in self._handlers.values():
            if handler.detect(obj):
                return handler
        return None

    def __len__(self) -> int:
        return len(self._handlers)
