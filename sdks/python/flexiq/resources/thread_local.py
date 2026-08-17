"""Thread-local storage for thread-scoped resources."""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from typing import Any

from flexiq.async_support.helpers import run_maybe_async

logger = logging.getLogger("flexiq.resources")


class ThreadLocalStore:
    """Thread-local storage for thread-scoped resources.

    One instance per thread, created lazily on first access.
    """

    def __init__(
        self,
        name: str,
        factory: Callable[..., Any],
        teardown: Callable[..., Any] | None,
        dep_kwargs: dict[str, Any] | None = None,
    ) -> None:
        self._name = name
        self._factory = factory
        self._teardown = teardown
        self._dep_kwargs = dep_kwargs or {}
        self._local = threading.local()
        self._instances: dict[int, Any] = {}
        self._lock = threading.Lock()

    def get_or_create(self) -> Any:
        """Return thread-local instance, creating it on first access."""
        instance = getattr(self._local, "instance", None)
        if instance is not None:
            return instance

        instance = run_maybe_async(self._factory(**self._dep_kwargs))

        self._local.instance = instance
        with self._lock:
            self._instances[threading.get_ident()] = instance
        return instance

    def teardown_all(self) -> None:
        """Tear down all thread-local instances."""
        with self._lock:
            for tid, instance in self._instances.items():
                if self._teardown is not None:
                    try:
                        run_maybe_async(self._teardown(instance))
                    except Exception:
                        logger.exception(
                            "Error tearing down thread-local resource '%s' for thread %d",
                            self._name,
                            tid,
                        )
            self._instances.clear()
