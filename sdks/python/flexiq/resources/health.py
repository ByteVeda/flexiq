"""Health checker daemon thread for worker resources."""

from __future__ import annotations

import logging
import threading
import time
from typing import TYPE_CHECKING

from flexiq.async_support.helpers import run_maybe_async
from flexiq.resources.definition import ResourceDefinition

if TYPE_CHECKING:
    from flexiq.resources.runtime import ResourceRuntime

logger = logging.getLogger("flexiq.resources")


class HealthChecker:
    """Periodically checks resource health and attempts recreation on failure.

    Runs in a single daemon thread. Each resource with a non-zero
    ``health_check_interval`` is checked independently on its own schedule.
    """

    def __init__(self, runtime: ResourceRuntime) -> None:
        self._runtime = runtime
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        """Start the health-check daemon thread."""
        defs = self._runtime._definitions
        has_checks = any(
            d.health_check is not None and d.health_check_interval > 0 for d in defs.values()
        )
        if not has_checks:
            return

        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run,
            daemon=True,
            name="flexiq-health-checker",
        )
        self._thread.start()

    def stop(self) -> None:
        """Signal the daemon thread to stop and wait for it."""
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None

    def _run(self) -> None:
        """Main loop — sleep in small increments, check resources on schedule."""
        defs = self._runtime._definitions
        next_check: dict[str, float] = {}
        fail_count: dict[str, int] = {}

        for name, defn in defs.items():
            if defn.health_check is not None and defn.health_check_interval > 0:
                next_check[name] = time.monotonic() + defn.health_check_interval
                fail_count[name] = 0

        while not self._stop_event.is_set():
            now = time.monotonic()
            for name, due in list(next_check.items()):
                if now < due:
                    continue
                if name in self._runtime._unhealthy:
                    continue

                defn = defs[name]
                healthy = self._check_one(name, defn)
                if healthy:
                    fail_count[name] = 0
                else:
                    fail_count[name] += 1
                    logger.warning(
                        "Resource '%s' health check failed (%d/%d)",
                        name,
                        fail_count[name],
                        defn.max_recreation_attempts,
                    )
                    recreated = self._runtime.recreate(name)
                    if not recreated and fail_count[name] >= defn.max_recreation_attempts:
                        self._runtime.mark_unhealthy(name)

                next_check[name] = time.monotonic() + defn.health_check_interval

            self._stop_event.wait(timeout=0.5)

    def _check_one(self, name: str, defn: ResourceDefinition) -> bool:
        """Run a single health check. Returns True if healthy."""
        if defn.health_check is None:
            return True
        try:
            instance = self._runtime._instances.get(name)
            if instance is None:
                return False
            return bool(run_maybe_async(defn.health_check(instance)))
        except Exception:
            logger.exception("Health check error for resource '%s'", name)
            return False
