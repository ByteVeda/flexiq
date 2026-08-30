"""A deadline for middleware hooks, and the watchdog that reports overruns.

A task's ``timeout_ms`` bounds the handler; it says nothing about the middleware
around it. One hook that blocks — an exporter flushing to an unreachable
collector, a tracing backend with no client-side timeout — holds the attempt
open past the task's own limit.

Python can report such a hook but not stop it. The call cannot be moved onto a
thread the runner could abandon: ``SentryMiddleware.before`` pushes a
thread-local Sentry scope that the handler and ``after`` share, and the ``ctx``
every hook receives is the per-thread ``current_job`` proxy — on another thread
it would not even resolve. So this module names the offender and leaves the
attempt to its fate. Under the prefork pool that fate is bounded anyway: its
watchdog SIGKILLs a child that outruns ``timeout_ms``, and that clock covers the
hook phase too.
"""

from __future__ import annotations

import heapq
import itertools
import logging
import math
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager

logger = logging.getLogger("flexiq")


class _Entry:
    """One armed hook deadline.

    ``cancelled`` is written by :meth:`_HookWatchdog.disarm` and read by the
    watchdog thread, both under the watchdog's lock, so a hook that returns
    while the watchdog is deciding cannot be reported after the fact.
    """

    __slots__ = ("cancelled", "deadline", "hook", "middleware", "slept", "timeout")

    def __init__(self, deadline: float, middleware: str, hook: str, timeout: float, slept: bool):
        self.deadline = deadline
        self.middleware = middleware
        self.hook = hook
        self.timeout = timeout
        self.slept = slept
        self.cancelled = False

    def report(self) -> None:
        """Name this hook in the log. Called once, off the watchdog's lock."""
        # A slept attempt is the one case nothing rescues: the job is already
        # Pending at its deadline and unclaimed, so no reaper will ever look at
        # it, and the slot this hook holds is held until the hook returns.
        detail = (
            " A slept attempt is never reaped, so this worker slot stays held "
            "until the hook returns."
            if self.slept
            else ""
        )
        logger.warning(
            "middleware %s %s() exceeded %dms and is still running; the attempt "
            "stays blocked on it.%s",
            self.middleware,
            self.hook,
            round(self.timeout * 1000),
            detail,
        )


class _HookWatchdog:
    """One daemon thread that reports every armed deadline as it passes.

    A ``threading.Timer`` per hook would start a thread per middleware per job.
    This keeps one thread for the process and sleeps until the earliest
    deadline; it is started on the first arm, so a queue with the bound disabled
    never pays for it.
    """

    def __init__(self) -> None:
        self._cond = threading.Condition()
        self._pending: list[tuple[float, int, _Entry]] = []
        self._seq = itertools.count()
        self._thread: threading.Thread | None = None

    def arm(self, entry: _Entry) -> None:
        with self._cond:
            # The counter breaks deadline ties: `_Entry` is not orderable, and
            # heapq would otherwise compare two of them.
            heapq.heappush(self._pending, (entry.deadline, next(self._seq), entry))
            if self._thread is None:
                self._thread = threading.Thread(
                    target=self._run, name="flexiq-hook-watchdog", daemon=True
                )
                self._thread.start()
            self._cond.notify()

    def disarm(self, entry: _Entry) -> None:
        """Mark ``entry`` spent. The heap entry is dropped when it surfaces."""
        with self._cond:
            entry.cancelled = True

    def _run(self) -> None:
        while True:
            with self._cond:
                while not self._pending:
                    self._cond.wait()
                deadline, _, entry = self._pending[0]
                if entry.cancelled:
                    heapq.heappop(self._pending)
                    continue
                remaining = deadline - time.monotonic()
                if remaining > 0:
                    # A nearer deadline armed meanwhile wakes this early; the
                    # loop re-reads the head rather than trusting the sleep.
                    self._cond.wait(remaining)
                    continue
                heapq.heappop(self._pending)
            entry.report()


_WATCHDOG = _HookWatchdog()


@contextmanager
def hook_deadline(
    timeout: float,
    middleware: str,
    hook: str,
    *,
    slept: bool = False,
    watchdog: _HookWatchdog | None = None,
) -> Iterator[None]:
    """Report ``middleware``'s ``hook`` if it outruns ``timeout`` seconds.

    Reporting is all this does — see the module docstring for why the call
    cannot be abandoned. A non-positive ``timeout`` disables the bound and costs
    nothing at all.

    Args:
        timeout: Budget in seconds, per hook call. ``0`` or less disables, and
            so does a non-finite one — the watchdog would otherwise wait on
            ``inf`` (``OverflowError``) or round ``nan`` (``ValueError``), and
            either kills the one thread every later deadline depends on.
            :class:`~flexiq.app.Queue` rejects a non-finite budget outright;
            this is the backstop for a direct caller.
        middleware: Stable key of the middleware, for the log line.
        hook: Hook name, for the log line (``"before"``, ``"after"``, …).
        slept: Whether this hook runs on the sleep path, where nothing will
            reclaim the attempt the hook is holding.
        watchdog: Overrides the process-wide watchdog. For tests.
    """
    if not math.isfinite(timeout) or timeout <= 0:
        yield
        return
    armed = watchdog if watchdog is not None else _WATCHDOG
    entry = _Entry(time.monotonic() + timeout, middleware, hook, timeout, slept)
    armed.arm(entry)
    try:
        yield
    finally:
        armed.disarm(entry)
