"""Async result polling for flexiq job results."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any, ClassVar

if TYPE_CHECKING:
    from flexiq._flexiq import PyJob
    from flexiq.app import Queue

log = logging.getLogger("flexiq.result")


class AsyncJobResultMixin:
    """Provides the async ``aresult()`` and ``astream()`` methods for JobResult."""

    if TYPE_CHECKING:
        _TERMINAL_STATUSES: ClassVar[frozenset[str]]
        _queue: Queue
        _py_job: PyJob

        @property
        def id(self) -> str: ...
        def _poll_once(self) -> tuple[str, Any]: ...
        def refresh(self) -> None: ...

    async def aresult(
        self,
        timeout: float = 30.0,
        poll_interval: float = 0.05,
        max_poll_interval: float = 0.5,
    ) -> Any:
        """Async version of :meth:`result`. Awaitable, non-blocking.

        Uses exponential backoff: starts polling at *poll_interval* and
        gradually increases to *max_poll_interval*.

        Args:
            timeout: Maximum seconds to wait.
            poll_interval: Initial seconds between status checks.
            max_poll_interval: Maximum seconds between status checks.

        Returns:
            The deserialized return value of the task function.

        Raises:
            TimeoutError: If the job doesn't complete within *timeout*.
            TaskFailedError: If the job failed.
            MaxRetriesExceededError: If the job was moved to DLQ.
            TaskCancelledError: If the job was cancelled.
            SerializationError: If result deserialization fails.

        Example::

            result = await job.aresult(timeout=30)
        """
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        deadline = time.monotonic() + timeout
        current_interval = poll_interval

        while True:
            status, value = self._poll_once()
            if status == "complete":
                return value
            if time.monotonic() >= deadline:
                # A terminal failure can land between the poll and this branch
                # (or during the storage read itself). Re-poll once so the
                # caller sees the real exception class, not `TimeoutError`.
                status, value = self._poll_once()
                if status == "complete":
                    return value
                raise TimeoutError(
                    f"Job {self.id} did not complete within {timeout}s (current status: {status})"
                )

            await asyncio.sleep(min(current_interval, max(0, deadline - time.monotonic())))
            current_interval = min(current_interval * 1.5, max_poll_interval)

    async def astream(
        self,
        timeout: float = 60.0,
        poll_interval: float = 0.5,
    ) -> AsyncIterator[Any]:
        """Async iterate over partial results published by the task.

        Async version of :meth:`JobResult.stream`. Uses ``asyncio.sleep`` so it
        won't block the event loop.

        Args:
            timeout: Maximum seconds to wait for results.
            poll_interval: Seconds between polls.

        Yields:
            Deserialized partial result data.
        """
        deadline = time.monotonic() + timeout
        last_seen_at: int = 0

        while time.monotonic() < deadline:
            logs = self._queue._inner.get_task_logs(self.id)
            for entry in logs:
                if entry["level"] != "result":
                    continue
                logged_at = entry.get("logged_at", 0)
                if logged_at <= last_seen_at:
                    continue
                last_seen_at = logged_at
                extra = entry.get("extra")
                if extra:
                    try:
                        yield json.loads(extra)
                    except (json.JSONDecodeError, TypeError):
                        log.warning("failed to deserialize partial result for job %s", self.id)
                        yield extra

            self.refresh()
            if self._py_job.status in self._TERMINAL_STATUSES:
                return

            await asyncio.sleep(min(poll_interval, max(0, deadline - time.monotonic())))
