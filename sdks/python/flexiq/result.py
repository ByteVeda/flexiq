"""Job result handle for checking status and retrieving results."""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

from flexiq.async_support.result import AsyncJobResultMixin
from flexiq.exceptions import (
    FlexiQError,
    MaxRetriesExceededError,
    SerializationError,
    TaskCancelledError,
    TaskFailedError,
)
from flexiq.task_errors import decode_task_error

if TYPE_CHECKING:
    from flexiq._flexiq import PyJob
    from flexiq.app import Queue

log = logging.getLogger("flexiq.result")

# Cap on the error summary surfaced in the job list/detail API. The full
# traceback (which can echo argument values and local context) stays available
# only via the per-job ``/api/jobs/{id}/errors`` endpoint.
_ERROR_SUMMARY_MAX = 500


def _summarize_error(error: str | None) -> str | None:
    """Reduce a stored error to one ``ExceptionType: message`` line.

    Structured errors (canonical JSON, see ``flexiq.task_errors``) summarize
    from their fields; legacy plain tracebacks fall back to the last non-empty
    line. Frames carry file paths and source snippets we don't want in the
    broadly-readable job list, so only the summary is surfaced here.
    """
    if not error:
        return error
    decoded = decode_task_error(error)
    if decoded is not None:
        summary = decoded.summary()
    else:
        summary = next((ln for ln in reversed(error.splitlines()) if ln.strip()), error)
    summary = summary.strip()
    if len(summary) > _ERROR_SUMMARY_MAX:
        summary = summary[:_ERROR_SUMMARY_MAX] + "…"
    return summary


class JobResult(AsyncJobResultMixin):
    """
    Handle to an enqueued job. Provides methods to check status
    and retrieve results (sync and async).
    """

    def __init__(self, py_job: PyJob, queue: Queue):
        """Initialize a job result handle.

        Args:
            py_job: The underlying Rust PyJob instance.
            queue: The parent Queue used for status refresh.
        """
        self._py_job = py_job
        self._queue = queue

    @property
    def id(self) -> str:
        """The unique job ID."""
        return self._py_job.id

    def refresh(self) -> None:
        """Refresh job state from the database.

        Call this before reading ``status``, ``progress``, or ``error``
        to get the latest values. Properties return the last-fetched
        state and do **not** hit the database automatically.
        """
        refreshed = self._queue._inner.get_job(self._py_job.id)
        if refreshed is not None:
            self._py_job = refreshed

    @property
    def status(self) -> str:
        """
        Last-fetched job status. Call :meth:`refresh` first for the latest value.
        Returns one of: "pending", "running", "complete", "failed", "dead", "cancelled".
        """
        return self._py_job.status

    @property
    def progress(self) -> int | None:
        """Last-fetched progress (0-100). Call :meth:`refresh` for the latest value."""
        return self._py_job.progress

    @property
    def error(self) -> str | None:
        """Last-fetched error message. Call :meth:`refresh` for the latest value."""
        return self._py_job.error

    @property
    def errors(self) -> list[dict]:
        """Error history for this job (one entry per failed attempt)."""
        return self._queue.job_errors(self.id)

    @property
    def metadata(self) -> str | None:
        """Raw metadata JSON string set at enqueue time (free-form blob)."""
        return self._py_job.metadata

    @property
    def notes(self) -> dict[str, Any] | None:
        """Structured notes set at enqueue time, parsed back to a dict.

        Returns ``None`` if no notes were attached. Decoding errors are
        treated as missing — the stored string was validated on the way
        in, so this branch only fires if the column was corrupted out of
        band.
        """
        raw = self._py_job.notes
        if raw is None:
            return None
        try:
            decoded = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            log.warning("failed to decode notes JSON for job %s", self.id)
            return None
        if not isinstance(decoded, dict):
            log.warning("notes for job %s is not a JSON object", self.id)
            return None
        return decoded

    @property
    def dependencies(self) -> list[str]:
        """IDs of jobs this job depends on."""
        return self._queue._inner.get_dependencies(self.id)

    @property
    def dependents(self) -> list[str]:
        """IDs of jobs that depend on this job."""
        return self._queue._inner.get_dependents(self.id)

    def _poll_once(self) -> tuple[str, Any]:
        """Refresh and return (status, deserialized result or None)."""
        self.refresh()
        status = self._py_job.status

        if status == "complete":
            result_bytes = self._py_job.result_bytes
            if result_bytes is None:
                return status, None
            try:
                return status, self._queue._serializer.loads(result_bytes)
            except Exception as exc:
                raise SerializationError(
                    f"Failed to deserialize result for job {self.id}: {exc}"
                ) from exc

        if status == "cancelled":
            error_msg = self._py_job.error or "job was cancelled"
            raise TaskCancelledError(f"Job {self.id} cancelled: {error_msg}")
        if status == "dead":
            raise self._failure_error(
                MaxRetriesExceededError, "dead-lettered", "max retries exceeded"
            )
        if status == "failed":
            raise self._failure_error(TaskFailedError, "failed", "task failed")

        return status, None

    def _failure_error(
        self,
        error_cls: type[TaskFailedError] | type[MaxRetriesExceededError],
        verb: str,
        default_msg: str,
    ) -> FlexiQError:
        """Build the outcome exception, attaching structured details when stored."""
        raw = self._py_job.error
        decoded = decode_task_error(raw)
        summary = decoded.summary() if decoded is not None else (raw or default_msg)
        error = error_cls(f"Job {self.id} {verb}: {summary}")
        error._attach_details(
            errtype=decoded.errtype if decoded else None,
            traceback=decoded.traceback if decoded else None,
            job_id=self.id,
            raw_error=raw,
        )
        return error

    def result(
        self,
        timeout: float = 30.0,
        poll_interval: float = 0.05,
        max_poll_interval: float = 0.5,
    ) -> Any:
        """
        Block until the job completes and return the result.

        Uses exponential backoff: starts polling at ``poll_interval`` and
        gradually increases to ``max_poll_interval``.

        Args:
            timeout: Maximum seconds to wait.
            poll_interval: Initial seconds between status checks.
            max_poll_interval: Maximum seconds between status checks.

        Returns:
            The deserialized return value of the task function.

        Raises:
            TimeoutError: If the job doesn't complete within the timeout.
            TaskFailedError: If the job failed.
            MaxRetriesExceededError: If the job was moved to DLQ.
            TaskCancelledError: If the job was cancelled.
            SerializationError: If result deserialization fails.
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
                    f"Job {self.id} did not complete within {timeout}s "
                    f"(current status: {self._py_job.status})"
                )

            time.sleep(min(current_interval, max(0, deadline - time.monotonic())))
            current_interval = min(current_interval * 1.5, max_poll_interval)

    _TERMINAL_STATUSES = frozenset({"complete", "failed", "dead", "cancelled"})

    def stream(
        self,
        timeout: float = 60.0,
        poll_interval: float = 0.5,
    ) -> Iterator[Any]:
        """Iterate over partial results published by the task via ``current_job.publish()``.

        Yields each partial result as it becomes available. Stops when the
        job reaches a terminal state (complete, failed, dead, cancelled).

        Args:
            timeout: Maximum seconds to wait for results.
            poll_interval: Seconds between polls.

        Yields:
            Deserialized partial result data (whatever was passed to ``publish()``).
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

            time.sleep(min(poll_interval, max(0, deadline - time.monotonic())))

    def to_dict(self) -> dict[str, Any]:
        """Convert to a plain dictionary for JSON serialization.

        Refreshes the job status from the database before building the dict.
        Does not include ``result_bytes`` (use :meth:`result` for that).
        """
        self.refresh()

        return {
            "id": self._py_job.id,
            "queue": self._py_job.queue,
            "task_name": self._py_job.task_name,
            "status": self._py_job.status,
            "priority": self._py_job.priority,
            "progress": self._py_job.progress,
            "retry_count": self._py_job.retry_count,
            "max_retries": self._py_job.max_retries,
            "created_at": self._py_job.created_at,
            "scheduled_at": self._py_job.scheduled_at,
            "started_at": self._py_job.started_at,
            "completed_at": self._py_job.completed_at,
            "error": _summarize_error(self._py_job.error),
            "timeout_ms": self._py_job.timeout_ms,
            "unique_key": self._py_job.unique_key,
            "metadata": self._py_job.metadata,
            # Emit the raw canonical JSON string (the dashboard API contract is
            # ``notes: string | null`` and the client JSON.parse-s it). The
            # ``notes`` property returns a parsed dict for Python callers, but
            # serializing that dict here would break the typed client contract.
            "notes": self._py_job.notes,
            "namespace": self._py_job.namespace,
        }

    def __repr__(self) -> str:
        """Return a developer-friendly string representation."""
        return f"<JobResult id={self.id} status={self._py_job.status}>"
