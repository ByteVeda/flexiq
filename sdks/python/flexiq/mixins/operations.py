"""Dead letters, replay, circuit breakers, logs, workers, queue management."""

from __future__ import annotations

from typing import Any

from flexiq.context import LogLevel
from flexiq.events import EventType
from flexiq.pagination import Page
from flexiq.result import JobResult


class QueueOperationsMixin:
    """Dead letters, replay, circuit breakers, logs, workers, queue management."""

    _inner: Any

    # -- Dead Letters --

    def dead_letters(self, limit: int = 10, offset: int = 0) -> list[dict]:
        """List dead letter queue entries."""
        return self._inner.dead_letters(limit=limit, offset=offset)  # type: ignore[no-any-return]

    def dead_letters_after(self, limit: int = 10, after: str | None = None) -> Page[dict]:
        """Keyset-paginated :meth:`dead_letters`.

        Returns a :class:`~flexiq.pagination.Page` of entry dicts; pass its
        ``next_cursor`` back as ``after`` for the next page.
        """
        entries, next_cursor = self._inner.dead_letters_after(limit=limit, after=after)
        return Page(items=entries, next_cursor=next_cursor)

    def retry_dead(self, dead_id: str) -> str:
        """Re-enqueue a dead letter job. Returns new job ID."""
        return self._inner.retry_dead(dead_id)  # type: ignore[no-any-return]

    def requeue_job(self, job_id: str) -> bool:
        """Force a stuck Running job back to Pending so a healthy worker re-runs it.

        Releases the job's execution claim atomically and preserves its retry
        budget. Returns False when the job doesn't exist or isn't Running.

        Warning: only use when the owning worker is confirmed dead or hung —
        a still-alive worker may finish the old attempt and the job runs twice.
        """
        return self._inner.requeue_job(job_id)  # type: ignore[no-any-return]

    def purge_dead(self, older_than: int = 86400) -> int:
        """Delete dead letter entries older than a given age."""
        return self._inner.purge_dead(older_than)  # type: ignore[no-any-return]

    def delete_dead(self, dead_id: str) -> bool:
        """Discard a single dead letter entry without re-enqueueing."""
        return self._inner.delete_dead(dead_id)  # type: ignore[no-any-return]

    # -- Replay --

    def replay(self, job_id: str) -> JobResult:
        """Re-enqueue a completed or failed job with the exact same payload."""
        new_id = self._inner.replay_job(job_id)
        return self.get_job(new_id)  # type: ignore[attr-defined, no-any-return]

    def replay_history(self, job_id: str) -> list[dict]:
        """Get replay history for a job."""
        return self._inner.get_replay_history(job_id)  # type: ignore[no-any-return]

    # -- Circuit Breakers --

    def circuit_breakers(self) -> list[dict]:
        """List all circuit breaker states."""
        return self._inner.list_circuit_breakers()  # type: ignore[no-any-return]

    # -- Logs --

    def task_logs(self, job_id: str) -> list[dict]:
        """Get structured logs for a specific job."""
        return self._inner.get_task_logs(job_id)  # type: ignore[no-any-return]

    def query_logs(
        self,
        task_name: str | None = None,
        level: LogLevel | None = None,
        since: int = 3600,
        limit: int = 100,
    ) -> list[dict]:
        """Query structured task logs with filters."""
        return self._inner.query_task_logs(  # type: ignore[no-any-return]
            task_name=task_name,
            level=level.value if level is not None else None,
            since_seconds=since,
            limit=limit,
        )

    # -- Workers --

    def workers(self) -> list[dict]:
        """List all registered workers and their heartbeat status."""
        return self._inner.list_workers()  # type: ignore[no-any-return]

    # -- Queue Pause/Resume --

    def pause(self, queue_name: str = "default") -> None:
        """Pause a queue so no new jobs are dispatched from it."""
        self._inner.pause_queue(queue_name)
        if hasattr(self, "_emit_event"):
            self._emit_event(EventType.QUEUE_PAUSED, {"queue": queue_name})

    def resume(self, queue_name: str = "default") -> None:
        """Resume a paused queue."""
        self._inner.resume_queue(queue_name)
        if hasattr(self, "_emit_event"):
            self._emit_event(EventType.QUEUE_RESUMED, {"queue": queue_name})

    def paused_queues(self) -> list[str]:
        """List currently paused queues."""
        return self._inner.list_paused_queues()  # type: ignore[no-any-return]

    # -- Job Revocation --

    def purge(self, queue_name: str) -> int:
        """Cancel all pending jobs in a queue. Returns count cancelled."""
        return self._inner.purge_queue(queue_name)  # type: ignore[no-any-return]

    def revoke_task(self, task_name: str) -> int:
        """Cancel all pending jobs for a task name. Returns count cancelled."""
        return self._inner.revoke_task(task_name)  # type: ignore[no-any-return]

    # -- Job Archival --

    def archive(self, older_than: int = 86400) -> int:
        """Archive completed/dead/cancelled jobs older than the given age (seconds)."""
        return self._inner.archive_old_jobs(older_than)  # type: ignore[no-any-return]

    def list_archived(self, limit: int = 50, offset: int = 0) -> list[JobResult]:
        """List archived jobs with pagination."""
        py_jobs = self._inner.list_archived(limit=limit, offset=offset)
        return [JobResult(py_job=pj, queue=self) for pj in py_jobs]  # type: ignore[arg-type]

    def list_archived_after(self, limit: int = 50, after: str | None = None) -> Page[JobResult]:
        """Keyset-paginated :meth:`list_archived`."""
        py_jobs, next_cursor = self._inner.list_archived_after(limit=limit, after=after)
        items = [JobResult(py_job=pj, queue=self) for pj in py_jobs]  # type: ignore[arg-type]
        return Page(items=items, next_cursor=next_cursor)
