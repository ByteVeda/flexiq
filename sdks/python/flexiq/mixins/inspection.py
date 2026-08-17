"""Read-only inspection, stats, and query methods for the Queue."""

from __future__ import annotations

import time
from collections import defaultdict
from typing import Any

from flexiq.mixins._shared import _UNSET
from flexiq.pagination import Page
from flexiq.result import JobResult
from flexiq.retention import EffectiveRetention, Retention, RetentionPreview


class QueueInspectionMixin:
    """Read-only inspection, stats, and query methods for the Queue."""

    _inner: Any
    _namespace: str | None

    def get_job(self, job_id: str) -> JobResult | None:
        """Retrieve a job by its unique ID."""
        py_job = self._inner.get_job(job_id)
        if py_job is None:
            return None
        return JobResult(py_job=py_job, queue=self)  # type: ignore[arg-type]

    def list_jobs(
        self,
        status: str | None = None,
        queue: str | None = None,
        task_name: str | None = None,
        limit: int = 50,
        offset: int = 0,
        namespace: Any = _UNSET,
    ) -> list[JobResult]:
        """List jobs with optional filters and pagination.

        By default, scoped to this queue's namespace. Pass ``namespace=None``
        explicitly to see jobs across all namespaces.
        """
        ns = self._namespace if namespace is _UNSET else namespace
        py_jobs = self._inner.list_jobs(
            status=status,
            queue=queue,
            task_name=task_name,
            limit=limit,
            offset=offset,
            namespace=ns,
        )
        return [JobResult(py_job=pj, queue=self) for pj in py_jobs]  # type: ignore[arg-type]

    def list_jobs_filtered(
        self,
        status: str | None = None,
        queue: str | None = None,
        task_name: str | None = None,
        metadata_like: str | None = None,
        error_like: str | None = None,
        created_after: int | None = None,
        created_before: int | None = None,
        limit: int = 50,
        offset: int = 0,
        namespace: Any = _UNSET,
    ) -> list[JobResult]:
        """List jobs with extended filters.

        By default, scoped to this queue's namespace. Pass ``namespace=None``
        explicitly to see jobs across all namespaces.
        """
        ns = self._namespace if namespace is _UNSET else namespace
        py_jobs = self._inner.list_jobs_filtered(
            status=status,
            queue=queue,
            task_name=task_name,
            metadata_like=metadata_like,
            error_like=error_like,
            created_after=created_after,
            created_before=created_before,
            limit=limit,
            offset=offset,
            namespace=ns,
        )
        return [JobResult(py_job=pj, queue=self) for pj in py_jobs]  # type: ignore[arg-type]

    def list_jobs_after(
        self,
        status: str | None = None,
        queue: str | None = None,
        task_name: str | None = None,
        limit: int = 50,
        after: str | None = None,
        namespace: Any = _UNSET,
    ) -> Page[JobResult]:
        """Keyset-paginated :meth:`list_jobs`.

        Returns a :class:`~flexiq.pagination.Page`; pass its ``next_cursor``
        back as ``after`` to fetch the next page. O(page) at any depth and
        stable under concurrent inserts, unlike offset pagination.
        """
        ns = self._namespace if namespace is _UNSET else namespace
        py_jobs, next_cursor = self._inner.list_jobs_after(
            status=status,
            queue=queue,
            task_name=task_name,
            limit=limit,
            after=after,
            namespace=ns,
        )
        items = [JobResult(py_job=pj, queue=self) for pj in py_jobs]  # type: ignore[arg-type]
        return Page(items=items, next_cursor=next_cursor)

    def list_jobs_filtered_after(
        self,
        status: str | None = None,
        queue: str | None = None,
        task_name: str | None = None,
        metadata_like: str | None = None,
        error_like: str | None = None,
        created_after: int | None = None,
        created_before: int | None = None,
        limit: int = 50,
        after: str | None = None,
        namespace: Any = _UNSET,
    ) -> Page[JobResult]:
        """Keyset-paginated :meth:`list_jobs_filtered`."""
        ns = self._namespace if namespace is _UNSET else namespace
        py_jobs, next_cursor = self._inner.list_jobs_filtered_after(
            status=status,
            queue=queue,
            task_name=task_name,
            metadata_like=metadata_like,
            error_like=error_like,
            created_after=created_after,
            created_before=created_before,
            limit=limit,
            after=after,
            namespace=ns,
        )
        items = [JobResult(py_job=pj, queue=self) for pj in py_jobs]  # type: ignore[arg-type]
        return Page(items=items, next_cursor=next_cursor)

    def stats(self) -> dict[str, int]:
        """Get queue statistics as a dict of status counts."""
        return self._inner.stats()  # type: ignore[no-any-return]

    def stats_by_queue(self, queue_name: str) -> dict[str, int]:
        """Get queue statistics for a specific queue."""
        return self._inner.stats_by_queue(queue_name)  # type: ignore[no-any-return]

    def stats_all_queues(self) -> dict[str, dict[str, int]]:
        """Get queue statistics broken down by queue name."""
        return self._inner.stats_all_queues()  # type: ignore[no-any-return]

    def job_dag(self, job_id: str) -> dict[str, Any]:
        """Get the dependency DAG for a job.

        Returns a dict with ``nodes`` (list of job dicts) and ``edges``
        (list of ``{from, to}`` dicts).
        """
        visited: set[str] = set()
        nodes: list[dict[str, Any]] = []
        edges: list[dict[str, str]] = []

        def walk(jid: str) -> None:
            if jid in visited:
                return
            visited.add(jid)
            job = self.get_job(jid)
            if job is None:
                return
            nodes.append(job.to_dict())
            for dep_id in self._inner.get_dependencies(jid):
                edges.append({"from": dep_id, "to": jid})
                walk(dep_id)
            for dep_id in self._inner.get_dependents(jid):
                edges.append({"from": jid, "to": dep_id})
                walk(dep_id)

        walk(job_id)
        return {"nodes": nodes, "edges": edges}

    def metrics_timeseries(
        self,
        task_name: str | None = None,
        since: int = 3600,
        bucket: int = 60,
    ) -> list[dict[str, Any]]:
        """Get metrics aggregated into time buckets.

        Args:
            task_name: Filter to a specific task.
            since: Lookback window in seconds.
            bucket: Bucket size in seconds.

        Returns:
            List of dicts with ``timestamp``, ``count``, ``success``,
            ``failure``, ``avg_ms``, ``p50_ms``, ``p95_ms``, ``p99_ms`` keys.
        """
        raw = self._inner.get_metrics(task_name=task_name, since_seconds=since)
        now_ms = int(time.time() * 1000)
        bucket_ms = bucket * 1000

        buckets: dict[int, list[dict]] = {}
        for r in raw:
            ts = r.get("recorded_at", 0)
            bucket_key = ((ts - (now_ms - since * 1000)) // bucket_ms) * bucket_ms + (
                now_ms - since * 1000
            )
            buckets.setdefault(bucket_key, []).append(r)

        result = []
        for ts in sorted(buckets):
            records = buckets[ts]
            n = len(records)
            success = sum(1 for r in records if r.get("succeeded"))
            times = sorted(r["wall_time_ns"] / 1_000_000 for r in records)
            result.append(
                {
                    "timestamp": ts,
                    "count": n,
                    "success": success,
                    "failure": n - success,
                    "avg_ms": round(sum(times) / n, 2) if n else 0,
                    "p50_ms": _percentile(times, 0.50),
                    "p95_ms": _percentile(times, 0.95),
                    "p99_ms": _percentile(times, 0.99),
                }
            )

        return result

    def metrics(
        self,
        task_name: str | None = None,
        since: int = 3600,
    ) -> dict[str, Any]:
        """Get aggregated metrics for tasks."""
        raw = self._inner.get_metrics(task_name=task_name, since_seconds=since)
        return _aggregate_metrics(raw)

    def cancel_job(self, job_id: str) -> bool:
        """Cancel a pending job before it starts executing."""
        return self._inner.cancel_job(job_id)  # type: ignore[no-any-return]

    def cancel_running_job(self, job_id: str) -> bool:
        """Request cancellation of a running job.

        The task must call ``current_job.check_cancelled()`` to observe
        the cancellation. Returns True if the cancel was requested.
        """
        return self._inner.request_cancel(job_id)  # type: ignore[no-any-return]

    def update_progress(self, job_id: str, progress: int) -> None:
        """Update the progress percentage for a running job."""
        self._inner.update_progress(job_id, progress)

    def job_errors(self, job_id: str) -> list[dict]:
        """Get the error history for a job."""
        return self._inner.get_job_errors(job_id)  # type: ignore[no-any-return]

    def purge_completed(self, older_than: int = 86400) -> int:
        """Delete completed jobs older than a given age."""
        return self._inner.purge_completed(older_than)  # type: ignore[no-any-return]

    def effective_retention(self) -> EffectiveRetention | None:
        """The retention windows a worker is applying to this namespace.

        Reported by the elected cleaner on each sweep, so it reflects the
        worker's configuration rather than this process's. ``None`` means no
        worker has swept yet — distinct from retention being disabled, which
        reports with ``enabled=False``.
        """
        raw = self._inner.effective_retention()
        if raw is None:
            return None
        return EffectiveRetention._from_json(raw)

    def dry_run_retention(self, retention: Retention | None = None) -> RetentionPreview:
        """Preview what a retention purge would delete, without deleting anything.

        Counts the rows each history table's purge would remove right now. With
        no argument the preview uses **this queue's** configured (or
        default-recommended) windows; pass candidate ``retention`` windows to
        size a window before committing to it — no worker reconfiguration needed.
        The counts are a point-in-time snapshot; nothing is deleted. Unlike
        :meth:`effective_retention`, this runs in-process against the live data,
        so it never returns ``None``.
        """
        windows = retention._as_map() if retention is not None else None
        return RetentionPreview._from_json(self._inner.dry_run_retention(windows))


def _aggregate_metrics(raw: list[dict]) -> dict[str, Any]:
    """Aggregate raw metric rows into per-task statistics."""
    by_task: dict[str, list[dict]] = defaultdict(list)
    for r in raw:
        by_task[r["task_name"]].append(r)

    result = {}
    for task_name, records in by_task.items():
        times = sorted(r["wall_time_ns"] / 1_000_000 for r in records)  # ms
        n = len(times)
        success = sum(1 for r in records if r["succeeded"])

        result[task_name] = {
            "count": n,
            "success_count": success,
            "failure_count": n - success,
            "avg_ms": round(sum(times) / n, 2) if n else 0,
            "p50_ms": _percentile(times, 0.50),
            "p95_ms": _percentile(times, 0.95),
            "p99_ms": _percentile(times, 0.99),
            "min_ms": round(times[0], 2) if n else 0,
            "max_ms": round(times[-1], 2) if n else 0,
        }

    return result


def _percentile(sorted_values: list[float], q: float) -> float:
    """Nearest-rank percentile from a sorted list, rounded to 2 decimals."""
    n = len(sorted_values)
    if n == 0:
        return 0
    idx = min(int(n * q), n - 1)
    return round(sorted_values[idx], 2)
