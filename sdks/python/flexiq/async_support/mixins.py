"""Async convenience methods for the Queue class."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import signal
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, TypeVar

from flexiq.async_support.locks import AsyncDistributedLock

if TYPE_CHECKING:
    from collections.abc import Sequence
    from concurrent.futures import Executor

    from flexiq.context import LogLevel
    from flexiq.mixins.periodic import PeriodicInfo
    from flexiq.pagination import Page
    from flexiq.result import JobResult
    from flexiq.retention import EffectiveRetention, Retention, RetentionPreview

logger = logging.getLogger("flexiq")

_T = TypeVar("_T")


class AsyncQueueMixin:
    """Async wrappers that delegate to sync Queue methods via run_in_executor."""

    _executor: Executor
    _inner: Any

    # -- Expected interface from sibling mixins (type-checking only) --

    if TYPE_CHECKING:

        def stats(self) -> dict[str, int]: ...
        def stats_by_queue(self, queue_name: str) -> dict[str, int]: ...
        def stats_all_queues(self) -> dict[str, dict[str, int]]: ...
        def get_job(self, job_id: str) -> JobResult | None: ...
        def list_jobs(
            self,
            status: str | None = ...,
            queue: str | None = ...,
            task_name: str | None = ...,
            limit: int = ...,
            offset: int = ...,
            namespace: Any = ...,
        ) -> list[JobResult]: ...
        def list_jobs_filtered(
            self,
            status: str | None = ...,
            queue: str | None = ...,
            task_name: str | None = ...,
            metadata_like: str | None = ...,
            error_like: str | None = ...,
            created_after: int | None = ...,
            created_before: int | None = ...,
            limit: int = ...,
            offset: int = ...,
            namespace: Any = ...,
        ) -> list[JobResult]: ...
        def cancel_job(self, job_id: str) -> bool: ...
        def cancel_running_job(self, job_id: str) -> bool: ...
        def metrics(self, task_name: str | None = ..., since: int = ...) -> dict[str, Any]: ...
        def metrics_timeseries(
            self,
            task_name: str | None = ...,
            since: int = ...,
            bucket: int = ...,
        ) -> list[dict]: ...
        def job_dag(self, job_id: str) -> dict[str, Any]: ...
        def job_errors(self, job_id: str) -> list[dict]: ...
        def task_logs(self, job_id: str) -> list[dict]: ...
        def query_logs(
            self,
            task_name: str | None = ...,
            level: LogLevel | None = ...,
            since: int = ...,
            limit: int = ...,
        ) -> list[dict]: ...
        def dead_letters(self, limit: int = ..., offset: int = ...) -> list[dict]: ...
        def list_jobs_after(
            self,
            status: str | None = ...,
            queue: str | None = ...,
            task_name: str | None = ...,
            limit: int = ...,
            after: str | None = ...,
            namespace: Any = ...,
        ) -> Page[JobResult]: ...
        def list_jobs_filtered_after(
            self,
            status: str | None = ...,
            queue: str | None = ...,
            task_name: str | None = ...,
            metadata_like: str | None = ...,
            error_like: str | None = ...,
            created_after: int | None = ...,
            created_before: int | None = ...,
            limit: int = ...,
            after: str | None = ...,
            namespace: Any = ...,
        ) -> Page[JobResult]: ...
        def dead_letters_after(self, limit: int = ..., after: str | None = ...) -> Page[dict]: ...
        def list_archived_after(
            self, limit: int = ..., after: str | None = ...
        ) -> Page[JobResult]: ...
        def retry_dead(self, dead_id: str) -> str: ...
        def requeue_job(self, job_id: str) -> bool: ...
        def delete_dead(self, dead_id: str) -> bool: ...
        def replay(self, job_id: str) -> JobResult: ...
        def replay_history(self, job_id: str) -> list[dict]: ...
        def circuit_breakers(self) -> list[dict]: ...
        def workers(self) -> list[dict]: ...
        def run_worker(
            self,
            queues: Sequence[str] | None = ...,
            tags: list[str] | None = ...,
            pool: str = ...,
            app: str | None = ...,
        ) -> None: ...
        def purge_completed(self, older_than: int = ...) -> int: ...
        def purge_dead(self, older_than: int = ...) -> int: ...
        def revoke_task(self, task_name: str) -> int: ...
        def archive(self, older_than: int = ...) -> int: ...
        def list_archived(self, limit: int = ..., offset: int = ...) -> list[JobResult]: ...
        def pause(self, queue_name: str = ...) -> None: ...
        def resume(self, queue_name: str = ...) -> None: ...
        def paused_queues(self) -> list[str]: ...
        def effective_retention(self) -> EffectiveRetention | None: ...
        def dry_run_retention(self, retention: Retention | None = ...) -> RetentionPreview: ...
        def list_periodic(self) -> list[PeriodicInfo]: ...
        def delete_periodic(self, name: str) -> bool: ...
        def pause_periodic(self, name: str) -> bool: ...
        def resume_periodic(self, name: str) -> bool: ...
        def resource_status(self) -> list[dict[str, Any]]: ...
        def reload_resources(self, names: list[str] | None = ...) -> dict[str, bool]: ...

    # -----------------------------------------------------------------

    async def _run_sync(self, fn: Callable[..., _T], *args: Any, **kwargs: Any) -> _T:
        loop = asyncio.get_running_loop()
        if kwargs:
            return await loop.run_in_executor(self._executor, lambda: fn(*args, **kwargs))
        return await loop.run_in_executor(self._executor, lambda: fn(*args))

    # -- Inspection --

    async def astats(self) -> dict[str, int]:
        """Async version of :meth:`stats`."""
        return await self._run_sync(self.stats)

    async def astats_by_queue(self, queue_name: str) -> dict[str, int]:
        """Async version of :meth:`stats_by_queue`."""
        return await self._run_sync(self.stats_by_queue, queue_name)

    async def astats_all_queues(self) -> dict[str, dict[str, int]]:
        """Async version of :meth:`stats_all_queues`."""
        return await self._run_sync(self.stats_all_queues)

    async def acancel_job(self, job_id: str) -> bool:
        """Async version of :meth:`cancel_job`."""
        return await self._run_sync(self.cancel_job, job_id)

    async def ametrics(
        self,
        task_name: str | None = None,
        since: int = 3600,
    ) -> dict[str, Any]:
        """Async version of :meth:`metrics`."""
        return await self._run_sync(self.metrics, task_name=task_name, since=since)

    # -- Operations --

    async def adead_letters(self, limit: int = 10, offset: int = 0) -> list[dict]:
        """Async version of :meth:`dead_letters`."""
        return await self._run_sync(self.dead_letters, limit=limit, offset=offset)

    async def aretry_dead(self, dead_id: str) -> str:
        """Async version of :meth:`retry_dead`."""
        return await self._run_sync(self.retry_dead, dead_id)

    async def arequeue_job(self, job_id: str) -> bool:
        """Async version of :meth:`requeue_job`."""
        return await self._run_sync(self.requeue_job, job_id)

    async def adelete_dead(self, dead_id: str) -> bool:
        """Async version of :meth:`delete_dead`."""
        return await self._run_sync(self.delete_dead, dead_id)

    async def areplay(self, job_id: str) -> JobResult:
        """Async version of :meth:`replay`."""
        return await self._run_sync(self.replay, job_id)

    async def acircuit_breakers(self) -> list[dict]:
        """Async version of :meth:`circuit_breakers`."""
        return await self._run_sync(self.circuit_breakers)

    async def aget_job(self, job_id: str) -> JobResult | None:
        """Async version of :meth:`get_job`."""
        return await self._run_sync(self.get_job, job_id)

    async def alist_jobs(self, **kwargs: Any) -> list[JobResult]:
        """Async version of :meth:`list_jobs`."""
        return await self._run_sync(self.list_jobs, **kwargs)

    async def alist_jobs_filtered(self, **kwargs: Any) -> list[JobResult]:
        """Async version of :meth:`list_jobs_filtered`."""
        return await self._run_sync(self.list_jobs_filtered, **kwargs)

    async def alist_jobs_after(self, **kwargs: Any) -> Page[JobResult]:
        """Async version of :meth:`list_jobs_after`."""
        return await self._run_sync(self.list_jobs_after, **kwargs)

    async def alist_jobs_filtered_after(self, **kwargs: Any) -> Page[JobResult]:
        """Async version of :meth:`list_jobs_filtered_after`."""
        return await self._run_sync(self.list_jobs_filtered_after, **kwargs)

    async def adead_letters_after(self, limit: int = 10, after: str | None = None) -> Page[dict]:
        """Async version of :meth:`dead_letters_after`."""
        return await self._run_sync(self.dead_letters_after, limit=limit, after=after)

    async def alist_archived_after(
        self, limit: int = 50, after: str | None = None
    ) -> Page[JobResult]:
        """Async version of :meth:`list_archived_after`."""
        return await self._run_sync(self.list_archived_after, limit=limit, after=after)

    async def ajob_dag(self, job_id: str) -> dict[str, Any]:
        """Async version of :meth:`job_dag`."""
        return await self._run_sync(self.job_dag, job_id)

    async def ametrics_timeseries(self, **kwargs: Any) -> list[dict]:
        """Async version of :meth:`metrics_timeseries`."""
        return await self._run_sync(self.metrics_timeseries, **kwargs)

    async def ajob_errors(self, job_id: str) -> list[dict]:
        """Async version of :meth:`job_errors`."""
        return await self._run_sync(self.job_errors, job_id)

    async def atask_logs(self, job_id: str) -> list[dict]:
        """Async version of :meth:`task_logs`."""
        return await self._run_sync(self.task_logs, job_id)

    async def aquery_logs(self, **kwargs: Any) -> list[dict]:
        """Async version of :meth:`query_logs`."""
        return await self._run_sync(self.query_logs, **kwargs)

    async def acancel_running_job(self, job_id: str) -> bool:
        """Async version of :meth:`cancel_running_job`."""
        return await self._run_sync(self.cancel_running_job, job_id)

    async def aworkers(self) -> list[dict]:
        """Async version of :meth:`workers`."""
        return await self._run_sync(self.workers)

    # -- Operations --

    async def aenqueue_many(self, **kwargs: Any) -> list[JobResult]:
        """Async version of :meth:`enqueue_many`."""
        return await self._run_sync(self.enqueue_many, **kwargs)  # type: ignore[attr-defined]

    async def apurge_completed(self, older_than: int = 86400) -> int:
        """Async version of :meth:`purge_completed`."""
        return await self._run_sync(self.purge_completed, older_than=older_than)

    async def apurge_dead(self, older_than: int = 86400) -> int:
        """Async version of :meth:`purge_dead`."""
        return await self._run_sync(self.purge_dead, older_than=older_than)

    async def arevoke_task(self, task_name: str) -> int:
        """Async version of :meth:`revoke_task`."""
        return await self._run_sync(self.revoke_task, task_name)

    async def areplay_history(self, job_id: str) -> list[dict]:
        """Async version of :meth:`replay_history`."""
        return await self._run_sync(self.replay_history, job_id)

    async def aarchive(self, older_than: int = 86400) -> int:
        """Async version of :meth:`archive`."""
        return await self._run_sync(self.archive, older_than=older_than)

    async def alist_archived(self, limit: int = 50, offset: int = 0) -> list[JobResult]:
        """Async version of :meth:`list_archived`."""
        return await self._run_sync(self.list_archived, limit=limit, offset=offset)

    async def apause(self, queue_name: str = "default") -> None:
        """Async version of :meth:`pause`."""
        await self._run_sync(self.pause, queue_name=queue_name)

    async def aresume(self, queue_name: str = "default") -> None:
        """Async version of :meth:`resume`."""
        await self._run_sync(self.resume, queue_name=queue_name)

    async def apaused_queues(self) -> list[str]:
        """Async version of :meth:`paused_queues`."""
        return await self._run_sync(self.paused_queues)

    async def aeffective_retention(self) -> EffectiveRetention | None:
        """Async version of :meth:`effective_retention`."""
        return await self._run_sync(self.effective_retention)

    async def adry_run_retention(self, retention: Retention | None = None) -> RetentionPreview:
        """Async version of :meth:`dry_run_retention`."""
        return await self._run_sync(self.dry_run_retention, retention)

    # -- Periodic --

    async def alist_periodic(self) -> list[PeriodicInfo]:
        """Async version of :meth:`list_periodic`."""
        return await self._run_sync(self.list_periodic)

    async def adelete_periodic(self, name: str) -> bool:
        """Async version of :meth:`delete_periodic`."""
        return await self._run_sync(self.delete_periodic, name)

    async def apause_periodic(self, name: str) -> bool:
        """Async version of :meth:`pause_periodic`."""
        return await self._run_sync(self.pause_periodic, name)

    async def aresume_periodic(self, name: str) -> bool:
        """Async version of :meth:`resume_periodic`."""
        return await self._run_sync(self.resume_periodic, name)

    # -- Locks --

    def alock(
        self,
        name: str,
        ttl: float = 30.0,
        auto_extend: bool = True,
        owner_id: str | None = None,
        timeout: float | None = None,
        retry_interval: float = 0.1,
    ) -> AsyncDistributedLock:
        """Return an async distributed lock context manager.

        Args:
            name: Lock name (unique across the cluster).
            ttl: Lock TTL in seconds. Auto-extended at ttl/3 intervals.
            auto_extend: Whether to auto-extend the lock in a background thread.
            owner_id: Unique owner identifier. Auto-generated if not provided.
            timeout: Max seconds to wait for acquisition. None = fail immediately.
            retry_interval: Seconds between retries when timeout is set.
        """
        return AsyncDistributedLock(
            inner=self._inner,
            name=name,
            ttl=ttl,
            owner_id=owner_id,
            auto_extend=auto_extend,
            timeout=timeout,
            retry_interval=retry_interval,
        )

    # -- Worker --

    async def arun_worker(
        self,
        queues: Sequence[str] | None = None,
        tags: list[str] | None = None,
        pool: str = "thread",
        app: str | None = None,
    ) -> None:
        """Async version of :meth:`run_worker`.

        Runs the blocking worker loop in a thread executor so it does not
        block the asyncio event loop.

        Args:
            queues: List of queue names to consume from.
            tags: Optional tags for worker specialization / routing.
            pool: Worker pool type — ``"thread"`` (default) or ``"prefork"``.
            app: Import path to the Queue instance (e.g. ``"myapp:queue"``).
                Required when ``pool="prefork"``.
        """
        loop = asyncio.get_running_loop()

        original_sigint = signal.getsignal(signal.SIGINT)
        original_sigterm = signal.getsignal(signal.SIGTERM)

        def _shutdown_once() -> None:
            logger.info("Warm shutdown (waiting for running tasks to finish)...")
            self._inner.request_shutdown()
            with contextlib.suppress(NotImplementedError):
                loop.remove_signal_handler(signal.SIGINT)
                loop.remove_signal_handler(signal.SIGTERM)
            signal.signal(signal.SIGINT, original_sigint)
            signal.signal(signal.SIGTERM, original_sigterm)

        loop.add_signal_handler(signal.SIGINT, _shutdown_once)
        loop.add_signal_handler(signal.SIGTERM, _shutdown_once)

        try:
            await loop.run_in_executor(
                None,
                lambda: self.run_worker(queues=queues, tags=tags, pool=pool, app=app),
            )
        finally:
            with contextlib.suppress(NotImplementedError):
                loop.remove_signal_handler(signal.SIGINT)
                loop.remove_signal_handler(signal.SIGTERM)
            signal.signal(signal.SIGINT, original_sigint)
            signal.signal(signal.SIGTERM, original_sigterm)

    # -- Resources --

    async def aresource_status(self) -> list[dict[str, Any]]:
        """Async version of :meth:`resource_status`."""
        return self.resource_status()

    async def areload_resources(self, names: list[str] | None = None) -> dict[str, bool]:
        """Async version of :meth:`reload_resources`."""
        return await self._run_sync(self.reload_resources, names)
