"""FastAPI integration for flexiq.

Provides a plug-and-play APIRouter with job status, progress SSE streaming,
and dead letter queue management.

Usage::

    from fastapi import FastAPI
    from flexiq import Queue
    from flexiq.contrib.fastapi import FlexiQRouter

    queue = Queue()
    app = FastAPI()
    app.include_router(FlexiQRouter(queue), prefix="/tasks")

Requires the ``fastapi`` optional dependency::

    pip install flexiq[fastapi]
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncGenerator, Callable, Sequence
from typing import TYPE_CHECKING, Any

logger = logging.getLogger(__name__)

try:
    from fastapi import APIRouter, HTTPException, Query
    from fastapi.responses import StreamingResponse
    from pydantic import BaseModel
except ImportError as e:
    raise ImportError(
        "FastAPI integration requires 'fastapi' and 'pydantic'. "
        "Install with: pip install flexiq[fastapi]"
    ) from e

if TYPE_CHECKING:
    from flexiq.app import Queue


# ── Pydantic response models ────────────────────────────


class StatsResponse(BaseModel):
    """Queue statistics."""

    pending: int = 0
    running: int = 0
    completed: int = 0
    failed: int = 0
    dead: int = 0
    cancelled: int = 0


class JobResponse(BaseModel):
    """Job status and metadata."""

    id: str
    queue: str
    task_name: str
    status: str
    priority: int
    progress: int | None = None
    retry_count: int
    max_retries: int
    created_at: int
    scheduled_at: int
    started_at: int | None = None
    completed_at: int | None = None
    error: str | None = None
    timeout_ms: int
    unique_key: str | None = None
    metadata: str | None = None


class JobErrorResponse(BaseModel):
    """A single error from a job attempt."""

    id: str
    job_id: str
    attempt: int
    error: str
    failed_at: int


class CancelResponse(BaseModel):
    """Response from cancelling a job."""

    cancelled: bool


class RetryResponse(BaseModel):
    """Response from retrying a dead letter."""

    new_job_id: str


class DeadLetterResponse(BaseModel):
    """A dead letter queue entry."""

    id: str
    original_job_id: str
    queue: str
    task_name: str
    error: str | None = None
    retry_count: int
    failed_at: int
    metadata: str | None = None


class JobResultResponse(BaseModel):
    """Job result (serialized to string for safety)."""

    id: str
    status: str
    result: Any = None
    error: str | None = None


class HealthResponse(BaseModel):
    """Health check response."""

    status: str


class ReadinessResponse(BaseModel):
    """Readiness check response."""

    status: str
    checks: dict[str, Any]


# ── All known route names ────────────────────────────────

_ALL_ROUTES: set[str] = {
    "stats",
    "jobs",
    "job-errors",
    "job-result",
    "job-progress",
    "cancel",
    "dead-letters",
    "retry-dead",
    "health",
    "readiness",
    "resources",
    "queue-stats",
}


# ── Router factory ───────────────────────────────────────


class FlexiQRouter(APIRouter):
    """FastAPI APIRouter pre-configured with flexiq endpoints.

    Args:
        queue: The flexiq Queue instance to expose.
        include_routes: If set, only register these route names. Cannot be
            used together with ``exclude_routes``.
        exclude_routes: If set, skip these route names when registering.
        dependencies: FastAPI dependency list applied to every route.
        sse_poll_interval: Seconds between SSE progress polls (default 0.5).
        result_timeout: Default timeout for blocking result fetch (default 1.0).
        default_page_size: Default page size for paginated endpoints (default 20).
        max_page_size: Maximum allowed page size (default 100).
        result_serializer: Custom result serializer. Receives any value and
            must return a JSON-serializable value. Falls back to
            :func:`_safe_serialize`.
        **kwargs: Passed to ``APIRouter.__init__()`` (e.g. ``prefix``,
            ``tags``).

    Example::

        app.include_router(
            FlexiQRouter(queue, tags=["tasks"]),
            prefix="/tasks",
        )
    """

    def __init__(
        self,
        queue: Queue,
        *,
        include_routes: set[str] | None = None,
        exclude_routes: set[str] | None = None,
        dependencies: Sequence[Any] | None = None,
        sse_poll_interval: float = 0.5,
        result_timeout: float = 1.0,
        default_page_size: int = 20,
        max_page_size: int = 100,
        result_serializer: Callable[[Any], Any] | None = None,
        **kwargs: Any,
    ) -> None:
        if include_routes is not None and exclude_routes is not None:
            raise ValueError("Cannot specify both include_routes and exclude_routes")

        kwargs.setdefault("tags", ["flexiq"])
        if dependencies is not None:
            kwargs.setdefault("dependencies", list(dependencies))
        super().__init__(**kwargs)
        self._queue = queue
        self._sse_poll_interval = sse_poll_interval
        self._result_timeout = result_timeout
        self._default_page_size = default_page_size
        self._max_page_size = max_page_size
        self._result_serializer = result_serializer or _safe_serialize

        # Compute active route set
        if include_routes is not None:
            self._active_routes = include_routes & _ALL_ROUTES
        elif exclude_routes is not None:
            self._active_routes = _ALL_ROUTES - exclude_routes
        else:
            self._active_routes = _ALL_ROUTES

        self._register_routes()

    def _should_register(self, name: str) -> bool:
        return name in self._active_routes

    def _register_routes(self) -> None:
        queue = self._queue
        serialize_result = self._result_serializer
        result_timeout = self._result_timeout
        sse_interval = self._sse_poll_interval
        default_page = self._default_page_size
        max_page = self._max_page_size

        if self._should_register("stats"):

            @self.get("/stats", response_model=StatsResponse)
            async def get_stats() -> StatsResponse:
                """Get queue statistics."""
                stats = await queue.astats()
                return StatsResponse(**stats)

        if self._should_register("jobs"):

            @self.get("/jobs/{job_id}", response_model=JobResponse)
            def get_job(job_id: str) -> JobResponse:
                """Get a job by ID."""
                job = queue.get_job(job_id)
                if job is None:
                    raise HTTPException(status_code=404, detail="Job not found")
                return JobResponse(**job.to_dict())

        if self._should_register("job-errors"):

            @self.get("/jobs/{job_id}/errors", response_model=list[JobErrorResponse])
            def get_job_errors(job_id: str) -> list[JobErrorResponse]:
                """Get error history for a job."""
                errors = queue.job_errors(job_id)
                return [JobErrorResponse(**e) for e in errors]

        if self._should_register("job-result"):

            @self.get("/jobs/{job_id}/result", response_model=JobResultResponse)
            async def get_job_result(
                job_id: str,
                timeout: float = Query(default=0, ge=0, le=300),
            ) -> JobResultResponse:
                """Get job result. Set timeout > 0 for blocking wait."""
                job = queue.get_job(job_id)
                if job is None:
                    raise HTTPException(status_code=404, detail="Job not found")

                if timeout > 0 and job.status not in (
                    "complete",
                    "failed",
                    "dead",
                    "cancelled",
                ):
                    try:
                        result = await job.aresult(timeout=timeout)
                        return JobResultResponse(
                            id=job_id,
                            status="complete",
                            result=serialize_result(result),
                        )
                    except TimeoutError:
                        job.refresh()
                        return JobResultResponse(
                            id=job_id,
                            status=job.status,
                        )
                    except RuntimeError as e:
                        job.refresh()
                        return JobResultResponse(
                            id=job_id,
                            status=job.status,
                            error=str(e),
                        )

                d = job.to_dict()
                result = None
                if d["status"] == "complete":
                    try:
                        result = serialize_result(job.result(timeout=result_timeout))
                    except Exception:
                        logger.exception("Failed to deserialize result for job %s", job_id)

                return JobResultResponse(
                    id=job_id,
                    status=d["status"],
                    result=result,
                    error=d.get("error"),
                )

        if self._should_register("job-progress"):

            @self.get("/jobs/{job_id}/progress")
            async def stream_progress(
                job_id: str, include_results: bool = False
            ) -> StreamingResponse:
                """SSE stream of progress updates until job reaches terminal state.

                Pass ``?include_results=true`` to also stream partial results
                published via ``current_job.publish()``.
                """
                job = queue.get_job(job_id)
                if job is None:
                    raise HTTPException(status_code=404, detail="Job not found")

                poll_interval = sse_interval

                async def event_stream() -> AsyncGenerator[str, None]:
                    terminal = {"complete", "failed", "dead", "cancelled"}
                    last_seen_at: int = 0
                    while True:
                        refreshed = queue.get_job(job_id)
                        if refreshed is None:
                            yield f"data: {json.dumps({'status': 'not_found'})}\n\n"
                            return

                        d = refreshed.to_dict()
                        event: dict[str, Any] = {
                            "status": d["status"],
                            "progress": d["progress"],
                        }
                        yield f"data: {json.dumps(event)}\n\n"

                        if include_results:
                            logs = queue._inner.get_task_logs(job_id)
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
                                        partial = json.loads(extra)
                                    except (json.JSONDecodeError, TypeError):
                                        partial = extra
                                    result_event = {
                                        "status": d["status"],
                                        "progress": d["progress"],
                                        "partial_result": partial,
                                    }
                                    yield f"data: {json.dumps(result_event)}\n\n"

                        if d["status"] in terminal:
                            return

                        await asyncio.sleep(poll_interval)

                return StreamingResponse(
                    event_stream(),
                    media_type="text/event-stream",
                    headers={
                        "Cache-Control": "no-cache",
                        "X-Accel-Buffering": "no",
                    },
                )

        if self._should_register("cancel"):

            @self.post("/jobs/{job_id}/cancel", response_model=CancelResponse)
            async def cancel_job(job_id: str) -> CancelResponse:
                """Cancel a pending job."""
                ok = await queue.acancel_job(job_id)
                return CancelResponse(cancelled=ok)

        if self._should_register("dead-letters"):

            @self.get("/dead-letters", response_model=list[DeadLetterResponse])
            async def list_dead_letters(
                limit: int = Query(default=default_page, ge=1, le=max_page),
                offset: int = Query(default=0, ge=0),
            ) -> list[DeadLetterResponse]:
                """List dead letter queue entries."""
                dead = await queue.adead_letters(limit=limit, offset=offset)
                return [DeadLetterResponse(**d) for d in dead]

        if self._should_register("retry-dead"):

            @self.post("/dead-letters/{dead_id}/retry", response_model=RetryResponse)
            async def retry_dead_letter(dead_id: str) -> RetryResponse:
                """Re-enqueue a dead letter job."""
                new_id = await queue.aretry_dead(dead_id)
                return RetryResponse(new_job_id=new_id)

        if self._should_register("health"):

            @self.get("/health", response_model=HealthResponse)
            async def health() -> HealthResponse:
                """Liveness check."""
                from flexiq.health import check_health

                return HealthResponse(**check_health())

        if self._should_register("readiness"):

            @self.get("/readiness", response_model=ReadinessResponse)
            async def readiness() -> ReadinessResponse:
                """Readiness check."""
                from flexiq.health import check_readiness

                return ReadinessResponse(**check_readiness(queue))

        if self._should_register("resources"):

            @self.get("/resources")
            async def get_resources() -> list[dict[str, Any]]:
                """Get resource status for all registered worker resources."""
                return await queue.aresource_status()

        if self._should_register("queue-stats"):

            @self.get("/stats/queues")
            async def get_queue_stats(
                queue_name: str | None = Query(default=None, alias="queue"),
            ) -> dict[str, Any]:
                """Get per-queue stats. Filter by queue name if provided."""
                if queue_name:
                    return await queue.astats_by_queue(queue_name)
                return await queue.astats_all_queues()


def _safe_serialize(value: Any) -> Any:
    """Convert a value to something JSON-serializable."""
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (list, tuple)):
        return [_safe_serialize(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _safe_serialize(v) for k, v in value.items()}
    return str(value)
