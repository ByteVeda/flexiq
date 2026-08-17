"""Job-related route handlers."""

from __future__ import annotations

from typing import TYPE_CHECKING

from flexiq.dashboard.errors import _NotFound
from flexiq.dashboard.handlers._qs import _parse_int_qs

if TYPE_CHECKING:
    from flexiq.app import Queue


def _handle_list_jobs(queue: Queue, qs: dict) -> list[dict]:
    status = qs.get("status", [None])[0]
    q = qs.get("queue", [None])[0]
    task = qs.get("task", [None])[0]
    metadata_like = qs.get("metadata", [None])[0]
    error_like = qs.get("error", [None])[0]
    created_after = qs.get("created_after", [None])[0]
    created_before = qs.get("created_before", [None])[0]
    limit = _parse_int_qs(qs, "limit", 20)
    offset = _parse_int_qs(qs, "offset", 0)

    if any(x is not None for x in [metadata_like, error_like, created_after, created_before]):
        ca = int(created_after) if created_after else None
        cb = int(created_before) if created_before else None
        jobs = queue.list_jobs_filtered(
            status=status,
            queue=q,
            task_name=task,
            metadata_like=metadata_like,
            error_like=error_like,
            created_after=ca,
            created_before=cb,
            limit=limit,
            offset=offset,
        )
    else:
        jobs = queue.list_jobs(status=status, queue=q, task_name=task, limit=limit, offset=offset)
    return [j.to_dict() for j in jobs]


def _handle_get_job(queue: Queue, _qs: dict, job_id: str) -> dict:
    job = queue.get_job(job_id)
    if job is None:
        raise _NotFound("Job not found")
    return job.to_dict()


def _handle_replay_post(queue: Queue, job_id: str) -> dict:
    result = queue.replay(job_id)
    return {"replay_job_id": result.id}
