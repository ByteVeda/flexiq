"""Workflow-related dashboard API handlers.

All timestamps in the returned JSON are Unix milliseconds — matches the
contract documented at the top of ``dashboard/src/lib/api-types.ts``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from flexiq.dashboard.errors import _NotFound
from flexiq.dashboard.handlers._qs import _parse_int_qs

if TYPE_CHECKING:
    from flexiq.app import Queue


def _run_to_dict(run: Any) -> dict[str, Any]:
    return {
        "id": run.id,
        "definition_id": run.definition_id,
        "state": run.state,
        "params": run.params,
        "started_at": run.started_at,
        "completed_at": run.completed_at,
        "error": run.error,
        "parent_run_id": run.parent_run_id,
        "parent_node_name": run.parent_node_name,
        "created_at": run.created_at,
    }


def _node_to_dict(node: Any) -> dict[str, Any]:
    return {
        "node_name": node.node_name,
        "status": node.status,
        "job_id": node.job_id,
        "result_hash": node.result_hash,
        "fan_out_count": node.fan_out_count,
        "started_at": node.started_at,
        "completed_at": node.completed_at,
        "error": node.error,
        "compensation_job_id": node.compensation_job_id,
        "compensation_started_at": node.compensation_started_at,
        "compensation_completed_at": node.compensation_completed_at,
        "compensation_error": node.compensation_error,
    }


def _handle_list_workflow_runs(queue: Queue, qs: dict) -> dict[str, Any]:
    """``GET /api/workflows/runs`` — paginated list with optional filters.

    Supports both offset (``limit``/``offset``) and keyset pagination. Keyset
    mode is selected by ``paginate=cursor`` for the first page — a client has no
    cursor yet — or by an ``after`` cursor for each page after it. The response
    carries a ``next_cursor`` to pass back as the next ``after``, and ``None``
    once the last page is reached.
    """
    definition_name = qs.get("definition_name", [None])[0]
    state = qs.get("state", [None])[0]
    limit = _parse_int_qs(qs, "limit", 50)
    after = qs.get("after", [None])[0]

    if after is not None or qs.get("paginate", [None])[0] == "cursor":
        runs, next_cursor = queue._inner.list_workflow_runs_after(
            definition_name=definition_name,
            state=state,
            limit=limit,
            after=after,
        )
        return {
            "runs": [_run_to_dict(r) for r in runs],
            "limit": limit,
            "next_cursor": next_cursor,
        }

    offset = _parse_int_qs(qs, "offset", 0)
    runs = queue._inner.list_workflow_runs(
        definition_name=definition_name,
        state=state,
        limit=limit,
        offset=offset,
    )
    return {
        "runs": [_run_to_dict(r) for r in runs],
        "limit": limit,
        "offset": offset,
    }


def _handle_get_workflow_run(queue: Queue, _qs: dict, run_id: str) -> dict[str, Any]:
    """``GET /api/workflows/runs/{run_id}`` — run header + per-node detail."""
    try:
        run, nodes = queue._inner.get_workflow_run_detail(run_id)
    except (ValueError, RuntimeError) as exc:
        raise _NotFound(str(exc)) from exc
    return {
        "run": _run_to_dict(run),
        "nodes": [_node_to_dict(n) for n in nodes],
    }


def _handle_get_workflow_dag(queue: Queue, _qs: dict, run_id: str) -> dict[str, Any]:
    """``GET /api/workflows/runs/{run_id}/dag`` — DAG JSON string.

    Returns the raw DAG JSON wrapped in ``{"dag": ...}`` so the frontend
    can parse it directly. PyO3 surfaces the underlying ``Vec<u8>`` as
    a Python ``list[int]``; we coerce to ``bytes`` then decode UTF-8.
    """
    try:
        dag_raw = queue._inner.get_workflow_definition_dag(run_id)
    except (ValueError, RuntimeError) as exc:
        raise _NotFound(str(exc)) from exc
    dag_bytes = bytes(dag_raw) if isinstance(dag_raw, list) else dag_raw
    return {"dag": dag_bytes.decode("utf-8")}


def _handle_get_workflow_children(queue: Queue, _qs: dict, run_id: str) -> dict[str, Any]:
    """``GET /api/workflows/runs/{run_id}/children`` — child sub-workflow runs."""
    children = queue._inner.get_child_workflow_runs(run_id)
    return {"children": [_run_to_dict(r) for r in children]}
