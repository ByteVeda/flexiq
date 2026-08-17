"""HTTP route tests for ``/api/workflows/*``.

Exercises both the PyO3 query bindings (list_workflow_runs etc.) and the
dashboard handler glue. Each test boots a real dashboard server on a
random port and drives it through the standard AuthedClient + admin
session pattern used by the other dashboard tests.
"""

from __future__ import annotations

import threading
from collections.abc import Generator
from pathlib import Path
from typing import Any

import pytest

from flexiq import Queue
from flexiq.dashboard._testing import AuthedClient, seed_admin_and_session
from flexiq.workflows import Workflow


def _start_dashboard(queue: Queue) -> tuple[str, Any]:
    from http.server import ThreadingHTTPServer

    from flexiq.dashboard import _make_handler

    handler = _make_handler(queue, static_assets=None)
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return f"http://127.0.0.1:{port}", server


@pytest.fixture
def workflows_dashboard(tmp_path: Path) -> Generator[tuple[AuthedClient, Queue]]:
    db_path = str(tmp_path / "wf_dashboard.db")
    q = Queue(db_path=db_path, workers=2)

    @q.task()
    def step_one() -> int:
        return 1

    @q.task()
    def step_two(x: int) -> int:
        return x + 1

    wf = Workflow(name="dash_test_workflow")
    wf.step("a", step_one)
    wf.step("b", step_two, after="a", args=("{{a}}",))
    run = q.submit_workflow(wf)
    # Don't run the worker — we only need the workflow row, not its execution.
    _ = run

    url, server = _start_dashboard(q)
    session = seed_admin_and_session(q)
    client = AuthedClient(base=url, session=session)
    try:
        yield client, q
    finally:
        server.shutdown()


# ── List endpoint ─────────────────────────────────────────────────────────


def test_list_workflow_runs_returns_paginated_list(
    workflows_dashboard: tuple[AuthedClient, Queue],
) -> None:
    client, _ = workflows_dashboard
    data = client.get("/api/workflows/runs")
    assert "runs" in data
    assert "limit" in data
    assert "offset" in data
    assert isinstance(data["runs"], list)
    assert len(data["runs"]) >= 1
    run = data["runs"][0]
    # Required fields per the api-types contract — all timestamps Unix ms.
    for key in (
        "id",
        "definition_id",
        "state",
        "created_at",
    ):
        assert key in run, f"missing key: {key}"
    assert isinstance(run["created_at"], int)


def test_list_workflow_runs_cursor_mode_starts_without_a_cursor(
    workflows_dashboard: tuple[AuthedClient, Queue],
) -> None:
    """``paginate=cursor`` opens keyset mode at the first page.

    Clients have no cursor to send for page one, so without this they could
    never enter cursor mode at all.
    """
    client, _ = workflows_dashboard
    data = client.get("/api/workflows/runs?paginate=cursor")
    assert "next_cursor" in data, "cursor mode must echo a next_cursor"
    assert "offset" not in data, "cursor mode does not page by offset"
    assert len(data["runs"]) >= 1


def test_list_workflow_runs_cursor_pages_every_run_once(
    workflows_dashboard: tuple[AuthedClient, Queue],
) -> None:
    """Walking from the first cursor page reaches every run exactly once."""
    client, q = workflows_dashboard
    expected = {r.id for r in q._inner.list_workflow_runs(limit=100, offset=0)}

    seen: list[str] = []
    query = "/api/workflows/runs?limit=1&paginate=cursor"
    while True:
        data = client.get(query)
        seen.extend(r["id"] for r in data["runs"])
        cursor = data["next_cursor"]
        if cursor is None:
            break
        query = f"/api/workflows/runs?limit=1&after={cursor}"

    assert len(seen) == len(set(seen)), "no run may appear on two pages"
    assert set(seen) == expected


def test_list_workflow_runs_respects_limit_offset(
    workflows_dashboard: tuple[AuthedClient, Queue],
) -> None:
    client, _ = workflows_dashboard
    data = client.get("/api/workflows/runs?limit=1&offset=0")
    assert data["limit"] == 1
    assert data["offset"] == 0
    assert len(data["runs"]) <= 1


def test_list_workflow_runs_filter_by_state(
    workflows_dashboard: tuple[AuthedClient, Queue],
) -> None:
    client, _ = workflows_dashboard
    # Filtering by an impossible state yields zero rows but doesn't error.
    data = client.get("/api/workflows/runs?state=cancelled")
    assert data["runs"] == []


# ── Detail endpoint ───────────────────────────────────────────────────────


def test_get_workflow_run_returns_run_and_nodes(
    workflows_dashboard: tuple[AuthedClient, Queue],
) -> None:
    client, _ = workflows_dashboard
    list_data = client.get("/api/workflows/runs")
    run_id = list_data["runs"][0]["id"]

    detail = client.get(f"/api/workflows/runs/{run_id}")
    assert "run" in detail
    assert "nodes" in detail
    assert detail["run"]["id"] == run_id
    assert isinstance(detail["nodes"], list)
    # Each node carries compensation fields (possibly None).
    for node in detail["nodes"]:
        for key in (
            "node_name",
            "status",
            "compensation_job_id",
            "compensation_started_at",
            "compensation_completed_at",
            "compensation_error",
        ):
            assert key in node, f"missing compensation key {key}"


def test_get_workflow_run_404(workflows_dashboard: tuple[AuthedClient, Queue]) -> None:
    client, _ = workflows_dashboard
    with pytest.raises(Exception) as info:
        client.get("/api/workflows/runs/does-not-exist")
    assert "404" in str(info.value) or "not found" in str(info.value).lower()


# ── DAG endpoint ──────────────────────────────────────────────────────────


def test_get_workflow_dag_returns_dag_string(
    workflows_dashboard: tuple[AuthedClient, Queue],
) -> None:
    client, _ = workflows_dashboard
    list_data = client.get("/api/workflows/runs")
    run_id = list_data["runs"][0]["id"]

    dag_data = client.get(f"/api/workflows/runs/{run_id}/dag")
    assert "dag" in dag_data
    assert isinstance(dag_data["dag"], str)
    # The DAG JSON should at least mention both step names.
    assert "a" in dag_data["dag"]
    assert "b" in dag_data["dag"]


# ── Children endpoint ─────────────────────────────────────────────────────


def test_get_workflow_children_empty_for_top_level_run(
    workflows_dashboard: tuple[AuthedClient, Queue],
) -> None:
    client, _ = workflows_dashboard
    list_data = client.get("/api/workflows/runs")
    run_id = list_data["runs"][0]["id"]

    children_data = client.get(f"/api/workflows/runs/{run_id}/children")
    assert "children" in children_data
    assert children_data["children"] == []
