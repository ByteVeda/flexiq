"""Tests for Phase 5D cron-scheduled workflows."""

from __future__ import annotations

from flexiq import Queue
from flexiq.workflows import Workflow


def test_periodic_workflow_registers_launcher(queue: Queue) -> None:
    """@queue.periodic + @queue.workflow registers a launcher task."""

    @queue.task()
    def extract() -> str:
        return "data"

    @queue.periodic(cron="0 0 2 * * *")
    @queue.workflow("nightly")
    def nightly() -> Workflow:
        wf = Workflow()
        wf.step("extract", extract)
        return wf

    # The launcher task should be registered
    launcher_name = "_wf_launcher_nightly"
    assert launcher_name in queue._task_registry
    # The periodic config should reference the launcher
    assert any(pc["task_name"] == launcher_name for pc in queue._periodic_configs)
    # The workflow proxy should still be returned
    assert hasattr(nightly, "submit")
    assert hasattr(nightly, "build")
