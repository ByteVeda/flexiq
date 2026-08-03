"""Tests for periodic (cron-scheduled) tasks."""

import threading
from collections.abc import Generator
from pathlib import Path
from typing import Any

import pytest

from taskito import PeriodicInfo, Queue


@pytest.fixture
def queue(tmp_path: Path) -> Queue:
    db_path = str(tmp_path / "test_periodic.db")
    return Queue(db_path=db_path, workers=1)


@pytest.fixture
def registered(queue: Queue, poll_until: Any) -> Generator[Queue]:
    """A queue whose declared schedules have reached storage.

    ``@queue.periodic()`` only records a declaration; ``run_worker()`` is what
    writes it to the catalog, so the catalog methods need a started worker to
    have anything to act on. Both crons here are daily, so neither fires during
    a test.
    """

    @queue.periodic(cron="0 0 0 * * *", name="nightly")
    def nightly() -> str:
        return "nightly"

    @queue.periodic(cron="0 30 3 * * *", name="cleanup", timezone="America/New_York")
    def cleanup() -> str:
        return "cleanup"

    worker = threading.Thread(target=queue.run_worker, daemon=True)
    worker.start()
    try:
        poll_until(
            lambda: len(queue.list_periodic()) == 2,
            timeout=30,
            message="periodic schedules never reached the catalog",
        )
        yield queue
    finally:
        # Also covers a failed poll_until, which would otherwise strand the
        # worker and its database handle for every later test.
        queue.shutdown()
        worker.join(timeout=5)


def _find(schedules: list[PeriodicInfo], name: str) -> PeriodicInfo:
    """The schedule registered under ``name``, or a failed assertion."""
    match = [p for p in schedules if p.name == name]
    assert match, f"expected periodic schedule '{name}'"
    return match[0]


def test_periodic_task_registration(queue: Queue) -> None:
    """Periodic tasks are registered as both regular tasks and periodic configs."""

    @queue.periodic(cron="0 * * * * *")
    def every_minute() -> str:
        return "tick"

    assert every_minute.name.endswith("every_minute")
    assert every_minute.name in queue._task_registry
    assert len(queue._periodic_configs) == 1
    assert queue._periodic_configs[0]["cron_expr"] == "0 * * * * *"


def test_periodic_task_direct_call(queue: Queue) -> None:
    """Periodic tasks can still be called directly like regular tasks."""

    @queue.periodic(cron="0 * * * * *")
    def add(a: int, b: int) -> int:
        return a + b

    assert add(3, 4) == 7


def test_periodic_task_triggers(queue: Queue, poll_until: Any) -> None:
    """Periodic task gets enqueued by the scheduler when due."""
    results: list[int] = []

    @queue.periodic(cron="* * * * * *")  # every second
    def frequent_task() -> str:
        results.append(1)
        return "done"

    worker_thread = threading.Thread(target=queue.run_worker, daemon=True)
    worker_thread.start()
    try:
        poll_until(
            lambda: queue.stats()["completed"] >= 1,
            timeout=30,
            message="periodic task never triggered",
        )

        assert queue.stats()["completed"] >= 1
    finally:
        # A once-a-second schedule left running keeps firing for the rest of the
        # session, and its lifecycle logs land in every later test's `caplog`.
        queue.shutdown()
        worker_thread.join(timeout=5)


def test_list_periodic_reports_every_field(registered: Queue) -> None:
    """Each catalog entry carries the whole schedule, not just its identity."""
    nightly = _find(registered.list_periodic(), "nightly")

    assert nightly.task_name == "nightly"
    assert nightly.cron_expr == "0 0 0 * * *"
    assert nightly.queue == "default"
    assert nightly.enabled is True
    assert nightly.last_run is None
    assert nightly.next_run > 0
    assert nightly.timezone is None

    assert _find(registered.list_periodic(), "cleanup").timezone == "America/New_York"


def test_pause_and_resume_keep_the_registration(registered: Queue) -> None:
    """Pausing stops a schedule firing without dropping it from the catalog."""
    assert registered.pause_periodic("nightly") is True
    assert _find(registered.list_periodic(), "nightly").enabled is False
    assert len(registered.list_periodic()) == 2

    assert registered.resume_periodic("nightly") is True
    assert _find(registered.list_periodic(), "nightly").enabled is True


def test_delete_periodic_unschedules(registered: Queue) -> None:
    """Deleting removes the row; a second delete reports not-found."""
    assert registered.delete_periodic("nightly") is True

    remaining = registered.list_periodic()
    assert [p.name for p in remaining] == ["cleanup"]
    assert registered.delete_periodic("nightly") is False


def test_unknown_schedule_names_report_not_found(registered: Queue) -> None:
    """Every catalog mutation returns False for a name that was never registered."""
    assert registered.delete_periodic("ghost") is False
    assert registered.pause_periodic("ghost") is False
    assert registered.resume_periodic("ghost") is False


async def test_async_periodic_catalog(registered: Queue) -> None:
    """The a* wrappers mirror their sync counterparts."""
    assert len(await registered.alist_periodic()) == 2

    assert await registered.apause_periodic("nightly") is True
    assert _find(await registered.alist_periodic(), "nightly").enabled is False

    assert await registered.aresume_periodic("nightly") is True
    assert _find(await registered.alist_periodic(), "nightly").enabled is True

    assert await registered.adelete_periodic("nightly") is True
    assert await registered.adelete_periodic("nightly") is False
