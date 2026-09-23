"""An operator's drain request stops the one worker it names, gracefully."""

import threading
from pathlib import Path

import pytest

from conftest import PollUntil, join_worker
from flexiq import Queue
from flexiq.mixins import lifecycle


@pytest.fixture(autouse=True)
def fast_heartbeat(monkeypatch: pytest.MonkeyPatch) -> None:
    """Beat often, so the drain request is read within the test's patience."""
    monkeypatch.setattr(lifecycle, "HEARTBEAT_INTERVAL_SECONDS", 0.2)


def start_worker(queue: Queue, poll_until: PollUntil) -> tuple[threading.Thread, str]:
    """Run a worker in the background and return it with its registered id."""
    thread = threading.Thread(target=queue.run_worker, daemon=True)
    thread.start()
    poll_until(lambda: len(queue.workers()) == 1, timeout=15, message="never registered")
    return thread, str(queue.workers()[0]["worker_id"])


def test_a_drained_worker_finishes_its_task_and_unregisters(
    tmp_path: Path, poll_until: PollUntil
) -> None:
    queue = Queue(db_path=str(tmp_path / "drain.db"), workers=2)
    started = threading.Event()
    release = threading.Event()

    @queue.task(name="hold")
    def hold() -> str:
        started.set()
        release.wait(timeout=30)
        return "done"

    job = hold.delay()
    thread, worker_id = start_worker(queue, poll_until)
    try:
        assert started.wait(timeout=15), "the task never started"

        assert queue.drain_worker(worker_id) is True
        assert queue.workers()[0]["status"] == "draining"
        # Several beats later the drain has been read, and the worker is still
        # waiting on the task it holds rather than abandoning it.
        thread.join(timeout=1.0)
        assert thread.is_alive(), "the drain abandoned a running task"

        release.set()
        join_worker(thread, message="a drained worker never stopped")
    finally:
        release.set()
        queue.shutdown()
        join_worker(thread)

    assert job.result(timeout=5) == "done"
    assert queue.workers() == [], "a drained worker left its row behind"


def test_a_drain_leaves_a_sibling_worker_on_the_same_queue_running(
    tmp_path: Path, poll_until: PollUntil
) -> None:
    """A drain names one worker; ``shutdown()`` is the verb that stops them all."""
    queue = Queue(db_path=str(tmp_path / "siblings.db"))
    first = threading.Thread(target=queue.run_worker, kwargs={"queues": ["a"]}, daemon=True)
    second = threading.Thread(target=queue.run_worker, kwargs={"queues": ["b"]}, daemon=True)
    first.start()
    second.start()
    try:
        poll_until(lambda: len(queue.workers()) == 2, timeout=15, message="never registered")
        by_queue = {str(w["queues"]): str(w["worker_id"]) for w in queue.workers()}
        assert queue.drain_worker(by_queue["a"]) is True

        join_worker(first, message="the drained worker never stopped")
        assert second.is_alive(), "the drain stopped a sibling it did not name"
        remaining = queue.workers()
        assert [w["worker_id"] for w in remaining] == [by_queue["b"]]
        assert remaining[0]["status"] == "active"
    finally:
        queue.shutdown()
        join_worker(first)
        join_worker(second)


def test_a_drain_reaches_only_this_namespaces_workers(
    tmp_path: Path, poll_until: PollUntil
) -> None:
    db = str(tmp_path / "ns.db")
    mine = Queue(db_path=db, namespace="mine")
    theirs = Queue(db_path=db, namespace="theirs")
    thread, worker_id = start_worker(theirs, poll_until)
    try:
        assert mine.drain_worker(worker_id) is False
        assert mine.drain_worker("no-such-worker") is False
        thread.join(timeout=1.0)
        assert thread.is_alive(), "another namespace's drain stopped this worker"
        assert theirs.workers()[0]["status"] == "active"
    finally:
        theirs.shutdown()
        join_worker(thread)
