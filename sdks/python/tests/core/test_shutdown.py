"""Tests for graceful shutdown."""

import threading
import time
from pathlib import Path
from typing import Any

from conftest import join_worker
from flexiq import Queue

PollUntil = Any  # the conftest fixture's runtime type


def test_graceful_shutdown_completes_inflight(queue: Queue, poll_until: PollUntil) -> None:
    """Graceful shutdown waits for in-flight tasks to complete."""
    completed = threading.Event()

    @queue.task()
    def slow_task() -> str:
        # Intentional pacing — the test asserts the worker waits for this
        # to finish before shutting down.
        time.sleep(1)
        completed.set()
        return "done"

    job = slow_task.delay()

    worker_thread = threading.Thread(target=queue.run_worker, daemon=True)
    worker_thread.start()

    # Wait for the task to actually start running before triggering shutdown.
    poll_until(
        lambda: (j := queue.get_job(job.id)) is not None and j.status == "running",
        message="slow_task never reached running state",
    )

    # Request graceful shutdown via the public API
    queue.shutdown()

    # Worker should finish the in-flight task
    join_worker(worker_thread)

    assert completed.is_set()
    fetched = queue.get_job(job.id)
    assert fetched is not None
    assert fetched.status == "complete"


def test_shutdown_stops_worker(queue: Queue, poll_until: PollUntil) -> None:
    """queue.shutdown() causes run_worker to return."""
    started = threading.Event()

    @queue.task()
    def noop() -> None:
        started.set()

    job = noop.delay()

    worker_thread = threading.Thread(target=queue.run_worker, daemon=True)
    worker_thread.start()

    # A job the worker actually ran, rather than a fixed grace window: reaching
    # the poll loop takes far longer than 100ms on a loaded runner, and a
    # shutdown that lands before it does leaves this waiting on a worker that
    # never saw the request.
    poll_until(
        lambda: (
            started.is_set()
            and (j := queue.get_job(job.id)) is not None
            and j.status == "complete"
        ),
        timeout=30,
        message="the worker never reached its poll loop",
    )
    queue.shutdown()

    join_worker(worker_thread)


def test_shutdown_without_worker_is_noop(queue: Queue) -> None:
    """shutdown() on a queue with no running worker affects nothing running."""
    queue.shutdown()  # must not raise


def test_shutdown_requested_before_the_loop_starts_is_not_lost(tmp_path: Path) -> None:
    """A stop that lands during startup must survive into the run.

    ``run_worker`` used to clear the flag as its first act, so a request made
    while its Python half was still registering schedules and building the
    registry was erased — and the worker then ignored it and never returned,
    which no join budget can bound. Requesting the stop before the thread even
    exists is the same window, made deterministic.
    """
    queue = Queue(db_path=str(tmp_path / "early.db"), workers=1)

    @queue.task(name="noop")
    def noop() -> None: ...

    queue.shutdown()

    worker_thread = threading.Thread(target=queue.run_worker, daemon=True)
    worker_thread.start()
    join_worker(worker_thread, message="a stop requested during startup was discarded")


def test_one_shutdown_stops_every_worker_on_the_queue(
    tmp_path: Path, poll_until: PollUntil
) -> None:
    """Sibling runs share the flag, so the first one out must not disarm the rest.

    One ``Queue`` can run several ``run_worker`` calls at once, each over its
    own queue list. They share a single shutdown flag, so a worker that clears
    it on its way out leaves any sibling that had not yet polled it running with
    no request to find.
    """
    queue = Queue(db_path=str(tmp_path / "siblings.db"), workers=1)

    @queue.task(name="on_alpha", queue="alpha")
    def on_alpha() -> None: ...

    @queue.task(name="on_beta", queue="beta")
    def on_beta() -> None: ...

    threads = [
        threading.Thread(target=queue.run_worker, kwargs={"queues": [q]}, daemon=True)
        for q in ("alpha", "beta")
    ]
    for thread in threads:
        thread.start()
    poll_until(
        lambda: len(queue.workers()) == 2, timeout=30, message="both workers never registered"
    )

    queue.shutdown()

    for thread in threads:
        join_worker(thread, message="a sibling worker was disarmed by the first one out")
