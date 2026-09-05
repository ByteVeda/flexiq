"""S26 — opt-in ``max_pending`` admission cap.

Jobs stay Pending without a running worker, so these tests exercise the cap
purely on the producer side.
"""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from conftest import join_worker
from flexiq import Queue, QueueFullError


def _register(queue: Queue) -> None:
    @queue.task(name="noop")
    def noop() -> None:  # pragma: no cover - never executed (no worker)
        return None


def test_count_pending_by_queue_primitive(queue: Queue) -> None:
    _register(queue)
    assert queue._inner.count_pending_by_queue("default") == 0
    queue.enqueue("noop")
    queue.enqueue("noop")
    assert queue._inner.count_pending_by_queue("default") == 2


def test_uncapped_queue_never_rejects(queue: Queue) -> None:
    _register(queue)
    for _ in range(50):
        queue.enqueue("noop")
    assert queue._inner.count_pending_by_queue("default") == 50


def test_runtime_setter_rejects_at_cap(queue: Queue) -> None:
    _register(queue)
    queue.set_queue_max_pending("default", 2)
    queue.enqueue("noop")
    queue.enqueue("noop")
    with pytest.raises(QueueFullError) as exc:
        queue.enqueue("noop")
    assert "max_pending 2" in str(exc.value)
    # Rejected enqueue inserted nothing.
    assert queue._inner.count_pending_by_queue("default") == 2


def test_constructor_cap(tmp_path: Path) -> None:
    q = Queue(db_path=str(tmp_path / "t.db"), workers=1, max_pending={"default": 1})
    _register(q)
    q.enqueue("noop")
    with pytest.raises(QueueFullError):
        q.enqueue("noop")


def test_cap_is_per_queue(queue: Queue) -> None:
    _register(queue)
    queue.set_queue_max_pending("tight", 1)
    queue.enqueue("noop", queue="tight")
    with pytest.raises(QueueFullError):
        queue.enqueue("noop", queue="tight")
    # A different, uncapped queue is unaffected.
    for _ in range(5):
        queue.enqueue("noop", queue="wide")


def test_queue_full_is_queue_error_subclass() -> None:
    from flexiq.exceptions import FlexiQError, QueueError

    assert issubclass(QueueFullError, QueueError)
    assert issubclass(QueueFullError, FlexiQError)


def test_enqueue_many_all_or_nothing(queue: Queue) -> None:
    _register(queue)
    queue.set_queue_max_pending("default", 3)
    queue.enqueue("noop")
    queue.enqueue("noop")
    queue.enqueue("noop")  # now at cap
    with pytest.raises(QueueFullError):
        queue.enqueue_many("noop", [(), (), ()])
    # None of the batch landed.
    assert queue._inner.count_pending_by_queue("default") == 3


def test_enqueue_many_accounts_for_batch_size(queue: Queue) -> None:
    _register(queue)
    queue.set_queue_max_pending("default", 3)
    # An empty queue but a batch bigger than the cap is rejected as a whole.
    with pytest.raises(QueueFullError):
        queue.enqueue_many("noop", [(), (), (), ()])
    assert queue._inner.count_pending_by_queue("default") == 0
    # A batch that exactly fits is admitted.
    queue.enqueue_many("noop", [(), (), ()])
    assert queue._inner.count_pending_by_queue("default") == 3
    # Now full: even a single more is rejected.
    with pytest.raises(QueueFullError):
        queue.enqueue("noop")


def test_negative_cap_rejected(tmp_path: Path) -> None:
    q = Queue(db_path=str(tmp_path / "n.db"), workers=1)
    with pytest.raises(ValueError):
        q.set_queue_max_pending("default", -1)
    with pytest.raises(ValueError):
        Queue(db_path=str(tmp_path / "n2.db"), workers=1, max_pending={"default": -1})


def test_cap_frees_after_drain(tmp_path: Path) -> None:
    """Fill a small cap, observe rejection, then confirm draining re-admits.

    A gated task blocks the worker so the cap can actually be reached; releasing
    the gate drains the backlog and a fresh enqueue must be admitted again.
    """
    import threading

    q = Queue(db_path=str(tmp_path / "drain.db"), workers=1)
    gate = threading.Event()

    @q.task(name="blocked")
    def blocked() -> None:
        gate.wait(timeout=10)

    q.set_queue_max_pending("default", 2)
    # No worker yet: two enqueues fill the cap, the third is rejected.
    q.enqueue("blocked")
    q.enqueue("blocked")
    with pytest.raises(QueueFullError):
        q.enqueue("blocked")

    worker = threading.Thread(target=q.run_worker, daemon=True)
    worker.start()
    try:
        # The worker claims a job (Running), so pending drops below the cap and a
        # new enqueue is admitted where it was rejected a moment ago.
        deadline = time.time() + 10
        admitted = False
        while time.time() < deadline:
            if q._inner.count_pending_by_queue("default") < 2:
                q.enqueue("blocked")
                admitted = True
                break
            time.sleep(0.05)
        assert admitted, "draining below the cap must re-admit an enqueue"
    finally:
        gate.set()
        q.shutdown()
        join_worker(worker, message="worker did not stop during cleanup")


def _register_debounced(queue: Queue) -> None:
    @queue.task(
        name="report",
        debounce="5m",
        debounce_key="report:{user_id}",
        debounce_max_wait="30m",
    )
    def report(user_id: int) -> None:  # pragma: no cover - never executed (no worker)
        return None


def test_debounced_enqueue_collapses_onto_a_full_queue(queue: Queue) -> None:
    """A full queue still admits an enqueue that only slides the open window.

    The cap is admission control on pending rows and a coalescing enqueue adds
    none, so it is applied inside the debounce write — on the branch that
    inserts — rather than before the call.
    """
    _register(queue)
    _register_debounced(queue)
    queue.set_queue_max_pending("default", 2)

    opened = queue.enqueue("report", (7,))
    queue.enqueue("noop")
    assert queue._inner.count_pending_by_queue("default") == 2

    slid = queue.enqueue("report", (7,))
    assert slid.id == opened.id
    assert queue._inner.count_pending_by_queue("default") == 2


def test_debounced_enqueue_still_rejected_with_no_window_open(queue: Queue) -> None:
    """No open window means a row to insert, so the same full queue refuses it."""
    _register(queue)
    _register_debounced(queue)
    queue.set_queue_max_pending("default", 2)
    queue.enqueue("noop")
    queue.enqueue("noop")

    with pytest.raises(QueueFullError) as exc:
        queue.enqueue("report", (7,))
    # The count comes back from the write that refused it, not from a producer-side read.
    assert "2 pending + 1 would exceed max_pending 2" in str(exc.value)
    assert queue._inner.count_pending_by_queue("default") == 2
