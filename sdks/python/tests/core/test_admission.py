"""S26 — opt-in ``max_pending`` admission cap.

Jobs stay Pending without a running worker, so these tests exercise the cap
purely on the producer side.
"""

from __future__ import annotations

import time
from pathlib import Path

import pytest

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
        worker.join(timeout=5)
        assert not worker.is_alive(), "worker did not stop during cleanup"
