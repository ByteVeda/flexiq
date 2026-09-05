"""S25 — opt-in LIFO dispatch order.

Jobs are enqueued before the worker starts, so all pile up pending; a
concurrency-1 worker then dispatches them one at a time and we record the order.
Same-priority ties break by ``(scheduled_at, id)`` direction — with UUIDv7 ids,
LIFO is deterministically newest-first even when jobs share a millisecond.
"""

from __future__ import annotations

import threading
import time
from pathlib import Path

import pytest

from conftest import join_worker
from flexiq import Queue


def _run_and_record(queue: Queue, total: int) -> list[int]:
    order: list[int] = []
    lock = threading.Lock()

    @queue.task(name="rec")
    def rec(n: int) -> None:
        with lock:
            order.append(n)

    # Enqueue everything before the worker exists so the queue is fully backed
    # up when dispatch begins.
    for i in range(total):
        queue.enqueue("rec", args=(i,))

    worker = threading.Thread(target=queue.run_worker, daemon=True)
    worker.start()
    try:
        deadline = time.time() + 20
        while time.time() < deadline:
            with lock:
                if len(order) == total:
                    break
            time.sleep(0.05)
    finally:
        queue.shutdown()
        join_worker(worker)

    with lock:
        return list(order)


def test_set_queue_dispatch_order_validates() -> None:
    q = Queue(db_path=":memory:", workers=1)
    with pytest.raises(ValueError):
        q.set_queue_dispatch_order("default", "sideways")


def test_lifo_dispatches_newest_first(tmp_path: Path) -> None:
    q = Queue(db_path=str(tmp_path / "lifo.db"), workers=1, scheduler_batch_size=1)
    q.set_queue_dispatch_order("default", "lifo")
    order = _run_and_record(q, 6)
    assert order == [5, 4, 3, 2, 1, 0], "LIFO runs newest-first"


def test_fifo_is_the_default(tmp_path: Path) -> None:
    q = Queue(db_path=str(tmp_path / "fifo.db"), workers=1, scheduler_batch_size=1)
    # No dispatch_order set — the fair default.
    order = _run_and_record(q, 6)
    assert order == [0, 1, 2, 3, 4, 5], "FIFO runs oldest-first by default"
