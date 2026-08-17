"""Tests for batch enqueue (enqueue_many / task.map)."""

import threading
from typing import Any

from flexiq import Queue
from flexiq.middleware import TaskMiddleware


def test_enqueue_many(queue: Queue) -> None:
    """enqueue_many enqueues all items in a single batch."""

    @queue.task()
    def double(x: int) -> int:
        return x * 2

    jobs = queue.enqueue_many(
        task_name=double.name,
        args_list=[(i,) for i in range(10)],
    )
    assert len(jobs) == 10

    stats = queue.stats()
    assert stats["pending"] == 10


def test_task_map(queue: Queue) -> None:
    """TaskWrapper.map() enqueues and returns results."""

    @queue.task()
    def add(a: int, b: int) -> int:
        return a + b

    jobs = add.map([(1, 2), (3, 4), (5, 6)])
    assert len(jobs) == 3

    worker_thread = threading.Thread(target=queue.run_worker, daemon=True)
    worker_thread.start()

    results = [j.result(timeout=10) for j in jobs]
    assert sorted(results) == [3, 7, 11]


def test_batch_stats(queue: Queue) -> None:
    """Batch enqueue of 50 items shows correct pending count."""

    @queue.task()
    def noop() -> None:
        pass

    queue.enqueue_many(
        task_name=noop.name,
        args_list=[() for _ in range(50)],
    )

    stats = queue.stats()
    assert stats["pending"] == 50


def test_enqueue_many_invokes_on_enqueue_per_job(tmp_path: Any) -> None:
    """`on_enqueue` middleware must receive each job's own args/kwargs.

    Regression: the previous implementation always passed `args_list[0]`
    and a fresh empty options dict to every middleware call, so middleware
    could not distinguish jobs in the batch.
    """

    class RecordingMiddleware(TaskMiddleware):
        def __init__(self) -> None:
            self.calls: list[tuple[tuple, dict]] = []

        def on_enqueue(self, task_name: str, args: tuple, kwargs: dict, options: dict) -> None:
            self.calls.append((args, dict(kwargs)))

    mw = RecordingMiddleware()
    q = Queue(db_path=str(tmp_path / "test.db"), middleware=[mw])

    @q.task()
    def add(a: int, b: int) -> int:
        return a + b

    q.enqueue_many(
        task_name=add.name,
        args_list=[(1, 2), (3, 4), (5, 6)],
        kwargs_list=[{"trace": "alpha"}, {"trace": "beta"}, {"trace": "gamma"}],
    )

    assert mw.calls == [
        ((1, 2), {"trace": "alpha"}),
        ((3, 4), {"trace": "beta"}),
        ((5, 6), {"trace": "gamma"}),
    ]


def test_enqueue_many_applies_option_mutations(tmp_path: Any) -> None:
    """Mutations to the options dict inside on_enqueue must propagate to the
    enqueued jobs — matching the documented behaviour of single-enqueue.
    Regression: the previous implementation discarded mutations because the
    hook ran *after* `enqueue_batch` and against a fresh empty dict.
    """

    class PerJobBoosterMiddleware(TaskMiddleware):
        def on_enqueue(self, task_name: str, args: tuple, kwargs: dict, options: dict) -> None:
            # Bump priority based on the first argument so each job sees a
            # distinct mutation.
            options["priority"] = int(args[0]) * 10

    q = Queue(db_path=str(tmp_path / "test.db"), middleware=[PerJobBoosterMiddleware()])

    @q.task()
    def task_one(n: int) -> int:
        return n

    results = q.enqueue_many(task_name=task_one.name, args_list=[(1,), (2,), (3,)])

    priorities = [q.get_job(r.id).to_dict()["priority"] for r in results]  # type: ignore[union-attr]
    assert priorities == [10, 20, 30]


def test_enqueue_many_logs_middleware_exceptions(tmp_path: Any, caplog: Any) -> None:
    """Middleware exceptions must be logged, not silently swallowed.

    Regression: the previous implementation used a bare `except: pass`,
    making misbehaving middleware effectively invisible.
    """
    import logging

    class ExplodingMiddleware(TaskMiddleware):
        def on_enqueue(self, task_name: str, args: tuple, kwargs: dict, options: dict) -> None:
            raise RuntimeError("middleware boom")

    q = Queue(db_path=str(tmp_path / "test.db"), middleware=[ExplodingMiddleware()])

    @q.task()
    def my_task(n: int) -> int:
        return n

    with caplog.at_level(logging.ERROR, logger="flexiq.app"):
        results = q.enqueue_many(task_name=my_task.name, args_list=[(1,), (2,)])

    # Jobs are still enqueued — middleware errors must not block enqueue
    assert len(results) == 2

    # And the error was surfaced via the logger
    assert any("middleware on_enqueue() error" in rec.message for rec in caplog.records)
    assert any("middleware boom" in (rec.exc_text or "") for rec in caplog.records)
