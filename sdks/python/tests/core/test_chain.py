"""Tests for task chaining (chain, group, chord)."""

from __future__ import annotations

import threading
from pathlib import Path

import pytest

from flexiq import Queue, chain, chord, group


@pytest.fixture
def queue(tmp_path: Path) -> Queue:
    db_path = str(tmp_path / "test_chain.db")
    q = Queue(db_path=db_path, workers=4)

    # Start worker in background
    t = threading.Thread(target=q.run_worker, daemon=True)
    t.start()

    return q


def test_chain_executes_in_order(queue: Queue) -> None:
    """chain pipes results through signatures in order."""

    @queue.task()
    def add(a: int, b: int) -> int:
        return a + b

    @queue.task()
    def double(x: int) -> int:
        return x * 2

    result = chain(add.s(2, 3), double.s())
    last_job = result.apply(queue)
    assert last_job.result(timeout=30) == 10  # (2+3) * 2 = 10


def test_chain_with_immutable(queue: Queue) -> None:
    """si() signatures ignore previous results."""

    @queue.task()
    def add(a: int, b: int) -> int:
        return a + b

    @queue.task()
    def constant() -> int:
        return 99

    result = chain(add.s(1, 2), constant.si())
    last_job = result.apply(queue)
    assert last_job.result(timeout=30) == 99


def test_group_parallel(queue: Queue) -> None:
    """group enqueues tasks in parallel."""

    @queue.task()
    def square(x: int) -> int:
        return x * x

    jobs = group(square.s(2), square.s(3), square.s(4)).apply(queue)
    results = [j.result(timeout=30) for j in jobs]
    assert sorted(results) == [4, 9, 16]


def test_chord_callback(queue: Queue) -> None:
    """chord runs group, then callback with collected results."""

    @queue.task()
    def add(a: int, b: int) -> int:
        return a + b

    @queue.task()
    def total(results: list[int]) -> int:
        return sum(results)

    grp = group(add.s(1, 2), add.s(3, 4), add.s(5, 6))
    result_job = chord(grp, total.s()).apply(queue)
    assert result_job.result(timeout=30) == 21  # 3 + 7 + 11 = 21
