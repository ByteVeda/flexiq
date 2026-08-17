"""S27 — opt-in CoDel load shedding.

Behavioral (timing-based): a slow task with concurrency 1 backs the queue up, so
later jobs' sojourn stays above target for a full interval and CoDel sheds the
stalest ones to the DLQ. The controller's algorithm itself is unit-tested in Rust
(``scheduler::codel``); here we assert the end-to-end shed path and its
invariants.
"""

from __future__ import annotations

import threading
import time
from pathlib import Path

import pytest

from flexiq import Queue


def test_set_queue_codel_validates() -> None:
    q = Queue(db_path=":memory:", workers=1)
    with pytest.raises(ValueError):
        q.set_queue_codel("default", target_ms=0, interval_ms=10)
    with pytest.raises(ValueError):
        q.set_queue_codel("default", target_ms=10, interval_ms=-1)


def test_codel_sheds_stale_jobs_under_overload(tmp_path: Path) -> None:
    q = Queue(db_path=str(tmp_path / "codel.db"), workers=1, scheduler_batch_size=1)
    q.set_queue_codel("default", target_ms=1, interval_ms=30)

    @q.task(name="slow")
    def slow() -> None:
        time.sleep(0.05)

    total = 20
    for _ in range(total):
        q.enqueue("slow")

    worker = threading.Thread(target=q.run_worker, daemon=True)
    worker.start()

    # Poll until every job is accounted for (ran or shed) and at least one was
    # shed — an aggregate-convergence check, never an instantaneous snapshot.
    deadline = time.time() + 25
    codel_dead: list[dict] = []
    stats = q.stats()
    try:
        while time.time() < deadline:
            dead = q.dead_letters(limit=100)
            codel_dead = [d for d in dead if str(d.get("error", "")).startswith("codel:")]
            stats = q.stats()
            if stats["completed"] + stats["dead"] == total and len(codel_dead) >= 1:
                break
            time.sleep(0.1)
        # A job can be shed between the dead-letter read and the stats read
        # that satisfied the exit condition; once converged nothing moves, so
        # a fresh dead-letter read gives the settled count.
        dead = q.dead_letters(limit=100)
        codel_dead = [d for d in dead if str(d.get("error", "")).startswith("codel:")]
    finally:
        q.shutdown()
        worker.join(timeout=5)

    assert len(codel_dead) >= 1, "sustained overload should shed at least one stale job"
    # Every shed job is a CoDel drop (the task never fails on its own).
    assert stats["dead"] == len(codel_dead)
    # Nothing is lost: every job either ran to completion or was shed.
    assert stats["completed"] + stats["dead"] == total


def test_codel_drops_are_not_auto_retried(tmp_path: Path) -> None:
    # A CoDel-shed entry must not be resurrected by DLQ auto-retry.
    q = Queue(
        db_path=str(tmp_path / "codel_retry.db"),
        workers=1,
        scheduler_batch_size=1,
        dlq_auto_retry_delay=0,
        dlq_auto_retry_max=5,
    )
    q.set_queue_codel("default", target_ms=1, interval_ms=30)

    @q.task(name="slow")
    def slow() -> None:
        time.sleep(0.05)

    for _ in range(20):
        q.enqueue("slow")

    worker = threading.Thread(target=q.run_worker, daemon=True)
    worker.start()
    try:
        deadline = time.time() + 25
        while time.time() < deadline:
            stats = q.stats()
            if stats["pending"] == 0 and stats["running"] == 0 and stats["dead"] > 0:
                break
            time.sleep(0.1)
        # Give auto-retry a few cycles to (not) fire.
        time.sleep(1.0)
    finally:
        q.shutdown()
        worker.join(timeout=5)

    dead = q.dead_letters(limit=100)
    codel_dead = [d for d in dead if str(d.get("error", "")).startswith("codel:")]
    assert codel_dead, "expected at least one CoDel-shed entry"
    # Still dead — auto-retry left them alone.
    assert q.stats()["dead"] == len(codel_dead)
