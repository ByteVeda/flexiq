"""``@queue.task(on_excess=...)`` — shed or defer a rate-limited job.

The limiter gates at *dispatch*, so these assert on job status and dead-letter
rows, never on execution timestamps. The gate itself is unit-tested in Rust
(``scheduler::shed``); here we prove the decorator reaches it and that opting
out changes nothing.
"""

from __future__ import annotations

import threading
import time
from pathlib import Path
from typing import Any

import pytest

from flexiq import OnExcess, Queue

PollUntil = Any  # the conftest fixture's runtime type


def _drain(q: Queue, worker: threading.Thread, poll_until: PollUntil) -> None:
    """Run the worker until nothing is pending or running, then stop it."""
    worker.start()
    try:
        poll_until(
            lambda: q.stats()["pending"] == 0 and q.stats()["running"] == 0,
            timeout=20,
            message="queue never drained",
        )
    finally:
        q.shutdown()
        worker.join(timeout=5)


def test_on_excess_rejects_an_unknown_value() -> None:
    q = Queue(db_path=":memory:", workers=1)
    with pytest.raises(ValueError, match="on_excess"):

        @q.task(name="typo", rate_limit="1/h", on_excess="discard")
        def typo() -> None: ...


def test_on_excess_drop_sheds_the_excess_to_the_dlq(tmp_path: Path, poll_until: PollUntil) -> None:
    # One token per hour, so exactly one of the five jobs can ever dispatch.
    # The other four are excess and must terminate rather than pile up.
    q = Queue(db_path=str(tmp_path / "shed.db"), workers=1, scheduler_batch_size=1)

    @q.task(name="sample", rate_limit="1/h", on_excess=OnExcess.DROP)
    def sample() -> None: ...

    for _ in range(5):
        q.enqueue("sample")

    _drain(q, threading.Thread(target=q.run_worker, daemon=True), poll_until)

    dead = q.dead_letters(limit=100)
    shed = [d for d in dead if str(d.get("error", "")).startswith("rate_limit:")]
    assert len(shed) == 4, f"every excess job should be shed, got {dead}"

    stats = q.stats()
    assert stats["pending"] == 0, "a shed job must not return to the queue"
    assert stats["completed"] + stats["dead"] == 5, "nothing is silently lost"


def test_on_excess_defer_keeps_the_excess_pending(tmp_path: Path, poll_until: PollUntil) -> None:
    # The default is opt-out-shaped: the excess jobs stay pending, waiting for
    # tokens that will not arrive for an hour, and nothing is dead-lettered.
    q = Queue(db_path=str(tmp_path / "defer.db"), workers=1, scheduler_batch_size=1)

    @q.task(name="sample", rate_limit="1/h")
    def sample() -> None: ...

    for _ in range(5):
        q.enqueue("sample")

    worker = threading.Thread(target=q.run_worker, daemon=True)
    worker.start()
    try:
        poll_until(
            lambda: q.stats()["completed"] == 1,
            timeout=20,
            message="the one admitted job never ran",
        )
        # Give the poller several cycles to (not) shed the rest.
        time.sleep(1.0)
    finally:
        q.shutdown()
        worker.join(timeout=5)

    assert q.dead_letters(limit=100) == [], "deferral must never dead-letter"
    assert q.stats()["pending"] == 4, "the excess jobs wait for tokens"
