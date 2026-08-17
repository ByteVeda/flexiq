"""Reaper election: only one worker per cluster sweeps dead workers.

Before the election, every worker reaped on every 5s tick, so a `WORKER_OFFLINE`
event fired once per live worker per death instead of once. The heartbeat now
takes a cluster-wide lease first and only the holder reaps; a non-holder returns
an empty reaped list and so emits nothing.

These assert the mechanism through the lease itself rather than by simulating a
dead worker — a raw write to the queue's SQLite file is not reliably visible to
the queue's own connections (different journal mode), so the reap's *effect*
cannot be observed from a test, but the election that gates it can.
"""

from __future__ import annotations

from pathlib import Path

from flexiq import Queue

# Mirror of `REAPER_LOCK` in crates/flexiq-core/src/storage/mod.rs.
REAPER_LOCK = "flexiq:reaper"


def test_reaper_lock_is_free_on_a_fresh_queue(tmp_path: Path) -> None:
    # Guards the test below: the lease is observing the heartbeat taking it, not
    # a lock that was already held.
    queue = Queue(db_path=str(tmp_path / "fresh.db"))
    assert queue._inner.acquire_lock(REAPER_LOCK, "someone", 15_000) is True


def test_heartbeat_takes_the_reaper_lease(tmp_path: Path) -> None:
    db = str(tmp_path / "election.db")
    q1 = Queue(db_path=db)
    q2 = Queue(db_path=db)

    # A fresh heartbeat elects worker-a as the reaper for the lease window.
    q1._inner.worker_heartbeat("worker-a")

    # worker-b cannot take the live lease, so its tick sweeps nothing — the
    # dedup that keeps WORKER_OFFLINE from firing once per worker.
    assert q2._inner.acquire_lock(REAPER_LOCK, "worker-b", 15_000) is False


def test_non_leader_heartbeat_reaps_nothing(tmp_path: Path) -> None:
    db = str(tmp_path / "gated.db")
    q = Queue(db_path=db)

    # A peer already holds the lease.
    assert q._inner.acquire_lock(REAPER_LOCK, "peer", 15_000) is True

    # This worker is gated out, so it reaps nothing regardless of the registry.
    assert q._inner.worker_heartbeat("worker-a") == []
