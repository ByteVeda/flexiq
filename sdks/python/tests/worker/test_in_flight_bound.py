"""A worker must not claim more jobs than it can actually run."""

import threading
import time
from pathlib import Path
from typing import Any

from flexiq import Queue

PollUntil = Any  # the conftest fixture's runtime type


def test_worker_does_not_claim_more_than_it_can_run(tmp_path: Path) -> None:
    """Claimed-but-unrunnable jobs sit Running, so peers sharing the DB skip them.

    Dequeue flips a job to Running, so over-claiming is invisible in results — every
    job still completes, just after stranding Running for however long the worker
    takes to reach it. Watch the Running count instead: with two worker threads it
    must stay near two, no matter how deep the backlog.
    """
    queue = Queue(db_path=str(tmp_path / "bound.db"), workers=2)
    job_count = 40

    @queue.task(name="slow")
    def slow() -> int:
        time.sleep(0.25)
        return 1

    for _ in range(job_count):
        slow.delay()

    thread = threading.Thread(target=queue.run_worker, daemon=True)
    thread.start()
    try:
        peak_running = 0
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            stats = queue.stats()
            peak_running = max(peak_running, stats.get("running", 0))
            if stats.get("completed", 0) >= job_count:
                break
            time.sleep(0.02)
    finally:
        queue.shutdown()
        thread.join(timeout=5)

    assert queue.stats().get("completed", 0) == job_count
    # The failure mode guarded here is the whole backlog going Running at once
    # (an uncapped scheduler claims all 40). The legitimate ceiling is the
    # worker's pipeline, every stage of which is a bounded channel or pool:
    #   2 executing + 4 job-channel + 1 held by the pool loop
    #   + 4 result-channel + ~5 in one finalize batch (Running until committed)
    # = 16. On native-async builds the in-flight cap is num_workers +
    # async_concurrency, so for sync work the channel is the binding limiter,
    # not the cap — observed peaks are 7-9, the rest is commit lag headroom.
    assert peak_running <= 16, (
        f"worker claimed {peak_running} jobs — pipeline ceiling is 16, "
        f"anything near the {job_count}-job backlog is the S01 over-claim"
    )
