"""Module-level Queue + tasks for the prefork durable-step tests.

A prefork child is a whole interpreter that re-imports this module, so the
tasks have to live at module level for the child to find them. The DB path
comes from the environment for the same reason: parent and child must build
the same Queue.

The step counters cannot be shared with the parent — the child is a separate
process — so each task records what it did in the step rows and the job result
instead of in a list the test can read.
"""

from __future__ import annotations

import os

from flexiq import Queue
from flexiq.context import current_job

queue = Queue(db_path=os.environ.get("FLEXIQ_STEPS_TEST_DB", "/tmp/flexiq-steps.db"))

#: Written by the charge step and read back by the test through the job result,
#: which is how a child process reports "the closure ran here".
_CHARGES: list[str] = []


@queue.task(max_retries=2, retry_backoff=0)
def charge_once() -> str:
    """Commit a step, fail the first attempt, and prove the step is memoized.

    The returned count is the child's own: a memoized step returns without
    running the closure, so the count stays at 1 across both attempts.
    """
    charge = current_job.step.run("charge", _charge)
    if len(_CHARGES) > 1:
        raise AssertionError(f"the charge ran {len(_CHARGES)} times")
    if not os.path.exists(_marker_path()):
        # First attempt: leave a marker and fail, so the job retries into a
        # replay. The marker is on disk because the retry lands on whichever
        # child is free, not necessarily this one.
        open(_marker_path(), "w").close()
        raise ValueError("crashed after the charge")
    return f"{charge}/{len(_CHARGES)}"


@queue.task(max_retries=0)
def naps() -> str:
    """End the attempt in a sleep, then finish on the wake."""
    current_job.step.sleep("200ms", name="nap")
    return "awake"


def _charge() -> str:
    _CHARGES.append(current_job.step.idempotency_key)
    return _CHARGES[-1]


def _marker_path() -> str:
    return f"{os.environ.get('FLEXIQ_STEPS_TEST_DB', '/tmp/flexiq-steps.db')}.attempted"
