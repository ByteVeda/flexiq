"""Module-level Queue + tasks used by the prefork cancel regression tests.

The Queue inside this module must be importable both in the parent test
process and inside each prefork child interpreter. The DB path comes from
``FLEXIQ_CANCEL_TEST_DB`` so each test run can use its own tmp file while
still letting the parent and child build identical Queue instances from
the same module path.
"""

from __future__ import annotations

import os
import time

from flexiq import Queue
from flexiq.context import current_job

queue = Queue(db_path=os.environ.get("FLEXIQ_CANCEL_TEST_DB", "/tmp/flexiq-cancel.db"))


@queue.task(timeout=30, max_retries=0)
def cooperative_loop(max_iters: int = 600) -> int:
    """Loop calling ``check_cancelled()`` so cancel can stop the task quickly."""
    for _ in range(max_iters):
        current_job.check_cancelled()
        time.sleep(0.05)
    return max_iters


@queue.task(max_retries=0)
def quick(x: int) -> int:
    """Returns immediately — used to verify the child still serves jobs after a cancel."""
    return x * 2
