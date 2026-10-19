"""The demo's fixed task set.

Every job the public demo can hold runs one of these. Nothing a visitor sends
chooses the task: the trigger names ``demo.hello`` and the seeder names the
rest, so the endpoint runs no submitted code.
"""

from __future__ import annotations

import os
import time

from flexiq import Queue, current_job

queue = Queue(
    db_path=os.environ.get("FLEXIQ_DEMO_DB", "/data/flexiq.db"),
    namespace=os.environ.get("FLEXIQ_NAMESPACE", "demo"),
)


@queue.task(name="demo.hello", max_retries=0, timeout=30)
def hello() -> str:
    """What the enqueue button runs: a short pause so the job is seen moving."""
    time.sleep(2)
    return f"hello from {current_job.id}"


# A budget no reset window exhausts, so the job is always between retries.
# The wait between them is the scheduler's (see FLAKY_BACKOFF_SECS in seed.py):
# an attached executor's decorator options do not reach it.
@queue.task(name="demo.flaky", max_retries=10_000, timeout=30)
def flaky() -> None:
    """Fails every attempt, so it is always seen retrying."""
    raise ConnectionError("upstream timed out (on purpose: this task is the retry demo)")


@queue.task(name="demo.doomed", max_retries=0, timeout=30)
def doomed() -> None:
    """Always fails with no retry budget, so it lands in the dead-letter queue."""
    raise ValueError("invalid invoice id (on purpose: this task is the dead-letter demo)")


@queue.task(name="demo.nap", timeout=30)
def nap() -> str:
    """Parks in a durable sleep: the attempt ends and no worker slot is held."""
    current_job.step.sleep("1h", name="cool-off")
    return "woke up"


@queue.task(name="demo.build", timeout=30)
def build() -> str:
    """First step of the release workflow."""
    time.sleep(1)
    return "artifact-1.0.0"


@queue.task(name="demo.ship", timeout=30)
def ship() -> str:
    """Last step of the release workflow, behind an approval gate nobody opens."""
    return "shipped"
