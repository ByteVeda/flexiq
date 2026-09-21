"""FlexiQ's side of the scenario, on whichever backend the run selected.

Run as ``python -m harness.workers.flexiq_worker`` for the worker; imported by
the producer for ``make_queue``. Both go through the same constructor so the
two processes cannot disagree about the queue, the task name or the backend.
"""

from __future__ import annotations

import os

from flexiq import Queue

from harness.workers.payload import handle

QUEUE = os.environ["BENCH_QUEUE"]
TASK = os.environ["BENCH_TASK"]

#: ``sqlite`` or ``redis``. The two are separate entrants, not one with a flag:
#: a local file and a network round trip are different deployments.
BACKEND = os.environ.get("BENCH_FLEXIQ_BACKEND", "sqlite")


def make_queue(workers: int = 0) -> Queue:
    """A queue handle with the benchmark task registered on the run's own queue."""
    if BACKEND == "redis":
        queue = Queue(backend="redis", db_url=os.environ["BENCH_REDIS_URL"], workers=workers)
    else:
        queue = Queue(db_path=os.environ["BENCH_DB"], workers=workers)
    queue.task(name=TASK, queue=QUEUE, max_retries=0)(handle)
    return queue


def main() -> None:
    queue = make_queue(int(os.environ["BENCH_CONCURRENCY"]))
    queue.run_worker(queues=[QUEUE])


if __name__ == "__main__":
    main()
