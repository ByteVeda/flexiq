"""RQ on Redis.

`rq worker-pool -n N` for concurrency: RQ's worker is a single fork-per-job
process, so N of them is the only way it holds N jobs at once, and it is what
its own documentation tells you to run.
"""

from __future__ import annotations

import argparse
import os
import time

from redis import Redis
from rq import Queue as RQQueue

from ._common import Sink, main, pace, system_name, venv_bin

SYSTEM = "rq"
QUEUE = "bench"

_sink = Sink(system_name(SYSTEM))


#: RQ pickles a reference, not the function, and refuses one that lives in
#: `__main__` — which is exactly where it lives when this file is run as
#: `python -m runners.rq_runner`. Enqueueing by dotted path sidesteps that
#: without changing what the worker ends up importing.
HANDLER = "runners.rq_runner.handle"


def handle(seq: int, enqueued_ns: int, body: str) -> None:
    """The task body, imported by the worker as `HANDLER`."""
    if len(body) < 1:
        raise ValueError("empty payload")
    _sink.done(seq, enqueued_ns)


def _url() -> str:
    return os.environ.get("BENCH_RQ_URL", "redis://127.0.0.1:6379/4")


def _queue() -> RQQueue:
    return RQQueue(QUEUE, connection=Redis.from_url(_url()))


def _worker(args: argparse.Namespace) -> None:
    os.execvp(
        venv_bin("rq"),
        ["rq", "worker-pool", "-n", str(args.concurrency), "-u", _url(),
         "-l", "WARNING", QUEUE],
    )


def _enqueue(args: argparse.Namespace, body: str) -> None:
    queue = _queue()
    # `result_ttl=0` and `failure_ttl=0`: RQ is the one system here that stores a
    # result by default, and leaving that on would have it paying for a write
    # the others never make.
    if args.mode == "batch":
        queue.enqueue_many(
            [
                RQQueue.prepare_data(
                    HANDLER,
                    args=(args.seq_offset + i, time.time_ns(), body),
                    result_ttl=0,
                    failure_ttl=0,
                )
                for i in range(args.jobs)
            ]
        )
        return
    for i in pace(args):
        queue.enqueue(
            HANDLER,
            args.seq_offset + i,
            time.time_ns(),
            body,
            result_ttl=0,
            failure_ttl=0,
        )


def _versions() -> dict[str, str | None]:
    from harness.machine import python_versions

    return python_versions(["rq", "redis"])


if __name__ == "__main__":
    main(
        SYSTEM,
        worker=_worker,
        enqueue=_enqueue,
        versions=_versions,
        concurrency_model="N single-job worker processes (`rq worker-pool -n N`)",
        supports_batch=True,
    )
