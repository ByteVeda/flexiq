"""FlexiQ, shared by its two backends.

The only difference between `flexiq_sqlite` and `flexiq_redis` is the two
constructor arguments below; everything measured is otherwise the same code,
which is the point — the pair is in the table so a reader can see what the
embedded database costs and what the network hop costs, rather than being told.

The queue is built at import because a FlexiQ task is registered against an
instance, and both roles — the worker process and the enqueue process — have to
agree on the registry.

Two configurations, and the difference between them is the most useful thing in
the results file. **Defaults** is what `pip install flexiq` gives you:
`scheduler_batch_size = 1` and a 50 ms poll. **Tuned** applies the two knobs
FlexiQ's own documentation tells you to reach for first.

Of the two it is the poll interval that matters, and not for the reason the
knob names suggest. The scheduler fills a dispatch channel holding
`num_workers * 2` jobs, then sleeps out the poll interval before looking again
— nothing wakes it when a worker frees up, because the completion wake is
keyed to `max_in_flight` (104 under the stock Python binding), which a channel
of eight never reaches. Throughput is therefore about
`2 * num_workers / poll_interval` regardless of backlog depth. Batching the
claim does not move a constraint that lives in the channel.

Only FlexiQ has a tuned row, which is a disclosure and not a thumb on
the scale: its default loses this benchmark badly, and a table that quietly
showed the tuned number would be hiding that, while one that showed only the
default would be hiding that the knob exists. No equivalent tuning was applied
to the other four — if you know the documented knob that moves one of their
numbers, the harness will take it.
"""

from __future__ import annotations

import argparse
import os
import time
from typing import Any

from flexiq import Queue

from ._common import Sink, pace, system_name, workdir


def build_queue(system: str, backend: str, concurrency: int) -> tuple[Queue, Any]:
    """The queue and its one task, wired to the shared completion sink."""
    kwargs: dict[str, Any] = {"workers": concurrency, "namespace": system.replace("-", "_")}
    if os.environ.get("BENCH_FLEXIQ_TUNING") == "tuned":
        # `docs/content/docs/shared/more/examples/benchmark.mdx` names both:
        # batch_size is "the first knob to move", and the poll interval is the
        # latency/database trade. Nothing here is undocumented or unsupported.
        kwargs |= {"scheduler_batch_size": 64, "scheduler_poll_interval_ms": 10}
    if backend == "redis":
        kwargs |= {"backend": "redis", "db_url": os.environ["BENCH_FLEXIQ_REDIS_URL"]}
    else:
        kwargs |= {"db_path": str(workdir() / f"{system}.db")}

    queue = Queue(**kwargs)
    sink = Sink(system)

    @queue.task(name="bench.drain", max_retries=0)
    def drain(seq: int, enqueued_ns: int, body: str) -> None:
        # The body every runner shares: touch the payload so a serialiser
        # cannot skip decoding it, then record the completion. Nothing else —
        # what is left is the queue's own cost, which is what is under test.
        if len(body) < 1:
            raise ValueError("empty payload")
        sink.done(seq, enqueued_ns)

    return queue, drain


def worker(system: str, backend: str) -> "Any":
    def run(args: argparse.Namespace) -> None:
        queue, _ = build_queue(system_name(system), backend, args.concurrency)
        queue.run_worker()

    return run


def enqueue(system: str, backend: str) -> "Any":
    def submit(args: argparse.Namespace, body: str) -> None:
        # Concurrency 1: this is the producer, and the worker under measurement
        # is a separate process. A producer that also owned worker threads
        # would be measuring both ends at once.
        _, drain = build_queue(system_name(system), backend, 1)
        if args.mode == "batch":
            drain.map(
                [
                    (args.seq_offset + i, time.time_ns(), body)
                    for i in range(args.jobs)
                ]
            )
            return
        for i in pace(args):
            drain.delay(args.seq_offset + i, time.time_ns(), body)

    return submit


def versions() -> dict[str, str | None]:
    from harness.machine import python_versions

    return python_versions(["flexiq", "redis"])
