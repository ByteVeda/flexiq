"""Dramatiq on Redis.

`--processes N --threads 1` to match Celery's `-c N`: N things executing at
once, each with its own interpreter. Dramatiq's own default is one process per
core with eight threads each, which is a different amount of concurrency than
every other row in the table — matching the scenario is the fairer distortion.
"""

from __future__ import annotations

import argparse
import os
import time

import dramatiq
from dramatiq.brokers.redis import RedisBroker

from ._common import Sink, main, pace, quiet_logs, system_name, venv_bin

SYSTEM = "dramatiq"

# Import-time, because the `dramatiq` CLI imports this module to find the broker
# and the actors, and sets up its own logging afterwards. Levelling the
# `dramatiq` logger here survives that, which `-l` cannot: the CLI has no such
# flag and defaults to INFO, a line per message.
quiet_logs()

broker = RedisBroker(url=os.environ.get("BENCH_DRAMATIQ_URL", "redis://127.0.0.1:6379/3"))
dramatiq.set_broker(broker)

_sink = Sink(system_name(SYSTEM))


# No results middleware is installed, so nothing is stored — which is what the
# other four are configured down to.
@dramatiq.actor(queue_name="bench", max_retries=0)
def drain(seq: int, enqueued_ns: int, body: str) -> None:
    if len(body) < 1:
        raise ValueError("empty payload")
    _sink.done(seq, enqueued_ns)


def _worker(args: argparse.Namespace) -> None:
    os.execvp(
        venv_bin("dramatiq"),
        ["dramatiq", "runners.dramatiq_runner", "--queues", "bench",
         "--processes", str(args.concurrency), "--threads", "1"],
    )


def _enqueue(args: argparse.Namespace, body: str) -> None:
    for i in pace(args):
        drain.send(args.seq_offset + i, time.time_ns(), body)


def _versions() -> dict[str, str | None]:
    from harness.machine import python_versions

    return python_versions(["dramatiq", "redis"])


if __name__ == "__main__":
    main(
        SYSTEM,
        worker=_worker,
        enqueue=_enqueue,
        versions=_versions,
        concurrency_model="N worker processes, 1 thread each (`--processes N --threads 1`)",
        # `Broker.enqueue` takes one message. A pipeline is a chain, not a batch.
        supports_batch=False,
    )
