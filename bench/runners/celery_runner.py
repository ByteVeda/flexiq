"""Celery on Redis.

Defaults, except for the three things normalised across every system: the log
level, result storage (off — the sink is the only record of a completion), and
concurrency. Gossip, mingle and the heartbeat stay on, because they are on for
everyone who types `celery worker`, and their cost is part of what this
measures.
"""

from __future__ import annotations

import argparse
import os
import time

from celery import Celery

from ._common import Sink, main, pace, quiet_logs, system_name, venv_bin

SYSTEM = "celery"

app = Celery("bench", broker=os.environ.get("BENCH_CELERY_BROKER", "redis://127.0.0.1:6379/2"))
app.conf.update(
    # No result backend at all. Celery would otherwise need one configured to
    # store anything, and a system paying for result writes that the others do
    # not is not being compared to them.
    task_ignore_result=True,
    broker_connection_retry_on_startup=True,
    task_default_queue="bench",
)

_sink = Sink(system_name(SYSTEM))


@app.task(name="bench.drain", ignore_result=True)
def drain(seq: int, enqueued_ns: int, body: str) -> None:
    if len(body) < 1:
        raise ValueError("empty payload")
    _sink.done(seq, enqueued_ns)


def _worker(args: argparse.Namespace) -> None:
    """Hand the process over to Celery's own launcher.

    `execvp` rather than a subprocess: the harness stops a worker by signalling
    its process group, and a Python parent that merely wrapped the real worker
    would have to forward that signal correctly to be worth the extra pid.
    """
    quiet_logs()
    os.execvp(
        venv_bin("celery"),
        ["celery", "-A", "runners.celery_runner", "worker",
         "-c", str(args.concurrency), "-l", "WARNING", "-Q", "bench"],
    )


def _enqueue(args: argparse.Namespace, body: str) -> None:
    for i in pace(args):
        drain.apply_async(args=(args.seq_offset + i, time.time_ns(), body), queue="bench")


def _versions() -> dict[str, str | None]:
    from harness.machine import python_versions

    return python_versions(["celery", "kombu", "billiard", "redis"])


if __name__ == "__main__":
    main(
        SYSTEM,
        worker=_worker,
        enqueue=_enqueue,
        versions=_versions,
        concurrency_model="N prefork child processes (`celery worker -c N`)",
        # `apply_async` is one round trip per job and there is no bulk producer
        # API; `group()` is a different object with a result backend behind it,
        # not a batch enqueue.
        supports_batch=False,
    )
