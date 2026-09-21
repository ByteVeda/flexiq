"""Celery's side of the scenario.

Entry point for both `celery -A harness.workers.celery_app worker` and the
producer, which submits by name through the same app.

Configuration is deliberately close to stock: the defaults are what a reader
gets, and a benchmark that tunes one entrant and not the others is not
measuring the entrants. The three settings that are not stock all remove
*optional* background chatter (gossip, mingle, heartbeat are worker CLI flags,
not here) or make a result backend's absence explicit.
"""

from __future__ import annotations

import os
from typing import Any

from celery import Celery

from harness.workers.payload import handle

BROKER = os.environ["BENCH_REDIS_URL"]
QUEUE = os.environ["BENCH_QUEUE"]
TASK = os.environ["BENCH_TASK"]

app = Celery("flexiq_bench", broker=BROKER)
app.conf.update(
    task_default_queue=QUEUE,
    task_ignore_result=True,
    # No result backend: none of the five runtimes stores a return value in
    # this scenario, and Celery writing one would be a round trip the others
    # are not paying.
    result_backend=None,
    broker_connection_retry_on_startup=True,
)


@app.task(name=TASK, ignore_result=True)
def bench_task(payload: dict[str, Any]) -> None:
    handle(payload)
