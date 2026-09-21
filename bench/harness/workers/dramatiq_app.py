"""Dramatiq's side of the scenario.

Entry point for `dramatiq harness.workers.dramatiq_app`, which discovers the
broker from the module's import side effect — so the broker is set here, at
import, rather than inside a factory the CLI would never call.
"""

from __future__ import annotations

import os
from typing import Any

import dramatiq
from dramatiq.brokers.redis import RedisBroker

from harness.workers.payload import handle

QUEUE = os.environ["BENCH_QUEUE"]
TASK = os.environ["BENCH_TASK"]

broker = RedisBroker(url=os.environ["BENCH_REDIS_URL"])
dramatiq.set_broker(broker)


@dramatiq.actor(actor_name=TASK, queue_name=QUEUE, max_retries=0)
def bench_task(payload: dict[str, Any]) -> None:
    handle(payload)
