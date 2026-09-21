"""The producer, as a subprocess — one per engine, one per phase.

Every entrant submits from a separate process, because that is how all five are
deployed and because an in-process FlexiQ producer would be racing four that
pay for a fork and an import. The client is built before the clock starts; only
the submission loop is timed, and it is serial in every engine — FlexiQ's
``enqueue_many`` would measure a feature the others do not have.

Prints one JSON line on stdout: ``seconds``, ``started_ms``, ``job_ids``.
"""

from __future__ import annotations

import argparse
import json
import os
import time
from collections.abc import Callable
from typing import Any

from celery import Celery
from dramatiq import Message
from dramatiq.brokers.redis import RedisBroker
from redis import Redis
from rq import Queue as RqQueue

from harness.workers import flexiq_worker
from harness.workers.payload import build

QUEUE = os.environ["BENCH_QUEUE"]
TASK = os.environ["BENCH_TASK"]


def _redis_url() -> str:
    return os.environ["BENCH_REDIS_URL"]


def _flexiq_client() -> Any:
    # ``workers=0`` — a producer that also ran workers would be measuring a
    # different deployment from the one every other entrant is in.
    return flexiq_worker.make_queue(workers=0)


def _flexiq_submit(client: Any, index: int, pad: str) -> str:
    return str(client.enqueue(TASK, args=(build(index, pad),), queue=QUEUE).id)


def _celery_client() -> Any:
    return Celery("flexiq_bench_producer", broker=_redis_url())


def _celery_submit(client: Any, index: int, pad: str) -> str:
    return str(client.send_task(TASK, args=[build(index, pad)], queue=QUEUE).id)


def _dramatiq_client() -> Any:
    # The raw broker rather than the actor module: importing the actor would
    # install a global broker in a process that only submits.
    return RedisBroker(url=_redis_url())


def _dramatiq_submit(client: Any, index: int, pad: str) -> str:
    message: Message[Any] = Message(
        queue_name=QUEUE,
        actor_name=TASK,
        args=(build(index, pad),),
        kwargs={},
        options={},
    )
    return str(client.enqueue(message).message_id)


def _rq_client() -> Any:
    return RqQueue(QUEUE, connection=Redis.from_url(_redis_url()))


def _rq_submit(client: Any, index: int, pad: str) -> str:
    return str(client.enqueue("harness.workers.rq_task.bench_task", build(index, pad)).id)


ENGINES: dict[str, tuple[Callable[[], Any], Callable[[Any, int, str], str]]] = {
    "flexiq": (_flexiq_client, _flexiq_submit),
    "celery": (_celery_client, _celery_submit),
    "dramatiq": (_dramatiq_client, _dramatiq_submit),
    "rq": (_rq_client, _rq_submit),
}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engine", required=True, choices=sorted(ENGINES))
    parser.add_argument("--first", type=int, required=True)
    parser.add_argument("--count", type=int, required=True)
    args = parser.parse_args()

    make_client, submit = ENGINES[args.engine]
    pad = os.environ["BENCH_PAD"]
    client = make_client()

    started_ms = time.time() * 1000
    clock = time.perf_counter()
    job_ids = [submit(client, i, pad) for i in range(args.first, args.first + args.count)]
    seconds = time.perf_counter() - clock

    print(json.dumps({"seconds": seconds, "started_ms": started_ms, "job_ids": job_ids}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
