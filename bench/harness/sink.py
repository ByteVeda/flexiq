"""The completion sink — the one thing every system under test has in common.

Each worker, whatever queue it drains, does exactly one extra thing per job:
`RPUSH` a `[seq, enqueued_ns, completed_ns]` record onto a Redis list. Nothing
reads a framework's own result backend, because five result backends mean five
different definitions of "done" and no comparison at all. One sink also means
one instrumentation cost, paid identically — including by FlexiQ on SQLite,
which has to reach Redis for it and is not given that back.

The record format is duplicated, by hand, in `runners/bullmq/sink.mjs`. It is
four fields and a list name; a shared package for it would cost more than it
saves. If you change it here, change it there, and the schema check in
`report.py` will tell you if you forgot.
"""

from __future__ import annotations

import json
import math
import time
from dataclasses import dataclass

import redis

#: Every key this harness writes lives under here, so a stray run cannot be
#: mistaken for application data and `flush()` never reaches past its own.
PREFIX = "flexiq-bench"

NS_PER_S = 1_000_000_000


def sink_key(system: str) -> str:
    return f"{PREFIX}:sink:{system}"


def queue_key(system: str) -> str:
    """Namespace a system's own broker keys, so two runs cannot share a backlog."""
    return f"{PREFIX}:q:{system}"


def connect(url: str) -> redis.Redis:
    return redis.Redis.from_url(url)


def record(client: redis.Redis, system: str, seq: int, enqueued_ns: int) -> None:
    """Called from inside a task body, on the worker, once per job."""
    client.rpush(sink_key(system), json.dumps([seq, enqueued_ns, time.time_ns()]))


def flush(client: redis.Redis, system: str) -> None:
    client.delete(sink_key(system))


def count(client: redis.Redis, system: str) -> int:
    return int(client.llen(sink_key(system)))


def read_all(client: redis.Redis, system: str) -> list[tuple[int, int, int]]:
    raw = client.lrange(sink_key(system), 0, -1)
    return [tuple(json.loads(entry)) for entry in raw]  # type: ignore[misc]


class DrainTimeout(RuntimeError):
    """The worker did not finish inside the scenario's ceiling.

    Raised rather than returning what arrived: percentiles over a truncated
    drain are the flattering half of a distribution, and reporting them as if
    they were the whole is the failure mode this whole harness exists to avoid.
    """


def wait_for(client: redis.Redis, system: str, target: int, timeout_s: int) -> None:
    """Block until `target` records land, or give up loudly."""
    deadline = time.monotonic() + timeout_s
    while True:
        have = count(client, system)
        if have >= target:
            return
        if time.monotonic() > deadline:
            raise DrainTimeout(
                f"{system}: {have} of {target} jobs completed in {timeout_s}s"
            )
        time.sleep(0.02)


def percentile(sorted_values: list[float], q: float) -> float:
    """Nearest-rank percentile of an already-sorted list.

    Nearest-rank rather than interpolated: every value it can return is a
    latency something actually measured, which is the right property for a
    figure that will be quoted.
    """
    if not sorted_values:
        raise ValueError("no values")
    rank = min(len(sorted_values), max(1, math.ceil(q * len(sorted_values))))
    return sorted_values[rank - 1]


@dataclass(frozen=True)
class Drain:
    """What the sink says about one measured round."""

    completed: int
    latency_ms: dict[str, float]
    drain_seconds: float
    completion_per_second: float

    def as_dict(self) -> dict[str, object]:
        return {
            "completed": self.completed,
            "latency_ms": self.latency_ms,
            "drain_seconds": round(self.drain_seconds, 4),
            "completion_per_second": round(self.completion_per_second, 1),
        }


def summarise(records: list[tuple[int, int, int]]) -> Drain:
    """Turn raw sink records into the two figures the issue asks to see apart.

    End-to-end latency is per job — enqueue to completion, the number a caller
    waits. Completion throughput is per round — first enqueue to last
    completion, the number a backlog clears at. A queue can be good at one and
    bad at the other, which is the entire point of reporting both.
    """
    if not records:
        raise ValueError("no records to summarise")

    latencies = sorted((done - sent) / 1e6 for _, sent, done in records)
    first_enqueue = min(sent for _, sent, _ in records)
    last_completion = max(done for _, _, done in records)
    window = (last_completion - first_enqueue) / NS_PER_S

    return Drain(
        completed=len(records),
        latency_ms={
            "p50": round(percentile(latencies, 0.50), 3),
            "p90": round(percentile(latencies, 0.90), 3),
            "p99": round(percentile(latencies, 0.99), 3),
            "max": round(latencies[-1], 3),
            "mean": round(sum(latencies) / len(latencies), 3),
        },
        drain_seconds=window,
        completion_per_second=len(records) / window if window > 0 else float("nan"),
    )


def paced(records: list[tuple[int, int, int]], target_rate: int) -> dict[str, object]:
    """Service latency from the paced phase, and whether to believe it.

    The phase is only meaningful while nothing queues. If the system could not
    keep up with the offered rate, latency climbs through the run and the
    percentiles are queueing delay again — the same thing the burst phase
    already reports. Rather than quietly hand that back as "latency", the last
    tenth is compared against the first: a tail that has grown by more than
    half sets `saturated`, and every consumer of this file is expected to say
    so rather than print the number bare.
    """
    if not records:
        raise ValueError("no records to summarise")

    in_order = [((done - sent) / 1e6) for _, sent, done in sorted(records, key=lambda r: r[1])]
    tenth = max(1, len(in_order) // 10)
    head = sum(in_order[:tenth]) / tenth
    tail = sum(in_order[-tenth:]) / tenth

    latencies = sorted(in_order)
    first_enqueue = min(sent for _, sent, _ in records)
    last_enqueue = max(sent for _, sent, _ in records)
    span = (last_enqueue - first_enqueue) / NS_PER_S
    achieved = (len(records) - 1) / span if span > 0 else float("nan")

    return {
        "completed": len(records),
        "target_rate_per_second": target_rate,
        "achieved_rate_per_second": round(achieved, 1),
        "latency_ms": {
            "p50": round(percentile(latencies, 0.50), 3),
            "p90": round(percentile(latencies, 0.90), 3),
            "p99": round(percentile(latencies, 0.99), 3),
            "max": round(latencies[-1], 3),
            "mean": round(sum(latencies) / len(latencies), 3),
        },
        "first_decile_mean_ms": round(head, 3),
        "last_decile_mean_ms": round(tail, 3),
        "saturated": tail > head * 1.5,
    }
