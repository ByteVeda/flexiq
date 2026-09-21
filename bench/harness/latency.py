"""End-to-end latency: one measurement path, five frameworks.

Every handler — Celery's, Dramatiq's, RQ's, BullMQ's and FlexiQ's — appends one
short line to the same file:

    <job index> <latency in ms> <completion wall clock in ms>

``O_APPEND`` writes below ``PIPE_BUF`` are atomic on Linux, so forked Celery
children, Dramatiq threads and a Node worker can all share one sink without a
lock or a coordinating process. The completion clock is in the line because it
makes the drain time a property of the run rather than of the harness's own
polling: the last completion minus the first enqueue, however late the harness
noticed.

FlexiQ stores ``created_at``/``completed_at`` per job and could answer this from
the database, but the competitors cannot. A number read one way for FlexiQ and
another way for everyone else is not a comparison, so the database columns are
only ever a cross-check.
"""

from __future__ import annotations

import math
import os
from dataclasses import dataclass
from pathlib import Path

#: Environment variables carrying the sink and the padding into every worker.
SINK_ENV = "BENCH_SINK"


@dataclass(frozen=True)
class Record:
    index: int
    latency_ms: float
    completed_ms: float


class LatencySink:
    """An append-only sample file, safe to share across processes and threads."""

    def __init__(self, path: Path) -> None:
        # 0o600: every process that writes here is a worker this harness
        # started, so the run's own user is the only reader it ever needs.
        self._fd = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o600)

    def record(self, index: int, latency_ms: float, completed_ms: float) -> None:
        os.write(self._fd, f"{index} {latency_ms:.3f} {completed_ms:.3f}\n".encode())

    def close(self) -> None:
        os.close(self._fd)


def read_records(path: Path) -> list[Record]:
    """Every completion the sink holds, warmup included."""
    if not path.exists():
        return []
    out: list[Record] = []
    for line in path.read_text(errors="replace").splitlines():
        parts = line.split()
        if len(parts) != 3:
            continue
        out.append(Record(int(parts[0]), float(parts[1]), float(parts[2])))
    return out


def measured(records: list[Record], warmup_jobs: int) -> list[Record]:
    """Warmup is identified by index, so it pays for connection setup and JIT in-run.

    A second timed phase would drift out of step with the first; an index
    threshold cannot.
    """
    return [r for r in records if r.index >= warmup_jobs]


def percentile(sorted_samples: list[float], q: float) -> float:
    """Nearest-rank percentile: the value at or above which ``q`` of the samples lie.

    Nearest-rank rather than an interpolating definition because every sample
    here is a real observed latency, and a p99 that no job actually experienced
    is worse than a slightly coarse one.
    """
    if not sorted_samples:
        return 0.0
    rank = min(len(sorted_samples), max(1, math.ceil(q * len(sorted_samples))))
    return sorted_samples[rank - 1]


def summarise(samples: list[float]) -> dict[str, float | int]:
    if not samples:
        return {"count": 0, "p50": 0.0, "p95": 0.0, "p99": 0.0, "max": 0.0, "mean": 0.0}
    ordered = sorted(samples)
    return {
        "count": len(ordered),
        "p50": round(percentile(ordered, 0.50), 2),
        "p95": round(percentile(ordered, 0.95), 2),
        "p99": round(percentile(ordered, 0.99), 2),
        "max": round(ordered[-1], 2),
        "mean": round(sum(ordered) / len(ordered), 2),
    }
