"""Idle cost — what a worker pool spends doing nothing.

The third axis, and the one a throughput benchmark normally hides. FlexiQ's
scheduler polls on an interval; RQ and BullMQ block on ``BRPOP`` and wake only
when work arrives. That difference does not show up in a drain time and it
shows up sharply on a small instance, so it is measured and published beside
the wins.

Sampled with the queue empty and every worker still running, over the whole
process tree: a Celery prefork pool, an RQ worker pool and a Node worker are
one, four and one processes respectively, and only the tree makes them
comparable.
"""

from __future__ import annotations

import time
from collections.abc import Sequence
from contextlib import suppress

import psutil


def _refresh(roots: Sequence[int], known: dict[int, psutil.Process]) -> dict[int, psutil.Process]:
    """The live process tree, reusing the handles already being sampled.

    ``cpu_percent(None)`` is a delta against the *same object's* previous call,
    so a tree rebuilt from scratch each round reports 0.0 forever. Keeping the
    handle per pid is what makes the measurement a measurement.
    """
    for pid in roots:
        try:
            parent = psutil.Process(pid)
        except psutil.NoSuchProcess:
            continue
        for proc in (parent, *parent.children(recursive=True)):
            if proc.pid not in known:
                _prime_cpu(proc)
                known[proc.pid] = proc
    return known


def _prime_cpu(proc: psutil.Process) -> None:
    """The first ``cpu_percent`` call on a process always returns 0.0 by definition."""
    with suppress(psutil.NoSuchProcess, psutil.AccessDenied):
        proc.cpu_percent(None)


def sample(roots: Sequence[int], window_s: float, interval_s: float = 1.0) -> dict[str, object]:
    """Average CPU and peak RSS across ``roots`` and their children.

    ``cpu_pct`` is a percentage of a single core summed over the tree, which is
    how ``top`` reports it — 100 means one core saturated, not the machine.
    """
    known: dict[int, psutil.Process] = {}
    _refresh(roots, known)

    cpu_samples: list[float] = []
    rss_samples: list[int] = []
    deadline = time.monotonic() + window_s
    while time.monotonic() < deadline:
        time.sleep(interval_s)
        _refresh(roots, known)
        cpu = 0.0
        rss = 0
        for proc in list(known.values()):
            try:
                cpu += proc.cpu_percent(None)
                rss += proc.memory_info().rss
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                known.pop(proc.pid, None)
        cpu_samples.append(cpu)
        rss_samples.append(rss)

    if not cpu_samples:
        return {"window_s": window_s, "cpu_pct": 0.0, "rss_mb": 0.0, "processes": len(known)}
    return {
        "window_s": window_s,
        "cpu_pct": round(sum(cpu_samples) / len(cpu_samples), 2),
        "rss_mb": round(max(rss_samples) / 1024**2, 1),
        "processes": len(known),
    }
