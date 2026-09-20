"""The parts of a runner that must not differ between systems.

If a knob can change a number, it is set here for everybody: the log level, the
result-retention policy, where the sink lives, and the shape of the JSON a
runner prints. A runner file should contain its queue's API and nothing else
that could be accused of favouring it.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from collections.abc import Callable, Iterator
from pathlib import Path

BENCH_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BENCH_ROOT))

from harness import scenario as scenario_mod  # noqa: E402
from harness import sink  # noqa: E402


def sink_url() -> str:
    """Where the completion sink lives — set by `run.py`, never defaulted silently."""
    url = os.environ.get("BENCH_SINK_URL")
    if not url:
        raise SystemExit("BENCH_SINK_URL is not set; runners are launched by run.py")
    return url


def system_name(default: str) -> str:
    """Which results row this process is being run as.

    A runner module can serve more than one row — the two FlexiQ
    configurations share one file — so the sink key, the namespace and the
    database filename all come from here rather than from the module's own
    constant. Getting this wrong is quiet: the worker drains happily and the
    harness waits on a sink key nobody is writing to.
    """
    return os.environ.get("BENCH_SYSTEM") or default


def venv_bin(name: str) -> str:
    """The console script next to the interpreter that is running us.

    Celery, Dramatiq and RQ are launched through their own CLIs, and those live
    in the same virtualenv as the libraries they are about to import. Looking
    them up on PATH finds whatever the shell was last pointed at — which on a
    machine with two environments is a benchmark of the wrong versions.
    """
    candidate = Path(sys.executable).parent / name
    return str(candidate) if candidate.exists() else name


def workdir() -> Path:
    path = Path(os.environ.get("BENCH_WORKDIR", BENCH_ROOT / ".work"))
    path.mkdir(parents=True, exist_ok=True)
    return path


def quiet_logs() -> None:
    """WARNING everywhere.

    Every one of these frameworks logs a line per job at INFO by default, and
    FlexiQ logs two. Left alone, a chunk of what looks like queue overhead is
    the logging module formatting timestamps, and the system that is chattiest
    out of the box loses a benchmark about something else entirely.
    """
    logging.basicConfig(level=logging.WARNING)
    logging.getLogger().setLevel(logging.WARNING)
    for name in ("celery", "dramatiq", "rq", "flexiq"):
        logging.getLogger(name).setLevel(logging.WARNING)


def emit(result: dict[str, object]) -> None:
    """Print the machine-readable result last, whatever a framework logged first."""
    print(json.dumps(result))


class Sink:
    """One Redis connection per worker process, opened once and reused.

    Opening it per job would measure connection setup five times over; opening
    it at import would have Celery's prefork children inherit a socket they
    must not share. Lazily, per process, is the only shape that is both fair
    and correct.
    """

    def __init__(self, system: str):
        self._system = system
        self._client = None

    def done(self, seq: int, enqueued_ns: int) -> None:
        if self._client is None:
            self._client = sink.connect(sink_url())
        sink.record(self._client, self._system, seq, enqueued_ns)


def pace(args: argparse.Namespace) -> "Iterator[int]":
    """Yield job indices, optionally holding each back to a fixed rate.

    The burst phase (`--rate 0`) submits as fast as the producer can, which
    measures enqueue throughput and — because a backlog forms — a latency that
    is really queueing delay. The paced phase submits below every system's
    measured capacity, so no backlog forms and the percentiles are service
    latency: what one caller waits.

    Paced against a schedule computed from the start, not by sleeping a fixed
    gap: a per-iteration sleep accumulates the submit cost into the interval
    and quietly delivers a slower rate than the one being reported.
    """
    if args.rate <= 0:
        yield from range(args.jobs)
        return

    started = time.perf_counter()
    for i in range(args.jobs):
        due = started + i / args.rate
        slack = due - time.perf_counter()
        if slack > 0:
            time.sleep(slack)
        yield i


def main(
    system: str,
    *,
    worker: Callable[[argparse.Namespace], None],
    enqueue: Callable[[argparse.Namespace, str], float],
    versions: Callable[[], dict[str, str | None]],
    concurrency_model: str,
    supports_batch: bool,
) -> None:
    """The shared command line. Called from every runner's `__main__`."""
    parser = argparse.ArgumentParser(prog=f"bench runner: {system}")
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("worker", help="run until SIGTERM")
    run.add_argument("--concurrency", type=int, required=True)

    put = sub.add_parser("enqueue", help="submit jobs and report the elapsed time")
    put.add_argument("--jobs", type=int, required=True)
    put.add_argument("--payload-bytes", type=int, required=True)
    put.add_argument("--seq-offset", type=int, default=0)
    put.add_argument("--mode", choices=("per-job", "batch"), default="per-job")
    put.add_argument("--rate", type=int, default=0,
                     help="jobs per second; 0 submits as fast as possible")

    sub.add_parser("versions", help="print the library versions in use")

    args = parser.parse_args()
    quiet_logs()

    if args.command == "worker":
        worker(args)
        return

    if args.command == "versions":
        emit({"versions": versions(), "concurrency_model": concurrency_model,
              "supports_batch": supports_batch})
        return

    if args.mode == "batch" and not supports_batch:
        raise SystemExit(f"{system}: no batch enqueue API")

    body = scenario_mod.payload(args.payload_bytes)
    started = time.perf_counter()
    enqueue(args, body)
    elapsed = time.perf_counter() - started
    emit(
        {
            "jobs": args.jobs,
            "mode": args.mode,
            "rate": args.rate,
            "seconds": round(elapsed, 6),
            "per_second": round(args.jobs / elapsed, 1) if elapsed > 0 else None,
        }
    )
