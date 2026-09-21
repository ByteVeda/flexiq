"""What every runtime has to provide, and the shape of the run they all share.

The template is in one place on purpose. Five frameworks measured by five
slightly different loops is five benchmarks, not one — so the phases, the
clocks and the completion test live here, and an adapter only says how to start
its workers and how to submit one job.
"""

from __future__ import annotations

import json
import os
import subprocess
from abc import ABC, abstractmethod
from collections.abc import Sequence
from contextlib import ExitStack
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from harness import idle, latency
from harness.process import BenchError, Worker, wait_until
from harness.redis_scope import RunNames
from harness.scenario import Scenario


@dataclass(frozen=True)
class RunContext:
    """Everything an adapter needs, and nothing it should have to derive twice."""

    adapter_id: str
    scenario: Scenario
    run_dir: Path
    names: RunNames
    bench_root: Path
    redis_url: str | None = None

    @property
    def sink(self) -> Path:
        return self.run_dir / "latency.sink"

    @property
    def queue(self) -> str:
        return self.names.queue(self.adapter_id)

    @property
    def task(self) -> str:
        return self.names.task(self.adapter_id)

    @property
    def db_path(self) -> Path:
        return self.run_dir / "bench.db"

    def require_redis(self) -> str:
        """The Redis URL, or a clear refusal — an adapter must not guess a default."""
        if not self.redis_url:
            raise BenchError(f"{self.adapter_id} needs Redis; set REDIS_URL")
        return self.redis_url

    def worker_env(self) -> dict[str, str]:
        """The scenario, handed to a worker process in the one form every runtime reads."""
        env = {
            latency.SINK_ENV: str(self.sink),
            "BENCH_QUEUE": self.queue,
            "BENCH_TASK": self.task,
            "BENCH_CONCURRENCY": str(self.scenario.concurrency),
            "BENCH_DB": str(self.db_path),
            "BENCH_PAD": self.scenario.pad,
            "PYTHONPATH": str(self.bench_root),
            "PYTHONUNBUFFERED": "1",
        }
        if self.redis_url:
            env["BENCH_REDIS_URL"] = self.redis_url
        return env


@dataclass
class EnqueueResult:
    seconds: float
    started_ms: float
    job_ids: list[str] = field(default_factory=list)


class Adapter(ABC):
    """One queue engine on one backend."""

    #: Stable identifier — the artifact key, the chart label and the queue name.
    id: str
    engine: str
    backend: str
    #: Written out verbatim. "4" means a different thing to Celery, RQ and
    #: BullMQ, and a chart that hides that is lying by omission.
    concurrency_model: str
    needs_redis: bool = False

    @abstractmethod
    def engine_version(self, ctx: RunContext) -> str:
        """The version actually installed, read at run time rather than restated."""

    @abstractmethod
    def language(self) -> str:
        """Runtime and version — ``Python 3.12.3``, ``Node v24.12.0``."""

    @abstractmethod
    def workers(self, ctx: RunContext) -> list[Worker]:
        """The worker processes, unstarted."""

    @abstractmethod
    def enqueue(self, ctx: RunContext, first_index: int, count: int) -> EnqueueResult:
        """Submit ``count`` jobs serially, starting at ``first_index``.

        Serially, and never through a batch API: FlexiQ has ``enqueue_many``
        and most of the others do not, so using it would measure a feature
        rather than a queue.
        """

    def measure(self, ctx: RunContext) -> dict[str, Any]:
        """Warm up, submit, drain, then sit idle — in that order, once."""
        scenario = ctx.scenario
        ctx.run_dir.mkdir(parents=True, exist_ok=True)
        ctx.sink.touch()

        with ExitStack() as stack:
            workers = [stack.enter_context(worker) for worker in self.workers(ctx)]

            # Warmup is untimed but not separate: the same queue, the same
            # handler, indices below the threshold the summary filters on.
            warm = self.enqueue(ctx, 0, scenario.warmup_jobs)
            wait_until(
                lambda: len(latency.read_records(ctx.sink)) >= scenario.warmup_jobs,
                workers=workers,
                timeout_s=scenario.drain_timeout_s,
                what=f"{self.id} warmup ({scenario.warmup_jobs} jobs)",
            )

            submitted = self.enqueue(ctx, scenario.warmup_jobs, scenario.jobs)
            wait_until(
                lambda: (
                    len(latency.measured(latency.read_records(ctx.sink), scenario.warmup_jobs))
                    >= scenario.jobs
                ),
                workers=workers,
                timeout_s=scenario.drain_timeout_s,
                what=f"{self.id} drain ({scenario.jobs} jobs)",
            )

            records = latency.measured(latency.read_records(ctx.sink), scenario.warmup_jobs)
            drain_s = (max(r.completed_ms for r in records) - submitted.started_ms) / 1000

            # The queue is empty and the workers are still up: this is the cost
            # of being available, which is the number a poller loses on.
            idle_stats = idle.sample(
                [pid for worker in workers for pid in worker.pids()],
                scenario.idle_window_s,
            )

        return {
            "id": self.id,
            "engine": self.engine,
            "engine_version": self.engine_version(ctx),
            "language": self.language(),
            "backend": self.backend,
            "concurrency_model": self.concurrency_model,
            "enqueue": {
                "jobs": scenario.jobs,
                "seconds": round(submitted.seconds, 3),
                "per_second": round(scenario.jobs / submitted.seconds, 1),
            },
            "drain": {
                "seconds": round(drain_s, 3),
                "per_second": round(scenario.jobs / drain_s, 1) if drain_s > 0 else 0.0,
            },
            "latency_ms": latency.summarise([r.latency_ms for r in records]),
            "idle": idle_stats,
            "job_ids": [*warm.job_ids, *submitted.job_ids],
        }


def run_producer(
    argv: Sequence[str], cwd: Path, env: dict[str, str], timeout_s: float
) -> EnqueueResult:
    """Run a producer subprocess and read its one JSON line.

    The timings come out of the producer rather than off the harness's clock so
    that process startup — an import of Celery, a Node boot — is never counted
    as submission time. The timeout is not optional: a client that submits
    everything and then declines to exit would otherwise hang the whole run
    with no output at all.
    """
    try:
        done = subprocess.run(
            list(argv),
            cwd=cwd,
            env={**os.environ, **env},
            capture_output=True,
            text=True,
            timeout=timeout_s,
        )
    except subprocess.TimeoutExpired as exc:
        raise BenchError(
            f"producer {' '.join(argv)} did not exit within {timeout_s:.0f}s"
        ) from exc
    if done.returncode != 0:
        raise BenchError(
            f"producer {' '.join(argv)} exited {done.returncode}\n{done.stderr.strip()[-2000:]}"
        )
    line = done.stdout.strip().splitlines()[-1] if done.stdout.strip() else ""
    try:
        payload = json.loads(line)
    except json.JSONDecodeError as exc:
        raise BenchError(f"producer wrote no result line: {done.stdout[-500:]!r}") from exc
    return EnqueueResult(
        seconds=float(payload["seconds"]),
        started_ms=float(payload["started_ms"]),
        job_ids=[str(i) for i in payload.get("job_ids", [])],
    )
