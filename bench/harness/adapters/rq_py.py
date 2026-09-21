"""RQ on Redis.

RQ has no in-process concurrency: a worker runs one job at a time and, by
default, forks a child for each one. Four at once therefore means four worker
processes, which is how an RQ deployment is actually supervised — and it is
four separate ``rq worker`` invocations rather than one ``rq worker-pool``,
because the pool wrapper forks its work horses out of a parent that is already
managing several workers, and two of five warmup jobs hung there indefinitely
over a high-latency link.

The fork-per-job stays: it is RQ's default, and the benchmark publishes
defaults rather than the tuning that flatters them.
"""

from __future__ import annotations

from importlib.metadata import version

from harness.adapters.base import RunContext
from harness.adapters.python_adapter import PythonAdapter
from harness.process import Worker


class Rq(PythonAdapter):
    id = "rq-redis"
    engine = "RQ"
    backend = "redis"
    producer_engine = "rq"
    concurrency_model = "4 `rq worker` processes (forks a child per job — RQ's default)"
    needs_redis = True

    def engine_version(self, ctx: RunContext) -> str:
        return version("rq")

    def workers(self, ctx: RunContext) -> list[Worker]:
        url = ctx.require_redis()
        return [
            Worker(
                name=f"{self.id}-worker-{n}",
                argv=[
                    "rq",
                    "worker",
                    "--url",
                    url,
                    # `rq worker` spells this with an underscore; `worker-pool`
                    # spells it with a dash. Not a typo.
                    "--logging_level",
                    "ERROR",
                    ctx.queue,
                ],
                cwd=ctx.bench_root,
                env=ctx.worker_env(),
                log_path=ctx.run_dir / f"worker-{n}.log",
            )
            for n in range(ctx.scenario.concurrency)
        ]
