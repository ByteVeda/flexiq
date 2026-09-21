"""RQ on Redis.

RQ has no in-process concurrency: a worker runs one job at a time and, by
default, forks a child for each one. ``rq worker-pool`` is how RQ itself
answers "run four at once", and the fork stays because it is RQ's default and
the benchmark publishes defaults rather than the tuning that flatters them.
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
    concurrency_model = "4 worker processes (rq worker-pool -n 4; forks per job)"
    needs_redis = True

    def engine_version(self, ctx: RunContext) -> str:
        return version("rq")

    def workers(self, ctx: RunContext) -> list[Worker]:
        return [
            Worker(
                name=f"{self.id}-worker",
                argv=[
                    "rq",
                    "worker-pool",
                    "--num-workers",
                    str(ctx.scenario.concurrency),
                    "--url",
                    ctx.require_redis(),
                    "--logging-level",
                    "ERROR",
                    ctx.queue,
                ],
                cwd=ctx.bench_root,
                env=ctx.worker_env(),
                log_path=ctx.run_dir / "worker.log",
            )
        ]
