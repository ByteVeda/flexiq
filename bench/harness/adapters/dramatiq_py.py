"""Dramatiq on Redis.

One process and four threads, which is the shape Dramatiq's own documentation
reaches for on an I/O-bound actor — and the scenario is entirely I/O bound.
"""

from __future__ import annotations

from importlib.metadata import version

from harness.adapters.base import RunContext
from harness.adapters.python_adapter import PythonAdapter
from harness.process import Worker


class Dramatiq(PythonAdapter):
    id = "dramatiq-redis"
    engine = "Dramatiq"
    backend = "redis"
    producer_engine = "dramatiq"
    concurrency_model = "1 process x 4 threads (--processes 1 --threads 4)"
    needs_redis = True

    def engine_version(self, ctx: RunContext) -> str:
        return version("dramatiq")

    def workers(self, ctx: RunContext) -> list[Worker]:
        return [
            Worker(
                name=f"{self.id}-worker",
                argv=[
                    "dramatiq",
                    "harness.workers.dramatiq_app",
                    "--processes",
                    "1",
                    "--threads",
                    str(ctx.scenario.concurrency),
                    "--queues",
                    ctx.queue,
                ],
                cwd=ctx.bench_root,
                env=ctx.worker_env(),
                log_path=ctx.run_dir / "worker.log",
            )
        ]
