"""Celery on Redis.

Started with the stock prefork pool. The three ``--without-*`` flags turn off
worker-to-worker chatter that has no counterpart in the other four entrants —
gossip, mingle and heartbeat are cluster features, not queue throughput — and
leaving them on would charge Celery for something the scenario never asks of
it.
"""

from __future__ import annotations

from importlib.metadata import version

from harness.adapters.base import RunContext
from harness.adapters.python_adapter import PythonAdapter
from harness.process import Worker


class Celery(PythonAdapter):
    id = "celery-redis"
    engine = "Celery"
    backend = "redis"
    producer_engine = "celery"
    concurrency_model = "4 prefork processes (-c 4, Celery's default pool)"
    needs_redis = True

    def engine_version(self, ctx: RunContext) -> str:
        return version("celery")

    def workers(self, ctx: RunContext) -> list[Worker]:
        return [
            Worker(
                name=f"{self.id}-worker",
                argv=[
                    "celery",
                    "-A",
                    "harness.workers.celery_app",
                    "worker",
                    "--queues",
                    ctx.queue,
                    "--concurrency",
                    str(ctx.scenario.concurrency),
                    "--loglevel",
                    "ERROR",
                    "--without-gossip",
                    "--without-mingle",
                    "--without-heartbeat",
                ],
                cwd=ctx.bench_root,
                env=ctx.worker_env(),
                log_path=ctx.run_dir / "worker.log",
            )
        ]
