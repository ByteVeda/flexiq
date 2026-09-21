"""FlexiQ, from the Python SDK, on SQLite and on Redis.

Two entrants rather than one with a switch. SQLite is a file on the local disk
and Redis is a network service: the same engine, but not the same deployment,
and folding them into a single row would let the local number stand in for the
networked one.
"""

from __future__ import annotations

import sys
from importlib.metadata import version

from harness.adapters.base import RunContext
from harness.adapters.python_adapter import PythonAdapter
from harness.process import Worker


class FlexiQPython(PythonAdapter):
    engine = "FlexiQ"
    producer_engine = "flexiq"
    concurrency_model = "4 worker threads (workers=4)"

    #: ``sqlite`` or ``redis`` — handed to both the worker and the producer so
    #: the two processes cannot open different stores.
    backend_key: str

    def engine_version(self, ctx: RunContext) -> str:
        return version("flexiq")

    def _env(self, ctx: RunContext) -> dict[str, str]:
        return {**ctx.worker_env(), "BENCH_FLEXIQ_BACKEND": self.backend_key}

    def producer_env(self, ctx: RunContext) -> dict[str, str]:
        return self._env(ctx)

    def workers(self, ctx: RunContext) -> list[Worker]:
        return [
            Worker(
                name=f"{self.id}-worker",
                argv=[sys.executable, "-m", "harness.workers.flexiq_worker"],
                cwd=ctx.bench_root,
                env=self._env(ctx),
                log_path=ctx.run_dir / "worker.log",
            )
        ]


class FlexiQSqlite(FlexiQPython):
    id = "flexiq-sqlite"
    backend = "sqlite"
    backend_key = "sqlite"


class FlexiQRedis(FlexiQPython):
    id = "flexiq-redis"
    backend = "redis"
    backend_key = "redis"
    needs_redis = True
