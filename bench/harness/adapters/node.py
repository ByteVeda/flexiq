"""The Node entrants: BullMQ, and FlexiQ's own Node SDK.

The harness stays in Python and shells out, the same way it does for every
other entrant. What differs is only that the producer is a Node process, so its
submission clock is Node's — which is why the producer reports its own timing
instead of the harness timing it from outside.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

from harness.adapters.base import Adapter, EnqueueResult, RunContext, run_producer
from harness.process import BenchError, Worker


class NodeAdapter(Adapter):
    """An entrant driven by one `.mjs` with a `worker` and a `produce` subcommand."""

    #: The script under `bench/node/`.
    script: str
    #: The package whose installed version is reported.
    package: str

    def node_dir(self, ctx: RunContext) -> Path:
        return ctx.bench_root / "node"

    def env(self, ctx: RunContext) -> dict[str, str]:
        return ctx.worker_env()

    def language(self) -> str:
        done = subprocess.run(["node", "--version"], capture_output=True, text=True, check=False)
        return f"Node {done.stdout.strip() or 'unknown'}"

    def engine_version(self, ctx: RunContext) -> str:
        """Read off the installed tree, so the artifact cannot claim a pin that failed."""
        manifest = self.node_dir(ctx) / "node_modules" / self.package / "package.json"
        if not manifest.exists():
            raise BenchError(
                f"{self.package} is not installed — run `pnpm install` in {self.node_dir(ctx)}"
            )
        return str(json.loads(manifest.read_text())["version"])

    def workers(self, ctx: RunContext) -> list[Worker]:
        return [
            Worker(
                name=f"{self.id}-worker",
                argv=["node", self.script, "worker"],
                cwd=self.node_dir(ctx),
                env=self.env(ctx),
                log_path=ctx.run_dir / "worker.log",
            )
        ]

    def enqueue(self, ctx: RunContext, first_index: int, count: int) -> EnqueueResult:
        return run_producer(
            ["node", self.script, "produce", "--first", str(first_index), "--count", str(count)],
            cwd=self.node_dir(ctx),
            env=self.env(ctx),
            timeout_s=ctx.scenario.drain_timeout_s,
        )


class BullMq(NodeAdapter):
    id = "bullmq-redis"
    engine = "BullMQ"
    backend = "redis"
    script = "bullmq.mjs"
    package = "bullmq"
    concurrency_model = "4 concurrent jobs in one process (concurrency: 4)"
    needs_redis = True


class FlexiQNode(NodeAdapter):
    engine = "FlexiQ"
    script = "flexiq.mjs"
    package = "@byteveda/flexiq"
    concurrency_model = "4 concurrent jobs in one process (concurrency: 4)"

    backend_key: str

    def env(self, ctx: RunContext) -> dict[str, str]:
        return {**ctx.worker_env(), "BENCH_FLEXIQ_BACKEND": self.backend_key}


class FlexiQNodeSqlite(FlexiQNode):
    id = "flexiq-node-sqlite"
    backend = "sqlite"
    backend_key = "sqlite"


class FlexiQNodeRedis(FlexiQNode):
    id = "flexiq-node-redis"
    backend = "redis"
    backend_key = "redis"
    needs_redis = True
