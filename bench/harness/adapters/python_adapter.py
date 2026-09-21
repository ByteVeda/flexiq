"""What the four Python entrants share.

Only the submission path: each of them still starts its workers its own way,
because "concurrency 4" is four processes to Celery, four threads to Dramatiq
and four forking workers to RQ, and flattening that would be the lie.
"""

from __future__ import annotations

import platform
import sys

from harness.adapters.base import Adapter, EnqueueResult, RunContext, run_producer


class PythonAdapter(Adapter):
    """An entrant whose producer is ``harness.workers.producer``."""

    #: Which branch of the producer to take — not the same string as ``id``,
    #: since FlexiQ has one producer and two backends.
    producer_engine: str

    def language(self) -> str:
        return f"Python {platform.python_version()}"

    def producer_env(self, ctx: RunContext) -> dict[str, str]:
        return ctx.worker_env()

    def enqueue(self, ctx: RunContext, first_index: int, count: int) -> EnqueueResult:
        return run_producer(
            [
                sys.executable,
                "-m",
                "harness.workers.producer",
                "--engine",
                self.producer_engine,
                "--first",
                str(first_index),
                "--count",
                str(count),
            ],
            cwd=ctx.bench_root,
            env=self.producer_env(ctx),
            timeout_s=ctx.scenario.drain_timeout_s,
        )
