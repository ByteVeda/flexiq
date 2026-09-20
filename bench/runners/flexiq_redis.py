"""FlexiQ on Redis — the same engine reaching the same broker as its rivals."""

from __future__ import annotations

from . import _flexiq_base as base
from ._common import main

SYSTEM = "flexiq-redis"

if __name__ == "__main__":
    main(
        SYSTEM,
        worker=base.worker(SYSTEM, "redis"),
        enqueue=base.enqueue(SYSTEM, "redis"),
        versions=base.versions,
        concurrency_model="N worker threads in the Rust core, one process",
        supports_batch=True,
    )
