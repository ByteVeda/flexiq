"""FlexiQ on embedded SQLite — the configuration the README sells."""

from __future__ import annotations

from . import _flexiq_base as base
from ._common import main

SYSTEM = "flexiq-sqlite"

if __name__ == "__main__":
    main(
        SYSTEM,
        worker=base.worker(SYSTEM, "sqlite"),
        enqueue=base.enqueue(SYSTEM, "sqlite"),
        versions=base.versions,
        concurrency_model="N worker threads in the Rust core, one process",
        supports_batch=True,
    )
