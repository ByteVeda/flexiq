"""RQ's side of the scenario.

RQ enqueues a *reference* to a callable and the worker imports it, so this
module has to be importable from the worker's ``PYTHONPATH`` and has to hold
nothing that depends on which process it is in.
"""

from __future__ import annotations

from typing import Any

from harness.workers.payload import handle


def bench_task(payload: dict[str, Any]) -> None:
    handle(payload)
