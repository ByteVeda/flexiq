"""The entrants, in the order the results are told in.

Same-backend first — the head-to-head the scenario is actually about — then the
two no-broker deployments, which are a different question and are kept visibly
apart from the answer to the first one.
"""

from __future__ import annotations

from collections.abc import Sequence

from harness.adapters.base import Adapter, EnqueueResult, RunContext, run_producer
from harness.adapters.celery_py import Celery
from harness.adapters.dramatiq_py import Dramatiq
from harness.adapters.flexiq_py import FlexiQRedis, FlexiQSqlite
from harness.adapters.node import BullMq, FlexiQNodeRedis, FlexiQNodeSqlite
from harness.adapters.rq_py import Rq

ADAPTERS: tuple[Adapter, ...] = (
    FlexiQRedis(),
    Celery(),
    Dramatiq(),
    Rq(),
    BullMq(),
    FlexiQNodeRedis(),
    FlexiQSqlite(),
    FlexiQNodeSqlite(),
)

ADAPTER_IDS = tuple(adapter.id for adapter in ADAPTERS)


def select(ids: Sequence[str] | None) -> list[Adapter]:
    """The requested entrants, in artifact order — an unknown id is a mistake, not a skip."""
    if not ids:
        return list(ADAPTERS)
    unknown = sorted(set(ids) - set(ADAPTER_IDS))
    if unknown:
        raise ValueError(
            f"unknown runtime(s): {', '.join(unknown)}; known: {', '.join(ADAPTER_IDS)}"
        )
    wanted = set(ids)
    return [adapter for adapter in ADAPTERS if adapter.id in wanted]


__all__ = [
    "ADAPTERS",
    "ADAPTER_IDS",
    "Adapter",
    "EnqueueResult",
    "RunContext",
    "run_producer",
    "select",
]
