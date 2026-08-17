"""Producer-side task batching.

A task declared with ``@queue.task(batch=...)`` collects per-call items in
memory and dispatches them as a single job whose payload is the list of
collected items. The task function receives the list:

.. code-block:: python

    @queue.task(batch=True)
    def send_emails(items: list[Email]) -> None:
        for item in items:
            send(item)

    queue.enqueue("send_emails", args=(email1,))
    queue.enqueue("send_emails", args=(email2,))
    # When the buffer reaches max_size or max_wait_ms elapses, the worker
    # eventually runs send_emails([email1, email2, ...]) as one job.

This trade-off matches Celery's ``Batches`` extension: items in the in-memory
buffer at process crash are lost. Critical tasks should not enable batching.

See :class:`BatchConfig` for tuning knobs. See :class:`BatchAccumulator` for
the runtime that owns the buffer.
"""

from __future__ import annotations

from flexiq.batching.accumulator import BatchAccumulator
from flexiq.batching.config import BatchConfig
from flexiq.batching.item_result import (
    BatchItemResult,
    BatchPartialFailureError,
    BatchResultTypeError,
)
from flexiq.batching.result import BatchedJobResult

__all__ = [
    "BatchAccumulator",
    "BatchConfig",
    "BatchItemResult",
    "BatchPartialFailureError",
    "BatchResultTypeError",
    "BatchedJobResult",
]
