"""Thread-safe per-task buffer that flushes by size or time."""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from flexiq.batching.config import BatchConfig
from flexiq.batching.result import BatchedJobResult

if TYPE_CHECKING:
    from flexiq.app import Queue
    from flexiq.result import JobResult

logger = logging.getLogger("flexiq.batching")


@dataclass
class _BufferState:
    """One entry per batched task name."""

    items: list[Any]
    deadline_monotonic: float  # earliest moment we must flush
    config: BatchConfig
    # All result handles handed out for items in this buffer. The accumulator
    # resolves them in one shot when the buffer dispatches. A fresh
    # ``threading.Event`` is created per buffer so callers waiting on a
    # previous batch don't block on this one.
    pending_results: list[BatchedJobResult] = field(default_factory=list)
    flush_event: threading.Event = field(default_factory=threading.Event)


# The dispatch callback enqueues one job for the batch and returns the
# corresponding ``JobResult`` so the accumulator can attach it to every
# pending ``BatchedJobResult`` handle. Returning ``None`` is allowed for
# legacy fire-and-forget callers but yields handles whose ``result()``
# raises ``RuntimeError`` after the flush event.
DispatchCallback = Callable[[str, list[Any], BatchConfig], "JobResult | None"]


class BatchAccumulator:
    """Owns per-task buffers and a daemon thread that flushes them on time.

    All operations are thread-safe. Items are dispatched via the
    ``dispatch`` callback, which receives ``(task_name, items_list,
    config)`` and is responsible for actually enqueueing the batched
    payload (see :meth:`Queue._dispatch_batched_payload` in ``app.py``).
    The callback returns the ``JobResult`` for the enqueued batch job so
    per-item ``BatchedJobResult`` handles can resolve to it.

    A single flusher thread serves every batched task on the queue. It
    sleeps on a condition variable that wakes either on the next deadline
    or on every ``add`` (so a newly-shortened deadline is picked up
    promptly). The thread starts lazily on the first ``add`` call and
    stops cleanly on :meth:`shutdown`.
    """

    def __init__(
        self,
        dispatch: DispatchCallback,
        queue: Queue | None = None,
    ) -> None:
        self._dispatch = dispatch
        # Back-reference used when constructing per-item BatchedJobResult
        # handles so they can fetch the underlying job's result. Optional
        # to keep backwards compatibility with tests that drive the
        # accumulator directly without a Queue.
        self._queue = queue
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self._buffers: dict[str, _BufferState] = {}
        self._flusher: threading.Thread | None = None
        self._stop = threading.Event()

    def add(self, task_name: str, item: Any, config: BatchConfig) -> BatchedJobResult:
        """Add an item to the named task's batch.

        Returns a :class:`BatchedJobResult` that resolves to the per-item
        outcome once the batch flushes. Flushes synchronously when the
        buffer reaches ``max_size``.
        """
        result_handle: BatchedJobResult
        to_flush_items: list[Any] | None = None
        flush_config = config
        flush_event_to_signal: threading.Event | None = None
        flush_pending: list[BatchedJobResult] = []
        with self._cond:
            state = self._buffers.get(task_name)
            if state is None:
                state = _BufferState(
                    items=[item],
                    deadline_monotonic=time.monotonic() + config.max_wait_ms / 1000.0,
                    config=config,
                )
                self._buffers[task_name] = state
                item_index = 0
            else:
                # If the per-call config changed, keep the original — the
                # decorator is the source of truth and shouldn't change
                # between calls. (Defensive: still honor the new max_size if
                # it's stricter.)
                item_index = len(state.items)
                state.items.append(item)

            result_handle = BatchedJobResult(
                task_name=task_name,
                item_index=item_index,
                per_item_results=state.config.per_item_results,
                queue=self._queue,  # type: ignore[arg-type]
                flush_event=state.flush_event,
            )
            state.pending_results.append(result_handle)

            if len(state.items) >= state.config.max_size:
                to_flush_items = state.items
                flush_config = state.config
                flush_event_to_signal = state.flush_event
                flush_pending = list(state.pending_results)
                del self._buffers[task_name]

            self._cond.notify_all()
            self._ensure_flusher_locked()

        if to_flush_items is not None and flush_event_to_signal is not None:
            self._safe_dispatch(
                task_name, to_flush_items, flush_config, flush_pending, flush_event_to_signal
            )
        return result_handle

    def flush_all(self) -> None:
        """Synchronously flush every non-empty buffer.

        Called from :meth:`Queue.close` and from atexit. Items still in the
        buffer at process exit without an explicit close are lost — this
        matches Celery ``Batches`` semantics.
        """
        with self._cond:
            pending = list(self._buffers.items())
            self._buffers.clear()
            self._cond.notify_all()
        for task_name, state in pending:
            self._safe_dispatch(
                task_name,
                state.items,
                state.config,
                list(state.pending_results),
                state.flush_event,
            )

    def shutdown(self, *, flush: bool = True) -> None:
        """Stop the flusher thread.

        When ``flush=True`` (the default), any pending items are dispatched
        first. When ``flush=False``, pending items are dropped — useful in
        tests that want to observe loss behavior deterministically.
        """
        if flush:
            self.flush_all()
        self._stop.set()
        with self._cond:
            self._cond.notify_all()
        if self._flusher is not None:
            self._flusher.join(timeout=2)
            self._flusher = None

    # ── Internal ───────────────────────────────────────────────────────

    def _ensure_flusher_locked(self) -> None:
        """Start the daemon thread if not already running. Caller holds the lock."""
        if self._flusher is not None or self._stop.is_set():
            return
        self._flusher = threading.Thread(
            target=self._flusher_loop,
            daemon=True,
            name="flexiq-batch-flusher",
        )
        self._flusher.start()

    def _flusher_loop(self) -> None:
        while not self._stop.is_set():
            with self._cond:
                if not self._buffers:
                    self._cond.wait()
                    continue
                now = time.monotonic()
                next_deadline = min(s.deadline_monotonic for s in self._buffers.values())
                if next_deadline > now:
                    self._cond.wait(timeout=next_deadline - now)
                    continue

                # Drain any buffer whose deadline has passed.
                expired: list[
                    tuple[str, list[Any], BatchConfig, list[BatchedJobResult], threading.Event]
                ] = []
                for task_name in list(self._buffers.keys()):
                    state = self._buffers[task_name]
                    if state.deadline_monotonic <= now:
                        expired.append(
                            (
                                task_name,
                                state.items,
                                state.config,
                                list(state.pending_results),
                                state.flush_event,
                            )
                        )
                        del self._buffers[task_name]

            for task_name, items, config, pending, flush_event in expired:
                self._safe_dispatch(task_name, items, config, pending, flush_event)

    def _safe_dispatch(
        self,
        task_name: str,
        items: list[Any],
        config: BatchConfig,
        pending_results: list[BatchedJobResult],
        flush_event: threading.Event,
    ) -> None:
        """Dispatch one batch and resolve every pending per-item handle.

        We always set ``flush_event`` before returning — failure or success
        — so callers waiting on ``BatchedJobResult.result()`` never hang
        when dispatch raises. On failure ``_jobresult`` stays ``None`` and
        ``result()`` raises ``RuntimeError`` with a clear message.
        """
        try:
            jobresult = self._dispatch(task_name, items, config)
        except Exception:
            # Don't let one bad batch take down the flusher thread.
            logger.exception("batch dispatch failed for task=%s items=%d", task_name, len(items))
            flush_event.set()
            return
        if jobresult is not None:
            for handle in pending_results:
                handle._resolve(jobresult)
        flush_event.set()
