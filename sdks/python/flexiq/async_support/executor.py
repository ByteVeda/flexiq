"""AsyncTaskExecutor — dedicated event loop for native async task execution."""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from flexiq.async_support.context import clear_async_context, set_async_context
from flexiq.exceptions import TaskCancelledError
from flexiq.task_errors import encode_task_error
from flexiq.task_lifecycle import run_lifecycle

if TYPE_CHECKING:
    from flexiq.app import Queue

logger = logging.getLogger("flexiq.async")

# Backoff bounds for handing a result to Rust while the result channel is full.
_REPORT_BACKOFF_START_S = 0.001
_REPORT_BACKOFF_MAX_S = 0.05


class AsyncTaskExecutor:
    """Runs async tasks natively on a dedicated event loop.

    Receives jobs from Rust via :meth:`submit_job`, awaits the coroutine through
    :func:`~flexiq.task_lifecycle.run_lifecycle` — the same body the blocking
    path runs, so both dispatch paths honour one lifecycle — and reports the
    outcome back via a ``PyResultSender``.
    """

    def __init__(
        self,
        result_sender: Any,
        task_registry: dict[str, Any],
        queue_ref: Queue,
        max_concurrency: int = 100,
    ) -> None:
        self._sender = result_sender
        self._registry = task_registry
        self._queue_ref = queue_ref
        self._max_concurrency = max_concurrency
        self._semaphore: asyncio.Semaphore | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        """Start the dedicated event loop thread."""
        self._loop = asyncio.new_event_loop()
        self._semaphore = asyncio.Semaphore(self._max_concurrency)
        self._thread = threading.Thread(
            target=self._loop.run_forever,
            daemon=True,
            name="flexiq-async-executor",
        )
        self._thread.start()

    def submit_job(
        self,
        job_id: str,
        task_name: str,
        payload: bytes,
        retry_count: int,
        max_retries: int,
        queue_name: str,
        permit: Any = None,
    ) -> None:
        """Submit an async job for execution. Called from Rust — brief GIL hold.

        ``permit`` is the dispatch slot the pool acquired for this job; holding it on
        the coroutine frame is what releases it on every exit path. It is optional so
        the executor can also be driven directly, without a pool.
        """
        if self._loop is None:
            raise RuntimeError("AsyncTaskExecutor not started")
        asyncio.run_coroutine_threadsafe(
            self._execute(
                job_id, task_name, payload, retry_count, max_retries, queue_name, permit
            ),
            self._loop,
        )

    async def _hand_off(self, send: Callable[[], bool]) -> None:
        """Push a result to Rust, backing off while the result channel is full.

        Never drops a result — a dropped one strands the job ``Running`` until the
        reaper mislabels it a timeout. Awaiting between attempts is what keeps the
        event loop responsive: a blocking send would freeze every other coroutine on
        this thread, and would deadlock against the drain loop, which re-acquires the
        GIL each iteration and so could never drain the channel being waited on.
        """
        delay = _REPORT_BACKOFF_START_S
        while not send():
            await asyncio.sleep(delay)
            delay = min(delay * 2, _REPORT_BACKOFF_MAX_S)

    def _hand_off_now(self, send: Callable[[], bool], job_id: str) -> None:
        """Single best-effort hand-off for a coroutine already being torn down.

        The cancellation path cannot use :meth:`_hand_off`: awaiting inside a
        ``CancelledError`` handler risks being cancelled again on a loop that may be
        stopping. The channel is drained continuously, so a full one is rare here; the
        stale-job reaper remains the backstop if this does drop.
        """
        if not send():
            logger.warning("result channel full while cancelling %s; job left to reaper", job_id)

    async def _execute(
        self,
        job_id: str,
        task_name: str,
        payload_bytes: bytes,
        retry_count: int,
        max_retries: int,
        queue_name: str,
        permit: Any = None,
    ) -> None:
        """Execute a single async task with full lifecycle support."""
        assert self._semaphore is not None
        try:
            await self._run_job(
                job_id, task_name, payload_bytes, retry_count, max_retries, queue_name
            )
        finally:
            # Hand the dispatch slot back as soon as the job is done rather than
            # waiting for the frame to be collected. Dropping the handle is the
            # backstop for paths that never reach here.
            if permit is not None:
                permit.release()

    async def _run_job(
        self,
        job_id: str,
        task_name: str,
        payload_bytes: bytes,
        retry_count: int,
        max_retries: int,
        queue_name: str,
    ) -> None:
        """Run one job: gate on the concurrency backstop, execute, report.

        Everything between the arguments and the result belongs to
        :func:`run_lifecycle`; what stays here is what native dispatch does
        differently — decoding the payload, and answering the result sender.
        """
        assert self._semaphore is not None
        async with self._semaphore:
            start_ns = time.monotonic_ns()
            token = set_async_context(job_id, task_name, retry_count, queue_name)
            # Set before the hand-off so a raising send can't also fire report_failure
            # for the same job — the scheduler would then see two results for one job.
            reported = False

            try:
                queue = self._queue_ref
                # Honor the per-task serializer (matches the sync worker path);
                # a hardcoded cloudpickle.loads would ignore @task(serializer=...).
                args, kwargs = queue._deserialize_payload(task_name, payload_bytes)

                # Awaited, not run_maybe_async'd: this is the executor's own loop,
                # and the coroutine belongs on it.
                result = await run_lifecycle(
                    queue,
                    task_name,
                    self._registry[task_name]._flexiq_async_fn,
                    args,
                    kwargs,
                )

                result_bytes = (
                    queue._serialize_result(task_name, result) if result is not None else None
                )
                wall_ns = time.monotonic_ns() - start_ns
                reported = True
                await self._hand_off(
                    lambda: self._sender.try_report_success(
                        job_id, task_name, result_bytes, wall_ns
                    )
                )

            except TaskCancelledError:
                if not reported:
                    wall_ns = time.monotonic_ns() - start_ns
                    reported = True
                    await self._hand_off(
                        lambda: self._sender.try_report_cancelled(job_id, task_name, wall_ns)
                    )

            except asyncio.CancelledError:
                # CancelledError is a BaseException, so `except Exception` misses it and
                # the job would sit Running until the reaper mislabelled it a timeout.
                # Report, then re-raise — swallowing a cancellation breaks loop shutdown.
                if not reported:
                    wall_ns = time.monotonic_ns() - start_ns
                    reported = True
                    self._hand_off_now(
                        lambda: self._sender.try_report_cancelled(job_id, task_name, wall_ns),
                        job_id,
                    )
                raise

            except Exception as exc:
                if not reported:
                    wall_ns = time.monotonic_ns() - start_ns
                    reported = True
                    error_msg = encode_task_error(exc)
                    should_retry = self._check_retry(task_name, exc)
                    await self._hand_off(
                        lambda: self._sender.try_report_failure(
                            job_id,
                            task_name,
                            error_msg,
                            retry_count,
                            max_retries,
                            wall_ns,
                            should_retry,
                        )
                    )

            except BaseException as exc:
                # Neither a cancellation nor an ordinary failure — KeyboardInterrupt,
                # SystemExit, or any other BaseException the clause above cannot see.
                # Unreported, the job sits Running until the reaper mislabels it a
                # timeout, so report it as the blocking path would: a failure, with
                # the retry policy left to decide. Hand off without awaiting — the
                # loop may be going down — then re-raise, since swallowing these
                # breaks interpreter and loop teardown.
                if not reported:
                    wall_ns = time.monotonic_ns() - start_ns
                    reported = True
                    error_msg = encode_task_error(exc)
                    self._hand_off_now(
                        lambda: self._sender.try_report_failure(
                            job_id,
                            task_name,
                            error_msg,
                            retry_count,
                            max_retries,
                            wall_ns,
                            True,
                        ),
                        job_id,
                    )
                raise

            finally:
                clear_async_context(token)

    def _check_retry(self, task_name: str, exc: Exception) -> bool:
        """Check retry filters to decide if this exception should be retried."""
        filters = self._queue_ref._task_retry_filters.get(task_name)
        if filters is None:
            return True

        dont_retry_on = filters.get("dont_retry_on", [])
        for cls in dont_retry_on:
            if isinstance(exc, cls):
                return False

        retry_on = filters.get("retry_on", [])
        if retry_on:
            return any(isinstance(exc, cls) for cls in retry_on)

        return True

    def stop(self) -> None:
        """Stop the executor's event loop, join the thread, release the sender."""
        # `not is_closed()` rather than `is_running()`: right after `start()`
        # the thread may not have entered `run_forever()` yet, and skipping the
        # stop here would leave it running forever once it does.
        if self._loop is not None and not self._loop.is_closed():
            self._loop.call_soon_threadsafe(self._loop.stop)
        if self._thread is not None:
            self._thread.join(timeout=5)
            if self._thread.is_alive():
                logger.warning("Async executor thread did not stop within 5s timeout")
                return
        # The sender wraps a clone of the worker's result channel, and a pinned
        # frame (a retained exception's traceback) can keep this object alive
        # long past shutdown — so the release must not wait for GC. Only safe
        # once the loop thread is down: no coroutine can report after that.
        self._sender = None
