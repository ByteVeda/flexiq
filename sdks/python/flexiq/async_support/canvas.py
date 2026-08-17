"""Async canvas mixins.

Hosts the ``apply_async`` methods for :class:`~flexiq.canvas.group` and
:class:`~flexiq.canvas.chord`. Lives under ``async_support/`` so that
``flexiq/canvas.py`` itself does not need to import ``asyncio`` — keeping the
module async-agnostic and consistent with the ``JobResult``/``AsyncJobResultMixin``
split in ``async_support/result.py``.
"""

from __future__ import annotations

import asyncio
import dataclasses
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from flexiq.app import Queue
    from flexiq.canvas import Signature, group
    from flexiq.result import JobResult


_DEFAULT_TIMEOUT = 300


def _signature_timeout(sig: Signature) -> float:
    """Per-signature timeout for awaiting results, falling back to the default."""
    timeout = sig.options.get("timeout", _DEFAULT_TIMEOUT)
    return float(timeout)


async def _await_wave(wave_jobs: list[JobResult], wave: list[Signature]) -> None:
    """Await every job in a wave in parallel, honoring per-signature timeouts."""
    await asyncio.gather(
        *(
            job.aresult(timeout=_signature_timeout(sig))
            for job, sig in zip(wave_jobs, wave, strict=True)
        )
    )


class AsyncGroupMixin:
    """Provides ``apply_async`` for :class:`~flexiq.canvas.group`."""

    if TYPE_CHECKING:
        signatures: list[Signature]
        max_concurrency: int | None

    async def apply_async(self, queue: Queue | None = None) -> list[JobResult]:
        """Enqueue all signatures for parallel execution (async-safe).

        With ``max_concurrency`` set, dispatches in waves, awaiting each
        wave before starting the next.
        """
        q = queue or self.signatures[0].task._queue

        if self.max_concurrency is None:
            return [sig.apply(q) for sig in self.signatures]

        all_jobs: list[JobResult] = []
        step = self.max_concurrency
        for start in range(0, len(self.signatures), step):
            wave = self.signatures[start : start + step]
            wave_jobs = [sig.apply(q) for sig in wave]
            await _await_wave(wave_jobs, wave)
            all_jobs.extend(wave_jobs)
        return all_jobs


class AsyncChordMixin:
    """Provides ``apply_async`` for :class:`~flexiq.canvas.chord`."""

    if TYPE_CHECKING:
        group: group
        callback: Signature

    async def apply_async(self, queue: Queue | None = None) -> JobResult:
        """Execute group, await all results, then run the callback (async-safe)."""
        q = queue or self.callback.task._queue

        jobs = self.group.apply(queue=q)
        max_timeout = max(
            (_signature_timeout(sig) for sig in self.group.signatures),
            default=_DEFAULT_TIMEOUT,
        )
        results = await asyncio.gather(*(job.aresult(timeout=max_timeout) for job in jobs))

        callback = self.callback
        if not callback.immutable:
            callback = dataclasses.replace(callback, args=(list(results), *callback.args))
        return callback.apply(q)
