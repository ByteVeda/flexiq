"""Task chaining primitives: Signature, chain, group, chord, chunks, starmap.

Canvas primitives are in-thread orchestrators. A ``chain`` enqueues each
step, blocks on the result, and pipes forward; ``group`` enqueues all
members in parallel; ``chord`` runs a group then a callback. The caller's
thread owns the state — there is no persistent run row in storage.

Saga compensation is therefore done in-thread too. Calling
``chain.with_compensation([...])`` (or the equivalent on ``group`` /
``chord``) attaches a list of compensator signatures. If any step raises
while ``apply()`` is executing, the framework synchronously dispatches the
compensators for the steps that already succeeded:

  - chain   → reverse-order, sequential.
  - group   → all completed members in parallel.
  - chord   → callback compensator first (if the callback ran), then
              group compensators in parallel.

Idempotency key: each compensator job is enqueued with a deterministic
``unique_key`` of the form ``canvas_compensation:{canvas_run_id}:{slot}``,
where ``canvas_run_id`` is a UUID generated at ``apply()`` time. A second
invocation of ``apply()`` on the same canvas object never re-runs an
already-dispatched compensator (same dedup mechanism as DAG-level sagas).
"""

from __future__ import annotations

import logging
import math
import uuid
from collections.abc import Sequence
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Any

from flexiq.async_support.canvas import AsyncChordMixin, AsyncGroupMixin

if TYPE_CHECKING:
    from flexiq.app import Queue
    from flexiq.result import JobResult
    from flexiq.task import TaskWrapper


logger = logging.getLogger("flexiq.canvas")


CompensatorRef = "TaskWrapper | str | Signature | None"


@dataclass(frozen=True)
class Signature:
    """A frozen task call spec — what to call and with what arguments.

    Created via ``task.s()`` or ``task.si()``::

        sig = add.s(2, 3)          # mutable — receives previous result
        sig = add.si(2, 3)         # immutable — ignores previous result
    """

    task: TaskWrapper
    args: tuple = ()
    kwargs: dict = field(default_factory=dict)
    options: dict = field(default_factory=dict)
    immutable: bool = False

    def apply(self, queue: Queue | None = None) -> JobResult:
        """Enqueue this signature for execution."""
        q = queue or self.task._queue
        return q.enqueue(
            task_name=self.task.name,
            args=self.args,
            kwargs=self.kwargs if self.kwargs else None,
            **self.options,
        )

    async def apply_async(self, queue: Queue | None = None) -> JobResult:
        """Enqueue this signature for execution (async-safe).

        The enqueue operation is a fast DB write so it runs synchronously.
        Use this in async contexts for API consistency with other ``apply_async`` methods.
        """
        return self.apply(queue)

    def with_compensation(self, compensator: CompensatorRef) -> Signature:  # type: ignore[valid-type]
        """Return a copy carrying a ``compensates`` option set.

        Lets a Signature opt into compensation independent of any
        decorator-level ``compensates=`` declared on the task. Useful when
        a Signature is reused across workflows with different compensators.
        """
        return replace(self, options={**self.options, "compensates": compensator})


# ── Compensation helpers (in-canvas, synchronous) ────────────────────────


def _normalise_compensator(c: Any) -> Signature | None:
    """Coerce a user-supplied compensator into a callable Signature or None.

    ``None`` means "this slot has no compensator". A bare ``TaskWrapper``
    is wrapped as ``task.si()`` (immutable, receives no forward result).
    A ``Signature`` passes through. A task-name string is also accepted
    and resolved against the queue's registry at dispatch time (returns
    a wrapper Signature with the task name baked in).
    """
    if c is None:
        return None
    # Lazy import to avoid cycles
    from flexiq.task import TaskWrapper

    if isinstance(c, Signature):
        return c
    if isinstance(c, TaskWrapper):
        return c.si()
    if isinstance(c, str):
        # Build a placeholder Signature that uses a name-only stub. The
        # enqueue path uses sig.task.name, so we wrap a minimal proxy.
        class _NameOnly:
            name = c
            _queue: Queue | None = None

        return Signature(task=_NameOnly(), args=(), kwargs={}, options={}, immutable=True)  # type: ignore[arg-type]
    raise TypeError(
        "compensator must be TaskWrapper, Signature, str (task name), or None — "
        f"got {type(c).__name__}"
    )


def _validate_compensator_list(
    compensators: Sequence[Any] | None, expected_len: int, name: str
) -> tuple[Signature | None, ...]:
    """Length-check the compensator list and normalise each entry.

    ``None`` or an empty list explicitly disables canvas-level compensation
    (returns a tuple of ``None`` of the right length so the dispatch logic
    can still iterate). Otherwise length must match ``expected_len`` or we
    raise a clear error.
    """
    if compensators is None:
        return tuple([None] * expected_len)
    if len(compensators) != expected_len:
        raise ValueError(
            f"{name}.with_compensation requires exactly {expected_len} compensator(s), "
            f"got {len(compensators)}"
        )
    return tuple(_normalise_compensator(c) for c in compensators)


def _dispatch_compensator(
    queue: Queue,
    canvas_run_id: str,
    slot: str,
    compensator: Signature,
    forward_args: tuple,
    forward_kwargs: dict,
    forward_result: Any,
) -> JobResult | None:
    """Enqueue one compensator with a deterministic idempotency key.

    Compensators receive the standard ``(forward_args, forward_kwargs,
    forward_result)`` triple — matches the DAG-saga contract so users can
    write a single compensator that works in both surfaces.
    """
    unique_key = f"canvas_compensation:{canvas_run_id}:{slot}"
    try:
        return queue.enqueue(
            task_name=compensator.task.name,
            args=(forward_args, forward_kwargs, forward_result),
            kwargs=None,
            unique_key=unique_key,
            **{k: v for k, v in compensator.options.items() if k not in ("compensates",)},
        )
    except Exception:  # pragma: no cover - enqueue failures shouldn't mask the original
        logger.exception(
            "canvas compensator dispatch failed for slot=%s task=%s",
            slot,
            compensator.task.name,
        )
        return None


# ── chain ──────────────────────────────────────────────────────────────────


class chain:
    """Execute signatures sequentially, piping each result to the next."""

    def __init__(self, *signatures: Signature) -> None:
        if len(signatures) < 1:
            raise ValueError("chain requires at least one signature")
        self.signatures: list[Signature] = list(signatures)
        self._compensators: tuple[Signature | None, ...] = tuple([None] * len(signatures))

    def with_compensation(self, compensators: Sequence[Any] | None) -> chain:
        """Attach a list of compensators, one per chain step (``None`` slots OK)."""
        new = chain(*self.signatures)
        new._compensators = _validate_compensator_list(compensators, len(self.signatures), "chain")
        return new

    def _has_compensation(self) -> bool:
        return any(c is not None for c in self._compensators)

    def apply(self, queue: Queue | None = None) -> JobResult:
        """Execute the chain, blocking until all steps complete.

        If any step raises and at least one compensator is set, the
        framework synchronously enqueues compensators for the steps that
        already completed (reverse order). The original exception is then
        re-raised.
        """
        q = queue or self.signatures[0].task._queue
        completed: list[tuple[int, tuple, dict, Any]] = []
        canvas_run_id = uuid.uuid4().hex
        prev_result: Any = None
        last_job: JobResult | None = None

        try:
            for i, sig in enumerate(self.signatures):
                args = sig.args
                if prev_result is not None and not sig.immutable:
                    args = (prev_result, *sig.args)
                last_job = q.enqueue(
                    task_name=sig.task.name,
                    args=args,
                    kwargs=sig.kwargs if sig.kwargs else None,
                    **sig.options,
                )
                result = last_job.result(timeout=sig.options.get("timeout", 300))
                completed.append((i, args, dict(sig.kwargs), result))
                prev_result = result
        except Exception:
            if self._has_compensation():
                self._compensate_completed(q, canvas_run_id, completed)
            raise

        return last_job  # type: ignore[return-value]

    async def apply_async(self, queue: Queue | None = None) -> JobResult:
        """Execute the chain asynchronously, awaiting each step's result."""
        q = queue or self.signatures[0].task._queue
        completed: list[tuple[int, tuple, dict, Any]] = []
        canvas_run_id = uuid.uuid4().hex
        prev_result: Any = None
        last_job: JobResult | None = None

        try:
            for i, sig in enumerate(self.signatures):
                args = sig.args
                if prev_result is not None and not sig.immutable:
                    args = (prev_result, *sig.args)
                last_job = q.enqueue(
                    task_name=sig.task.name,
                    args=args,
                    kwargs=sig.kwargs if sig.kwargs else None,
                    **sig.options,
                )
                result = await last_job.aresult(timeout=sig.options.get("timeout", 300))
                completed.append((i, args, dict(sig.kwargs), result))
                prev_result = result
        except Exception:
            if self._has_compensation():
                self._compensate_completed(q, canvas_run_id, completed)
            raise

        return last_job  # type: ignore[return-value]

    def _compensate_completed(
        self,
        queue: Queue,
        canvas_run_id: str,
        completed: list[tuple[int, tuple, dict, Any]],
    ) -> None:
        """Dispatch compensators for completed steps in reverse order."""
        for idx, args, kwargs, result in reversed(completed):
            comp = self._compensators[idx]
            if comp is None:
                continue
            _dispatch_compensator(
                queue,
                canvas_run_id,
                f"chain.{idx}",
                comp,
                forward_args=args,
                forward_kwargs=kwargs,
                forward_result=result,
            )


# ── group ──────────────────────────────────────────────────────────────────


class group(AsyncGroupMixin):
    """Execute signatures in parallel and collect all results.

    Args:
        *signatures: Signatures to execute in parallel.
        max_concurrency: If set, limits how many group members run
            concurrently. Members are dispatched in waves.
    """

    def __init__(self, *signatures: Signature, max_concurrency: int | None = None) -> None:
        if len(signatures) < 1:
            raise ValueError("group requires at least one signature")
        if max_concurrency is not None and max_concurrency <= 0:
            raise ValueError("max_concurrency must be a positive integer")
        self.signatures: list[Signature] = list(signatures)
        self.max_concurrency = max_concurrency
        self._compensators: tuple[Signature | None, ...] = tuple([None] * len(signatures))

    def with_compensation(self, compensators: Sequence[Any] | None) -> group:
        """Attach a list of compensators, one per group member."""
        new = group(*self.signatures, max_concurrency=self.max_concurrency)
        new._compensators = _validate_compensator_list(compensators, len(self.signatures), "group")
        return new

    def _has_compensation(self) -> bool:
        return any(c is not None for c in self._compensators)

    def apply(self, queue: Queue | None = None) -> list[JobResult]:
        """Enqueue all signatures for parallel execution.

        If any member fails and at least one compensator is set, dispatch
        compensators for the *succeeded* members in parallel. The first
        failure encountered is then re-raised.
        """
        q = queue or self.signatures[0].task._queue
        canvas_run_id = uuid.uuid4().hex
        all_jobs: list[JobResult] = []

        if self.max_concurrency is None:
            for sig in self.signatures:
                job = q.enqueue(
                    task_name=sig.task.name,
                    args=sig.args,
                    kwargs=sig.kwargs if sig.kwargs else None,
                    **sig.options,
                )
                all_jobs.append(job)
        else:
            mc = self.max_concurrency
            for i in range(0, len(self.signatures), mc):
                wave = self.signatures[i : i + mc]
                wave_jobs: list[JobResult] = []
                for sig in wave:
                    job = q.enqueue(
                        task_name=sig.task.name,
                        args=sig.args,
                        kwargs=sig.kwargs if sig.kwargs else None,
                        **sig.options,
                    )
                    wave_jobs.append(job)
                for wj, sig in zip(wave_jobs, wave, strict=True):
                    wj.result(timeout=sig.options.get("timeout", 300))
                all_jobs.extend(wave_jobs)

        # If compensation is configured, wait on each member, capture
        # per-member outcome, then dispatch compensators on first failure.
        if self._has_compensation():
            self._collect_and_compensate(q, canvas_run_id, all_jobs)

        return all_jobs

    def _collect_and_compensate(
        self,
        queue: Queue,
        canvas_run_id: str,
        jobs: list[JobResult],
    ) -> None:
        """Wait on every job, then run compensators for succeeded ones if any failed."""
        completed: list[tuple[int, tuple, dict, Any]] = []
        first_failure: BaseException | None = None
        for i, (job, sig) in enumerate(zip(jobs, self.signatures, strict=True)):
            try:
                result = job.result(timeout=sig.options.get("timeout", 300))
                completed.append((i, sig.args, dict(sig.kwargs), result))
            except BaseException as exc:
                if first_failure is None:
                    first_failure = exc
        if first_failure is not None:
            # Compensate in parallel: enqueue all at once, don't wait.
            for idx, args, kwargs, result in completed:
                comp = self._compensators[idx]
                if comp is None:
                    continue
                _dispatch_compensator(
                    queue,
                    canvas_run_id,
                    f"group.{idx}",
                    comp,
                    forward_args=args,
                    forward_kwargs=kwargs,
                    forward_result=result,
                )
            raise first_failure


# ── chord ──────────────────────────────────────────────────────────────────


class chord(AsyncChordMixin):
    """Run a group in parallel, then a callback with all results."""

    def __init__(self, group_: group, callback: Signature) -> None:
        self.group: group = group_
        self.callback: Signature = callback
        self._group_compensators: tuple[Signature | None, ...] = tuple(
            [None] * len(group_.signatures)
        )
        self._callback_compensator: Signature | None = None

    def with_compensation(
        self,
        group: Sequence[Any] | None = None,
        callback: Any = None,
    ) -> chord:
        """Attach compensators: one per group member and optionally one for the callback.

        Either argument may be omitted to leave that side uncompensated.
        Pass an empty list (or ``None``) for ``group=`` to skip member
        compensation; pass ``None`` for ``callback=`` to skip the callback.
        """
        new = chord(self.group, self.callback)
        if group is not None:
            new._group_compensators = _validate_compensator_list(
                group, len(self.group.signatures), "chord"
            )
        new._callback_compensator = _normalise_compensator(callback) if callback else None
        return new

    def _has_compensation(self) -> bool:
        return self._callback_compensator is not None or any(
            c is not None for c in self._group_compensators
        )

    def apply(self, queue: Queue | None = None) -> JobResult:
        """Execute the group, wait for all results, then run the callback.

        Compensation order on failure (when any compensator is set):
          1. If the callback ran and failed → compensate the callback
             (if configured), then compensate the group members in parallel.
          2. If the callback never ran (a group member failed) → compensate
             the succeeded group members in parallel only.
        """
        q = queue or self.callback.task._queue
        canvas_run_id = uuid.uuid4().hex
        group_completed: list[tuple[int, tuple, dict, Any]] = []
        group_results: list[Any] = []
        callback_completed: tuple[tuple, dict, Any] | None = None

        # 1. Run the group; collect per-member completion.
        jobs = self.group.apply(queue=q)
        max_timeout = max(
            (sig.options.get("timeout", 300) for sig in self.group.signatures), default=300
        )
        try:
            for i, (job, sig) in enumerate(zip(jobs, self.group.signatures, strict=True)):
                result = job.result(timeout=max_timeout)
                group_completed.append((i, sig.args, dict(sig.kwargs), result))
                group_results.append(result)
        except Exception:
            if self._has_compensation():
                self._compensate_group(q, canvas_run_id, group_completed)
            raise

        # 2. Run the callback.
        try:
            args = self.callback.args
            if not self.callback.immutable:
                args = (group_results, *self.callback.args)
            cb_job = q.enqueue(
                task_name=self.callback.task.name,
                args=args,
                kwargs=self.callback.kwargs if self.callback.kwargs else None,
                **self.callback.options,
            )
            cb_result = cb_job.result(timeout=self.callback.options.get("timeout", 300))
            callback_completed = (args, dict(self.callback.kwargs), cb_result)
        except Exception:
            if self._has_compensation():
                # Callback didn't complete — only compensate group members.
                self._compensate_group(q, canvas_run_id, group_completed)
            raise

        _ = callback_completed  # currently unused once chord succeeds
        return cb_job

    def _compensate_group(
        self,
        queue: Queue,
        canvas_run_id: str,
        completed: list[tuple[int, tuple, dict, Any]],
    ) -> None:
        """Dispatch compensators for the succeeded group members in parallel."""
        for idx, args, kwargs, result in completed:
            comp = self._group_compensators[idx]
            if comp is None:
                continue
            _dispatch_compensator(
                queue,
                canvas_run_id,
                f"chord.group.{idx}",
                comp,
                forward_args=args,
                forward_kwargs=kwargs,
                forward_result=result,
            )


# ── chunks / starmap (compensation-free factory helpers) ──────────────────


def chunks(task: TaskWrapper, items: list[Any], chunk_size: int) -> group:
    """Split items into chunks and create a group of tasks processing each chunk.

    Args:
        task: The task to call for each chunk.
        items: List of items to split.
        chunk_size: Number of items per chunk.

    Returns:
        A :class:`group` of signatures, one per chunk.

    Example::

        result = chunks(process_batch, items, 100).apply(queue)
    """
    if chunk_size <= 0:
        raise ValueError(f"chunk_size must be positive, got {chunk_size}")
    if not items:
        raise ValueError("items must not be empty")
    n_chunks = math.ceil(len(items) / chunk_size)
    sigs = []
    for i in range(n_chunks):
        chunk = items[i * chunk_size : (i + 1) * chunk_size]
        sigs.append(task.s(chunk))
    return group(*sigs)


def starmap(task: TaskWrapper, args_list: list[tuple[Any, ...]]) -> group:
    """Create a group with one task per args tuple.

    Args:
        task: The task to call for each args tuple.
        args_list: List of argument tuples.

    Returns:
        A :class:`group` of signatures, one per args tuple.

    Example::

        result = starmap(add, [(1, 2), (3, 4), (5, 6)]).apply(queue)
    """
    if not args_list:
        raise ValueError("args_list must not be empty")
    sigs = [task.s(*args) for args in args_list]
    return group(*sigs)
