"""``ctx.step`` — durable inline steps on the task context.

A step is a checkpoint inside one job: it runs once, its result is committed,
and every later attempt of that job returns the committed value instead of
running it again. The rules — identity, divergence, the caps, the sleep
decision — all live in the Rust core, which is what makes them identical across
the SDKs. This module is the Python side of the split the core exposes for
exactly that reason: the core decides, the closure runs here, and the core
commits the bytes this shell encoded.

Memoization alone is not exactly-once. The process can die between a payment
API returning 200 and the step row committing, and the replay has no record the
call happened. Nothing on this side of the network closes that window; only a
key the other side dedupes on does, which is what
:attr:`~StepContext.idempotency_key` is for::

    charge = current_job.step.run(
        "charge",
        lambda: stripe.charge(order, idempotency_key=current_job.step.idempotency_key),
    )
"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any, TypeVar, cast, overload

from flexiq._active_context import _ActiveContext
from flexiq.steps.durations import (
    SleepDeadline,
    SleepDuration,
    sleep_deadline_ms,
    sleep_duration_ms,
)
from flexiq.steps.errors import (
    StepControlSignal,
    StepError,
    StepSleepSignal,
    StepUnavailableError,
)
from flexiq.steps.latch import latch

#: A step's result type. Carried through so a caller reads the closure's own
#: type rather than ``Any`` — true of a fresh run, and true of a replay for
#: anything the queue's serializer round-trips exactly.
_T = TypeVar("_T")

if TYPE_CHECKING:
    from flexiq._flexiq import StepDecision, StepSession, StepSleepOutcome
    from flexiq.app import Queue
    from flexiq.serializers import Serializer


class StepContext:
    """The ``step`` attribute of a running job's context.

    One per attempt, held on the active context so the session it opens — and
    the identity of the step currently running — survive across calls.
    """

    __slots__ = (
        "_ctx",
        "_current_key",
        "_current_step",
        "_inline_occurrences",
        "_queue",
        "_session",
    )

    def __init__(self, ctx: _ActiveContext, queue: Queue | None) -> None:
        self._ctx = ctx
        self._queue = queue
        self._session: StepSession | None = None
        self._current_key: str | None = None
        self._current_step: str | None = None
        self._inline_occurrences: dict[str, int] = {}

    # ---------------------------------------------------------------- run

    def run(self, name: str, fn: Callable[[], _T], *, key: str | None = None) -> _T:
        """Run ``fn`` once for this job, or return what it returned last time.

        ``name`` is positional and required: an inferred name changes whenever
        the callable is renamed or inlined, and a step whose identity moves is a
        step whose memo answers a different question.

        Pass ``key`` when the step's position is not stable — a loop over an
        unordered collection. A keyed step is matched by key wherever it sits in
        the recorded sequence; an unkeyed one is matched at its position.

        The first run returns exactly what ``fn`` returned; a replay returns
        that value decoded from its stored bytes, so anything the queue's
        serializer does not round-trip exactly — a tuple, a set, a custom class
        without support — comes back in its decoded shape. Return something the
        serializer preserves, or a handle to it.

        Args:
            name: Identity of the step within the job.
            fn: The work, called with no arguments. Not called on a memo hit.
            key: Explicit identity, for steps whose order may change.

        Returns:
            The step's result — freshly computed, or decoded from the row.

        Raises:
            TypeError: ``name`` is empty or not a string.
            StepDivergedError: The recorded sequence and this code disagree.
            StepLimitExceededError: The result, or the job's total, is past the cap.
        """
        with self._control():
            decision = self._begin(name, key)
            if decision is None:
                return self._inline(name, key, fn)
            if decision.memoized is not None:
                return cast("_T", self._replay(decision.memoized))
            value = self._invoke(decision.step_key, decision.idempotency_key, fn)
            self._commit(decision, value)
            return value

    @overload
    async def arun(
        self, name: str, fn: Callable[[], Awaitable[_T]], *, key: str | None = None
    ) -> _T: ...

    @overload
    async def arun(self, name: str, fn: Callable[[], _T], *, key: str | None = None) -> _T: ...

    # The awaitable overload comes first, or an ``async def`` body solves ``_T``
    # to the coroutine itself — a union of ``_T`` and ``Awaitable[_T]`` gives a
    # checker two ways to match one argument and it picks neither.
    async def arun(self, name: str, fn: Callable[[], Any], *, key: str | None = None) -> Any:
        """Await twin of :meth:`run`. ``fn`` may return a value or an awaitable.

        **Steps run one at a time, even here.** A step's position in the
        sequence is what identifies it, so a second step started while the
        first is still uncommitted has no position to take —
        ``asyncio.gather`` over two ``arun`` calls fails the attempt
        permanently rather than interleaving them. Await them in order.
        """
        with self._control():
            decision = self._begin(name, key)
            if decision is None:
                return await self._ainline(name, key, fn)
            if decision.memoized is not None:
                return self._replay(decision.memoized)
            value = await self._ainvoke(decision.step_key, decision.idempotency_key, fn)
            self._commit(decision, value)
            return value

    # -------------------------------------------------------------- sleep

    def sleep(
        self,
        duration: SleepDuration,
        *,
        name: str | None = None,
        key: str | None = None,
    ) -> None:
        """Sleep for ``duration``, ending this attempt if the deadline is ahead.

        The attempt ends: the claim is released and the job goes back to
        ``Pending`` at its deadline, so a sleeping job holds no worker slot and
        cannot be timed out while it waits. On wake the job replays from the
        top, every earlier step is a memo hit, and this sleep returns
        immediately.

        A sleep costs no retry — the retry count, the retry budget, the circuit
        breaker and the task metrics are all untouched.

        The deadline is fixed by the **first** commit. Replaying a ``"1h"``
        sleep wakes at the original instant rather than an hour later each time,
        which is what stops a crash loop from producing a sleep that outlives
        the job.

        Naming the sleep is strongly recommended: a sequence that reads
        ``sleep#0, sleep#1, sleep#2`` tells nobody which one diverged.
        """
        with self._control():
            millis = sleep_duration_ms(duration)
            session = self._session_or_inline()
            if session is None:
                return
            self._end_attempt_if_sleeping(session.sleep_for(millis, name, key))

    async def asleep(
        self,
        duration: SleepDuration,
        *,
        name: str | None = None,
        key: str | None = None,
    ) -> None:
        """Await twin of :meth:`sleep`."""
        self.sleep(duration, name=name, key=key)

    def sleep_until(
        self,
        when: SleepDeadline,
        *,
        name: str | None = None,
        key: str | None = None,
    ) -> None:
        """Sleep until an absolute instant.

        Reach for this over :meth:`sleep` when the deadline means something
        outside the job — a billing date, a market open — because an absolute
        instant is unaffected by how many times the attempt replayed.
        """
        with self._control():
            millis = sleep_deadline_ms(when)
            session = self._session_or_inline()
            if session is None:
                return
            self._end_attempt_if_sleeping(session.sleep_until(millis, name, key))

    async def asleep_until(
        self,
        when: SleepDeadline,
        *,
        name: str | None = None,
        key: str | None = None,
    ) -> None:
        """Await twin of :meth:`sleep_until`."""
        self.sleep_until(when, name=name, key=key)

    # ---------------------------------------------------------------- keys

    @property
    def idempotency_key(self) -> str:
        """The key to hand the downstream service for the step running now.

        Stable across a retry, across a sleep/wake and across an operator's DLQ
        retry, and no serializer or codec touches it. Readable only from inside
        a step body — outside one there is no step for it to name.
        """
        if self._current_key is None:
            raise RuntimeError(
                "step.idempotency_key names the step that is running, so it is only "
                "readable inside a step body — pass it from within the callable given "
                "to step.run()"
            )
        return self._current_key

    @property
    def run_key(self) -> str:
        """The id this durable run began under.

        The job's own id, except across an operator's DLQ retry, which mints a
        new job for the same run and keeps the original key so a charge is not
        made twice.
        """
        session = self._session_or_inline()
        return self._ctx.job_id if session is None else session.run_key()

    # ------------------------------------------------------------- runner

    def finish(self) -> None:
        """Close the attempt out. Called by the runner; never raises."""
        if self._session is not None:
            self._session.finish()

    # ------------------------------------------------------------ private

    def _control(self) -> _ControlScope:
        """Latch the context for anything that unwinds the body from here."""
        return _ControlScope(self._ctx)

    def _begin(self, name: str, key: str | None) -> StepDecision | None:
        """Decide this step, or ``None`` when steps run inline (test mode)."""
        if not isinstance(name, str) or not name:
            raise TypeError("a step needs a name: step.run('charge', ...)")
        session = self._session_or_inline()
        if session is None:
            return None
        return session.begin_run(name, key)

    def _invoke(self, step_key: str, key: str, fn: Callable[[], _T]) -> _T:
        self._enter(step_key, key)
        try:
            return fn()
        finally:
            self._leave()

    async def _ainvoke(self, step_key: str, key: str, fn: Callable[[], Any]) -> Any:
        self._enter(step_key, key)
        try:
            return await _resolve(fn())
        finally:
            self._leave()

    def _enter(self, step_key: str, key: str) -> None:
        """Take the single in-flight slot, or refuse.

        A step's identity is its position in the sequence, so a second one
        started while the first is uncommitted has no position to take. The core
        refuses that for a real session — this guard is what makes ``test_mode``,
        which has no session, refuse it the same way instead of letting two
        bodies overwrite each other's key and quietly pass a test for code that
        dead-letters in production.

        Raises before the caller's ``try``, so a refused step never clears the
        state of the one already running.
        """
        if self._current_step is not None:
            raise StepError(
                f"step '{step_key}' started while step '{self._current_step}' is still "
                "uncommitted: steps run one at a time, so two started together have no "
                "second position to take. Await them in order."
            )
        self._current_step = step_key
        self._current_key = key

    def _leave(self) -> None:
        self._current_step = None
        self._current_key = None

    def _commit(self, decision: StepDecision, value: Any) -> None:
        self._session_required().commit_run(decision, self._encode(value))

    def _encode(self, value: Any) -> bytes:
        """Encode a step result with the **queue's** serializer, not the task's.

        That is how ``Queue(codecs=…)`` encryption reaches ``job_steps`` with no
        extra plumbing: the codec chain is already part of this serializer, so
        the core stores ciphertext without knowing it did.
        """
        return self._serializer().dumps(value)

    def _replay(self, blob: bytes) -> Any:
        """Decode a stored result.

        Typed ``Any`` on purpose: what comes back is whatever the queue's
        serializer decodes. :meth:`run` narrows it to the closure's type, which
        holds for everything the serializer round-trips exactly and is what its
        docstring qualifies.
        """
        return self._serializer().loads(bytes(blob))

    def _serializer(self) -> Serializer:
        if self._queue is None:  # pragma: no cover - guarded by _open()
            raise StepUnavailableError(
                "durable steps need a queue to encode results with", should_retry=True
            )
        return self._queue._serializer

    def _end_attempt_if_sleeping(self, outcome: StepSleepOutcome) -> None:
        """Unwind the body unless the deadline had already passed."""
        if outcome.elapsed:
            return
        raise StepSleepSignal(outcome.step_key, outcome.wake_at)

    def _session_or_inline(self) -> StepSession | None:
        """The session, or ``None`` when this queue runs steps inline."""
        if self._queue is not None and self._queue._test_mode_active:
            return None
        return self._session_required()

    def _session_required(self) -> StepSession:
        session = self._session
        if session is None:
            session = self._open()
        if session is None:
            raise StepUnavailableError(
                "durable steps need a queue that reaches storage; this task is running "
                "without one",
                should_retry=True,
            )
        return session

    def _open(self) -> StepSession | None:
        if self._queue is None:
            return None
        # An attached executor's queue stands in for the native one and answers
        # a capability probe with "no". Checked before the call so the failure
        # names the step rather than surfacing as a missing attribute — and so
        # it is a control signal the task body cannot catch away.
        if not hasattr(self._queue._inner, "open_step_session"):
            raise StepUnavailableError(
                "durable steps need a worker that reaches storage, and this task is "
                "running on an attached executor, which has none. Run it on an "
                "in-process or prefork worker.",
                should_retry=True,
            )
        self._session = self._queue._inner.open_step_session(
            self._ctx.job_id,
            self._ctx.retry_count,
            self._ctx.namespace,
        )
        return self._session

    def _inline(self, name: str, key: str | None, fn: Callable[[], _T]) -> _T:
        """Test mode: no job row exists, so the step runs and is not memoized.

        Documented rather than refused. Refusing would make every task that uses
        a step untestable with ``queue.test_mode()``, which is already an
        explicit stand-in for a worker rather than one. The idempotency key is
        still derived, so a test can assert on what the step would have sent —
        and the one-at-a-time rule still holds, so a test fails on the same
        misuse a worker would.
        """
        return self._invoke(*self._inline_identity(name, key), fn)

    async def _ainline(self, name: str, key: str | None, fn: Callable[[], Any]) -> Any:
        return await self._ainvoke(*self._inline_identity(name, key), fn)

    def _inline_identity(self, name: str, key: str | None) -> tuple[str, str]:
        """This step's key and downstream key, derived without a session."""
        occurrence = self._inline_occurrences.get(name, 0)
        self._inline_occurrences[name] = occurrence + 1
        step_key = key or f"{name}#{occurrence}"
        return step_key, f"{self._ctx.job_id}:{step_key}"


class _ControlScope:
    """Latches the active context when a step control signal leaves it."""

    __slots__ = ("_ctx",)

    def __init__(self, ctx: _ActiveContext) -> None:
        self._ctx = ctx

    def __enter__(self) -> None:
        return None

    def __exit__(self, exc_type: type[BaseException] | None, *_: object) -> None:
        if exc_type is not None and issubclass(exc_type, StepControlSignal):
            latch(self._ctx)


async def _resolve(value: Any) -> Any:
    """Await whatever is awaitable, so a step body may be either kind."""
    resolved: Any = await value if inspect.isawaitable(value) else value
    return resolved


__all__ = ["StepContext"]
