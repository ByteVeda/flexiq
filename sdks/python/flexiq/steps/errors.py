"""What ``ctx.step`` raises, and why none of it is an ``Exception``.

A step failure and a step sleep both unwind the task body, and neither may be
caught away by user code — a swallowed sleep runs the rest of the task
unclaimed, and a swallowed divergence returns a memoized answer to a different
question. So every class here descends from :class:`BaseException`, the same
way :class:`KeyboardInterrupt` does: a bare ``except Exception`` in a task body
misses them.

That is only the first of the two layers. It cannot stop ``except
BaseException``, so the runner also latches — see :mod:`flexiq.steps.signals`.
"""

from __future__ import annotations


class StepControlSignal(BaseException):
    """Base for everything ``ctx.step`` raises to end an attempt.

    Deliberately not a :class:`~flexiq.exceptions.FlexiQError`: that is an
    ``Exception``, and this must not be catchable as one.
    """

    #: What the attempt should do, decided by the core's step-failure
    #: classification rather than by the task's ``retry_on`` filters. Every
    #: worker path reads this attribute before consulting those filters.
    flexiq_should_retry: bool = False


class StepSleepSignal(StepControlSignal):
    """Raised by ``ctx.step.sleep`` once the sleep row is committed.

    By the time this is raised the job is already ``Pending`` at ``wake_at``
    and this worker's claim is gone, so the body must unwind now. It is not a
    failure: the attempt ends without touching the retry count, the retry
    budget, the circuit breaker or the task metrics.
    """

    def __init__(self, step_key: str, wake_at: int) -> None:
        super().__init__(f"step {step_key} sleeps until {wake_at}")
        self.step_key = step_key
        self.wake_at = wake_at


class StepError(StepControlSignal):
    """A step operation failed.

    ``should_retry`` comes from the core: a divergence, a cap or a bad encoding
    will be just as wrong next attempt, while an unreachable backend may not
    be.
    """

    def __init__(self, message: str, *, should_retry: bool = False) -> None:
        super().__init__(message)
        self.flexiq_should_retry = should_retry


class StepUnavailableError(StepError):
    """Durable steps are not available where this task is running.

    The attempt fails rather than running the step un-memoized: a
    heterogeneous fleet mid-rollout may place the next attempt on a worker that
    can commit, and there is no version of "your charge step silently lost its
    memo" that beats a failure naming the reason.
    """


class StepDivergedError(StepError):
    """The recorded step sequence and the running code no longer agree.

    Deliberately loud and non-retryable. A memoized result handed to a step
    that now asks a different question is worse than re-running the step.
    """


class StepLimitExceededError(StepError):
    """A step result, or the job's total, is past the cap.

    The answer is not a bigger cap — it is storing the value somewhere else and
    memoizing the handle.
    """


class StepSupersededError(StepError):
    """This attempt lost its execution claim while a step was in flight.

    The job is running under another owner right now. The attempt still reports
    a failure, because every worker path owes the scheduler a result, but the
    scheduler fences on ``(owner, attempt)`` before it mutates anything and
    drops this one — so the run proceeding elsewhere is untouched.
    """


class StepSwallowedError(StepError):
    """The task body caught a step control signal and returned anyway.

    The second of the two swallow layers. Whatever the body went on to do ran
    without a claim, so the attempt cannot be trusted and is failed here.
    """


__all__ = [
    "StepControlSignal",
    "StepDivergedError",
    "StepError",
    "StepLimitExceededError",
    "StepSleepSignal",
    "StepSupersededError",
    "StepSwallowedError",
    "StepUnavailableError",
]
