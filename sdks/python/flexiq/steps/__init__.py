"""Durable inline steps: checkpointing inside a single job.

``ctx.step.run`` memoizes a piece of work against the job row, so a retry
replays it instead of re-running it, and ``ctx.step.sleep`` ends the attempt
rather than holding a worker slot. Reached through the task context::

    from flexiq.context import current_job

    @queue.task()
    def checkout(order):
        charge = current_job.step.run("charge", lambda: charge_card(order))
        current_job.step.sleep("1h")
        current_job.step.run("receipt", lambda: send_receipt(charge))

A step belongs to one job. Work that must outlive a job, be distributed across
machines or be inspected as a graph is a workflow node, not a step.
"""

from flexiq.steps.context import StepContext
from flexiq.steps.durations import SleepDeadline, SleepDuration
from flexiq.steps.errors import (
    StepControlSignal,
    StepDivergedError,
    StepError,
    StepLimitExceededError,
    StepSleepSignal,
    StepSupersededError,
    StepSwallowedError,
    StepUnavailableError,
)
from flexiq.steps.failure import step_retry_decision
from flexiq.steps.latch import was_swallowed

__all__ = [
    "SleepDeadline",
    "SleepDuration",
    "StepContext",
    "StepControlSignal",
    "StepDivergedError",
    "StepError",
    "StepLimitExceededError",
    "StepSleepSignal",
    "StepSupersededError",
    "StepSwallowedError",
    "StepUnavailableError",
    "step_retry_decision",
    "was_swallowed",
]
