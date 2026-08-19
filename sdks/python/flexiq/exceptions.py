"""Custom exception hierarchy for flexiq."""


class FlexiQError(Exception):
    """Base exception for all flexiq errors."""


class TaskTimeoutError(FlexiQError):
    """Raised when a task exceeds its hard timeout."""


class SoftTimeoutError(FlexiQError):
    """Raised when a task exceeds its soft timeout (checked via context)."""


class TaskCancelledError(FlexiQError):
    """Raised when a running task detects it has been cancelled."""


class JobErrorDetails:
    """Structured failure details mixin for job-outcome exceptions.

    ``job_id`` and ``raw_error`` are always populated. ``errtype`` and
    ``traceback`` carry the decoded canonical task error (see
    ``flexiq.task_errors``) and are ``None`` when the stored error is a
    legacy/system plain string.
    """

    errtype: str | None = None
    traceback: list[str] | None = None
    job_id: str | None = None
    raw_error: str | None = None

    def _attach_details(
        self,
        *,
        errtype: str | None,
        traceback: list[str] | None,
        job_id: str | None,
        raw_error: str | None,
    ) -> None:
        self.errtype = errtype
        self.traceback = traceback
        self.job_id = job_id
        self.raw_error = raw_error


class TaskFailedError(FlexiQError, JobErrorDetails):
    """Raised when a task has failed."""


class MaxRetriesExceededError(FlexiQError, JobErrorDetails):
    """Raised when a task has exhausted all retry attempts."""


class SerializationError(FlexiQError):
    """Raised on serialization or deserialization failures."""


class CryptoError(SerializationError):
    """Raised when a payload codec fails to decrypt or verify a payload."""


class CircuitBreakerOpenError(FlexiQError):
    """Raised when a task's circuit breaker is open."""


class RateLimitExceededError(FlexiQError):
    """Raised when a task's rate limit is exceeded."""


class JobNotFoundError(FlexiQError, KeyError):
    """Raised when a job ID is not found in storage."""


class QueueError(FlexiQError):
    """Raised on queue-level operational errors."""


class QueueFullError(QueueError):
    """Raised when enqueuing would exceed a queue's ``max_pending`` admission cap."""


class ResourceError(FlexiQError):
    """Base exception for resource system errors."""


class ResourceInitError(ResourceError):
    """Raised when a resource factory fails during initialization."""


class ResourceUnavailableError(ResourceError):
    """Raised when a resource is permanently unhealthy and cannot be resolved."""


class CircularDependencyError(ResourceError):
    """Raised when resource dependencies form a cycle."""


class ResourceNotFoundError(ResourceError, KeyError):
    """Raised when resolving a resource name that was never registered."""


class ProxyReconstructionError(ResourceError):
    """Raised when a proxy handler fails to reconstruct an object from its recipe."""


class ProxyCleanupError(ResourceError):
    """Raised when a proxy handler fails during cleanup."""


class NotesValidationError(FlexiQError, ValueError):
    """Raised when a ``notes`` dict violates the structured-notes contract.

    Subclasses :class:`ValueError` so existing ``except ValueError`` clauses
    keep working when validation fails inside enqueue paths.
    """


class PredicateRejectedError(FlexiQError):
    """Raised when an enqueue-time predicate cancels the submission.

    The ``reason`` attribute carries the message attached to
    :class:`~flexiq.predicates.Cancel` (or an empty string when a plain
    ``False`` outcome triggers cancellation under ``on_false="cancel"``).
    """

    def __init__(self, task_name: str, reason: str = "") -> None:
        self.task_name = task_name
        self.reason = reason
        msg = f"predicate rejected enqueue of {task_name!r}"
        if reason:
            msg = f"{msg}: {reason}"
        super().__init__(msg)


class DuplicateTaskError(FlexiQError, ValueError):
    """Raised when two tasks claim the same registered name.

    Deferred registration makes the registry implicit, so a collision that
    ``@queue.task()`` would have resolved by overwriting has to be loud
    instead: the losing task would keep accepting submissions that dispatch
    to the winner's function.

    Subclasses :class:`ValueError` so ``except ValueError`` around a task
    module import keeps working.
    """


class TaskNotBoundError(FlexiQError, RuntimeError):
    """Raised when a deferred task is submitted before any queue drained it.

    ``@flexiq.task`` records the task in the pending registry; a ``Queue``
    claims it at construction, at :meth:`~flexiq.Queue.autodiscover`, or at
    worker start. Submitting before then has no queue to submit to.
    """
