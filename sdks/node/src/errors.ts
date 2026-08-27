import { decodeTaskError } from "./task-error";

/** Base class for all FlexiQ SDK errors. Every error below extends this. */
export class FlexiQError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "FlexiQError";
  }
}

// ── Tasks & jobs ────────────────────────────────────────────────────────────

/** Thrown when a worker dequeues a job whose task name was never registered. */
export class TaskNotRegisteredError extends FlexiQError {
  constructor(readonly taskName: string) {
    super(`No task registered with name "${taskName}"`);
    this.name = "TaskNotRegisteredError";
  }
}

/**
 * Thrown when two declarations claim one task name — at declaration for a second
 * module-level {@link task}, or at drain for a deferred task colliding with one
 * the queue already registered.
 *
 * Deferred registration makes the registry implicit, so a collision that
 * `queue.task()` would settle by overwriting has to be loud instead: the losing
 * declaration would keep accepting enqueues that dispatch to the winner's
 * handler.
 */
export class DuplicateTaskError extends FlexiQError {
  constructor(
    readonly taskName: string,
    readonly owner: string,
  ) {
    super(
      `task "${taskName}" is already registered by ${owner} — ` +
        "give one of them a different name",
    );
    this.name = "DuplicateTaskError";
  }
}

/**
 * Thrown when a deferred task is enqueued before any queue drained the pending
 * registry. A `Queue` claims declarations at construction, at
 * {@link Queue.discover}, and at worker start; before then there is no queue to
 * enqueue to.
 */
export class TaskNotBoundError extends FlexiQError {
  constructor(readonly taskName: string) {
    super(
      `task "${taskName}" is not bound to a queue — no Queue has drained the pending ` +
        "registry. Construct the Queue after importing the task modules, call " +
        "queue.discover(...), or start the worker.",
    );
    this.name = "TaskNotBoundError";
  }
}

/**
 * Thrown by {@link Queue.discover} when a task directory cannot be read or one of
 * its modules throws on import. Carries the offending `path` and the underlying
 * failure as `cause`.
 *
 * Never downgraded to a warning: the dispatcher treats an unregistered task as a
 * fatal, non-retryable failure, so a worker that discovered all but one module
 * dead-letters every job belonging to the one it missed.
 */
export class TaskDiscoveryError extends FlexiQError {
  constructor(
    readonly path: string,
    message: string,
    cause: unknown,
  ) {
    super(message);
    this.name = "TaskDiscoveryError";
    this.cause = cause;
  }
}

/**
 * Thrown by {@link Queue.result} when the awaited job failed or dead-lettered.
 * A structured (cross-SDK JSON) reason exposes `errtype`/`traceback`; a plain
 * legacy/system string is surfaced verbatim with those fields undefined.
 */
export class JobFailedError extends FlexiQError {
  /** The task exception's class name, when the reason is structured. */
  readonly errtype?: string;
  /** Best-effort stack lines from the failing worker, when structured. */
  readonly traceback?: readonly string[];
  /** The stored error string exactly as persisted. */
  readonly raw: string;

  constructor(
    readonly jobId: string,
    reason: string,
  ) {
    const decoded = decodeTaskError(reason);
    super(
      decoded
        ? `Job ${jobId} failed: ${decoded.errtype}: ${decoded.message}`
        : `Job ${jobId} failed: ${reason}`,
    );
    this.name = "JobFailedError";
    this.raw = reason;
    if (decoded) {
      this.errtype = decoded.errtype;
      this.traceback = decoded.traceback;
    }
  }
}

/** Thrown by {@link Queue.result} when the awaited job was cancelled. */
export class JobCancelledError extends FlexiQError {
  constructor(readonly jobId: string) {
    super(`Job ${jobId} was cancelled`);
    this.name = "JobCancelledError";
  }
}

/** Thrown by {@link Queue.result} when a job doesn't settle within the timeout. */
export class ResultTimeoutError extends FlexiQError {
  constructor(
    readonly jobId: string,
    readonly timeoutMs: number,
  ) {
    super(`timed out after ${timeoutMs}ms waiting for job ${jobId}`);
    this.name = "ResultTimeoutError";
  }
}

// ── Queue & locks ───────────────────────────────────────────────────────────

/** Thrown on queue-level configuration or operational errors. */
export class QueueError extends FlexiQError {
  constructor(message: string) {
    super(message);
    this.name = "QueueError";
  }
}

/** Thrown by `enqueue`/`enqueueMany` when a queue's `maxPending` admission cap is reached. */
export class QueueFullError extends QueueError {
  constructor(
    readonly queue: string,
    readonly pending: number,
    readonly cap: number,
  ) {
    super(`queue '${queue}' is full: ${pending} pending >= maxPending ${cap}`);
    this.name = "QueueFullError";
  }
}

/**
 * The tail of the core's admission refusal. A debounced enqueue is the one
 * enqueue whose cap is applied by storage — it alone knows whether the call
 * inserts a row or slides an open window — so its counts come back on the
 * error. Anchored on the end, where a queue name can never be mistaken for
 * the two integers.
 */
const QUEUE_FULL_PATTERN = /\bis full: (\d+) pending >= max_pending (\d+)$/;

/**
 * Rebuild the {@link QueueFullError} a native rejection describes, or
 * `undefined` when `error` is any other failure — which the caller rethrows
 * untouched rather than reporting as a full queue.
 */
export function queueFullFrom(error: unknown, queue: string): QueueFullError | undefined {
  const message = error instanceof Error ? error.message : String(error);
  const match = QUEUE_FULL_PATTERN.exec(message);
  return match === null ? undefined : new QueueFullError(queue, Number(match[1]), Number(match[2]));
}

/** Thrown by {@link Queue.withLock} when the lock is held by another owner. */
export class LockNotAcquiredError extends FlexiQError {
  constructor(readonly lockName: string) {
    super(`Could not acquire lock "${lockName}"`);
    this.name = "LockNotAcquiredError";
  }
}

/** Thrown when a held lock's lease was lost before the guarded section finished. */
export class LockLostError extends FlexiQError {
  constructor(readonly lockName: string) {
    super(`Lost lock "${lockName}" before the guarded section completed`);
    this.name = "LockLostError";
  }
}

// ── Serialization & validation ──────────────────────────────────────────────

/** Thrown on serialization, deserialization, or payload-integrity failures. */
export class SerializationError extends FlexiQError {
  constructor(message: string) {
    super(message);
    this.name = "SerializationError";
  }
}

/** Thrown when a payload codec fails to decrypt or verify a payload. */
export class CryptoError extends SerializationError {
  constructor(message: string) {
    super(message);
    this.name = "CryptoError";
  }
}

/** Thrown when a `notes` object violates the structured-notes contract. */
export class NotesValidationError extends FlexiQError {
  constructor(message: string) {
    super(message);
    this.name = "NotesValidationError";
  }
}

// ── Resources ───────────────────────────────────────────────────────────────

/** Base class for resource dependency-injection errors. */
export class ResourceError extends FlexiQError {
  constructor(message: string) {
    super(message);
    this.name = "ResourceError";
  }
}

/** Thrown when resolving a resource name that was never registered. */
export class ResourceNotFoundError extends ResourceError {
  constructor(readonly resourceName: string) {
    super(`No resource registered with name "${resourceName}"`);
    this.name = "ResourceNotFoundError";
  }
}

/** Thrown when a resource is resolved from a scope that outlives it. */
export class ResourceScopeError extends ResourceError {
  constructor(
    readonly resourceName: string,
    scope: string = "task",
    context: string = "worker",
  ) {
    super(
      `Resource "${resourceName}" is ${scope}-scoped and cannot be resolved at ${context} scope`,
    );
    this.name = "ResourceScopeError";
  }
}

/** Thrown when a pooled resource cannot be checked out before its acquire timeout. */
export class ResourceUnavailableError extends ResourceError {
  constructor(message: string) {
    super(message);
    this.name = "ResourceUnavailableError";
  }
}

// ── Workflows ───────────────────────────────────────────────────────────────

/** Thrown on workflow definition, submission, or query errors. */
export class WorkflowError extends FlexiQError {
  constructor(message: string) {
    super(message);
    this.name = "WorkflowError";
  }
}

// ── Predicates ──────────────────────────────────────────────────────────────

/** Thrown when an enqueue-time predicate gate rejects the submission. */
export class PredicateRejectedError extends FlexiQError {
  constructor(
    readonly taskName: string,
    readonly reason?: string,
  ) {
    super(
      reason
        ? `predicate rejected enqueue of "${taskName}": ${reason}`
        : `predicate rejected enqueue of "${taskName}"`,
    );
    this.name = "PredicateRejectedError";
  }
}

/**
 * Thrown when a gate decided to skip the enqueue. A skip is deliberate, not a
 * failure — callers that expect it should use `Queue.tryEnqueue`, which returns
 * `null` instead of throwing.
 */
export class EnqueueSkippedError extends FlexiQError {
  constructor(
    readonly taskName: string,
    readonly reason?: string,
  ) {
    super(
      reason
        ? `a gate skipped the enqueue of "${taskName}": ${reason} (use tryEnqueue)`
        : `a gate skipped the enqueue of "${taskName}" (use tryEnqueue)`,
    );
    this.name = "EnqueueSkippedError";
  }
}

/**
 * Thrown when a predicate is misconfigured: an unknown registry name, a bad
 * recipe argument, or a gate returning an unrecognized decision.
 */
export class PredicateValidationError extends FlexiQError {
  constructor(message: string) {
    super(message);
    this.name = "PredicateValidationError";
  }
}

// ── Proxies ─────────────────────────────────────────────────────────────────

/** Thrown on proxy handler, signature, expiry, purpose, or allowlist failures. */
export class ProxyError extends FlexiQError {
  constructor(message: string) {
    super(message);
    this.name = "ProxyError";
  }
}

/** Thrown when an enqueue interceptor rejects, misbehaves, or redirects illegally. */
export class InterceptionError extends FlexiQError {
  constructor(message: string) {
    super(message);
    this.name = "InterceptionError";
  }
}
