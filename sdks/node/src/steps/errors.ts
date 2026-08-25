/**
 * What `ctx.step` rejects with, and why catching it does not help.
 *
 * The Python shell puts every one of these under `BaseException`, so a bare
 * `except Exception` in a task body misses them. JavaScript has no such tier:
 * `catch` catches everything, and a control signal is an ordinary `Error`. So
 * this layer is documentation, not enforcement — the enforcement is the latch
 * (see {@link StepLatch}), which fails the attempt when a body catches one of
 * these and returns anyway.
 *
 * That is not a weaker guarantee, only a later one. A swallowed divergence
 * still never lets the attempt report a result.
 */

/**
 * Base for everything `ctx.step` rejects with to end an attempt.
 *
 * Deliberately not a `FlexiQError`: that is the SDK's ordinary-failure family,
 * and a control signal is not a failure the task is meant to handle.
 */
export class StepControlSignal extends Error {
  /**
   * What the attempt should do, decided by the core's step-failure
   * classification rather than by the task's `retryOn` filter. Every retry
   * decision reads this before consulting that filter.
   */
  readonly flexiqShouldRetry: boolean = false;

  constructor(message: string) {
    super(message);
    this.name = "StepControlSignal";
  }
}

/**
 * Rejected by `ctx.step.sleep` once the sleep row is committed.
 *
 * By the time this is thrown the job is already `Pending` at `wakeAt` and this
 * worker's claim is gone, so the body must unwind now. It is not a failure:
 * the attempt ends without touching the retry count, the retry budget, the
 * circuit breaker or the task metrics.
 */
export class StepSleepSignal extends StepControlSignal {
  constructor(
    readonly stepKey: string,
    readonly wakeAt: number,
  ) {
    super(`step ${stepKey} sleeps until ${wakeAt}`);
    this.name = "StepSleepSignal";
  }
}

/**
 * A step operation failed.
 *
 * `shouldRetry` comes from the core: a divergence, a cap or a bad encoding will
 * be just as wrong next attempt, while an unreachable backend may not be.
 */
export class StepError extends StepControlSignal {
  override readonly flexiqShouldRetry: boolean;

  constructor(message: string, shouldRetry = false) {
    super(message);
    this.name = "StepError";
    this.flexiqShouldRetry = shouldRetry;
  }
}

/**
 * Durable steps are not available where this task is running.
 *
 * The attempt fails rather than running the step un-memoized: a heterogeneous
 * fleet mid-rollout may place the next attempt on a worker that can commit, and
 * there is no version of "your charge step silently lost its memo" that beats a
 * failure naming the reason.
 */
export class StepUnavailableError extends StepError {
  constructor(message: string, shouldRetry = true) {
    super(message, shouldRetry);
    this.name = "StepUnavailableError";
  }
}

/**
 * The recorded step sequence and the running code no longer agree.
 *
 * Deliberately loud and non-retryable. A memoized result handed to a step that
 * now asks a different question is worse than re-running the step.
 */
export class StepDivergedError extends StepError {
  constructor(message: string, shouldRetry = false) {
    super(message, shouldRetry);
    this.name = "StepDivergedError";
  }
}

/**
 * A step result, or the job's total, is past the cap.
 *
 * The answer is not a bigger cap — it is storing the value somewhere else and
 * memoizing the handle.
 */
export class StepLimitExceededError extends StepError {
  constructor(message: string, shouldRetry = false) {
    super(message, shouldRetry);
    this.name = "StepLimitExceededError";
  }
}

/**
 * This attempt lost its execution claim while a step was in flight.
 *
 * The job is running under another owner right now. The attempt still reports a
 * failure, because every worker path owes the scheduler a result, but the
 * scheduler fences on `(owner, attempt)` before it mutates anything and drops
 * this one — so the run proceeding elsewhere is untouched.
 */
export class StepSupersededError extends StepError {
  constructor(message: string, shouldRetry = false) {
    super(message, shouldRetry);
    this.name = "StepSupersededError";
  }
}

/**
 * The task body caught a step control signal and returned anyway.
 *
 * In a language where `catch` catches everything, this is the whole defence.
 * Whatever the body went on to do ran without a claim, or on a memoized answer
 * to a different question, so the attempt cannot be trusted and is failed here.
 */
export class StepSwallowedError extends StepError {
  constructor(message: string) {
    super(message, false);
    this.name = "StepSwallowedError";
  }
}

/** The step-error classes the native reason can name, by their `flexiqStep` tag. */
const CLASSES = {
  diverged: StepDivergedError,
  limit: StepLimitExceededError,
  superseded: StepSupersededError,
  unavailable: StepUnavailableError,
  error: StepError,
} as const;

/** The JSON reason `steps.rs` puts in a napi error's message. */
interface NativeStepReason {
  flexiqStep: keyof typeof CLASSES;
  message: string;
  retryable: boolean;
}

function isNativeStepReason(value: unknown): value is NativeStepReason {
  if (typeof value !== "object" || value === null) {
    return false;
  }
  const reason = value as Record<string, unknown>;
  return (
    typeof reason.flexiqStep === "string" &&
    reason.flexiqStep in CLASSES &&
    typeof reason.message === "string" &&
    typeof reason.retryable === "boolean"
  );
}

/**
 * Rebuild the step error a native rejection describes.
 *
 * napi carries a status and a string, so the binding encodes the class and the
 * core's retry verdict as JSON in the message (`steps.rs::step_error`). A
 * message that is not one of ours is wrapped verbatim as a retryable
 * {@link StepError} — an addon older than this shell, or a failure raised
 * before the binding could classify it, is a reason to fail the attempt, not to
 * guess that it is permanent.
 */
export function stepErrorFrom(error: unknown): StepControlSignal {
  if (error instanceof StepControlSignal) {
    return error;
  }
  const message = error instanceof Error ? error.message : String(error);
  let parsed: unknown;
  try {
    parsed = JSON.parse(message);
  } catch {
    return new StepError(message, true);
  }
  if (!isNativeStepReason(parsed)) {
    return new StepError(message, true);
  }
  return new CLASSES[parsed.flexiqStep](parsed.message, parsed.retryable);
}

/**
 * The core's retry decision for `error`, or `undefined` if it is not a step
 * failure.
 *
 * Read by name rather than by class so an error raised by a future step surface
 * participates without this module having to know about it.
 */
export function stepRetryDecision(error: unknown): boolean | undefined {
  const decision = (error as { flexiqShouldRetry?: unknown } | null)?.flexiqShouldRetry;
  return typeof decision === "boolean" ? decision : undefined;
}
