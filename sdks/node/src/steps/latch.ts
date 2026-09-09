/**
 * The swallow defence: the signal the runner checks after the task body returns.
 *
 * A `try/catch` in a task body catches a step control signal like anything
 * else, and a body that catches a sleep and carries on runs the rest of itself
 * with no execution claim — every side effect after that point happens again on
 * wake. A body that catches a divergence goes on to return a value derived from
 * a memo that answers a different question.
 *
 * Nothing in the language stops the catch. So `ctx.step` latches before it
 * rejects, and the runner refuses to report what the body did afterwards. This
 * is §7.7's second layer, and in JavaScript it is the only one there is.
 *
 * The signal is latched rather than a flag because what the runner owes the
 * scheduler depends on which one it was — see {@link StepLatch.check} and
 * {@link StepLatch.sleep}.
 */

import { type StepControlSignal, StepSleepSignal, StepSwallowedError } from "./errors";

/** One invocation's latched signal, shared by its step context and its runner. */
export class StepLatch {
  private signal: StepControlSignal | undefined;

  /**
   * Record the step control signal being thrown out of the body.
   *
   * The first one wins. A sleep releases the claim, so a step called after one
   * was swallowed throws in its turn; latching that would lose the fact that
   * the attempt is already asleep.
   */
  latch(signal: StepControlSignal): void {
    this.signal ??= signal;
  }

  /** Whether a control signal was raised at some point during the attempt. */
  get swallowed(): boolean {
    return this.signal !== undefined;
  }

  /**
   * The sleep this attempt committed, if it committed one.
   *
   * Set from the moment `sleep` throws — and it throws only once the row, the
   * claim revocation and the reschedule are on disk — so a body that caught the
   * signal cannot hide that its attempt is over. The runner asks before it
   * reports a failure.
   */
  get sleep(): StepSleepSignal | undefined {
    return this.signal instanceof StepSleepSignal ? this.signal : undefined;
  }

  /**
   * Throw if the body returned normally after swallowing a control signal.
   *
   * Called the moment the handler resolves, before the `after` hooks: what the
   * body returned is not a result, and the hooks exist to see one.
   *
   * A swallowed **sleep** is rethrown as the sleep it is. `sleepFor` committed
   * the row, revoked the claim and moved the job to `Pending` before the signal
   * was ever thrown, so the attempt is over either way and a failure would
   * speak for a claim it no longer holds. And a sleep leaves `retry_count`
   * alone, so the woken attempt reuses the same `(owner, attempt)`: the
   * scheduler's fence cannot tell the stale failure from the live claim,
   * authorizes it, and dead-letters a job that is sleeping correctly. Anything
   * else fails the attempt, which is where the latch has to bite — that body
   * still holds its claim and nothing downstream would question the value it
   * goes on to return.
   */
  check(): void {
    const sleeping = this.sleep;
    if (sleeping) {
      throw sleeping;
    }
    if (this.signal) {
      throw new StepSwallowedError(
        "the task body caught a step control signal and returned anyway. Whatever it did " +
          "after that ran without an execution claim, or on a memo answering a different " +
          "question, so this attempt cannot be trusted. Let ctx.step's rejections propagate.",
      );
    }
  }
}
