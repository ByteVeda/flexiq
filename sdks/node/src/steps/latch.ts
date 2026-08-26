/**
 * The swallow defence: a flag the runner checks after the task body returns.
 *
 * A `try/catch` in a task body catches a step control signal like anything
 * else, and a body that catches a sleep and carries on runs the rest of itself
 * with no execution claim — every side effect after that point happens again on
 * wake. A body that catches a divergence goes on to return a value derived from
 * a memo that answers a different question.
 *
 * Nothing in the language stops the catch. So `ctx.step` latches before it
 * rejects, and the runner fails the attempt if the body returns normally with
 * the latch set. This is §7.7's second layer, and in JavaScript it is the only
 * one there is.
 */

import { StepSwallowedError } from "./errors";

/** One invocation's swallow flag, shared by its step context and its runner. */
export class StepLatch {
  private raised = false;

  /** Record that a step control signal is being thrown out of the body. */
  latch(): void {
    this.raised = true;
  }

  /** Whether a control signal was raised at some point during the attempt. */
  get swallowed(): boolean {
    return this.raised;
  }

  /**
   * Throw if the body returned normally after swallowing a control signal.
   *
   * Called the moment the handler resolves, before the `after` hooks: what the
   * body returned is not a result, and the hooks exist to see one.
   *
   * A swallowed **sleep** reaches here too, and the failure it raises is then
   * dropped — `sleepFor` already left the job `Pending` and unclaimed, which is
   * exactly what `handle_result`'s `(owner, attempt)` fence calls superseded.
   * The job wakes, the sleep is a memo hit, and the body finishes. One attempt
   * wasted, nothing broken. The latch only *bites* on a swallowed divergence,
   * where the attempt still holds its claim and nothing downstream would
   * question the value it goes on to return.
   */
  check(): void {
    if (this.raised) {
      throw new StepSwallowedError(
        "the task body caught a step control signal and returned anyway. Whatever it did " +
          "after that ran without an execution claim, or on a memo answering a different " +
          "question, so this attempt cannot be trusted. Let ctx.step's rejections propagate.",
      );
    }
  }
}
