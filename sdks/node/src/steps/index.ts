/**
 * Durable inline steps: checkpointing inside a single job.
 *
 * `ctx.step.run` memoizes a piece of work against the job row, so a retry
 * replays it instead of re-running it, and `ctx.step.sleep` ends the attempt
 * rather than holding a worker slot. Reached through the task context:
 *
 * ```ts
 * import { currentJob } from "@byteveda/flexiq";
 *
 * queue.task("checkout", async (order: Order) => {
 *   const { step } = currentJob()!;
 *   const charge = await step.run("charge", () =>
 *     stripe.charge(order, { idempotencyKey: step.idempotencyKey }),
 *   );
 *   await step.sleep("1h");
 *   await step.run("receipt", () => sendReceipt(charge));
 * });
 * ```
 *
 * A step belongs to one job. Work that must outlive a job, be distributed
 * across machines or be inspected as a graph is a workflow node, not a step.
 */

export {
  StepContext,
  type StepRunOptions,
  type StepSleepOptions,
  type StepStore,
} from "./context";
export type { SleepDeadline } from "./durations";
export {
  StepControlSignal,
  StepDivergedError,
  StepError,
  StepLimitExceededError,
  StepSleepSignal,
  StepSupersededError,
  StepSwallowedError,
  StepUnavailableError,
  stepErrorFrom,
  stepRetryDecision,
} from "./errors";
export { StepLatch } from "./latch";
