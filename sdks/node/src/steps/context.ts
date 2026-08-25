/**
 * `ctx.step` — durable inline steps on the task context.
 *
 * A step is a checkpoint inside one job: it runs once, its result is committed,
 * and every later attempt of that job returns the committed value instead of
 * running it again. The rules — identity, divergence, the caps, the sleep
 * decision — all live in the Rust core, which is what makes them identical
 * across the SDKs. This module is the JavaScript side of the split the core
 * exposes for exactly that reason: the core decides, the callback runs here,
 * and the core commits the bytes this shell encoded.
 *
 * Memoization alone is not exactly-once. The process can die between a payment
 * API returning 200 and the step row committing, and the replay has no record
 * the call happened. Nothing on this side of the network closes that window;
 * only a key the other side dedupes on does, which is what
 * {@link StepContext.idempotencyKey} is for:
 *
 * ```ts
 * const { step } = currentJob()!;
 * const charge = await step.run("charge", () =>
 *   stripe.charge(order, { idempotencyKey: step.idempotencyKey }),
 * );
 * ```
 *
 * A step belongs to one job. Work that must outlive a job, be distributed
 * across machines or be inspected as a graph is a workflow node, not a step.
 */

import type { NativeStepSession } from "../native";
import type { Serializer } from "../serializers";
import type { Duration } from "../types";
import { type SleepDeadline, sleepDeadlineMs, sleepDurationMs } from "./durations";
import { StepError, StepSleepSignal, StepUnavailableError, stepErrorFrom } from "./errors";
import type { StepLatch } from "./latch";

/** Where a durable step commits. Satisfied by the native queue handle. */
export interface StepStore {
  openStepSession(jobId: string, attempt: number): Promise<NativeStepSession>;
}

/** Options for {@link StepContext.run}. */
export interface StepRunOptions {
  /**
   * Explicit identity, for a step whose position may change — a loop over an
   * unordered collection. A keyed step is matched by key wherever it sits in
   * the recorded sequence; an unkeyed one is matched at its position.
   */
  key?: string;
}

/** Options for {@link StepContext.sleep} and {@link StepContext.sleepUntil}. */
export interface StepSleepOptions {
  /**
   * Name for this sleep. Strongly recommended: a sequence that reads
   * `sleep#0, sleep#1, sleep#2` tells nobody which one diverged.
   */
  name?: string;
  /** Explicit identity, as {@link StepRunOptions.key}. */
  key?: string;
}

/** What `beginRun`/`sleepFor` return, narrowed to what this module reads. */
interface SleepOutcome {
  elapsed: boolean;
  stepKey: string;
  wakeAt: number;
}

/**
 * The `step` property of a running job's context.
 *
 * One per attempt, held on the job context so the session it opens — and the
 * identity of the step currently running — survive across calls.
 */
export class StepContext {
  private opening?: Promise<NativeStepSession>;
  private opened?: NativeStepSession;
  private currentKey?: string;

  /** @internal Built by the task runner; reach it through `currentJob().step`. */
  constructor(
    private readonly jobId: string,
    private readonly attempt: number,
    private readonly serializer: Serializer,
    private readonly latch: StepLatch,
    /** Absent where this process cannot commit a step — an attached executor. */
    private readonly store?: StepStore,
  ) {}

  // ------------------------------------------------------------------ run

  /**
   * Run `fn` once for this job, or return what it returned last time.
   *
   * `name` is required and positional: an inferred name changes whenever the
   * callback is renamed or inlined, and a step whose identity moves is a step
   * whose memo answers a different question.
   *
   * Async even on a memo hit, so the call shape does not change between a fresh
   * run and a replay.
   *
   * The first run resolves to exactly what `fn` returned; a replay resolves to
   * that value decoded from its stored bytes, so anything the queue's
   * serializer does not round-trip exactly — a `Map`, a `Set`, a class
   * instance — comes back in its decoded shape. Return something the serializer
   * preserves, or a handle to it.
   *
   * **Steps run one at a time.** A step's position in the sequence is what
   * identifies it, so a second step started while the first is still
   * uncommitted has no position to take: `Promise.all` over two `run` calls
   * fails the attempt permanently rather than interleaving them. Await them in
   * order.
   *
   * **Its rejections must reach the runner.** A divergence, a cap violation or
   * a lost claim rejects with a {@link StepControlSignal}. JavaScript has no
   * error a `catch` misses, so catching one is possible — but the runner
   * latches when one is thrown and fails the attempt if the body returns
   * anyway, so swallowing it buys nothing and costs a clear error message.
   *
   * @throws TypeError if `name` is empty or not a string.
   */
  async run<T>(name: string, fn: () => T | Promise<T>, options: StepRunOptions = {}): Promise<T> {
    if (typeof name !== "string" || name.length === 0) {
      throw new TypeError("a step needs a name: step.run('charge', ...)");
    }
    const session = await this.guard(() => this.session());
    const decision = await this.guard(() => session.beginRun(name, options.key));
    if (decision.memoized != null) {
      return this.serializer.deserialize(decision.memoized) as T;
    }
    const value = await this.invoke(decision.idempotencyKey, fn);
    const encoded = this.encode(decision.stepKey, value);
    await this.guard(() => session.commitRun(encoded));
    return value;
  }

  // ---------------------------------------------------------------- sleep

  /**
   * Sleep for `duration`, ending this attempt if the deadline is ahead.
   *
   * The attempt ends: the claim is released and the job goes back to `Pending`
   * at its deadline, so a sleeping job holds no worker slot and cannot be timed
   * out while it waits. On wake the job replays from the top, every earlier
   * step is a memo hit, and this sleep resolves immediately.
   *
   * A sleep costs no retry — the retry count, the retry budget, the circuit
   * breaker and the task metrics are all untouched.
   *
   * The deadline is fixed by the **first** commit. Replaying a `"1h"` sleep
   * wakes at the original instant rather than an hour later each time, which is
   * what stops a crash loop from producing a sleep that outlives the job.
   *
   * When the deadline is still ahead this rejects rather than resolving — the
   * attempt is over, and anything the body does past this point runs unclaimed
   * and runs again on wake. Let it propagate.
   */
  async sleep(duration: Duration, options: StepSleepOptions = {}): Promise<void> {
    const millis = sleepDurationMs(duration);
    const session = await this.guard(() => this.session());
    this.endAttemptIfSleeping(
      await this.guard(() => session.sleepFor(millis, options.name, options.key)),
    );
  }

  /**
   * Sleep until an absolute instant.
   *
   * Reach for this over {@link sleep} when the deadline means something outside
   * the job — a billing date, a market open — because an absolute instant is
   * unaffected by how many times the attempt replayed.
   */
  async sleepUntil(when: SleepDeadline, options: StepSleepOptions = {}): Promise<void> {
    const millis = sleepDeadlineMs(when);
    const session = await this.guard(() => this.session());
    this.endAttemptIfSleeping(
      await this.guard(() => session.sleepUntil(millis, options.name, options.key)),
    );
  }

  // ----------------------------------------------------------------- keys

  /**
   * The key to hand the downstream service for the step running now.
   *
   * Stable across a retry, across a sleep/wake and across an operator's
   * dead-letter retry, and no serializer or codec touches it. Readable only
   * from inside a step body — outside one there is no step for it to name.
   */
  get idempotencyKey(): string {
    if (this.currentKey === undefined) {
      throw new StepError(
        "step.idempotencyKey names the step that is running, so it is only readable inside " +
          "a step body — read it from within the callback given to step.run()",
        false,
      );
    }
    return this.currentKey;
  }

  /**
   * The id this durable run began under.
   *
   * The job's own id, except across an operator's dead-letter retry, which
   * mints a new job for the same run and keeps the original key so a charge is
   * not made twice.
   */
  async runKey(): Promise<string> {
    const session = await this.guard(() => this.session());
    return this.guard(() => session.runKey());
  }

  // --------------------------------------------------------------- runner

  /** @internal Close the attempt out. Called by the runner; never throws. */
  finish(): void {
    this.opened?.finish();
  }

  // -------------------------------------------------------------- private

  /**
   * Run a native step call, converting its rejection and latching the body.
   *
   * Only the step machinery goes through here. The step *callback* is invoked
   * bare: a payment API that fails is the task failing, not a control signal,
   * and the task's own `retryOn` filter should have its say about it.
   */
  private async guard<T>(body: () => Promise<T>): Promise<T> {
    try {
      return await body();
    } catch (error) {
      this.latch.latch();
      throw stepErrorFrom(error);
    }
  }

  /** The session for this attempt, opened once and reused. */
  private session(): Promise<NativeStepSession> {
    this.opening ??= this.open();
    return this.opening;
  }

  private async open(): Promise<NativeStepSession> {
    if (!this.store) {
      // An attached executor has no storage and no channel to commit a step on
      // (the design's `job_steps` / `step_commit` / `step_ack` frames do not
      // exist), so it refuses rather than running the step un-memoized.
      // Retryable: a heterogeneous fleet mid-rollout may put the next attempt
      // on a worker that can commit.
      throw new StepUnavailableError(
        "durable steps need a worker that reaches storage, and this task is running on an " +
          "attached executor, which has none. Run it on an in-process worker (runWorker).",
      );
    }
    const session = await this.store.openStepSession(this.jobId, this.attempt);
    this.opened = session;
    return session;
  }

  /** Bind this step's downstream key for the length of its callback. */
  private async invoke<T>(idempotencyKey: string, fn: () => T | Promise<T>): Promise<T> {
    this.currentKey = idempotencyKey;
    try {
      return await fn();
    } finally {
      this.currentKey = undefined;
    }
  }

  /**
   * Encode a step result with the **queue's** serializer, not a task's.
   *
   * That is how `new Queue({ codec })` encryption reaches `job_steps` with no
   * extra plumbing: the codec chain is already part of this serializer, so the
   * core stores ciphertext without knowing it did.
   *
   * A value the serializer cannot encode is a permanent step failure — the
   * replay would produce the same value — and a control signal, because the
   * step has already run its side effect and nothing was committed.
   */
  private encode(stepKey: string, value: unknown): Buffer {
    try {
      return Buffer.from(this.serializer.serialize(value));
    } catch (error) {
      this.latch.latch();
      const reason = error instanceof Error ? error.message : String(error);
      throw new StepError(
        `step '${stepKey}' returned a value the queue serializer cannot encode: ${reason}`,
        false,
      );
    }
  }

  /** Unwind the body unless the deadline had already passed. */
  private endAttemptIfSleeping(outcome: SleepOutcome): void {
    if (outcome.elapsed) {
      return;
    }
    this.latch.latch();
    throw new StepSleepSignal(outcome.stepKey, outcome.wakeAt);
  }
}
