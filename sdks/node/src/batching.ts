// Producer-side batching: buffer enqueues for one task and send them as a
// single `enqueueMany` round-trip. The counterpart of Python's
// `BatchAccumulator` and Java's `Batcher<T>`.

import { QueueError } from "./errors";
import type { Queue } from "./queue";
import type { EnqueueOptions, TaskMap } from "./types";
import { createLogger } from "./utils";

const DEFAULT_MAX_SIZE = 100;
const DEFAULT_MAX_WAIT_MS = 500;
// Node clamps any `setTimeout` delay above this to 1ms, which would turn a long
// wait into an immediate flush — reject it at construction instead.
const MAX_TIMER_DELAY_MS = 2_147_483_647;
const log = createLogger("batching");

/** Construction options for a {@link Batcher}. */
export interface BatcherOptions {
  /** Flush as soon as this many entries have accumulated. Default 100. */
  maxSize?: number;
  /**
   * Flush this long after the first buffered entry arrived, even if `maxSize`
   * isn't reached — the worst-case latency any single entry can see. Default 500.
   */
  maxWaitMs?: number;
  /**
   * Called when a timer-driven flush throws. A timed flush has no caller to
   * raise to, so without this the failure is only logged. The entries stay
   * buffered either way and are retried on the next window.
   */
  onError?: (error: unknown) => void;
}

/** One buffered enqueue: the same `{ args, options }` shape `enqueueMany` takes. */
interface BufferedEntry<Args extends unknown[]> {
  args?: Args;
  options?: EnqueueOptions;
}

/**
 * Buffers enqueues for one task and flushes them through
 * {@link Queue.enqueueMany} once the buffer reaches `maxSize` or `maxWaitMs`
 * elapses since the first buffered entry — producer-side batching, to cut
 * storage round-trips when many small jobs share a task.
 *
 * ```ts
 * using batcher = queue.batcher("sendEmail", { maxSize: 100, maxWaitMs: 500 });
 * for (const email of emails) {
 *   batcher.add([email]);
 * }
 * // block exit flushes the remainder
 * ```
 *
 * The flush timer is `unref`'d, so a partially filled buffer never keeps the
 * process alive — and is lost if the process exits without {@link Batcher.close}
 * (or `using`). This matches Java's daemon scheduler and Python's atexit-only
 * flush.
 *
 * Distinct from worker-side dequeue batching (`batchSize` on the worker), which
 * controls how many jobs one poll claims.
 */
export class Batcher<
  TTasks extends TaskMap = TaskMap,
  Name extends keyof TTasks & string = keyof TTasks & string,
> {
  private buffer: BufferedEntry<Parameters<TTasks[Name]>>[] = [];
  private readonly maxSize: number;
  private readonly maxWaitMs: number;
  private readonly onError: ((error: unknown) => void) | undefined;
  private timer?: ReturnType<typeof setTimeout>;
  private isClosed = false;

  constructor(
    private readonly queue: Queue<TTasks>,
    readonly name: Name,
    options: BatcherOptions = {},
  ) {
    const maxSize = options.maxSize ?? DEFAULT_MAX_SIZE;
    if (!Number.isInteger(maxSize) || maxSize < 1) {
      throw new RangeError(`Batcher maxSize must be a positive integer, got ${maxSize}`);
    }
    const maxWaitMs = options.maxWaitMs ?? DEFAULT_MAX_WAIT_MS;
    if (!Number.isFinite(maxWaitMs) || maxWaitMs <= 0 || maxWaitMs > MAX_TIMER_DELAY_MS) {
      throw new RangeError(
        `Batcher maxWaitMs must be between 1 and ${MAX_TIMER_DELAY_MS}, got ${maxWaitMs}`,
      );
    }
    this.maxSize = maxSize;
    this.maxWaitMs = maxWaitMs;
    this.onError = options.onError;
  }

  /** How many entries are buffered right now. */
  get size(): number {
    return this.buffer.length;
  }

  /** Whether {@link Batcher.close} has run — `add` throws once closed. */
  get closed(): boolean {
    return this.isClosed;
  }

  /**
   * Buffer one enqueue, typed exactly like {@link Queue.enqueue}. Returns the
   * job ids if this call filled the buffer to `maxSize` (flushing immediately),
   * otherwise an empty array — the timed flush is still pending.
   */
  add(args?: Parameters<TTasks[Name]>, options?: EnqueueOptions): string[] {
    if (this.isClosed) {
      throw new QueueError(`batcher for task "${this.name}" is closed`);
    }
    this.buffer.push({ args, options });
    if (this.buffer.length >= this.maxSize) {
      return this.flush();
    }
    this.arm();
    return [];
  }

  /**
   * Enqueue whatever is buffered now, cancelling any pending timed flush.
   * Returns the new job ids, or an empty array if the buffer was empty. Stays
   * callable after {@link Batcher.close} so a failed final flush can be retried.
   */
  flush(): string[] {
    this.disarm();
    if (this.buffer.length === 0) {
      return [];
    }
    // Take the entries out before enqueueing: `job.enqueued` handlers run
    // synchronously inside `enqueueMany` and may re-enter `add`, so the buffer
    // must already be free of what's in flight.
    const entries = this.buffer.splice(0, this.buffer.length);
    try {
      return this.queue.enqueueMany(this.name, entries);
    } catch (error) {
      // A failed flush keeps its payloads rather than silently dropping them —
      // ahead of anything buffered meanwhile, so submission order holds.
      this.buffer = entries.concat(this.buffer);
      throw error;
    }
  }

  /**
   * Flush the remainder and stop the timer. Returns the final job ids. Idempotent;
   * rethrows if that last flush fails, leaving the entries buffered for a manual
   * {@link Batcher.flush} retry.
   */
  close(): string[] {
    if (this.isClosed) {
      return [];
    }
    this.isClosed = true;
    return this.flush();
  }

  [Symbol.dispose](): void {
    this.close();
  }

  /** Start the timed flush, unless one is already scheduled. */
  private arm(): void {
    if (this.timer) {
      return;
    }
    this.timer = setTimeout(() => {
      this.timer = undefined;
      this.flushOnTimer();
    }, this.maxWaitMs);
    this.timer.unref();
  }

  private disarm(): void {
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = undefined;
    }
  }

  private flushOnTimer(): void {
    try {
      this.flush();
    } catch (error) {
      try {
        if (this.onError) {
          this.onError(error);
        } else {
          log.warn(() => `batched flush for task "${this.name}" failed`, error);
        }
      } catch (reportingError) {
        // A throwing handler must not escape the timer callback — that would be
        // an uncaught exception and would skip the re-arm below.
        log.warn(() => `batcher onError for task "${this.name}" threw`, reportingError);
      } finally {
        // The entries are still buffered — re-arm so a transient failure doesn't
        // strand them until the next `add` or an explicit flush.
        if (!this.isClosed && this.buffer.length > 0) {
          this.arm();
        }
      }
    }
  }
}
