// The module-global pending registry behind `task()`.
//
// `queue.task()` binds a handler to a Queue instance, so a task module has to
// import the module that constructs the queue — and the queue module wants to
// import the task modules back. `task()` breaks that cycle: it records the
// handler and its options here, and a Queue claims them later.
//
// This module reaches `Queue` only through `import type`, which
// `verbatimModuleSyntax` erases. Both sides reach the registry, so the registry
// must not reach back.

import { DebounceOptions } from "./debounce";
import { DuplicateTaskError, TaskNotBoundError } from "./errors";
import type { Queue } from "./queue";
import type { AnyHandler, EnqueueOptions, TaskOptions } from "./types";

/**
 * The handle {@link task} returns: the declared handler, callable as itself, plus
 * the enqueue path a queue lends it once it drains the registry.
 */
export interface DeferredTask<H extends AnyHandler = AnyHandler> {
  /** Run the handler in this process — the plain function call, unchanged. */
  (...args: Parameters<H>): ReturnType<H>;
  /** The name the task registered under. */
  readonly name: string;
  /** The handler as declared, before any queue wrapped it. */
  readonly handler: H;
  /** The options the declaration asked for, replayed verbatim at drain. */
  readonly options?: TaskOptions;
  /** Whether a queue has claimed this declaration. */
  readonly bound: boolean;
  /**
   * Enqueue this task on the queue that drained it most recently — the same call
   * as `queue.enqueue(name, args, options)`, without needing the queue in scope.
   *
   * @throws {TaskNotBoundError} No queue has drained the pending registry yet.
   */
  enqueue(args?: Parameters<H>, options?: EnqueueOptions): string;
}

/** One {@link task} declaration awaiting (or already claimed by) a queue. */
export interface PendingTask {
  readonly name: string;
  readonly handler: AnyHandler;
  readonly options?: TaskOptions;
  /** The handle handed back to the declaring module. */
  readonly handle: DeferredTask;
  /** The queue that drained this entry most recently; unset until one does. */
  queue?: Queue;
}

/**
 * A class rather than an object literal so the handle can close over the entry
 * that owns it: `makeHandle` reads nothing off `this` at construction, only
 * later, from the callbacks it installs.
 */
class PendingEntry implements PendingTask {
  readonly handle: DeferredTask;
  queue?: Queue;

  constructor(
    readonly name: string,
    readonly handler: AnyHandler,
    readonly options?: TaskOptions,
  ) {
    this.handle = makeHandle(this);
  }
}

const PENDING = new Map<string, PendingTask>();

/** Build the callable handle for a pending entry, closing over its live binding. */
function makeHandle(entry: PendingTask): DeferredTask {
  const call = (...args: unknown[]): unknown => entry.handler(...args);
  return Object.defineProperties(call, {
    // `Function.name` is configurable, so the handle can report the name it
    // registered under instead of the arrow function's.
    name: { value: entry.name, configurable: true },
    handler: { value: entry.handler, enumerable: true },
    options: { value: entry.options, enumerable: true },
    bound: { get: () => entry.queue !== undefined, enumerable: true },
    enqueue: {
      value: (args?: unknown[], options?: EnqueueOptions): string => {
        if (!entry.queue) {
          throw new TaskNotBoundError(entry.name);
        }
        return entry.queue.enqueue(entry.name, args, options);
      },
      enumerable: true,
    },
  }) as unknown as DeferredTask;
}

/**
 * Register a task without a queue.
 *
 * Takes everything {@link Queue.task} takes; the options are replayed verbatim
 * when a queue drains the registry, so live values (a `retryOn` predicate, a
 * codec list) survive the deferral.
 *
 * ```ts
 * import { task } from "@byteveda/flexiq";
 *
 * export const sendInvoice = task("invoices.send", (userId: string) => {
 *   // ...
 * }, { rateLimit: "10/s" });
 * ```
 *
 * A `Queue` claims the declaration at construction, at {@link Queue.discover}, or
 * at worker start. Enqueueing through the handle before then throws
 * {@link TaskNotBoundError}.
 *
 * @throws {DuplicateTaskError} Another declaration already claimed this name.
 */
export function task<H extends AnyHandler>(
  name: string,
  handler: H,
  options?: TaskOptions,
): DeferredTask<H> {
  if (PENDING.has(name)) {
    throw new DuplicateTaskError(name, "another module-level task() declaration");
  }
  // Validated here rather than at drain, for the reason `queue.task` validates
  // at registration: an unbounded window or an unkeyed one is a configuration
  // mistake, and for a deferred task the declaration *is* the registration. The
  // drain rebuilds the policy from the same options.
  DebounceOptions.from(`task "${name}"`, options ?? {});
  const entry = new PendingEntry(name, handler, options);
  PENDING.set(name, entry);
  // The handle is built untyped — only this signature knows `H`, and the cast is
  // where the declaration's argument types are attached to it.
  return entry.handle as DeferredTask<H>;
}

/**
 * Every declaration awaiting (or already claimed by) a queue, in declaration
 * order.
 *
 * The registry is never drained destructively: a second `Queue` in the same
 * process gets the same tasks, and re-running discovery is a no-op rather than a
 * conflict.
 */
export function pendingTasks(): PendingTask[] {
  return [...PENDING.values()];
}

/** Drop every pending declaration. Intended for tests. */
export function clearPendingTasks(): void {
  PENDING.clear();
}
