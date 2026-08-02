import type { NativeQueue } from "./native";
import { createLogger } from "./utils";

const log = createLogger("executor");

/**
 * Marks this process as an executor, so a {@link Queue} built here opens no
 * storage. Set by `taskito executor` before it imports the app. Internal:
 * applications should not set it.
 */
export const DETACHED_ENV = "TASKITO_DETACHED_EXECUTOR";

/** Whether this process runs task bodies without any storage of its own. */
export function isDetached(): boolean {
  return process.env[DETACHED_ENV] === "1";
}

/**
 * Where an executor's storage-shaped writes go instead of a database.
 *
 * Implemented over the attached executor handle, which frames each call to the
 * scheduler — the side that still holds the connection. Both methods are
 * fire-and-forget: a task reporting progress must not be able to fail, or
 * block, because of what is happening at the far end.
 */
export interface ExecutorSink {
  updateProgress(jobId: string, progress: number): void;
  writeTaskLog(
    jobId: string,
    taskName: string,
    level: string,
    message: string,
    extra?: string,
  ): void;
}

/** Installed by {@link Executor} once it has attached. */
let sink: ExecutorSink | undefined;

/** Route this process's progress and task logs through `next`. @internal */
export function installSink(next: ExecutorSink): void {
  sink = next;
}

/** Stop routing writes, so they degrade to a warning again. @internal */
export function clearSink(): void {
  sink = undefined;
}

/** An executor was asked for something only a database could answer. */
export class DetachedStorageError extends Error {
  constructor(operation: string) {
    super(
      `'${operation}' needs a database, and an attached executor has none. ` +
        "Only running tasks is supported here — the scheduler owns storage. " +
        "Use an in-process worker (`runWorker`) if this app needs to reach the queue itself.",
    );
    this.name = "DetachedStorageError";
  }
}

/**
 * Properties the JavaScript runtime itself probes on any object.
 *
 * They must answer "absent" rather than throwing: `await`ing a value reads
 * `then`, `JSON.stringify` reads `toJSON`, and Node's inspector reads several
 * more. A throwing getter would turn a harmless probe into a crash.
 */
const RUNTIME_PROBES = new Set([
  "then",
  "toJSON",
  "inspect",
  "constructor",
  "valueOf",
  "toString",
  "nodeType",
]);

/**
 * The native queue's job-scoped conveniences, degraded.
 *
 * Reads answer empty, because that is exactly what a queue with no such row
 * returns and callers already handle it. Progress and task logs are forwarded
 * to the installed {@link ExecutorSink}: the executor has no storage, but the
 * scheduler does, and it applies them on this process's behalf. Without a sink
 * — an app imported outside `taskito executor`, or a scheduler that advertised
 * no side-channel — they degrade to one warning rather than throwing, because a
 * task that only wanted to report progress must not fail for running detached.
 */
function degraded(warnOnce: (what: string) => void): Record<string, unknown> {
  return {
    updateProgress(jobId: string, progress: number): void {
      if (!sink) {
        warnOnce("setProgress");
        return;
      }
      sink.updateProgress(jobId, progress);
    },
    writeTaskLog(
      jobId: string,
      taskName: string,
      level: string,
      message: string,
      extra?: string,
    ): void {
      if (!sink) {
        warnOnce("log/publish");
        return;
      }
      sink.writeTaskLog(jobId, taskName, level, message, extra);
    },
    // A cancel reaches an executor as a protocol frame; the executor overrides
    // this check with its own native state (see `Executor.start`).
    isCancelRequested(): boolean {
      return false;
    },
    getSetting(): string | null {
      return null;
    },
    listSettings(): Record<string, string> {
      return {};
    },
  };
}

/**
 * A stand-in for the native queue in an executor.
 *
 * An attached executor exists so the app image needs no database credentials:
 * the scheduler holds the connection and dispatches over a socket. But an
 * executor still imports the user's app module to find its handlers, and that
 * module builds a `Queue` — which would otherwise connect the moment it is
 * constructed, putting the credentials right back in the app image.
 *
 * Everything outside the degraded set throws, because an enqueue that quietly
 * vanished would be worse than one that failed.
 *
 * The same split shows up in the job a handler receives. A dispatch frame
 * carries what running the task needs, so `createdAt`, `scheduledAt`,
 * `priority`, `metadata`, `uniqueKey` and `notes` arrive as zeros and nulls on
 * an executor where an in-process worker would show the stored values. A task
 * that needs them wants a worker, not an executor.
 */
export function createDetachedNative(): NativeQueue {
  const warned = new Set<string>();
  const warnOnce = (what: string): void => {
    // Once per process, not per call: a progress-reporting loop would other-
    // wise bury the log it is trying to be useful in.
    if (!warned.has(what)) {
      warned.add(what);
      log.warn(
        () =>
          `${what} is unavailable on an attached executor with no side-channel to the ` +
          "scheduler; ignoring. Run an in-process worker if you need it.",
      );
    }
  };

  const supported = degraded(warnOnce);
  const proxy = new Proxy(supported, {
    get(target, property): unknown {
      if (typeof property !== "string") {
        return undefined;
      }
      if (property in target) {
        return target[property];
      }
      if (RUNTIME_PROBES.has(property)) {
        return undefined;
      }
      // Returned rather than thrown here: the caller wanted the method, and
      // failing at the call site keeps the stack pointing at their code.
      return () => {
        throw new DetachedStorageError(property);
      };
    },
    has(target, property): boolean {
      return typeof property === "string" && !RUNTIME_PROBES.has(property)
        ? true
        : property in target;
    },
  });

  // The stand-in answers the calls a running task makes and throws on the rest,
  // so a union type would force every storage call site to handle a case only
  // an executor ever sees.
  return proxy as unknown as NativeQueue;
}
