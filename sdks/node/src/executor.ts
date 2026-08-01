import type { Emitter } from "./events";
import type { Middleware } from "./middleware";
import {
  type NativeExecutor,
  type NativeQueue,
  startExecutor as startNativeExecutor,
} from "./native";
import type { ResourceRuntime } from "./resources";
import type { PayloadCodec, Serializer } from "./serializers";
import { createTaskCallback } from "./task-callback";
import type { RegisteredTask } from "./types";
import { createLogger } from "./utils";

const log = createLogger("executor");

/** How an executor attaches. Durations are milliseconds, per Node convention. */
export interface ExecutorRunOptions {
  /**
   * Scheduler address: `host:port`, `:port`, or `unix:/run/taskito.sock`.
   * Defaults to `$TASKITO_ATTACH`.
   */
  attach?: string;
  /** Jobs to run at once. Defaults to `$TASKITO_SLOTS`, then 1. */
  slots?: number;
  /** Shared secret, when the scheduler requires one. Defaults to `$TASKITO_ATTACH_TOKEN`. */
  token?: string;
  /** Identity announced to the scheduler. Defaults to one generated per process. */
  executorId?: string;
  /** How long to wait for the connection (default 10000). */
  connectTimeoutMs?: number;
  /** How often to send a liveness heartbeat (default 5000). */
  heartbeatIntervalMs?: number;
  /** How long a drain waits for in-flight jobs before disconnecting (default 30000). */
  shutdownDrainMs?: number;
  /** Only advertise these tasks. Defaults to every registered task. */
  tasks?: readonly string[];
}

/** Inputs assembled by {@link Queue.runExecutor}. @internal */
export interface ExecutorStartParams {
  tasks: ReadonlyMap<string, RegisteredTask>;
  serializer: Serializer;
  codecs?: ReadonlyMap<string, PayloadCodec>;
  middlewareFor: (taskName: string) => readonly Middleware[];
  emitter: Emitter;
  resources: ResourceRuntime;
  run?: ExecutorRunOptions;
}

/**
 * A running attachment to a detached scheduler.
 *
 * The inverse of a {@link Worker}: instead of polling storage for work, it
 * dials a scheduler that already holds the database connection and runs
 * whatever it is sent. Task execution is identical — same middleware, codecs,
 * resources and cancel signal — because only the transport differs.
 */
export class Executor {
  private stopped?: Promise<void>;

  private constructor(
    private readonly native: NativeExecutor,
    private readonly resources: ResourceRuntime,
    private readonly emitter: Emitter,
  ) {}

  /**
   * Attach and start running jobs. Use {@link Queue.runExecutor} rather than
   * calling this directly.
   *
   * @internal
   */
  static async start(queue: NativeQueue, params: ExecutorStartParams): Promise<Executor> {
    const { tasks, serializer, codecs, middlewareFor, emitter, resources, run } = params;

    const address = run?.attach ?? process.env.TASKITO_ATTACH;
    if (!address) {
      throw new Error(
        "no scheduler address: pass `attach` or set TASKITO_ATTACH (e.g. scheduler:7749)",
      );
    }
    const advertised = [...(run?.tasks ?? tasks.keys())];

    // The executor does not exist yet, and the callback it needs must already
    // be able to reach it — a cancel frame lands in native state that a running
    // handler polls. Resolved through this holder, assigned once the attach
    // succeeds; until then nothing is running, so nothing can be cancelled.
    let attached: NativeExecutor | undefined;

    const taskCallback = createTaskCallback({
      tasks,
      serializer,
      codecs,
      middlewareFor,
      emitter,
      resources,
      queue,
      isCancelled: (jobId) => attached?.isCancelRequested(jobId) ?? false,
    });

    const native = await startNativeExecutor(taskCallback, {
      address,
      tasks: advertised,
      slots: run?.slots ?? envInt("TASKITO_SLOTS"),
      // Env by preference for the token: in argv it shows up in `ps` output and
      // shell history.
      token: run?.token ?? process.env.TASKITO_ATTACH_TOKEN,
      executorId: run?.executorId,
      connectTimeoutMs: run?.connectTimeoutMs,
      heartbeatIntervalMs: run?.heartbeatIntervalMs,
      shutdownDrainMs: run?.shutdownDrainMs,
    });

    attached = native;
    // Only lease the resource runtime once the attach actually succeeded, so a
    // refused handshake leaks nothing.
    resources.acquireWorker();
    emitter.emit("worker.started", { workerId: native.executorId });

    return new Executor(native, resources, emitter);
  }

  /** Identity the scheduler announced when it accepted this attach. */
  get schedulerId(): string {
    return this.native.schedulerId;
  }

  /** Identity this executor attached under. */
  get executorId(): string {
    return this.native.executorId;
  }

  /** Peer label of the scheduler connection. */
  get peer(): string {
    return this.native.peer;
  }

  /** Whether the scheduler session is still open. */
  get running(): boolean {
    return this.native.isRunning();
  }

  /**
   * Resolve once the scheduler ends the session — a shutdown frame, or the
   * connection dropping. Does not drain; call {@link Executor.stop} for that.
   */
  wait(): Promise<void> {
    return this.native.wait();
  }

  /**
   * Drain in-flight work, disconnect, and release worker-scoped resources.
   *
   * Idempotent, and memoized like {@link Worker.stop} so a signal handler
   * racing a scheduler shutdown does not tear down twice.
   */
  stop(): Promise<void> {
    this.stopped ??= this.teardown();
    return this.stopped;
  }

  private async teardown(): Promise<void> {
    try {
      await this.native.shutdown();
    } finally {
      try {
        await this.resources.teardownWorker();
      } catch (error) {
        log.debug(() => "resource release during executor shutdown failed", error);
      }
      this.emitter.emit("worker.stopped", { workerId: this.executorId });
    }
  }
}

/** Read a positive integer from the environment, or `undefined` when unusable. */
function envInt(name: string): number | undefined {
  const raw = process.env[name];
  if (raw === undefined || raw === "") {
    return undefined;
  }
  const value = Number(raw);
  if (!Number.isInteger(value) || value < 1) {
    throw new RangeError(`${name} must be a positive integer, got "${raw}"`);
  }
  return value;
}
