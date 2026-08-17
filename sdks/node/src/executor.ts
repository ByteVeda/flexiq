import { clearSink, type ExecutorSink, installSink } from "./detached";
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
   * Defaults to `$FLEXIQ_ATTACH`.
   */
  attach?: string;
  /** Jobs to run at once. Defaults to `$FLEXIQ_SLOTS`, then 1. */
  slots?: number;
  /** Shared secret, when the scheduler requires one. Defaults to `$FLEXIQ_ATTACH_TOKEN`. */
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
  /** Called once the executor has stopped, so the queue can forget it. */
  onStopped?: () => void;
  tasks: ReadonlyMap<string, RegisteredTask>;
  serializer: Serializer;
  codecs?: ReadonlyMap<string, PayloadCodec>;
  /**
   * The middleware chain for a task, minus `disabled`.
   *
   * Takes the disable list rather than reading one: an executor has no
   * settings store, so the scheduler resolves it and attaches it to each
   * dispatch.
   */
  middlewareFor: (taskName: string, disabled: readonly string[]) => readonly Middleware[];
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
    /** The detached stand-in this attach answers for, and whose sink it holds. */
    private readonly queue: NativeQueue,
    private readonly sink: ExecutorSink,
    private readonly resources: ResourceRuntime,
    private readonly emitter: Emitter,
    private readonly onStopped?: () => void,
  ) {}

  /**
   * Attach and start running jobs. Use {@link Queue.runExecutor} rather than
   * calling this directly.
   *
   * @internal
   */
  static async start(queue: NativeQueue, params: ExecutorStartParams): Promise<Executor> {
    const { tasks, serializer, codecs, middlewareFor, emitter, resources, run, onStopped } = params;

    const address = run?.attach ?? process.env.FLEXIQ_ATTACH;
    if (!address) {
      throw new Error(
        "no scheduler address: pass `attach` or set FLEXIQ_ATTACH (e.g. scheduler:7777)",
      );
    }
    const advertised = [...(run?.tasks ?? tasks.keys())];

    // The executor does not exist yet, and the callback it needs must already
    // be able to reach it — a cancel frame lands in native state that a running
    // handler polls. Resolved through this holder, assigned once the attach
    // succeeds.
    //
    // The native attach starts its job loop before this promise resolves, so
    // the scheduler can dispatch into that window. An invocation waits for the
    // holder to be filled rather than reading it empty, which would run a
    // middleware the dispatch said was disabled and drop the job's progress.
    let attached: NativeExecutor | undefined;
    let markAttached: () => void = () => {};
    const attachedReady = new Promise<void>((resolve) => {
      markAttached = resolve;
    });

    const invoke = createTaskCallback({
      tasks,
      serializer,
      codecs,
      // Every one of these reaches for the executor rather than for storage,
      // which this process deliberately has none of: the scheduler holds the
      // connection and does the work on its behalf.
      middlewareFor: (taskName, jobId) =>
        middlewareFor(taskName, attached?.disabledMiddleware(jobId) ?? []),
      emitter,
      resources,
      queue,
      isCancelled: (jobId) => attached?.isCancelRequested(jobId) ?? false,
      // Overridden rather than left to `queue`, which is only the detached
      // stand-in when this process is a `taskito executor`. An executor started
      // from a process that *does* have storage would otherwise write progress
      // into its own database, where the job it names does not exist — the row
      // belongs to the scheduler. `queue`'s own route to the scheduler is the
      // sink installed below, for app code that calls the queue directly.
      setProgress: (jobId, progress) => attached?.reportProgress(jobId, progress),
      writeTaskLog: (jobId, taskName, level, message, extra) =>
        attached?.writeTaskLog(jobId, taskName, level, message, extra),
    });

    const taskCallback: typeof invoke = async (invocation) => {
      await attachedReady;
      return invoke(invocation);
    };

    const native = await startNativeExecutor(taskCallback, {
      address,
      tasks: advertised,
      slots: run?.slots ?? envInt("FLEXIQ_SLOTS"),
      // Env by preference for the token: in argv it shows up in `ps` output and
      // shell history.
      token: run?.token ?? process.env.FLEXIQ_ATTACH_TOKEN,
      executorId: run?.executorId,
      connectTimeoutMs: run?.connectTimeoutMs,
      heartbeatIntervalMs: run?.heartbeatIntervalMs,
      shutdownDrainMs: run?.shutdownDrainMs,
    });

    attached = native;
    const sink: ExecutorSink = {
      updateProgress: (jobId, progress) => native.reportProgress(jobId, progress),
      writeTaskLog: (jobId, taskName, level, message, extra) =>
        native.writeTaskLog(jobId, taskName, level, message, extra),
    };
    // Before `markAttached`, so the first invocation through the gate already
    // has somewhere to report to. Scoped to `queue`, so a second executor in
    // this process routes its own writes and neither steals the other's.
    installSink(queue, sink);
    if (!native.supportsSideChannel()) {
      log.warn(
        () =>
          `scheduler ${native.schedulerId} applies no progress or task logs on this executor's ` +
          "behalf; they will be dropped. Upgrade the scheduler to keep them.",
      );
    }
    markAttached();
    try {
      // Only lease the resource runtime once the attach actually succeeded, so a
      // refused handshake leaks nothing.
      resources.acquireWorker();
      emitter.emit("worker.started", { workerId: native.executorId });
    } catch (error) {
      // The session is live by now and no caller holds an `Executor` to stop
      // it, so a throwing resource factory or `worker.started` listener would
      // leak the attach until the process exits.
      await native.shutdown().catch((failure) => {
        log.debug(() => "releasing the attach after a failed start failed", failure);
      });
      throw error;
    }

    return new Executor(native, queue, sink, resources, emitter, onStopped);
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
      // The sink frames to a session that is over; leaving it installed would
      // turn a late report into a silent no-op instead of the warning it is.
      // Only this attach's own — another executor may have taken the slot since.
      clearSink(this.queue, this.sink);
      try {
        await this.resources.teardownWorker();
      } catch (error) {
        log.debug(() => "resource release during executor shutdown failed", error);
      }
      this.emitter.emit("worker.stopped", { workerId: this.executorId });
      this.onStopped?.();
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
