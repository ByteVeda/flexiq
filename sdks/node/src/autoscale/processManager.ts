import { type ChildProcess, spawn } from "node:child_process";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { createLogger } from "../utils";
import type { AutoscaleConfig } from "./config";

const log = createLogger("autoscale");

/** Extra grace on top of the worker's own drain budget before SIGKILL. */
const KILL_ESCALATION_MS = 5_000;

/** How long to wait for a SIGKILLed worker to disappear before giving up on it. */
const KILL_REAP_MS = 5_000;

/**
 * Body of every spawned worker, evaluated by `node --input-type=module -e`.
 *
 * Passing the source inline rather than resolving a shipped entry file keeps
 * the child independent of how this package was built or installed — there is
 * no `dist/` layout to locate from a bundled module. It imports the user's app
 * module and drives it through the public `Queue` API only, so it never has to
 * import taskito itself.
 *
 * `argv[1]` is the app module's file URL and `argv[2]` its run options as JSON;
 * neither is interpolated into the source, so this is not a dynamic-code sink.
 */
const WORKER_BOOTSTRAP = `
const appUrl = process.argv[1];
const app = await import(appUrl);
const queue = app.default ?? app.queue;
if (!queue || typeof queue.runWorker !== "function") {
  throw new Error('module "' + appUrl + '" must export a Queue (default export or queue)');
}
const worker = queue.runWorker(JSON.parse(process.argv[2]));
let stopping = false;
const stop = async () => {
  if (stopping) return;
  stopping = true;
  const teardown = worker.stop();
  await new Promise((done) => setTimeout(done, 200));
  await teardown;
  process.exit(0);
};
process.once("SIGINT", stop);
process.once("SIGTERM", stop);
`;

/** One tracked worker process. */
interface WorkerProcess {
  child: ChildProcess;
  /** Set once we asked it to leave, so its exit isn't reported as a crash. */
  stopping: boolean;
}

/**
 * Spawns, drains, and reaps worker subprocesses by PID.
 *
 * The autoscaler manages independent OS processes rather than threads inside
 * one worker: a Node process is single-threaded, so real parallelism means
 * more processes, and a separate process heartbeats on its own — letting
 * {@link WorkerProcessManager.reapDead} notice and replace a crashed worker.
 */
export class WorkerProcessManager {
  private readonly processes = new Map<number, WorkerProcess>();
  /** PIDs that exited on their own since the last {@link reapDead}. */
  private crashed: number[] = [];

  constructor(private readonly config: AutoscaleConfig) {}

  /**
   * Spawn a worker and return its PID.
   *
   * The child runs in its own process group (`detached`), so a Ctrl-C in the
   * autoscaler's terminal doesn't cascade into workers mid-job — the
   * autoscaler drains them itself. Stdio is inherited so worker logs surface
   * alongside the controller's.
   *
   * @throws Error if the OS refused to spawn the process.
   */
  spawnWorker(): number {
    const runOptions = {
      queues: this.config.queues,
      concurrency: this.config.concurrencyPerWorker,
      batchSize: this.config.batchSize,
    };
    const child = spawn(
      this.config.nodeExecutable,
      [
        ...this.config.nodeArgs,
        "--input-type=module",
        "-e",
        WORKER_BOOTSTRAP,
        pathToFileURL(resolve(this.config.app)).href,
        JSON.stringify(runOptions),
      ],
      { stdio: "inherit", detached: true },
    );
    // `error` fires instead of `exit` when the spawn itself failed (bad
    // executable, EAGAIN), asynchronously and with no pid. It has to be
    // attached before the pid check below: an `error` with no listener is
    // rethrown globally, so a failed spawn would take the autoscaler down.
    child.once("error", (error) => {
      log.error(() => `worker pid=${child.pid ?? "?"} failed to start`, error);
      if (child.pid !== undefined) {
        this.forget(child.pid);
      }
    });
    const pid = child.pid;
    if (pid === undefined) {
      throw new Error(`failed to spawn worker with ${this.config.nodeExecutable}`);
    }
    const record: WorkerProcess = { child, stopping: false };
    this.processes.set(pid, record);
    child.once("exit", (code, signal) => {
      if (!record.stopping) {
        log.warn(() => `worker pid=${pid} exited unexpectedly (code=${code} signal=${signal})`);
      }
      this.forget(pid);
    });
    log.info(() => `spawned worker pid=${pid}`);
    return pid;
  }

  /**
   * Drain one worker: SIGTERM, wait out its drain budget, then SIGKILL.
   *
   * Resolves `true` when SIGTERM was enough, `false` when it had to escalate.
   * The worker stops counting as live the moment it's asked to leave, so the
   * next tick sizes the pool against workers that are actually taking jobs.
   */
  async terminateWorker(pid: number): Promise<boolean> {
    const record = this.processes.get(pid);
    if (!record || record.stopping) {
      return true;
    }
    record.stopping = true;
    const exited = this.waitForExit(record.child);
    record.child.kill("SIGTERM");

    const grace = this.config.drainTimeoutMs + KILL_ESCALATION_MS;
    if (await raceTimeout(exited, grace)) {
      return true;
    }
    log.warn(() => `worker pid=${pid} did not drain within ${grace}ms; SIGKILL`);
    record.child.kill("SIGKILL");
    await raceTimeout(exited, KILL_REAP_MS);
    return false;
  }

  /**
   * PIDs of workers that exited on their own since the last call, cleared as
   * they're returned. Deliberate terminations never appear here — the
   * controller uses this only to replace crashed workers.
   */
  reapDead(): number[] {
    const dead = this.crashed;
    this.crashed = [];
    return dead;
  }

  /** Workers currently taking jobs (draining ones excluded). */
  countLive(): number {
    return this.livePids().length;
  }

  /** PIDs of the workers {@link countLive} counts, oldest first. */
  livePids(): number[] {
    return [...this.processes].filter(([, record]) => !record.stopping).map(([pid]) => pid);
  }

  /** SIGTERM every worker in parallel and wait for them all to drain. */
  async shutdown(): Promise<void> {
    await Promise.all([...this.processes.keys()].map((pid) => this.terminateWorker(pid)));
  }

  /** Hard-kill every worker without draining. For emergency teardown only. */
  killAll(): void {
    for (const [, record] of this.processes) {
      record.stopping = true;
      record.child.kill("SIGKILL");
    }
  }

  /** Resolve once `child` has exited — settled or already gone. */
  private waitForExit(child: ChildProcess): Promise<void> {
    if (child.exitCode !== null || child.signalCode !== null) {
      return Promise.resolve();
    }
    return new Promise((done) => child.once("exit", () => done()));
  }

  /** Drop a process from tracking, recording an unasked-for exit as a crash. */
  private forget(pid: number): void {
    const record = this.processes.get(pid);
    if (!record) {
      return; // already handled — `error` and `exit` can both fire
    }
    this.processes.delete(pid);
    if (!record.stopping) {
      this.crashed.push(pid);
    }
  }
}

/** Resolve `true` if `promise` settled within `timeoutMs`, `false` otherwise. */
async function raceTimeout(promise: Promise<void>, timeoutMs: number): Promise<boolean> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<false>((done) => {
    timer = setTimeout(() => done(false), timeoutMs);
  });
  try {
    return await Promise.race([promise.then(() => true), timeout]);
  } finally {
    clearTimeout(timer);
  }
}

/**
 * Call `onSignal` on the first SIGTERM/SIGINT; returns a function that removes
 * the handlers again so an embedded autoscaler leaves no listeners behind.
 */
export function installSignalHandlers(onSignal: () => void): () => void {
  const handler = (signal: NodeJS.Signals): void => {
    log.info(() => `received ${signal}, draining workers`);
    onSignal();
  };
  process.once("SIGTERM", handler);
  process.once("SIGINT", handler);
  return () => {
    process.off("SIGTERM", handler);
    process.off("SIGINT", handler);
  };
}
