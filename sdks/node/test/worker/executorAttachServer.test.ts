/**
 * The same attach assertions, against the real `taskito-server` binary.
 *
 * Gated on `TASKITO_SERVER_BIN` so the default suite needs no Rust build. Its
 * job is to keep `executorAttach.test.ts`'s hand-rolled scheduler honest: that
 * file proves the executor speaks the protocol it was told to, this one proves
 * the protocol it was told to is the one the server actually speaks.
 *
 *   cargo build -p taskito-server
 *   TASKITO_SERVER_BIN=../../target/debug/taskito-server npx vitest run
 */

import { type ChildProcess, spawn } from "node:child_process";
import { mkdtempSync } from "node:fs";
import { createConnection, createServer } from "node:net";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { type Executor, Queue } from "../../src/index";

const SERVER_BIN = process.env.TASKITO_SERVER_BIN;
const SETTLE_MS = 60_000;

let executor: Executor | undefined;
let server: ChildProcess | undefined;

afterEach(async () => {
  await executor?.stop();
  executor = undefined;
  server?.kill("SIGTERM");
  server = undefined;
});

function sleep(ms: number): Promise<void> {
  return new Promise((done) => setTimeout(done, ms));
}

async function freePort(): Promise<number> {
  const probe = createServer();
  await new Promise<void>((done) => probe.listen(0, "127.0.0.1", done));
  const address = probe.address();
  if (address === null || typeof address === "string") {
    throw new Error("expected a TCP address");
  }
  const { port } = address;
  await new Promise<void>((done) => probe.close(() => done()));
  return port;
}

/** Start a real scheduler over a temp SQLite database. */
async function startScheduler(options?: { token?: string }): Promise<{
  port: number;
  dbPath: string;
}> {
  const dbPath = join(mkdtempSync(join(tmpdir(), "taskito-server-")), "server.db");
  const port = await freePort();

  const env: NodeJS.ProcessEnv = {
    ...process.env,
    TASKITO_BACKEND: "sqlite",
    TASKITO_DSN: dbPath,
    TASKITO_LISTEN: `127.0.0.1:${port}`,
  };
  // Unset, not "off": the dashboard is disabled by having no bind address.
  delete env.TASKITO_DASHBOARD;
  delete env.TASKITO_ATTACH_TOKEN;
  if (options?.token) {
    env.TASKITO_ATTACH_TOKEN = options.token;
  }

  // Discarded, not piped: nothing reads these, and a scheduler that logs per
  // job fills the pipe buffer and then blocks on its next write.
  server = spawn(resolve(SERVER_BIN as string), { env, stdio: "ignore" });
  await waitForPort(port);
  return { port, dbPath };
}

async function waitForPort(port: number): Promise<void> {
  const deadline = Date.now() + SETTLE_MS;
  while (Date.now() < deadline) {
    const open = await new Promise<boolean>((done) => {
      const socket = createConnection({ port, host: "127.0.0.1" });
      socket.once("connect", () => {
        socket.destroy();
        done(true);
      });
      socket.once("error", () => done(false));
    });
    if (open) {
      return;
    }
    await sleep(50);
  }
  throw new Error(`the server never bound port ${port}`);
}

async function waitFor(predicate: () => Promise<boolean>, what: string): Promise<void> {
  const deadline = Date.now() + SETTLE_MS;
  while (Date.now() < deadline) {
    if (await predicate()) {
      return;
    }
    await sleep(50);
  }
  throw new Error(what);
}

describe.skipIf(!SERVER_BIN)("against a real taskito-server", () => {
  it("runs a job the real scheduler dispatched", async () => {
    const { port, dbPath } = await startScheduler();
    const queue = new Queue({ dbPath });
    queue.task("echo", (value: string) => `echo:${value}`);

    executor = await queue.runExecutor({ attach: `127.0.0.1:${port}` });
    // The scheduler starts on the first attach, so enqueue after attaching.
    await sleep(1000);
    const jobId = queue.enqueue("echo", ["hello"]);

    await waitFor(
      async () => queue.getJob(String(jobId))?.status === "complete",
      "the job never completed on the attached executor",
    );
  }, 120_000);

  it("retries a failure through the real scheduler", async () => {
    const { port, dbPath } = await startScheduler();
    const queue = new Queue({ dbPath });
    queue.task("boom", () => {
      throw new Error("deliberate failure");
    });

    executor = await queue.runExecutor({ attach: `127.0.0.1:${port}` });
    await sleep(1000);
    const jobId = queue.enqueue("boom", []);

    // The error reaching storage proves the executor's failure crossed the wire
    // and the scheduler applied it.
    await waitFor(
      async () => Boolean(queue.getJob(String(jobId))?.error?.includes("deliberate failure")),
      "the failure never reached storage",
    );
  }, 120_000);

  it("refuses an attach with the wrong token", async () => {
    const { port, dbPath } = await startScheduler({ token: "correct-token-0123456789" });
    const queue = new Queue({ dbPath });
    queue.task("echo", (value: string) => value);

    await expect(
      queue.runExecutor({ attach: `127.0.0.1:${port}`, token: "wrong-token-0123456789" }),
    ).rejects.toThrow(/refused|token/i);
  }, 120_000);

  it("drains in-flight work when stopped", async () => {
    const { port, dbPath } = await startScheduler();
    const queue = new Queue({ dbPath });
    let release = (): void => {};
    const released = new Promise<void>((done) => {
      release = done;
    });
    let started = false;
    queue.task("slow", async () => {
      started = true;
      await released;
      return "done";
    });

    const running = await queue.runExecutor({ attach: `127.0.0.1:${port}` });
    await sleep(1000);
    const jobId = queue.enqueue("slow", []);
    await waitFor(async () => started, "the job never started");

    const stopping = running.stop();
    release();
    await stopping;

    await waitFor(
      async () => queue.getJob(String(jobId))?.status === "complete",
      "in-flight work was not drained before disconnecting",
    );
  }, 120_000);
});
