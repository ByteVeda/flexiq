import { execSync } from "node:child_process";
import { once } from "node:events";
import { existsSync, mkdtempSync } from "node:fs";
import type { Server } from "node:http";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { seedAdminAndSession } from "../../src/dashboard/testing";
import { Queue, serveDashboard, type Worker } from "../../src/index";
import type { Middleware } from "../../src/middleware";

const pkgRoot = fileURLToPath(new URL("../..", import.meta.url));
const staticDir = join(pkgRoot, "static", "dashboard");

beforeAll(() => {
  if (!existsSync(join(staticDir, "index.html"))) {
    execSync("pnpm run build:dashboard", { cwd: pkgRoot, stdio: "ignore" });
  }
}, 120_000);

let server: Server | undefined;
let queue: Queue;
let base = "";
let headers: Record<string, string> = {};

beforeEach(async () => {
  const db = join(mkdtempSync(join(tmpdir(), "flexiq-dashovr-")), "q.db");
  queue = new Queue({ dbPath: db });
  queue.task("add", (a: number, b: number) => a + b, { maxRetries: 2, rateLimit: "100/m" });
  ({ headers } = await seedAdminAndSession(queue));
  server = serveDashboard(queue, { port: 0, staticDir, secureCookies: false });
  await once(server, "listening");
  base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
});

afterEach(() => {
  server?.close();
  server = undefined;
});

const put = (path: string, body: unknown) =>
  fetch(`${base}${path}`, {
    method: "PUT",
    headers: { ...headers, "content-type": "application/json" },
    body: JSON.stringify(body),
  });

describe("task overrides", () => {
  it("lists registered tasks with defaults, override, and effective values", async () => {
    await put("/api/tasks/add/override", { max_retries: 7, paused: true });

    const tasks = (await (await fetch(`${base}/api/tasks`, { headers })).json()) as Array<{
      name: string;
      defaults: { max_retries: number; rate_limit: string };
      override: { max_retries: number; paused: boolean };
      effective: { max_retries: number; rate_limit: string };
      paused: boolean;
    }>;
    const add = tasks.find((t) => t.name === "add");
    expect(add?.defaults.max_retries).toBe(2);
    expect(add?.override.max_retries).toBe(7);
    expect(add?.effective.max_retries).toBe(7);
    expect(add?.effective.rate_limit).toBe("100/m"); // default survives
    expect(add?.paused).toBe(true);
  });

  it("merges updates, clears fields with null, and round-trips GET", async () => {
    await put("/api/tasks/add/override", { max_retries: 5, rate_limit: "10/s" });
    await put("/api/tasks/add/override", { rate_limit: null });

    const got = (await (await fetch(`${base}/api/tasks/add/override`, { headers })).json()) as {
      max_retries: number;
      rate_limit: string | null;
    };
    expect(got.max_retries).toBe(5);
    expect(got.rate_limit).toBeNull();
  });

  it("rejects unknown fields and bad values", async () => {
    expect((await put("/api/tasks/add/override", { bogus: 1 })).status).toBe(400);
    expect((await put("/api/tasks/add/override", { rate_limit: "nope" })).status).toBe(400);
    expect((await put("/api/tasks/add/override", { max_concurrent: -1 })).status).toBe(400);
  });

  it("clears an override and 404s when none exists", async () => {
    await put("/api/tasks/add/override", { max_retries: 9 });
    const del = (await (
      await fetch(`${base}/api/tasks/add/override`, { method: "DELETE", headers })
    ).json()) as { cleared: boolean };
    expect(del.cleared).toBe(true);
    expect((await fetch(`${base}/api/tasks/add/override`, { headers })).status).toBe(404);
  });
});

describe("queue overrides", () => {
  it("propagates paused immediately and lists queues", async () => {
    await put("/api/queues/emails/override", { paused: true, max_concurrent: 3 });
    expect(queue.listPausedQueues()).toContain("emails");

    const queues = (await (await fetch(`${base}/api/queues`, { headers })).json()) as Array<{
      name: string;
      paused: boolean;
      effective: { max_concurrent?: number };
    }>;
    const emails = queues.find((q) => q.name === "emails");
    expect(emails?.paused).toBe(true);
    expect(emails?.effective.max_concurrent).toBe(3);

    await put("/api/queues/emails/override", { paused: false });
    expect(queue.listPausedQueues()).not.toContain("emails");
  });
});

describe("middleware toggles", () => {
  class Recorder implements Middleware {
    calls: string[] = [];
    before(ctx: { taskName: string }): void {
      this.calls.push(ctx.taskName);
    }
  }

  it("lists middleware and reflects per-task disables in the chain view", async () => {
    queue.use(new Recorder());
    const list = (await (await fetch(`${base}/api/middleware`, { headers })).json()) as Array<{
      name: string;
      scopes: Array<{ kind: string }>;
    }>;
    expect(list.map((m) => m.name)).toContain("Recorder");

    const res = await put("/api/tasks/add/middleware/Recorder", { enabled: false });
    expect(res.status).toBe(200);
    expect(((await res.json()) as { disabled: string[] }).disabled).toEqual(["Recorder"]);

    const chain = (await (await fetch(`${base}/api/tasks/add/middleware`, { headers })).json()) as {
      middleware: Array<{ name: string; disabled: boolean; effective: boolean }>;
    };
    const entry = chain.middleware.find((m) => m.name === "Recorder");
    expect(entry?.disabled).toBe(true);
    expect(entry?.effective).toBe(false);
  });

  it("404s a toggle for an unregistered middleware", async () => {
    expect((await put("/api/tasks/add/middleware/Ghost", { enabled: false })).status).toBe(404);
  });

  it("skips disabled middleware at execution time without a restart", async () => {
    const recorder = new Recorder();
    queue.use(recorder);
    const worker: Worker = queue.runWorker();
    try {
      const first = queue.enqueue("add", [1, 1]);
      await queue.result(first);
      expect(recorder.calls).toEqual(["add"]);

      queue.disableMiddlewareForTask("add", "Recorder");
      const second = queue.enqueue("add", [2, 2]);
      await queue.result(second);
      expect(recorder.calls).toEqual(["add"]); // unchanged — hook skipped

      queue.enableMiddlewareForTask("add", "Recorder");
      const third = queue.enqueue("add", [3, 3]);
      await queue.result(third);
      expect(recorder.calls).toEqual(["add", "add"]);
    } finally {
      worker.stop();
    }
  });
});
