import { mkdtempSync } from "node:fs";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import express from "express";
import { afterEach, expect, it } from "vitest";
import { flexiqDashboard, flexiqRouter } from "../../src/contrib/express";
import { Queue, type Worker } from "../../src/index";

let worker: Worker | undefined;
let close: (() => Promise<void>) | undefined;

afterEach(async () => {
  worker?.stop();
  worker = undefined;
  await close?.();
  close = undefined;
});

function newQueue(): Queue {
  return new Queue({ dbPath: join(mkdtempSync(join(tmpdir(), "flexiq-exp-")), "q.db") });
}

/** Mount `configure` on a fresh Express app and return its base URL. */
async function serve(configure: (app: express.Express) => void): Promise<string> {
  const app = express();
  configure(app);
  const server = app.listen(0);
  await new Promise<void>((resolve) => server.once("listening", resolve));
  close = () => new Promise<void>((resolve) => server.close(() => resolve()));
  const { port } = server.address() as AddressInfo;
  return `http://127.0.0.1:${port}`;
}

async function waitFor(
  predicate: () => boolean | Promise<boolean>,
  timeoutMs = 20_000,
): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) {
      return true;
    }
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  return false;
}

it("enqueues, runs, and reports results over the REST router", async () => {
  const queue = newQueue();
  queue.task("add", (a: number, b: number) => a + b);
  const base = await serve((app) => app.use("/tasks", flexiqRouter(queue)));

  const enqueued = await fetch(`${base}/tasks/enqueue`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ task: "add", args: [2, 3] }),
  });
  expect(enqueued.status).toBe(201);
  const { jobId } = (await enqueued.json()) as { jobId: string };
  expect(typeof jobId).toBe("string");

  worker = queue.runWorker();
  expect(await waitFor(async () => (await queue.stats()).completed >= 1)).toBe(true);

  const result = await (await fetch(`${base}/tasks/jobs/${jobId}/result`)).json();
  expect(result).toMatchObject({ jobId, status: "completed", result: 5 });

  const stats = (await (await fetch(`${base}/tasks/stats`)).json()) as { completed: number };
  expect(stats.completed).toBeGreaterThanOrEqual(1);

  const job = (await (await fetch(`${base}/tasks/jobs/${jobId}`)).json()) as Record<
    string,
    unknown
  >;
  expect(job.taskName).toBe("add");
  expect(job).not.toHaveProperty("result"); // raw buffer stripped from the job view
});

it("rejects an enqueue without a task name", async () => {
  const queue = newQueue();
  const base = await serve((app) => app.use("/tasks", flexiqRouter(queue)));
  const res = await fetch(`${base}/tasks/enqueue`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ args: [1] }),
  });
  expect(res.status).toBe(400);
});

it("honors excludeRoutes", async () => {
  const queue = newQueue();
  const base = await serve((app) =>
    app.use("/tasks", flexiqRouter(queue, { excludeRoutes: ["stats"] })),
  );
  expect((await fetch(`${base}/tasks/stats`)).status).toBe(404);
  expect((await fetch(`${base}/tasks/dead-letters`)).status).toBe(200);
});

it("mounts the dashboard JSON API", async () => {
  const queue = newQueue();
  const base = await serve((app) => app.use("/admin", flexiqDashboard(queue)));
  const res = await fetch(`${base}/admin/api/auth/status`);
  expect(res.status).toBe(200);
  expect(await res.json()).toMatchObject({ auth_enabled: false, setup_required: false });
});
