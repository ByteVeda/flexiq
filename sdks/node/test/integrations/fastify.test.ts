import { mkdtempSync } from "node:fs";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import Fastify from "fastify";
import { afterEach, expect, it } from "vitest";
import { flexiqDashboardPlugin, flexiqFastify } from "../../src/contrib/fastify";
import { Queue, type Worker } from "../../src/index";

let worker: Worker | undefined;
let app: ReturnType<typeof Fastify> | undefined;

afterEach(async () => {
  worker?.stop();
  worker = undefined;
  await app?.close();
  app = undefined;
});

function newQueue(): Queue {
  return new Queue({ dbPath: join(mkdtempSync(join(tmpdir(), "flexiq-fastify-")), "q.db") });
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

it("serves the REST API through a prefixed plugin", async () => {
  const queue = newQueue();
  queue.task("add", (a: number, b: number) => a + b);
  app = Fastify();
  await app.register(flexiqFastify, { queue, prefix: "/tasks" });
  await app.ready();

  const enqueued = await app.inject({
    method: "POST",
    url: "/tasks/enqueue",
    payload: { task: "add", args: [4, 5] },
  });
  expect(enqueued.statusCode).toBe(201);
  const { jobId } = enqueued.json() as { jobId: string };

  worker = queue.runWorker();
  expect(await waitFor(async () => (await queue.stats()).completed >= 1)).toBe(true);

  const result = await app.inject({ method: "GET", url: `/tasks/jobs/${jobId}/result` });
  expect(result.json()).toMatchObject({ jobId, status: "completed", result: 9 });

  const stats = await app.inject({ method: "GET", url: "/tasks/stats" });
  expect((stats.json() as { completed: number }).completed).toBeGreaterThanOrEqual(1);
});

it("applies excludeRoutes", async () => {
  const queue = newQueue();
  app = Fastify();
  await app.register(flexiqFastify, { queue, prefix: "/tasks", excludeRoutes: ["stats"] });
  await app.ready();

  expect((await app.inject({ method: "GET", url: "/tasks/stats" })).statusCode).toBe(404);
  expect((await app.inject({ method: "GET", url: "/tasks/dead-letters" })).statusCode).toBe(200);
});

it("mounts the dashboard JSON API under a prefix", async () => {
  const queue = newQueue();
  app = Fastify();
  await app.register(flexiqDashboardPlugin, { queue, prefix: "/admin" });
  await app.listen({ port: 0, host: "127.0.0.1" });
  const { port } = app.server.address() as AddressInfo;

  const res = await fetch(`http://127.0.0.1:${port}/admin/api/auth/status`);
  expect(res.status).toBe(200);
  expect(await res.json()).toMatchObject({ auth_enabled: false, setup_required: false });
});
