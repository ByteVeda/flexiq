import { execSync } from "node:child_process";
import { once } from "node:events";
import { existsSync, mkdtempSync } from "node:fs";
import type { Server } from "node:http";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, beforeAll, beforeEach, expect, it } from "vitest";
import { seedAdminAndSession } from "../../src/dashboard/testing";
import { Queue, serveDashboard, type Worker } from "../../src/index";

const pkgRoot = fileURLToPath(new URL("../..", import.meta.url));
const staticDir = join(pkgRoot, "static", "dashboard");

beforeAll(() => {
  if (!existsSync(join(staticDir, "index.html"))) {
    execSync("pnpm run build:dashboard", { cwd: pkgRoot, stdio: "ignore" });
  }
}, 120_000);

/** Start a session-mode dashboard with a seeded admin session. */
async function startDash(queue: Queue): Promise<{
  base: string;
  headers: Record<string, string>;
  server: Server;
}> {
  const { headers } = await seedAdminAndSession(queue);
  const server = serveDashboard(queue, {
    port: 0,
    staticDir,
    secureCookies: false,
    authEnabled: true,
  });
  await once(server, "listening");
  const address = server.address() as AddressInfo;
  return { base: `http://127.0.0.1:${address.port}`, headers, server };
}

let server: Server | undefined;
let base = "";
let headers: Record<string, string> = {};

beforeEach(async () => {
  const db = join(mkdtempSync(join(tmpdir(), "flexiq-dash-")), "q.db");
  const queue = new Queue({ dbPath: db });
  queue.task("add", (a: number, b: number) => a + b);
  queue.enqueue("add", [1, 2]);
  queue.pauseQueue("emails");
  ({ base, headers, server } = await startDash(queue));
});

afterEach(() => {
  server?.close();
  server = undefined;
});

it("serves queue stats", async () => {
  const stats = (await (await fetch(`${base}/api/stats`, { headers })).json()) as {
    pending: number;
  };
  expect(stats.pending).toBe(1);
});

it("serves jobs in the snake_case SPA contract", async () => {
  const jobs = (await (await fetch(`${base}/api/jobs`, { headers })).json()) as Array<{
    task_name: string;
    created_at: number;
  }>;
  expect(jobs[0]?.task_name).toBe("add");
  expect(typeof jobs[0]?.created_at).toBe("number");
});

it("exposes paused queues", async () => {
  const paused = await (await fetch(`${base}/api/queues/paused`, { headers })).json();
  expect(paused).toContain("emails");
});

it("answers whoami for the seeded session", async () => {
  const status = (await (await fetch(`${base}/api/auth/status`)).json()) as {
    setup_required: boolean;
  };
  expect(status.setup_required).toBe(false);
  const who = (await (await fetch(`${base}/api/auth/whoami`, { headers })).json()) as {
    user: { username: string; role: string };
    csrf_token: string;
  };
  expect(who.user.username).toBe("admin");
  expect(who.user.role).toBe("admin");
  expect(who.csrf_token.length).toBeGreaterThan(10);
});

it("serves the SPA shell with deep-link fallback", async () => {
  expect((await fetch(`${base}/`)).status).toBe(200);
  expect((await fetch(`${base}/jobs`)).status).toBe(200);
  expect((await fetch(`${base}/assets/missing-xyz.js`)).status).toBe(404);
});

it("creates and lists webhooks via the dashboard api", async () => {
  const created = (await (
    await fetch(`${base}/api/webhooks`, {
      method: "POST",
      headers: { ...headers, "content-type": "application/json" },
      body: JSON.stringify({ url: "http://example.com/h", events: ["job.completed"], secret: "x" }),
    })
  ).json()) as { id: string; has_secret: boolean; secret: string | null };
  expect(created.has_secret).toBe(true);
  expect(created.secret).toBe("x");

  const list = (await (await fetch(`${base}/api/webhooks`, { headers })).json()) as Array<{
    id: string;
    secret?: unknown;
  }>;
  expect(list.map((w) => w.id)).toContain(created.id);
  expect(list[0]?.secret).toBeUndefined();

  const eventTypes = (await (
    await fetch(`${base}/api/event-types`, { headers })
  ).json()) as string[];
  expect(eventTypes).toContain("job.completed");
});

it("lists a running worker", async () => {
  const db = join(mkdtempSync(join(tmpdir(), "flexiq-dash-")), "q.db");
  const queue = new Queue({ dbPath: db });
  queue.task("noop", () => null);
  const worker: Worker = queue.runWorker({ queues: ["default"] });
  const dash = await startDash(queue);

  try {
    const workers = (await (
      await fetch(`${dash.base}/api/workers`, { headers: dash.headers })
    ).json()) as Array<{
      pool_type: string;
      worker_id: string;
    }>;
    expect(workers.length).toBeGreaterThanOrEqual(1);
    expect(workers[0]?.pool_type).toBe("node");
    expect(typeof workers[0]?.worker_id).toBe("string");
  } finally {
    worker.stop();
    dash.server.close();
  }
});

it("aggregates metrics after a job completes", async () => {
  const db = join(mkdtempSync(join(tmpdir(), "flexiq-dash-")), "q.db");
  const queue = new Queue({ dbPath: db });
  queue.task("add", (a: number, b: number) => a + b);
  const id = queue.enqueue("add", [2, 3]);
  const worker: Worker = queue.runWorker();
  const dash = await startDash(queue);

  try {
    await queue.result(id);
    let metrics: Record<string, { count: number; success_count: number }> = {};
    for (let i = 0; i < 60 && metrics.add === undefined; i++) {
      metrics = (await (
        await fetch(`${dash.base}/api/metrics?since=3600`, { headers: dash.headers })
      ).json()) as Record<string, { count: number; success_count: number }>;
      if (metrics.add === undefined) {
        await new Promise((resolve) => setTimeout(resolve, 25));
      }
    }
    expect(metrics.add?.count).toBeGreaterThanOrEqual(1);
    expect(metrics.add?.success_count).toBeGreaterThanOrEqual(1);
  } finally {
    worker.stop();
    dash.server.close();
  }
});

it("serves workflow runs, detail, dag, and children", async () => {
  const db = join(mkdtempSync(join(tmpdir(), "flexiq-dash-")), "q.db");
  const queue = new Queue({ dbPath: db });
  queue.task("a", () => 1);
  queue.task("b", () => 2);
  const handle = queue.workflows
    .define("etl")
    .step("a", "a")
    .step("b", "b", { after: "a" })
    .submit();
  const worker: Worker = queue.runWorker();
  const dash = await startDash(queue);
  const root = `${dash.base}/api/workflows/runs`;
  const headers_ = dash.headers;

  try {
    await handle.wait({ timeoutMs: 8000 });

    const list = (await (await fetch(root, { headers: headers_ })).json()) as {
      runs: Array<{ id: string; state: string }>;
    };
    expect(list.runs.find((r) => r.id === handle.runId)?.state).toBe("completed");

    const detail = (await (
      await fetch(`${root}/${handle.runId}`, { headers: headers_ })
    ).json()) as {
      run: { state: string };
      nodes: Array<{ node_name: string; status: string }>;
    };
    expect(detail.run.state).toBe("completed");
    expect(detail.nodes.map((n) => n.node_name).sort()).toEqual(["a", "b"]);

    const dag = (await (
      await fetch(`${root}/${handle.runId}/dag`, { headers: headers_ })
    ).json()) as { dag: string };
    const parsed = JSON.parse(dag.dag) as {
      edges: Array<{ from: string; to: string; weight: number }>;
      nodes: Array<{ name: string; node_name: string; status: string; id: string; deps: string[] }>;
    };
    expect(parsed.edges).toEqual([{ from: "a", to: "b", weight: 1.0 }]);
    // The SPA's DAG visualizer reads per-node deps / status / id, so the handler
    // enriches the raw graph with live status, edges-as-deps, and job-id links.
    const b = parsed.nodes.find((n) => n.name === "b");
    expect(b?.deps).toEqual(["a"]);
    expect(b?.status).toBe("completed");
    expect(b?.id).not.toBe("b"); // resolved to the job id, not the node name
    expect(parsed.nodes.find((n) => n.name === "a")?.deps).toEqual([]);

    const children = (await (
      await fetch(`${root}/${handle.runId}/children`, { headers: headers_ })
    ).json()) as {
      children: unknown[];
    };
    expect(children.children).toEqual([]);
  } finally {
    worker.stop();
    dash.server.close();
  }
});
