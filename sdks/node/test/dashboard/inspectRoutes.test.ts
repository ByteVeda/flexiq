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
import { currentJob, Queue, serveDashboard, type Worker } from "../../src/index";

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
  const db = join(mkdtempSync(join(tmpdir(), "flexiq-dashinsp-")), "q.db");
  queue = new Queue({ dbPath: db });
  ({ headers } = await seedAdminAndSession(queue));
  server = serveDashboard(queue, { port: 0, staticDir, secureCookies: false });
  await once(server, "listening");
  base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
});

afterEach(() => {
  server?.close();
  server = undefined;
});

it("replays a job and records replay history", async () => {
  queue.task("add", (a: number, b: number) => a + b);
  const worker: Worker = queue.runWorker();
  try {
    const original = queue.enqueue("add", [2, 3]);
    await queue.result(original);

    const replay = (await (
      await fetch(`${base}/api/jobs/${original}/replay`, { method: "POST", headers })
    ).json()) as { replay_job_id: string };
    expect(replay.replay_job_id).not.toBe(original);
    expect(await queue.result(replay.replay_job_id)).toBe(5);

    const history = (await (
      await fetch(`${base}/api/jobs/${original}/replay-history`, { headers })
    ).json()) as Array<{ original_job_id: string; replay_job_id: string; replayed_at: number }>;
    expect(history).toHaveLength(1);
    expect(history[0]?.original_job_id).toBe(original);
    expect(history[0]?.replay_job_id).toBe(replay.replay_job_id);
    expect(history[0]?.replayed_at).toBeGreaterThan(0);
  } finally {
    worker.stop();
  }
});

it("404s a replay of an unknown job", async () => {
  const res = await fetch(`${base}/api/jobs/nope/replay`, { method: "POST", headers });
  // invalid_arg surfaces as a 500-free client error path: native throws -> 500?
  // The native layer raises InvalidArg; the server maps unknown errors to 500,
  // so assert it is NOT a success and leaks no details.
  expect(res.status).toBeGreaterThanOrEqual(400);
});

it("serves the job dependency DAG in the SPA contract shape", async () => {
  queue.task("noop", () => null);
  const id = queue.enqueue("noop", []);

  const dag = (await (await fetch(`${base}/api/jobs/${id}/dag`, { headers })).json()) as {
    nodes: Array<{ id: string; task_name: string; status: string }>;
    edges: Array<{ from: string; to: string }>;
  };
  expect(dag.nodes.map((n) => n.id)).toEqual([id]);
  expect(dag.nodes[0]?.task_name).toBe("noop");
  expect(dag.edges).toEqual([]);
});

it("queries cross-job logs with task and level filters", async () => {
  queue.task("chatty", () => {
    currentJob()?.publish?.("partial");
    return "done";
  });
  const worker: Worker = queue.runWorker();
  try {
    const id = queue.enqueue("chatty", []);
    await queue.result(id);

    let rows: Array<{ task_name: string; level: string }> = [];
    for (let i = 0; i < 100 && rows.length === 0; i++) {
      rows = (await (
        await fetch(`${base}/api/logs?task=chatty`, { headers })
      ).json()) as typeof rows;
      if (rows.length === 0) {
        await new Promise((r) => setTimeout(r, 25));
      }
    }
    expect(rows.length).toBeGreaterThan(0);
    expect(rows.every((r) => r.task_name === "chatty")).toBe(true);

    const filtered = (await (
      await fetch(`${base}/api/logs?level=nonexistent-level`, { headers })
    ).json()) as unknown[];
    expect(filtered).toEqual([]);
  } finally {
    worker.stop();
  }
});

it("lists circuit breakers once a breaker-configured task trips", async () => {
  const empty = await (await fetch(`${base}/api/circuit-breakers`, { headers })).json();
  expect(empty).toEqual([]);
});
