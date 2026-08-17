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
import { Queue, serveDashboard } from "../../src/index";

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
  const db = join(mkdtempSync(join(tmpdir(), "taskito-dashops-")), "q.db");
  queue = new Queue({ dbPath: db });
  queue.task("add", (a: number, b: number) => a + b);
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

describe("settings api", () => {
  it("round-trips a setting and lists it", async () => {
    const created = await put("/api/settings/branding.title", { value: "Ops" });
    expect(created.status).toBe(200);
    expect(await created.json()).toEqual({ key: "branding.title", value: "Ops" });

    const got = (await (
      await fetch(`${base}/api/settings/branding.title`, { headers })
    ).json()) as {
      value: string;
    };
    expect(got.value).toBe("Ops");

    const all = (await (await fetch(`${base}/api/settings`, { headers })).json()) as Record<
      string,
      string
    >;
    expect(all["branding.title"]).toBe("Ops");
  });

  it("json-encodes non-string values", async () => {
    await put("/api/settings/links", { value: [{ label: "Docs" }] });
    const got = (await (await fetch(`${base}/api/settings/links`, { headers })).json()) as {
      value: string;
    };
    expect(JSON.parse(got.value)).toEqual([{ label: "Docs" }]);
  });

  it("hides and refuses protected namespaces", async () => {
    const all = (await (await fetch(`${base}/api/settings`, { headers })).json()) as Record<
      string,
      string
    >;
    expect(Object.keys(all).some((k) => k.startsWith("auth:"))).toBe(false);

    expect((await fetch(`${base}/api/settings/auth:users`, { headers })).status).toBe(404);
    expect((await put("/api/settings/auth:users", { value: "{}" })).status).toBe(400);
    expect(
      (await fetch(`${base}/api/settings/auth:users`, { method: "DELETE", headers })).status,
    ).toBe(404);

    // The published retention document is a report, not an editable row.
    expect((await put("/api/settings/retention:effective:default", { value: "{}" })).status).toBe(
      400,
    );
  });

  it("deletes settings and 404s unknown keys", async () => {
    await put("/api/settings/tmp", { value: "x" });
    const del = (await (
      await fetch(`${base}/api/settings/tmp`, { method: "DELETE", headers })
    ).json()) as { deleted: boolean };
    expect(del.deleted).toBe(true);
    expect((await fetch(`${base}/api/settings/tmp`, { headers })).status).toBe(404);
  });
});

describe("ops endpoints", () => {
  it("serves a public health probe", async () => {
    const res = await fetch(`${base}/health`);
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ status: "ok" });
  });

  it("serves readiness with storage and worker checks", async () => {
    const body = (await (await fetch(`${base}/readiness`)).json()) as {
      status: string;
      checks: { storage: string; workers: { status: string } };
    };
    expect(body.checks.storage).toBe("ok");
    expect(["ready", "degraded"]).toContain(body.status);
  });

  it("gates readiness behind the metrics token when set", async () => {
    process.env.FLEXIQ_DASHBOARD_METRICS_TOKEN = "probe-secret";
    try {
      expect((await fetch(`${base}/readiness`)).status).toBe(401);
      const ok = await fetch(`${base}/readiness`, {
        headers: { authorization: "Bearer probe-secret" },
      });
      expect(ok.status).toBe(200);
    } finally {
      delete process.env.FLEXIQ_DASHBOARD_METRICS_TOKEN;
    }
  });

  it("serves the KEDA scaler payload", async () => {
    queue.enqueue("add", [1, 2]);
    const body = (await (await fetch(`${base}/api/scaler`, { headers })).json()) as {
      metricName: string;
      metricValue: number;
      isActive: boolean;
      perQueue: Record<string, { pending: number }>;
    };
    expect(body.metricName).toBe("taskito_queue_depth");
    expect(body.metricValue).toBe(1);
    expect(body.isActive).toBe(true);
    expect(body.perQueue.default?.pending).toBe(1);
  });

  it("serves resource status from worker heartbeats", async () => {
    const body = await (await fetch(`${base}/api/resources`, { headers })).json();
    expect(Array.isArray(body)).toBe(true);
  });

  it("serves job errors and logs for a job", async () => {
    const id = queue.enqueue("add", [1, 2]);
    const errors = await (await fetch(`${base}/api/jobs/${id}/errors`, { headers })).json();
    expect(errors).toEqual([]);
    const logs = await (await fetch(`${base}/api/jobs/${id}/logs`, { headers })).json();
    expect(logs).toEqual([]);
  });

  it("purges dead letters via POST", async () => {
    const res = await fetch(`${base}/api/dead-letters/purge`, { method: "POST", headers });
    expect(res.status).toBe(200);
    expect(((await res.json()) as { purged: number }).purged).toBe(0);
  });
});

describe("proxy and interception stats", () => {
  it("serves interception stats after interceptors run", async () => {
    queue.intercept((_task, _args) => ({ type: "pass" }));
    queue.enqueue("add", [1, 2]);
    const stats = (await (await fetch(`${base}/api/interception-stats`, { headers })).json()) as {
      total_intercepts: number;
      strategy_counts: Record<string, number>;
    };
    expect(stats.total_intercepts).toBe(1);
    expect(stats.strategy_counts.pass).toBe(1);
  });

  it("serves proxy stats as a list", async () => {
    const stats = await (await fetch(`${base}/api/proxy-stats`, { headers })).json();
    expect(Array.isArray(stats)).toBe(true);
  });
});
