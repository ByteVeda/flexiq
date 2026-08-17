import { execSync } from "node:child_process";
import { existsSync, mkdtempSync } from "node:fs";
import type { Server } from "node:http";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, beforeAll, beforeEach, expect, it } from "vitest";
import { Queue, serveDashboard } from "../../src/index";

const pkgRoot = fileURLToPath(new URL("../..", import.meta.url));
const staticDir = join(pkgRoot, "static", "dashboard");
const TOKEN = "s3cret-token";

beforeAll(() => {
  if (!existsSync(join(staticDir, "index.html"))) {
    execSync("pnpm run build:dashboard", { cwd: pkgRoot, stdio: "ignore" });
  }
}, 120_000);

let server: Server | undefined;
let base = "";

beforeEach(async () => {
  const db = join(mkdtempSync(join(tmpdir(), "taskito-dashauth-")), "q.db");
  const queue = new Queue({ dbPath: db });
  queue.task("add", (a: number, b: number) => a + b);
  server = serveDashboard(queue, { port: 0, staticDir, auth: { token: TOKEN } });
  await new Promise((resolve) => setTimeout(resolve, 80));
  base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
});

afterEach(() => {
  server?.close();
  server = undefined;
});

it("rejects an API request without a token", async () => {
  const res = await fetch(`${base}/api/stats`);
  expect(res.status).toBe(401);
});

it("rejects a wrong token", async () => {
  const res = await fetch(`${base}/api/stats`, { headers: { authorization: "Bearer nope" } });
  expect(res.status).toBe(401);
});

it("accepts a Bearer token", async () => {
  const res = await fetch(`${base}/api/stats`, { headers: { authorization: `Bearer ${TOKEN}` } });
  expect(res.status).toBe(200);
});

it("accepts an X-Flexiq-Token header", async () => {
  const res = await fetch(`${base}/api/stats`, { headers: { "x-flexiq-token": TOKEN } });
  expect(res.status).toBe(200);
});

it("rejects a ?token= query on API calls", async () => {
  const res = await fetch(`${base}/api/stats?token=${TOKEN}`);
  expect(res.status).toBe(401);
});

it("bootstraps a cookie from ?token= on a page load and strips it", async () => {
  const res = await fetch(`${base}/?token=${TOKEN}&tab=jobs`, { redirect: "manual" });
  expect(res.status).toBe(302);
  expect(res.headers.get("location")).toBe("/?tab=jobs");
  expect(res.headers.get("set-cookie")).toContain("taskito_token=");
});

it("does not bootstrap from a wrong ?token= on a page load", async () => {
  const res = await fetch(`${base}/?token=nope`, { redirect: "manual" });
  expect(res.status).toBe(200);
  expect(res.headers.get("set-cookie")).toBeNull();
});

it("leaves /api/auth/status public", async () => {
  const res = await fetch(`${base}/api/auth/status`);
  expect(res.status).toBe(200);
});

it("gates probes behind the legacy token; /health stays public", async () => {
  expect((await fetch(`${base}/health`)).status).toBe(200);
  expect((await fetch(`${base}/readiness`)).status).toBe(401);
  expect((await fetch(`${base}/metrics`)).status).toBe(401);
  const ok = await fetch(`${base}/readiness`, { headers: { "x-flexiq-token": TOKEN } });
  expect(ok.status).not.toBe(401);
});

it("sets security headers on every response", async () => {
  for (const path of ["/health", "/api/auth/status", "/"]) {
    const res = await fetch(`${base}${path}`);
    expect(res.headers.get("x-content-type-options"), path).toBe("nosniff");
    expect(res.headers.get("x-frame-options"), path).toBe("DENY");
    expect(res.headers.get("referrer-policy"), path).toBe("same-origin");
    expect(res.headers.get("content-security-policy"), path).toContain("default-src 'self'");
  }
});
