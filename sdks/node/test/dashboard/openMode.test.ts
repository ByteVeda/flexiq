// Open mode — the default: no authentication, every route serves openly and
// the auth endpoints (except /api/auth/status) respond 404.

import { execSync } from "node:child_process";
import { once } from "node:events";
import { existsSync, mkdtempSync } from "node:fs";
import type { Server } from "node:http";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { AuthStore } from "../../src/dashboard/auth";
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

beforeEach(async () => {
  const db = join(mkdtempSync(join(tmpdir(), "flexiq-dashopen-")), "q.db");
  queue = new Queue({ dbPath: db });
  queue.task("add", (a: number, b: number) => a + b);
  server = serveDashboard(queue, { port: 0, staticDir, secureCookies: false });
  await once(server, "listening");
  base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
});

afterEach(() => {
  server?.close();
  server = undefined;
});

describe("open mode (auth disabled by default)", () => {
  it("reports auth disabled on /api/auth/status", async () => {
    const res = await fetch(`${base}/api/auth/status`);
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ auth_enabled: false, setup_required: false });
  });

  it("serves the API without a session", async () => {
    const res = await fetch(`${base}/api/stats`);
    expect(res.status).toBe(200);
  });

  it("allows mutations without a CSRF token", async () => {
    const res = await fetch(`${base}/api/dead-letters/purge`, { method: "POST" });
    expect(res.status).toBe(200);
  });

  it("stays open even when users exist in the auth store", async () => {
    await new AuthStore(queue).createUser("alice", "hunter2-secret", "admin");
    const res = await fetch(`${base}/api/stats`);
    expect(res.status).toBe(200);
  });

  it("responds 404 on the auth endpoints", async () => {
    const paths: Array<[string, string]> = [
      ["GET", "/api/auth/whoami"],
      ["GET", "/api/auth/providers"],
      ["POST", "/api/auth/login"],
      ["POST", "/api/auth/setup"],
      ["POST", "/api/auth/logout"],
    ];
    for (const [method, path] of paths) {
      const res = await fetch(`${base}${path}`, { method });
      expect(res.status, path).toBe(404);
      expect(await res.json(), path).toEqual({ error: "auth_disabled" });
    }
  });
});
