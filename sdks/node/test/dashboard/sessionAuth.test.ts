import { execSync } from "node:child_process";
import { once } from "node:events";
import { existsSync, mkdtempSync } from "node:fs";
import type { Server } from "node:http";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, beforeAll, beforeEach, describe, expect, it } from "vitest";
import {
  AuthStore,
  bootstrapAdminFromEnv,
  hashPassword,
  verifyPassword,
} from "../../src/dashboard/auth";
import { authedHeaders, seedAdminAndSession } from "../../src/dashboard/testing";
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
  const db = join(mkdtempSync(join(tmpdir(), "taskito-dashsess-")), "q.db");
  queue = new Queue({ dbPath: db });
  queue.task("add", (a: number, b: number) => a + b);
  server = serveDashboard(queue, { port: 0, staticDir, secureCookies: false, authEnabled: true });
  await once(server, "listening");
  base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
});

afterEach(() => {
  server?.close();
  server = undefined;
});

const post = (path: string, body: unknown, headers: Record<string, string> = {}) =>
  fetch(`${base}${path}`, {
    method: "POST",
    headers: { "content-type": "application/json", ...headers },
    body: JSON.stringify(body),
  });

describe("password hashing", () => {
  it("round-trips and rejects wrong passwords", async () => {
    const encoded = await hashPassword("hunter22!");
    expect(encoded).toMatch(/^pbkdf2_sha256\$600000\$[0-9a-f]{32}\$[0-9a-f]{64}$/);
    expect(await verifyPassword("hunter22!", encoded)).toBe(true);
    expect(await verifyPassword("hunter23!", encoded)).toBe(false);
  });

  it("verifies a hash produced by the cross-SDK reference implementation", async () => {
    // Golden vector: pbkdf2_hmac('sha256', 'password123', 'saltsaltsaltsalt',
    // 1000 iters, 32 bytes). Pins the `pbkdf2_sha256$<iters>$<salt_hex>$<hash_hex>`
    // wire format shared across SDK auth stores.
    const reference =
      "pbkdf2_sha256$1000$73616c7473616c7473616c7473616c74$" +
      "9db0df06e36654310bbab81b2e3b7b1bf700b4f8cd1f13a88d64cf9fa7594408";
    expect(await verifyPassword("password123", reference)).toBe(true);
    expect(await verifyPassword("password124", reference)).toBe(false);
  });

  it("rejects the oauth sentinel hash", async () => {
    expect(await verifyPassword("anything", "oauth:google")).toBe(false);
  });
});

describe("setup flow", () => {
  it("gates every non-public API route until the first admin exists", async () => {
    const res = await fetch(`${base}/api/stats`);
    expect(res.status).toBe(503);
    expect(((await res.json()) as { error: string }).error).toBe("setup_required");

    const status = (await (await fetch(`${base}/api/auth/status`)).json()) as {
      setup_required: boolean;
    };
    expect(status.setup_required).toBe(true);
  });

  it("creates the first admin via setup, then rejects a second setup", async () => {
    const created = await post("/api/auth/setup", { username: "root", password: "password123" });
    expect(created.status).toBe(200);
    const body = (await created.json()) as { user: { username: string; role: string } };
    expect(body.user).toMatchObject({ username: "root", role: "admin" });

    const again = await post("/api/auth/setup", { username: "x", password: "password123" });
    expect(again.status).toBe(400);
  });
});

describe("login and sessions", () => {
  beforeEach(async () => {
    await new AuthStore(queue).createUser("root", "password123", "admin");
  });

  it("logs in, sets session cookies, and redacts the raw token", async () => {
    const res = await post("/api/auth/login", { username: "root", password: "password123" });
    expect(res.status).toBe(200);
    const cookies = res.headers.getSetCookie();
    expect(cookies.some((c) => c.startsWith("taskito_session=") && c.includes("HttpOnly"))).toBe(
      true,
    );
    expect(cookies.some((c) => c.startsWith("taskito_csrf=") && !c.includes("HttpOnly"))).toBe(
      true,
    );
    const body = (await res.json()) as { session: Record<string, unknown> };
    expect(body.session.token).toBeUndefined();
    expect(typeof body.session.csrf_token).toBe("string");
  });

  it("rejects a wrong password and an unknown user with the same error", async () => {
    const wrong = await post("/api/auth/login", { username: "root", password: "nope-nope" });
    const unknown = await post("/api/auth/login", { username: "ghost", password: "nope-nope" });
    expect(wrong.status).toBe(400);
    expect(unknown.status).toBe(400);
    expect(await wrong.json()).toEqual(await unknown.json());
  });

  it("requires a session for protected GETs and accepts a valid one", async () => {
    expect((await fetch(`${base}/api/stats`)).status).toBe(401);
    const { headers } = await seedAdminAndSession(queue, { username: "seeded" });
    expect((await fetch(`${base}/api/stats`, { headers })).status).toBe(200);
  });

  it("logout invalidates the session and clears cookies", async () => {
    const { session, headers } = await seedAdminAndSession(queue, { username: "seeded" });
    const res = await post("/api/auth/logout", {}, headers);
    expect(res.status).toBe(200);
    expect(res.headers.getSetCookie().some((c) => c.includes("Max-Age=0"))).toBe(true);
    expect(new AuthStore(queue).getSession(session.token)).toBeUndefined();
  });

  it("deleting a user revokes its live sessions immediately", async () => {
    const { session, headers } = await seedAdminAndSession(queue, { username: "doomed" });
    new AuthStore(queue).deleteUser("doomed");
    // The session row is gone, so the gate rejects before whoami runs.
    expect(new AuthStore(queue).getSession(session.token)).toBeUndefined();
    expect((await fetch(`${base}/api/auth/whoami`, { headers })).status).toBe(401);
  });
});

describe("csrf", () => {
  it("rejects state-changing requests without or with a mismatched token", async () => {
    const { session } = await seedAdminAndSession(queue);
    const cookieOnly = { cookie: `taskito_session=${session.token}` };
    const missing = await post("/api/queues/emails/pause", {}, cookieOnly);
    expect(missing.status).toBe(403);
    expect(((await missing.json()) as { error: string }).error).toBe("csrf_failed");

    const mismatched = await post(
      "/api/queues/emails/pause",
      {},
      {
        cookie: `taskito_session=${session.token}; taskito_csrf=${session.csrfToken}`,
        "x-csrf-token": "attacker-value",
      },
    );
    expect(mismatched.status).toBe(403);
  });

  it("accepts a valid double-submit pair", async () => {
    const { headers } = await seedAdminAndSession(queue);
    const res = await post("/api/queues/emails/pause", {}, headers);
    expect(res.status).toBe(200);
  });
});

describe("rbac", () => {
  it("blocks viewers from admin actions but allows reads and self-service", async () => {
    await new AuthStore(queue).createUser("root", "password123", "admin");
    const { session } = await seedAdminAndSession(queue, {
      username: "watcher",
      role: "viewer",
    });
    const headers = authedHeaders(session);

    expect((await fetch(`${base}/api/stats`, { headers })).status).toBe(200);

    const denied = await post("/api/queues/emails/pause", {}, headers);
    expect(denied.status).toBe(403);
    expect(((await denied.json()) as { error: string }).error).toBe("forbidden");

    const changed = await post(
      "/api/auth/change-password",
      { old_password: "password123", new_password: "password456" },
      headers,
    );
    expect(changed.status).toBe(200);

    const logout = await post("/api/auth/logout", {}, headers);
    expect(logout.status).toBe(200);
  });
});

describe("env bootstrap", () => {
  it("creates the admin once and scrubs the password from the environment", async () => {
    process.env.FLEXIQ_DASHBOARD_ADMIN_USER = "envadmin";
    process.env.FLEXIQ_DASHBOARD_ADMIN_PASSWORD = "password123";
    try {
      const user = await bootstrapAdminFromEnv(queue);
      expect(user?.username).toBe("envadmin");
      expect(process.env.FLEXIQ_DASHBOARD_ADMIN_PASSWORD).toBeUndefined();
      // Idempotent: second call is a no-op.
      process.env.FLEXIQ_DASHBOARD_ADMIN_PASSWORD = "password123";
      expect(await bootstrapAdminFromEnv(queue)).toBeUndefined();
    } finally {
      delete process.env.FLEXIQ_DASHBOARD_ADMIN_USER;
      delete process.env.FLEXIQ_DASHBOARD_ADMIN_PASSWORD;
    }
  });
});

describe("providers listing", () => {
  it("is public and password-only until OAuth is configured", async () => {
    const res = await fetch(`${base}/api/auth/providers`);
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ password_enabled: true, providers: [] });
  });
});

describe("probes", () => {
  it("gates /readiness and /metrics behind a session; /health stays public", async () => {
    expect((await fetch(`${base}/health`)).status).toBe(200);
    expect((await fetch(`${base}/readiness`)).status).toBe(401);
    expect((await fetch(`${base}/metrics`)).status).toBe(401);

    const { headers } = await seedAdminAndSession(queue);
    expect((await fetch(`${base}/readiness`, { headers })).status).not.toBe(401);
  });

  it("accepts the metrics bearer token alongside sessions", async () => {
    const previous = process.env.FLEXIQ_DASHBOARD_METRICS_TOKEN;
    process.env.FLEXIQ_DASHBOARD_METRICS_TOKEN = "scrape-secret";
    try {
      const ok = await fetch(`${base}/readiness`, {
        headers: { authorization: "Bearer scrape-secret" },
      });
      expect(ok.status).not.toBe(401);
      expect(
        (await fetch(`${base}/readiness`, { headers: { authorization: "Bearer wrong" } })).status,
      ).toBe(401);
    } finally {
      if (previous === undefined) {
        delete process.env.FLEXIQ_DASHBOARD_METRICS_TOKEN;
      } else {
        process.env.FLEXIQ_DASHBOARD_METRICS_TOKEN = previous;
      }
    }
  });
});
