import { execSync } from "node:child_process";
import { once } from "node:events";
import { existsSync, mkdtempSync } from "node:fs";
import type { Server } from "node:http";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, beforeAll, beforeEach, expect, it } from "vitest";
import {
  AllowlistDenied,
  AuthStore,
  OAuthFlow,
  type OAuthProvider,
  type ProviderIdentity,
} from "../../src/dashboard/auth";
import { Queue, serveDashboard } from "../../src/index";

const pkgRoot = fileURLToPath(new URL("../..", import.meta.url));
const staticDir = join(pkgRoot, "static", "dashboard");

beforeAll(() => {
  if (!existsSync(join(staticDir, "index.html"))) {
    execSync("pnpm run build:dashboard", { cwd: pkgRoot, stdio: "ignore" });
  }
}, 120_000);

class FakeProvider implements OAuthProvider {
  readonly slot = "fake";
  readonly label = "Fake";
  readonly type = "oidc";
  lastState = "";
  deny = false;
  identity: ProviderIdentity = {
    slot: "fake",
    subject: "sub-9",
    email: "dev@corp.com",
    emailVerified: true,
    name: "Dev",
    picture: null,
  };

  authorizationUrl(params: { state: string }): Promise<string> {
    this.lastState = params.state;
    return Promise.resolve(`https://provider.example/authorize?state=${params.state}`);
  }

  exchangeCode(): Promise<ProviderIdentity> {
    return Promise.resolve(this.identity);
  }

  checkAllowlist(): void {
    if (this.deny) {
      throw new AllowlistDenied("denied");
    }
  }
}

let server: Server | undefined;
let queue: Queue;
let provider: FakeProvider;
let base = "";

beforeEach(async () => {
  const db = join(mkdtempSync(join(tmpdir(), "flexiq-oauthep-")), "q.db");
  queue = new Queue({ dbPath: db });
  provider = new FakeProvider();
  const flow = new OAuthFlow(
    queue,
    {
      redirectBaseUrl: "https://ops.example.com",
      oidc: [],
      passwordAuthEnabled: false,
      adminEmails: [],
    },
    { providers: new Map([["fake", provider]]) },
  );
  server = serveDashboard(queue, {
    port: 0,
    staticDir,
    secureCookies: false,
    authEnabled: true,
    oauth: flow,
  });
  await once(server, "listening");
  base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
});

afterEach(() => {
  server?.close();
  server = undefined;
});

const noRedirect: RequestInit = { redirect: "manual" };

it("lists providers with the password flag from the flow", async () => {
  const res = await fetch(`${base}/api/auth/providers`);
  expect(res.status).toBe(200);
  expect(await res.json()).toEqual({
    password_enabled: false,
    providers: [{ slot: "fake", label: "Fake", type: "oidc" }],
  });
});

it("oauth start 302s to the provider and 404s unknown slots", async () => {
  const res = await fetch(`${base}/api/auth/oauth/start/fake?next=/jobs`, noRedirect);
  expect(res.status).toBe(302);
  expect(res.headers.get("location")).toBe(
    `https://provider.example/authorize?state=${provider.lastState}`,
  );

  expect((await fetch(`${base}/api/auth/oauth/start/ghost`, noRedirect)).status).toBe(404);
});

it("oauth paths bypass the setup-required gate", async () => {
  // No users exist, yet start/callback/providers respond instead of 503.
  expect((await fetch(`${base}/api/auth/providers`)).status).toBe(200);
  expect((await fetch(`${base}/api/auth/oauth/start/fake`, noRedirect)).status).toBe(302);
  expect((await fetch(`${base}/api/stats`)).status).toBe(503);
});

it("callback creates a session, sets cookies, and redirects to next", async () => {
  await fetch(`${base}/api/auth/oauth/start/fake?next=/jobs`, noRedirect);
  const res = await fetch(
    `${base}/api/auth/oauth/callback/fake?code=c1&state=${provider.lastState}`,
    noRedirect,
  );
  expect(res.status).toBe(302);
  expect(res.headers.get("location")).toBe("/jobs");
  const cookies = res.headers.getSetCookie();
  expect(cookies.some((c) => c.startsWith("flexiq_session=") && c.includes("HttpOnly"))).toBe(true);
  expect(cookies.some((c) => c.startsWith("flexiq_csrf="))).toBe(true);

  const user = new AuthStore(queue).getUser("fake:sub-9");
  expect(user?.role).toBe("viewer"); // admin comes only from the allowlist
  expect(user?.email).toBe("dev@corp.com");

  // The session cookie authenticates API calls.
  const session = cookies.find((c) => c.startsWith("flexiq_session=")) ?? "";
  const token = session.split(";")[0]?.split("=")[1] ?? "";
  const stats = await fetch(`${base}/api/stats`, {
    headers: { cookie: `flexiq_session=${token}` },
  });
  expect(stats.status).toBe(200);
});

it("redirects to login with an error code on replayed state", async () => {
  await fetch(`${base}/api/auth/oauth/start/fake`, noRedirect);
  const url = `${base}/api/auth/oauth/callback/fake?code=c1&state=${provider.lastState}`;
  expect((await fetch(url, noRedirect)).status).toBe(302);

  const replayed = await fetch(url, noRedirect);
  expect(replayed.status).toBe(302);
  expect(replayed.headers.get("location")).toBe("/login?error=oauth_state_invalid");
});

it("redirects with oauth_denied when the allowlist rejects", async () => {
  provider.deny = true;
  await fetch(`${base}/api/auth/oauth/start/fake`, noRedirect);
  const res = await fetch(
    `${base}/api/auth/oauth/callback/fake?code=c1&state=${provider.lastState}`,
    noRedirect,
  );
  expect(res.headers.get("location")).toBe("/login?error=oauth_denied");
});

it("redirects with oauth_failed when the provider reports an error", async () => {
  const res = await fetch(`${base}/api/auth/oauth/callback/fake?error=access_denied`, noRedirect);
  expect(res.headers.get("location")).toBe("/login?error=oauth_failed");
});
