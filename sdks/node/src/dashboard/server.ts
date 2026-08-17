// Dashboard HTTP dispatch: /api/* -> JSON handlers, everything else -> the SPA.
//
// Three auth modes:
// - Open mode (default): no authentication — every route serves openly and
//   the auth endpoints (except /api/auth/status) respond 404.
// - Session mode (`authEnabled: true`): full login flow — first-run setup,
//   password sessions, CSRF double-submit, and admin/viewer roles, all
//   persisted in the queue's settings store.
// - Token mode (legacy, `auth: {token}`): a single shared token gates the
//   API; the SPA gets a fixed identity once past the token check.

import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import type { Queue } from "../index";
import { createLogger } from "../utils";
import { WebhookValidationError } from "../webhooks";
import {
  AllowlistDenied,
  AuthStore,
  buildContext,
  CSRF_COOKIE,
  csrfValid,
  type DashboardAuth,
  isPublicApiPath,
  type OAuthFlow,
  ProviderNotConfigured,
  presentedToken,
  type RequestContext,
  SESSION_COOKIE,
  StateValidationError,
  setTokenCookie,
  tokenMatches,
} from "./auth";
import { DashboardError, ValidationError } from "./errors";
import { checkHealth, checkReadiness, openAuthStatus, openWhoami } from "./handlers";
import { isCsrfExempt, isPublicPath, isStateChangingMethod, requiresAdmin } from "./policy";
import { routes } from "./routes";
import { StaticAssets } from "./static";

const log = createLogger("dashboard");

/** Max request body size (1 MiB) — reject larger payloads to bound memory. */
const MAX_BODY_BYTES = 1024 * 1024;

/**
 * Defense-in-depth headers on every response. The CSP assumes a fully
 * self-contained SPA bundle (no inline scripts — the theme bootstrap ships as
 * /theme-init.js); inline style attributes need 'unsafe-inline' in style-src.
 */
const SECURITY_HEADERS: ReadonlyArray<readonly [string, string]> = [
  [
    "content-security-policy",
    "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; " +
      "img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; " +
      "base-uri 'self'; form-action 'self'; object-src 'none'",
  ],
  ["x-content-type-options", "nosniff"],
  ["x-frame-options", "DENY"],
  ["referrer-policy", "same-origin"],
];

/** Options accepted by {@link createDashboardHandler} / {@link createDashboardServer}. */
export interface DashboardHandlerOptions {
  /** Legacy shared-token gate. When set, the session-auth flow is disabled. */
  auth?: DashboardAuth;
  /** Enable the session-auth flow (default false — the dashboard serves openly). */
  authEnabled?: boolean;
  /** Mark session cookies `Secure` (default true). Disable only for plain-HTTP dev. */
  secureCookies?: boolean /** OAuth login flow (built from env by `serveDashboard` when omitted). */;
  oauth?: OAuthFlow;
}

/** Accept the pre-options `DashboardAuth` third argument for compatibility. */
function normalizeOptions(
  options?: DashboardHandlerOptions | DashboardAuth,
): DashboardHandlerOptions {
  if (options && "token" in options && typeof options.token === "string") {
    return { auth: options };
  }
  return (options as DashboardHandlerOptions | undefined) ?? {};
}

/**
 * Build a Node `http` request handler that serves the dashboard SPA from `staticDir`
 * plus the `/api/*` JSON contract over `queue`. Use this to mount the dashboard into an
 * existing server (e.g. an Express or Fastify app); {@link createDashboardServer} wraps
 * it in a standalone server.
 */
export function createDashboardHandler(
  queue: Queue,
  staticDir: string,
  options?: DashboardHandlerOptions | DashboardAuth,
): (req: IncomingMessage, res: ServerResponse) => void {
  const assets = new StaticAssets(staticDir);
  const resolved = normalizeOptions(options);
  return (req, res) => {
    // Single choke point: every response — JSON, assets, probes, redirects —
    // carries the security headers.
    for (const [name, value] of SECURITY_HEADERS) {
      res.setHeader(name, value);
    }
    void dispatch(queue, assets, req, res, resolved).catch((error) => {
      log.error(() => "dashboard dispatch failed", error);
      if (!res.headersSent) {
        sendJson(res, 500, { error: "internal server error" });
      }
    });
  };
}

/** Build (but do not start) the dashboard server over `queue`, serving the SPA from `staticDir`. */
export function createDashboardServer(
  queue: Queue,
  staticDir: string,
  options?: DashboardHandlerOptions | DashboardAuth,
): Server {
  return createServer(createDashboardHandler(queue, staticDir, options));
}

async function dispatch(
  queue: Queue,
  assets: StaticAssets,
  req: IncomingMessage,
  res: ServerResponse,
  options: DashboardHandlerOptions,
): Promise<void> {
  const url = new URL(req.url ?? "/", "http://localhost");
  const path = url.pathname;
  const auth = options.auth;

  if (path === "/health" || path === "/readiness" || path === "/metrics") {
    await serveProbe(queue, req, res, path, options);
    return;
  }

  if (!path.startsWith("/api/")) {
    // Token mode: a valid `?token=` on a page load (never an API call)
    // bootstraps an httpOnly cookie, then redirects to strip the token from
    // the URL so it can't leak via history or the Referer header.
    const queryToken = url.searchParams.get("token");
    if (auth && req.method === "GET" && queryToken && tokenMatches(auth.token, queryToken)) {
      setTokenCookie(res, auth.token, options.secureCookies !== false);
      url.searchParams.delete("token");
      redirect(res, `${url.pathname}${url.search}`);
      return;
    }
    if (assets.serve(path, res)) {
      return;
    }
    sendJson(res, 503, { error: "dashboard assets not built — run `pnpm build:dashboard`" });
    return;
  }

  if (auth) {
    await dispatchTokenMode(queue, req, res, url, path, auth);
    return;
  }

  if (options.authEnabled !== true) {
    await dispatchOpenMode(queue, req, res, url, path);
    return;
  }

  const store = new AuthStore(queue);
  const ctx = buildContext(req, (token) => store.getSession(token));
  const denied = authorize(store, ctx, path, req.method ?? "GET");
  if (denied) {
    sendJson(res, denied.status, { error: denied.code });
    return;
  }
  const secure = options.secureCookies !== false;
  if (req.method === "GET" && (await serveOauth(options.oauth, res, url, path, secure))) {
    return;
  }
  await runRoute(queue, req, res, url, path, ctx, secure);
}

/**
 * OAuth endpoints (redirect-emitting, so they live outside the JSON route
 * table): provider listing, `/start/<slot>` 302s to the provider, and
 * `/callback/<slot>` lands the session. Returns false when `path` is not
 * an OAuth path so normal dispatch continues.
 */
async function serveOauth(
  flow: OAuthFlow | undefined,
  res: ServerResponse,
  url: URL,
  path: string,
  secure: boolean,
): Promise<boolean> {
  if (path === "/api/auth/providers") {
    if (!flow) {
      return false; // fall through to the password-only route handler
    }
    sendJson(res, 200, {
      password_enabled: flow.passwordAuthEnabled,
      providers: flow.providersListing(),
    });
    return true;
  }

  const start = matchSlot(path, "/api/auth/oauth/start/");
  if (start !== undefined) {
    if (!flow) {
      sendJson(res, 404, { error: "oauth_not_configured" });
      return true;
    }
    try {
      redirect(res, await flow.start(start, url.searchParams.get("next")));
    } catch (error) {
      if (error instanceof ProviderNotConfigured) {
        sendJson(res, 404, { error: error.message });
      } else {
        log.error(() => `oauth start for '${start}' failed`, error);
        sendJson(res, 500, { error: "internal server error" });
      }
    }
    return true;
  }

  const callback = matchSlot(path, "/api/auth/oauth/callback/");
  if (callback !== undefined) {
    if (!flow) {
      sendJson(res, 404, { error: "oauth_not_configured" });
      return true;
    }
    try {
      const { session, nextUrl } = await flow.handleCallback(callback, {
        code: url.searchParams.get("code"),
        stateToken: url.searchParams.get("state"),
        error: url.searchParams.get("error"),
      });
      flow.pruneState();
      const ttl = Math.max(0, session.expiresAt - Math.floor(Date.now() / 1000));
      res.setHeader("set-cookie", sessionCookies(session.token, session.csrfToken, ttl, secure));
      redirect(res, nextUrl);
    } catch (error) {
      if (error instanceof ProviderNotConfigured) {
        sendJson(res, 404, { error: error.message });
      } else if (error instanceof StateValidationError) {
        redirect(res, "/login?error=oauth_state_invalid");
      } else if (error instanceof AllowlistDenied) {
        redirect(res, "/login?error=oauth_denied");
      } else {
        // Covers IdentityFetchError and anything unexpected — never leak details.
        log.error(() => `oauth callback for '${callback}' failed`, error);
        redirect(res, "/login?error=oauth_failed");
      }
    }
    return true;
  }

  return false;
}

function matchSlot(path: string, prefix: string): string | undefined {
  if (!path.startsWith(prefix)) {
    return undefined;
  }
  let slot: string;
  try {
    slot = decodeURIComponent(path.slice(prefix.length));
  } catch {
    // Malformed percent-encoding on a public path is a 404, not a 500.
    return undefined;
  }
  return slot && !slot.includes("/") ? slot : undefined;
}

function redirect(res: ServerResponse, location: string): void {
  res.writeHead(302, { location });
  res.end();
}

/** The session/CSRF/role gate. Returns the denial, or undefined to proceed. */
function authorize(
  store: AuthStore,
  ctx: RequestContext,
  path: string,
  method: string,
): { status: number; code: string } | undefined {
  if (isPublicPath(path)) {
    return undefined;
  }
  if (store.countUsers() === 0) {
    return { status: 503, code: "setup_required" };
  }
  if (!ctx.session) {
    return { status: 401, code: "not_authenticated" };
  }
  if (isStateChangingMethod(method) && !isCsrfExempt(path) && !csrfValid(ctx)) {
    return { status: 403, code: "csrf_failed" };
  }
  if (requiresAdmin(path, method) && ctx.session.role !== "admin") {
    return { status: 403, code: "forbidden" };
  }
  return undefined;
}

async function runRoute(
  queue: Queue,
  req: IncomingMessage,
  res: ServerResponse,
  url: URL,
  path: string,
  ctx: RequestContext,
  secureCookies: boolean,
): Promise<void> {
  for (const route of routes) {
    if (route.method !== req.method) {
      continue;
    }
    const match = path.match(route.pattern);
    if (!match) {
      continue;
    }
    const params = match.slice(1).map((value) => decodeURIComponent(value ?? ""));
    const body = req.method === "POST" || req.method === "PUT" ? await readBody(req) : undefined;
    try {
      const result = await route.handle(queue, url, params, body, ctx);
      if (result === undefined) {
        sendJson(res, 404, { error: "not found" });
        return;
      }
      sendJson(res, 200, applyAuthCookies(res, path, result, secureCookies));
    } catch (error) {
      if (error instanceof DashboardError) {
        sendJson(res, error.status, { error: error.message });
        return;
      }
      if (error instanceof ValidationError || error instanceof WebhookValidationError) {
        sendJson(res, 400, { error: error.message });
        return;
      }
      // Log internally but never leak error/stack details to the HTTP client.
      log.error(() => `${req.method} ${path} failed`, error);
      sendJson(res, 500, { error: "internal server error" });
    }
    return;
  }

  sendJson(res, 404, { error: "not found" });
}

/**
 * Cookie side effects for auth flows: a successful login sets the session +
 * CSRF cookies (and the raw token is redacted from the JSON body); logout
 * clears them.
 */
function applyAuthCookies(
  res: ServerResponse,
  path: string,
  result: unknown,
  secure: boolean,
): unknown {
  if (path === "/api/auth/logout") {
    res.setHeader("set-cookie", clearSessionCookies(secure));
    return result;
  }
  if (path !== "/api/auth/login") {
    return result;
  }
  const login = result as {
    session?: { token?: string; csrf_token?: string; expires_at?: number };
  };
  const session = login.session;
  if (!session?.token || !session.csrf_token) {
    return result;
  }
  const ttl = Math.max(0, (session.expires_at ?? 0) - Math.floor(Date.now() / 1000));
  res.setHeader("set-cookie", sessionCookies(session.token, session.csrf_token, ttl, secure));
  const { token: _redacted, ...rest } = session;
  return { ...login, session: rest };
}

function sessionCookies(token: string, csrf: string, ttl: number, secure: boolean): string[] {
  const attrs = `SameSite=Strict; Path=/; Max-Age=${ttl}${secure ? "; Secure" : ""}`;
  return [`${SESSION_COOKIE}=${token}; HttpOnly; ${attrs}`, `${CSRF_COOKIE}=${csrf}; ${attrs}`];
}

function clearSessionCookies(secure: boolean): string[] {
  const attrs = `SameSite=Strict; Path=/; Max-Age=0${secure ? "; Secure" : ""}`;
  return [`${SESSION_COOKIE}=; HttpOnly; ${attrs}`, `${CSRF_COOKIE}=; ${attrs}`];
}

/** Open dispatch (auth disabled, the default): every route serves without a
 * session; the auth endpoints respond 404 so the SPA hides login affordances. */
async function dispatchOpenMode(
  queue: Queue,
  req: IncomingMessage,
  res: ServerResponse,
  url: URL,
  path: string,
): Promise<void> {
  if (path.startsWith("/api/auth/")) {
    if (path === "/api/auth/status") {
      sendJson(res, 200, openAuthStatus(false));
    } else {
      sendJson(res, 404, { error: "auth_disabled" });
    }
    return;
  }
  const openCtx: RequestContext = {
    session: undefined,
    csrfCookie: undefined,
    csrfHeader: undefined,
  };
  await runRoute(queue, req, res, url, path, openCtx, true);
}

/** Legacy shared-token dispatch: stub auth boot endpoints + token-gated API. */
async function dispatchTokenMode(
  queue: Queue,
  req: IncomingMessage,
  res: ServerResponse,
  url: URL,
  path: string,
  auth: DashboardAuth,
): Promise<void> {
  if (!isPublicApiPath(path) && !tokenMatches(auth.token, presentedToken(req))) {
    sendJson(res, 401, { error: "unauthorized" });
    return;
  }
  if (path.startsWith("/api/auth/")) {
    if (path === "/api/auth/status") {
      sendJson(res, 200, openAuthStatus());
    } else if (path === "/api/auth/whoami") {
      setOpenAuthCookies(res);
      sendJson(res, 200, openWhoami());
    } else {
      sendJson(res, 404, { error: "not found" });
    }
    return;
  }
  const openCtx: RequestContext = {
    session: undefined,
    csrfCookie: undefined,
    csrfHeader: undefined,
  };
  await runRoute(queue, req, res, url, path, openCtx, true);
}

/** Read and JSON-parse a request body (undefined when empty, invalid, or oversized).
 * Caps total size at {@link MAX_BODY_BYTES} and resolves on stream error/abort. */
function readBody(req: IncomingMessage): Promise<unknown> {
  return new Promise((resolve) => {
    const chunks: Buffer[] = [];
    let size = 0;
    let aborted = false;
    const finish = (value: unknown) => {
      if (!aborted) {
        aborted = true;
        resolve(value);
      }
    };
    req.on("data", (chunk: Buffer) => {
      size += chunk.length;
      if (size > MAX_BODY_BYTES) {
        req.destroy();
        finish(undefined);
        return;
      }
      chunks.push(chunk);
    });
    req.on("end", () => {
      if (chunks.length === 0) {
        finish(undefined);
        return;
      }
      try {
        // Decode once over the joined buffer so multi-byte UTF-8 split across
        // chunk boundaries isn't corrupted into replacement characters.
        finish(JSON.parse(Buffer.concat(chunks).toString("utf8")));
      } catch {
        finish(undefined);
      }
    });
    req.on("error", () => finish(undefined));
    req.on("aborted", () => finish(undefined));
  });
}

/**
 * Probe endpoints. `/health` is always public (liveness). `/readiness` and
 * `/metrics` accept the optional `FLEXIQ_DASHBOARD_METRICS_TOKEN` bearer or,
 * when dashboard auth is on, that mode's own credential — see
 * {@link probeAuthorized}.
 */
async function serveProbe(
  queue: Queue,
  req: IncomingMessage,
  res: ServerResponse,
  path: string,
  options: DashboardHandlerOptions,
): Promise<void> {
  if (path === "/health") {
    sendJson(res, 200, checkHealth());
    return;
  }
  if (!probeAuthorized(queue, req, options)) {
    sendJson(res, 401, { error: "unauthorized" });
    return;
  }
  if (path === "/readiness") {
    const payload = await checkReadiness(queue);
    // Non-2xx on degraded so orchestrators stop routing to this instance.
    sendJson(res, payload.status === "ready" ? 200 : 503, payload);
    return;
  }
  try {
    const { register } = await import("prom-client");
    const body = await register.metrics();
    res.writeHead(200, { "content-type": register.contentType });
    res.end(body);
  } catch {
    sendJson(res, 501, { error: "prom-client not installed" });
  }
}

/**
 * Gate for `/readiness` and `/metrics`. Accepted credentials: the optional
 * `FLEXIQ_DASHBOARD_METRICS_TOKEN` bearer (scraper-friendly), the legacy
 * shared token in token mode, or a valid session in session mode. Open mode
 * with no metrics token stays public (probe-friendly default).
 */
function probeAuthorized(
  queue: Queue,
  req: IncomingMessage,
  options: DashboardHandlerOptions,
): boolean {
  const metricsToken = process.env.FLEXIQ_DASHBOARD_METRICS_TOKEN;
  if (metricsToken) {
    if (tokenMatches(`Bearer ${metricsToken}`, req.headers.authorization ?? "")) {
      return true;
    }
  } else if (!options.auth && options.authEnabled !== true) {
    return true;
  }
  if (options.auth) {
    return tokenMatches(options.auth.token, presentedToken(req));
  }
  if (options.authEnabled === true) {
    const store = new AuthStore(queue);
    const ctx = buildContext(req, (token) => store.getSession(token));
    return ctx.session !== undefined;
  }
  return false;
}

/** Token-mode session/CSRF cookies so the SPA proceeds without a login. */
function setOpenAuthCookies(res: ServerResponse): void {
  res.setHeader("set-cookie", [
    "taskito_session=open; HttpOnly; SameSite=Strict; Path=/; Max-Age=86400",
    "taskito_csrf=open; SameSite=Strict; Path=/; Max-Age=86400",
  ]);
}

function sendJson(res: ServerResponse, status: number, body: unknown): void {
  res.writeHead(status, { "content-type": "application/json" });
  res.end(JSON.stringify(body));
}
