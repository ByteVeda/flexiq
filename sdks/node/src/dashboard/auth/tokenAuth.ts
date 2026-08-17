import { timingSafeEqual } from "node:crypto";
import type { IncomingMessage, ServerResponse } from "node:http";

/** Bearer-token protection for the dashboard. A single shared token gates the API. */
export interface DashboardAuth {
  /** Shared token required on every `/api/*` request (except `/api/auth/status`). */
  token: string;
}

/** Cookie the SPA carries once a valid `?token=` has bootstrapped a session. */
const TOKEN_COOKIE = "taskito_token";

/** API paths reachable without a token, so the SPA can detect that auth is on. */
const PUBLIC_API_PATHS = new Set(["/api/auth/status"]);

export function isPublicApiPath(path: string): boolean {
  return PUBLIC_API_PATHS.has(path);
}

/**
 * The token presented on a request, from (in order) the `Authorization: Bearer`
 * header, an `X-FlexiQ-Token` header, or the `taskito_token` cookie. A
 * `?token=` query param is deliberately NOT accepted here — query strings leak
 * into access logs, browser history, and the Referer header; it is only
 * honoured once on a page load to bootstrap the cookie.
 */
export function presentedToken(req: IncomingMessage): string | undefined {
  const header = req.headers.authorization;
  if (header?.startsWith("Bearer ")) {
    return header.slice("Bearer ".length);
  }
  const custom = req.headers["x-flexiq-token"];
  if (typeof custom === "string" && custom.length > 0) {
    return custom;
  }
  return readCookie(req, TOKEN_COOKIE);
}

/** Constant-time comparison of the expected token against what was presented. */
export function tokenMatches(expected: string, presented: string | undefined): boolean {
  if (!presented) {
    return false;
  }
  const a = Buffer.from(expected);
  const b = Buffer.from(presented);
  return a.length === b.length && timingSafeEqual(a, b);
}

/** Persist a valid token as an httpOnly cookie so the SPA's later calls authenticate. */
export function setTokenCookie(res: ServerResponse, token: string, secure: boolean): void {
  res.setHeader(
    "set-cookie",
    `${TOKEN_COOKIE}=${encodeURIComponent(token)}; HttpOnly; SameSite=Strict; Path=/; Max-Age=86400${secure ? "; Secure" : ""}`,
  );
}

function readCookie(req: IncomingMessage, name: string): string | undefined {
  const raw = req.headers.cookie;
  if (!raw) {
    return undefined;
  }
  for (const part of raw.split(";")) {
    const [key, ...rest] = part.trim().split("=");
    if (key === name) {
      return decodeURIComponent(rest.join("="));
    }
  }
  return undefined;
}
