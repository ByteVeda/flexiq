// Per-request authentication context: session + CSRF material parsed from
// the incoming request, handed to handlers that need the calling user.

import type { IncomingMessage } from "node:http";
import type { DashboardSession } from "./store";

// Session cookie: HttpOnly + SameSite=Strict — never readable from JS.
export const SESSION_COOKIE = "flexiq_session";
// CSRF cookie: NOT HttpOnly — the SPA reads it and echoes it in the header.
export const CSRF_COOKIE = "taskito_csrf";
export const CSRF_HEADER = "x-csrf-token";

/** Auth state attached to a single HTTP request. */
export interface RequestContext {
  session: DashboardSession | undefined;
  csrfCookie: string | undefined;
  csrfHeader: string | undefined;
}

export const isAuthenticated = (ctx: RequestContext): boolean => ctx.session !== undefined;

/**
 * Double-submit cookie check: a non-empty CSRF cookie, a matching
 * `X-CSRF-Token` header, and both equal to the session's stored CSRF token
 * (defends against an attacker who pre-seeds the cookie).
 */
export function csrfValid(ctx: RequestContext): boolean {
  if (!ctx.session || !ctx.csrfCookie || !ctx.csrfHeader) {
    return false;
  }
  return ctx.csrfCookie === ctx.csrfHeader && ctx.csrfCookie === ctx.session.csrfToken;
}

/** Parse a raw `Cookie:` header; first value wins for duplicate names. */
export function parseCookies(header: string | undefined): Map<string, string> {
  const cookies = new Map<string, string>();
  if (!header) {
    return cookies;
  }
  for (const part of header.split(";")) {
    const eq = part.indexOf("=");
    if (eq < 0) {
      continue;
    }
    const name = part.slice(0, eq).trim();
    const value = part.slice(eq + 1).trim();
    if (name && !cookies.has(name)) {
      cookies.set(name, value);
    }
  }
  return cookies;
}

/** Build the request context from raw headers and a session resolver. */
export function buildContext(
  req: IncomingMessage,
  resolveSession: (token: string) => DashboardSession | undefined,
): RequestContext {
  const cookies = parseCookies(req.headers.cookie);
  const token = cookies.get(SESSION_COOKIE) ?? "";
  const header = req.headers[CSRF_HEADER];
  return {
    session: token ? resolveSession(token) : undefined,
    csrfCookie: cookies.get(CSRF_COOKIE),
    csrfHeader: typeof header === "string" ? header : undefined,
  };
}
