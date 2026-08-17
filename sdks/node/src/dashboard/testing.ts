// Test seam for session-authenticated dashboard requests. Internal — used by
// the SDK's own tests; not exported from the package barrel.

import type { Queue } from "../index";
import { AuthStore, type DashboardSession } from "./auth";

export interface SeededAuth {
  session: DashboardSession;
  /** Headers carrying the session cookie + CSRF pair for state-changing calls. */
  headers: Record<string, string>;
}

/** Create an admin (or given-role) user plus a live session for tests. */
export async function seedAdminAndSession(
  queue: Queue,
  options: { username?: string; password?: string; role?: string } = {},
): Promise<SeededAuth> {
  const store = new AuthStore(queue);
  const user = await store.createUser(
    options.username ?? "admin",
    options.password ?? "password123",
    options.role ?? "admin",
  );
  const session = store.createSession(user);
  return { session, headers: authedHeaders(session) };
}

/** Cookie + CSRF headers for an existing session. */
export function authedHeaders(session: DashboardSession): Record<string, string> {
  return {
    cookie: `flexiq_session=${session.token}; flexiq_csrf=${session.csrfToken}`,
    "x-csrf-token": session.csrfToken,
  };
}
