import type { Server } from "node:http";
import { fileURLToPath } from "node:url";
import type { Queue } from "../index";
import { createLogger } from "../utils";
import type { DashboardAuth } from "./auth";
import { bootstrapAdminFromEnv, OAuthFlow, oauthConfigFromEnv } from "./auth";
import { createDashboardServer } from "./server";

const log = createLogger("dashboard");

export interface DashboardOptions {
  /** Port to listen on (default 8787). */
  port?: number;
  /** Host to bind (default 127.0.0.1). */
  host?: string;
  /** Path to the built SPA assets. Defaults to the package's bundled `static/dashboard`. */
  staticDir?: string;
  /**
   * Enable session authentication (first-run setup, password login, CSRF,
   * admin/viewer roles). Off by default — the dashboard serves openly.
   * Ignored when the legacy `auth` token gate is set.
   */
  authEnabled?: boolean;
  /**
   * Legacy shared-token gate: require this bearer token on every `/api/*`
   * request (except `/api/auth/status`). When set, the session-auth login
   * flow is disabled. Open `/?token=<token>` once to use the SPA behind it.
   */
  auth?: DashboardAuth;
  /**
   * Mark session cookies `Secure` so browsers only send them over HTTPS
   * (default true). Disable only for plain-HTTP development.
   */
  secureCookies?: boolean;
  /**
   * OAuth login flow. When omitted, one is built from the
   * `FLEXIQ_DASHBOARD_OAUTH_*` environment variables if they are set.
   */
  oauth?: OAuthFlow;
}

// Built relative to dist/ at runtime. The path is assembled dynamically so the
// bundler does not treat it as a static asset import.
const STATIC_REL = ["..", "static", "dashboard"].join("/");
const defaultStaticDir = (): string => fileURLToPath(new URL(STATIC_REL, import.meta.url));

/**
 * Start the web dashboard over `queue` — serves the React SPA plus a JSON API.
 * By default the dashboard serves openly with no authentication. Pass
 * `authEnabled: true` to run the full session flow: first-run setup, password
 * login, CSRF protection, and admin/viewer roles. An initial admin can be
 * bootstrapped from `FLEXIQ_DASHBOARD_ADMIN_USER` /
 * `FLEXIQ_DASHBOARD_ADMIN_PASSWORD`.
 * Returns the listening HTTP server; call `.close()` to stop it.
 */
export function serveDashboard(queue: Queue, options: DashboardOptions = {}): Server {
  const sessionMode = !options.auth && options.authEnabled === true;
  const bootstrap = sessionMode
    ? bootstrapAdminFromEnv(queue)
        .then(() => undefined)
        .catch((error) => {
          log.warn(() => `admin bootstrap failed: ${String(error)}`);
        })
    : Promise.resolve();
  const server = createDashboardServer(queue, options.staticDir ?? defaultStaticDir(), {
    auth: options.auth,
    authEnabled: options.authEnabled,
    secureCookies: options.secureCookies,
    oauth: options.oauth ?? (sessionMode ? buildOauthFlowFromEnv(queue) : undefined),
  });
  // A bind failure (e.g. EADDRINUSE) without an 'error' listener crashes the process.
  server.on("error", (error) => {
    log.error(() => "dashboard server error", error);
  });
  // Bind only after the env admin exists, so a first request can't race
  // the intended bootstrap admin through the open setup endpoint.
  void bootstrap.then(() => {
    server.listen(options.port ?? 8787, options.host ?? "127.0.0.1");
  });
  return server;
}

export type { DashboardAuth } from "./auth";
export {
  AuthStore,
  bootstrapAdminFromEnv,
  type DashboardSession,
  type DashboardUser,
  type OAuthConfig,
  OAuthConfigError,
  OAuthFlow,
  type OAuthProvider,
  oauthConfigFromEnv,
  type ProviderIdentity,
  ROLES,
  type Role,
} from "./auth";
export {
  createDashboardHandler,
  createDashboardServer,
  type DashboardHandlerOptions,
} from "./server";

/** Build the OAuth flow from env; invalid config logs and disables OAuth. */
function buildOauthFlowFromEnv(queue: Queue): OAuthFlow | undefined {
  try {
    const config = oauthConfigFromEnv();
    if (!config || !(config.google || config.github || config.oidc.length > 0)) {
      return undefined;
    }
    return new OAuthFlow(queue, config);
  } catch (error) {
    log.warn(() => `OAuth disabled — invalid configuration: ${String(error)}`);
    return undefined;
  }
}
