// Operator-facing OAuth configuration parsed from environment variables.
// The variable names are part of the cross-SDK contract; secrets are never
// stored in the dashboard settings DB.

/** Raised when env-var configuration is invalid. */
export class OAuthConfigError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "OAuthConfigError";
  }
}

const SLOT_RE = /^[a-z][a-z0-9_-]{0,31}$/;
const RESERVED_SLOTS = new Set(["google", "github"]);

// Hostnames where http:// is accepted for `redirectBaseUrl` (dev only).
const LOCAL_HOSTS = new Set(["localhost", "127.0.0.1", "::1", "[::1]"]);

export interface GoogleConfig {
  type: "google";
  slot: "google";
  label: string;
  clientId: string;
  clientSecret: string;
  allowedDomains: string[];
}

export interface GitHubConfig {
  type: "github";
  slot: "github";
  label: string;
  clientId: string;
  clientSecret: string;
  allowedOrgs: string[];
}

export interface OidcConfig {
  type: "oidc";
  slot: string;
  label: string;
  clientId: string;
  clientSecret: string;
  discoveryUrl: string;
  allowedDomains: string[];
}

export type ProviderConfig = GoogleConfig | GitHubConfig | OidcConfig;

/** Top-level OAuth configuration. */
export interface OAuthConfig {
  /** Public origin the dashboard is served at; callback URLs derive from it. */
  redirectBaseUrl: string;
  google?: GoogleConfig;
  github?: GitHubConfig;
  oidc: OidcConfig[];
  passwordAuthEnabled: boolean;
  adminEmails: string[];
}

/** Configured providers in display order: Google, GitHub, then OIDC slots. */
export function configuredProviders(config: OAuthConfig): ProviderConfig[] {
  const out: ProviderConfig[] = [];
  if (config.google) {
    out.push(config.google);
  }
  if (config.github) {
    out.push(config.github);
  }
  out.push(...config.oidc);
  return out;
}

export function callbackUrl(config: OAuthConfig, slot: string): string {
  let base = config.redirectBaseUrl;
  // Strip trailing slashes without a regex — the base URL is operator
  // input, and `/\/+$/` backtracks polynomially on long slash runs.
  while (base.endsWith("/")) {
    base = base.slice(0, -1);
  }
  return `${base}/api/auth/oauth/callback/${slot}`;
}

function validateRedirectBaseUrl(url: string): void {
  if (!url) {
    throw new OAuthConfigError("redirect_base_url must be set when OAuth is enabled");
  }
  let parsed: URL;
  try {
    parsed = new URL(url);
  } catch {
    throw new OAuthConfigError(`redirect_base_url is not a valid URL: ${url}`);
  }
  if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
    throw new OAuthConfigError(`redirect_base_url must be http(s), got '${parsed.protocol}'`);
  }
  if (parsed.protocol === "http:" && !LOCAL_HOSTS.has(parsed.hostname)) {
    throw new OAuthConfigError(
      `redirect_base_url must use https for non-local hosts (got http://${parsed.hostname})`,
    );
  }
}

function splitCsv(raw: string | undefined): string[] {
  if (!raw) {
    return [];
  }
  return raw
    .split(",")
    .map((part) => part.trim())
    .filter((part) => part.length > 0);
}

function parseBool(raw: string | undefined, fallback: boolean): boolean {
  const lowered = (raw ?? "").trim().toLowerCase();
  if (["1", "true", "yes", "on"].includes(lowered)) {
    return true;
  }
  if (["0", "false", "no", "off"].includes(lowered)) {
    return false;
  }
  return fallback;
}

/**
 * Parse an {@link OAuthConfig} from the environment. Returns `undefined`
 * when OAuth is not configured at all; throws {@link OAuthConfigError} on
 * partial configuration (fail-fast).
 */
export function oauthConfigFromEnv(
  env: Record<string, string | undefined> = process.env,
): OAuthConfig | undefined {
  const baseUrl = (env.FLEXIQ_DASHBOARD_OAUTH_REDIRECT_BASE_URL ?? "").trim();
  const googleId = (env.FLEXIQ_DASHBOARD_OAUTH_GOOGLE_CLIENT_ID ?? "").trim();
  const githubId = (env.FLEXIQ_DASHBOARD_OAUTH_GITHUB_CLIENT_ID ?? "").trim();
  const oidcSlotsRaw = (env.FLEXIQ_DASHBOARD_OAUTH_OIDC_PROVIDERS ?? "").trim();

  const anyProviderSignal = Boolean(googleId || githubId || oidcSlotsRaw);
  if (!anyProviderSignal && !baseUrl) {
    return undefined;
  }
  if (anyProviderSignal && !baseUrl) {
    throw new OAuthConfigError(
      "FLEXIQ_DASHBOARD_OAUTH_REDIRECT_BASE_URL must be set when any OAuth provider is configured",
    );
  }
  validateRedirectBaseUrl(baseUrl);

  const config: OAuthConfig = {
    redirectBaseUrl: baseUrl,
    google: googleId ? parseGoogle(env) : undefined,
    github: githubId ? parseGithub(env) : undefined,
    oidc: parseOidcSlots(env, oidcSlotsRaw),
    passwordAuthEnabled: parseBool(env.FLEXIQ_DASHBOARD_PASSWORD_AUTH_ENABLED, true),
    adminEmails: splitCsv(env.FLEXIQ_DASHBOARD_OAUTH_ADMIN_EMAILS),
  };

  const enabled = Boolean(config.google || config.github || config.oidc.length > 0);
  if (!enabled && !config.passwordAuthEnabled) {
    throw new OAuthConfigError(
      "password auth disabled but no OAuth providers configured — no way to log in",
    );
  }
  return config;
}

function parseGoogle(env: Record<string, string | undefined>): GoogleConfig {
  const clientId = (env.FLEXIQ_DASHBOARD_OAUTH_GOOGLE_CLIENT_ID ?? "").trim();
  const clientSecret = (env.FLEXIQ_DASHBOARD_OAUTH_GOOGLE_CLIENT_SECRET ?? "").trim();
  if (!clientSecret) {
    throw new OAuthConfigError(
      "FLEXIQ_DASHBOARD_OAUTH_GOOGLE_CLIENT_SECRET is required when google client_id is set",
    );
  }
  return {
    type: "google",
    slot: "google",
    label: "Google",
    clientId,
    clientSecret,
    allowedDomains: splitCsv(env.FLEXIQ_DASHBOARD_OAUTH_GOOGLE_ALLOWED_DOMAINS),
  };
}

function parseGithub(env: Record<string, string | undefined>): GitHubConfig {
  const clientId = (env.FLEXIQ_DASHBOARD_OAUTH_GITHUB_CLIENT_ID ?? "").trim();
  const clientSecret = (env.FLEXIQ_DASHBOARD_OAUTH_GITHUB_CLIENT_SECRET ?? "").trim();
  if (!clientSecret) {
    throw new OAuthConfigError(
      "FLEXIQ_DASHBOARD_OAUTH_GITHUB_CLIENT_SECRET is required when github client_id is set",
    );
  }
  return {
    type: "github",
    slot: "github",
    label: "GitHub",
    clientId,
    clientSecret,
    allowedOrgs: splitCsv(env.FLEXIQ_DASHBOARD_OAUTH_GITHUB_ALLOWED_ORGS),
  };
}

function parseOidcSlots(env: Record<string, string | undefined>, slotsRaw: string): OidcConfig[] {
  const slots = splitCsv(slotsRaw);
  const seen = new Set<string>();
  // `foo-bar` and `foo_bar` are distinct slots but read the same
  // `..._FOO_BAR_*` env vars — reject the collision instead of silently
  // sharing one credential set.
  const seenPrefixes = new Map<string, string>();
  const out: OidcConfig[] = [];
  for (const rawSlot of slots) {
    const slot = rawSlot.toLowerCase();
    if (seen.has(slot)) {
      throw new OAuthConfigError(
        `OIDC slot '${slot}' listed twice in FLEXIQ_DASHBOARD_OAUTH_OIDC_PROVIDERS`,
      );
    }
    seen.add(slot);
    const prefix = envPrefix(slot);
    const collidesWith = seenPrefixes.get(prefix);
    if (collidesWith !== undefined) {
      throw new OAuthConfigError(
        `OIDC slots '${collidesWith}' and '${slot}' share the env prefix ${prefix}_*`,
      );
    }
    seenPrefixes.set(prefix, slot);
    out.push(parseOidcSlot(env, slot));
  }
  return out;
}

function envPrefix(slot: string): string {
  return `FLEXIQ_DASHBOARD_OAUTH_OIDC_${slot.toUpperCase().replace(/-/g, "_")}`;
}

function parseOidcSlot(env: Record<string, string | undefined>, slot: string): OidcConfig {
  if (!SLOT_RE.test(slot)) {
    throw new OAuthConfigError(`OIDC slot '${slot}' must match ${SLOT_RE.source}`);
  }
  if (RESERVED_SLOTS.has(slot)) {
    throw new OAuthConfigError(`OIDC slot '${slot}' collides with built-in provider`);
  }
  const prefix = envPrefix(slot);
  const clientId = (env[`${prefix}_CLIENT_ID`] ?? "").trim();
  const clientSecret = (env[`${prefix}_CLIENT_SECRET`] ?? "").trim();
  const discoveryUrl = (env[`${prefix}_DISCOVERY_URL`] ?? "").trim();
  if (!clientId || !clientSecret || !discoveryUrl) {
    throw new OAuthConfigError(
      `OIDC slot '${slot}' requires ${prefix}_CLIENT_ID, _CLIENT_SECRET, and _DISCOVERY_URL`,
    );
  }
  const defaultLabel = slot.replace(/[-_]/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
  return {
    type: "oidc",
    slot,
    label: (env[`${prefix}_LABEL`] ?? "").trim() || defaultLabel,
    clientId,
    clientSecret,
    discoveryUrl,
    allowedDomains: splitCsv(env[`${prefix}_ALLOWED_DOMAINS`]),
  };
}
