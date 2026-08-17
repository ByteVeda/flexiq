// End-to-end OAuth flow orchestration: the seam between the HTTP layer and
// the providers. Owns the provider registry, the state store, and the
// AuthStore integration.

import type { Queue } from "../../../index";
import { createLogger } from "../../../utils";
import { isSafeRedirect } from "../../urlSafety";
import { AuthStore, type DashboardSession } from "../store";
import { callbackUrl, configuredProviders, type OAuthConfig } from "./config";
import {
  IdentityFetchError,
  type OAuthProvider,
  ProviderNotConfigured,
  StateValidationError,
} from "./identity";
import { s256Challenge } from "./pkce";
import type { FetchLike } from "./providers";
import { GenericOidcProvider, GitHubProvider, GoogleProvider } from "./providers";
import { OAuthStateStore } from "./stateStore";

const log = createLogger("dashboard");

/** Instantiate one provider per configured slot, keyed by slot. */
export function buildProviders(
  config: OAuthConfig,
  http: FetchLike = fetch,
): Map<string, OAuthProvider> {
  const registry = new Map<string, OAuthProvider>();
  for (const entry of configuredProviders(config)) {
    if (entry.type === "google") {
      registry.set(entry.slot, new GoogleProvider(entry, http));
    } else if (entry.type === "github") {
      registry.set(entry.slot, new GitHubProvider(entry, http));
    } else {
      registry.set(entry.slot, new GenericOidcProvider(entry, http));
    }
  }
  return registry;
}

/** Ties together config, providers, state store, and the auth store. */
export class OAuthFlow {
  private readonly providers: Map<string, OAuthProvider>;
  private readonly stateStore: OAuthStateStore;

  constructor(
    private readonly queue: Queue,
    private readonly config: OAuthConfig,
    options: { providers?: Map<string, OAuthProvider>; stateStore?: OAuthStateStore } = {},
  ) {
    this.providers = options.providers ?? buildProviders(config);
    this.stateStore = options.stateStore ?? new OAuthStateStore(queue);
    if (this.providers.size > 0 && config.adminEmails.length === 0) {
      // OAuth users only ever get the viewer role without an allowlist, so an
      // OAuth-only deployment would silently have zero admins.
      log.warn(
        () =>
          "OAuth is configured without admin emails: every OAuth login gets the " +
          "viewer role. Set FLEXIQ_DASHBOARD_OAUTH_ADMIN_EMAILS (or " +
          "OAuthConfig.adminEmails) to grant admin access.",
      );
    }
  }

  get passwordAuthEnabled(): boolean {
    return this.config.passwordAuthEnabled;
  }

  hasProvider(slot: string): boolean {
    return this.providers.has(slot);
  }

  /** Compact provider summary for the login UI (no secrets). */
  providersListing(): Array<{ slot: string; label: string; type: string }> {
    return [...this.providers.values()].map((p) => ({
      slot: p.slot,
      label: p.label,
      type: p.type,
    }));
  }

  /**
   * Mint a state row and return the provider's authorize URL. `nextUrl` is
   * sanitised against {@link isSafeRedirect}, falling back to `/`.
   */
  async start(slot: string, nextUrl: string | null): Promise<string> {
    const provider = this.requireProvider(slot);
    const safeNext = nextUrl && isSafeRedirect(nextUrl) ? nextUrl : "/";
    const state = this.stateStore.create(slot, safeNext);
    return provider.authorizationUrl({
      state: state.state,
      nonce: state.nonce,
      codeChallenge: s256Challenge(state.codeVerifier),
      redirectUri: callbackUrl(this.config, slot),
    });
  }

  /**
   * Exchange `code` for an identity and create a session. Returns the
   * session plus the post-login redirect target. Throws
   * {@link StateValidationError} / {@link IdentityFetchError} /
   * {@link AllowlistDenied} for the handler layer to map.
   */
  async handleCallback(
    slot: string,
    params: { code: string | null; stateToken: string | null; error: string | null },
  ): Promise<{ session: DashboardSession; nextUrl: string }> {
    if (params.error) {
      throw new IdentityFetchError(`provider returned error: ${params.error}`);
    }
    if (!params.code || !params.stateToken) {
      throw new StateValidationError("missing code or state parameter");
    }

    const row = this.stateStore.consume(params.stateToken);
    if (!row) {
      throw new StateValidationError("state is invalid, expired, or already used");
    }
    if (row.slot !== slot) {
      throw new StateValidationError("state slot does not match callback slot");
    }

    const provider = this.requireProvider(slot);
    const identity = await provider.exchangeCode({
      code: params.code,
      codeVerifier: row.codeVerifier,
      redirectUri: callbackUrl(this.config, slot),
      expectedNonce: row.nonce,
    });
    provider.checkAllowlist(identity);

    const store = new AuthStore(this.queue);
    const user = store.getOrCreateOauthUser({
      slot: identity.slot,
      subject: identity.subject,
      email: identity.email,
      name: identity.name,
      emailVerified: identity.emailVerified,
      adminEmails: this.config.adminEmails,
    });
    return { session: store.createSession(user), nextUrl: row.nextUrl };
  }

  /** Best-effort sweep of expired state rows. */
  pruneState(): number {
    return this.stateStore.pruneExpired();
  }

  private requireProvider(slot: string): OAuthProvider {
    const provider = this.providers.get(slot);
    if (!provider) {
      throw new ProviderNotConfigured(`OAuth provider '${slot}' is not configured`);
    }
    return provider;
  }
}
