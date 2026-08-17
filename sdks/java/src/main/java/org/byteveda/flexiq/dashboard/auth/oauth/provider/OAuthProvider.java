package org.byteveda.flexiq.dashboard.auth.oauth.provider;

import org.byteveda.flexiq.dashboard.auth.oauth.model.OAuthState;
import org.byteveda.flexiq.dashboard.auth.oauth.model.ProviderIdentity;

/**
 * Contract every concrete provider (Google, GitHub, generic OIDC) satisfies.
 *
 * <p>The split between {@link #exchangeCode} (network IO + claim normalisation)
 * and {@link #checkAllowlist} (pure-data permission check) is deliberate so
 * tests can drive either path in isolation. GitHub is the exception: its org
 * membership needs the access token, so it enforces the allowlist inside
 * {@code exchangeCode} and leaves {@code checkAllowlist} a no-op.
 */
public interface OAuthProvider {

    /** URL-safe registry key used in the callback path ({@code google}, …). */
    String slot();

    /** Human-readable button label rendered by the dashboard. */
    String label();

    /** One of {@code "google"}, {@code "github"}, {@code "oidc"} — picks the icon. */
    String type();

    /** Build the provider-side authorize URL the browser is redirected to. */
    String authorizationUrl(OAuthState state, String redirectUri);

    /** Exchange the auth code for an identity, raising on any failure. */
    ProviderIdentity exchangeCode(String code, String codeVerifier, String redirectUri, String expectedNonce);

    /** Raise {@code AllowlistDenied} if the identity is not permitted. */
    void checkAllowlist(ProviderIdentity identity);
}
