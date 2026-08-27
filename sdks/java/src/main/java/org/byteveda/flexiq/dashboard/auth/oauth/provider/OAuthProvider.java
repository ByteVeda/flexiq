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

    /**
     * URL-safe registry key used in the callback path ({@code google}, …).
     *
     * @return the slot
     */
    String slot();

    /**
     * Human-readable button label rendered by the dashboard.
     *
     * @return the label
     */
    String label();

    /**
     * One of {@code "google"}, {@code "github"}, {@code "oidc"} — picks the icon.
     *
     * @return the type
     */
    String type();

    /**
     * Build the provider-side authorize URL the browser is redirected to.
     *
     * @param state the pending flow, carrying its PKCE challenge and nonce
     * @param redirectUri where the provider must send the browser back
     * @return the URL to redirect to
     */
    String authorizationUrl(OAuthState state, String redirectUri);

    /**
     * Exchange the auth code for an identity, raising on any failure.
     *
     * @param code the one-time code the callback carried
     * @param codeVerifier the PKCE verifier stashed when the flow started
     * @param redirectUri the same URI the authorize step declared
     * @param expectedNonce the nonce the id token must echo
     * @return the normalised identity
     */
    ProviderIdentity exchangeCode(String code, String codeVerifier, String redirectUri, String expectedNonce);

    /**
     * Raise {@code AllowlistDenied} if the identity is not permitted.
     *
     * @param identity the identity {@link #exchangeCode} produced; a provider that
     *     already enforced its allowlist there leaves this a no-op
     */
    void checkAllowlist(ProviderIdentity identity);
}
