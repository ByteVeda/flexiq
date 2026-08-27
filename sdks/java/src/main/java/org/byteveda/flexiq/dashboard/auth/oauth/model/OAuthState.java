package org.byteveda.flexiq.dashboard.auth.oauth.model;

import org.jspecify.annotations.Nullable;

/**
 * One in-flight OAuth flow, stashed server-side between the authorize redirect
 * and the callback. {@code createdAt}/{@code expiresAt} are Unix <b>seconds</b>
 * (matching sessions). The {@code state} token is the KV key suffix and is never
 * serialised into the stored record.
 *
 * @param state the CSRF token tying the callback to this flow — the KV key suffix, never part of the stored record
 * @param nonce the value the provider must echo back in its id token
 * @param codeVerifier this flow's PKCE verifier
 * @param slot which configured provider the flow was started against
 * @param nextUrl where to land after login, or {@code null} for the default
 * @param createdAt when the flow started, in Unix <b>seconds</b>
 * @param expiresAt when the flow stops being accepted, in Unix <b>seconds</b>
 */
public record OAuthState(
        String state,
        String nonce,
        String codeVerifier,
        String slot,
        @Nullable String nextUrl,
        long createdAt,
        long expiresAt) {

    public boolean isExpired(long nowSeconds) {
        return nowSeconds >= expiresAt;
    }
}
