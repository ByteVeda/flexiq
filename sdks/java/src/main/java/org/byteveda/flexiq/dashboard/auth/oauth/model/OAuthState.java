package org.byteveda.flexiq.dashboard.auth.oauth.model;

import org.jspecify.annotations.Nullable;

/**
 * One in-flight OAuth flow, stashed server-side between the authorize redirect
 * and the callback. {@code createdAt}/{@code expiresAt} are Unix <b>seconds</b>
 * (matching sessions). The {@code state} token is the KV key suffix and is never
 * serialised into the stored record.
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
