package org.byteveda.flexiq.dashboard.auth;

/**
 * An authenticated session. {@code createdAt}/{@code expiresAt} are Unix
 * <b>seconds</b> (unlike user/webhook timestamps, which are milliseconds) —
 * this matches the reference wire contract exactly. The {@code token} is the KV
 * key suffix and is never serialised into the stored record.
 */
public record Session(String token, String username, String role, long createdAt, long expiresAt, String csrfToken) {

    public boolean isExpired(long nowSeconds) {
        return nowSeconds >= expiresAt;
    }
}
