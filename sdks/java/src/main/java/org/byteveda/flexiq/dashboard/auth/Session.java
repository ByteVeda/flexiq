package org.byteveda.flexiq.dashboard.auth;

/**
 * An authenticated session. {@code createdAt}/{@code expiresAt} are Unix
 * <b>seconds</b> (unlike user/webhook timestamps, which are milliseconds) —
 * this matches the reference wire contract exactly. The {@code token} is the KV
 * key suffix and is never serialised into the stored record.
 *
 * @param token the session token — the KV key suffix, never part of the stored record
 * @param username the user this session authenticates
 * @param role that user's role at the time the session was issued
 * @param createdAt when the session was issued, in Unix <b>seconds</b>
 * @param expiresAt when it stops being accepted, in Unix <b>seconds</b>
 * @param csrfToken the token the double-submit check binds this session to
 */
public record Session(String token, String username, String role, long createdAt, long expiresAt, String csrfToken) {

    public boolean isExpired(long nowSeconds) {
        return nowSeconds >= expiresAt;
    }
}
