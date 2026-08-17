package org.byteveda.flexiq.dashboard.auth.oauth.error;

/** A token exchange, userinfo fetch, or id-token claim check failed. */
public final class IdentityFetchError extends OAuthException {
    private static final long serialVersionUID = 1L;

    public IdentityFetchError(String message) {
        super(message);
    }

    public IdentityFetchError(String message, Throwable cause) {
        super(message, cause);
    }
}
