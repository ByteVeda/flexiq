package org.byteveda.flexiq.dashboard.auth.oauth.error;

/** A token exchange, userinfo fetch, or id-token claim check failed. */
public final class IdentityFetchError extends OAuthException {
    private static final long serialVersionUID = 1L;

    /**
     * A failure this layer diagnosed itself — a missing claim, a rejected token.
     *
     * @param message which step failed, without echoing token material
     */
    public IdentityFetchError(String message) {
        super(message);
    }

    /**
     * A failure raised by the HTTP or JWT layer underneath.
     *
     * @param message which step failed, without echoing token material
     * @param cause the transport or parse failure
     */
    public IdentityFetchError(String message, Throwable cause) {
        super(message, cause);
    }
}
