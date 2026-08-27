package org.byteveda.flexiq.dashboard.auth.oauth.error;

/** Base class for any OAuth-flow error surfaced to the handler layer. */
public class OAuthException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    /**
     * An OAuth failure with nothing underlying it.
     *
     * @param message what went wrong, safe to log
     */
    public OAuthException(String message) {
        super(message);
    }

    /**
     * An OAuth failure raised by something underneath.
     *
     * @param message what went wrong, safe to log
     * @param cause the underlying failure
     */
    public OAuthException(String message, Throwable cause) {
        super(message, cause);
    }
}
