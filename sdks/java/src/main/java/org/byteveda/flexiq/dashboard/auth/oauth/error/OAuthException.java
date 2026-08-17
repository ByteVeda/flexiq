package org.byteveda.flexiq.dashboard.auth.oauth.error;

/** Base class for any OAuth-flow error surfaced to the handler layer. */
public class OAuthException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public OAuthException(String message) {
        super(message);
    }

    public OAuthException(String message, Throwable cause) {
        super(message, cause);
    }
}
