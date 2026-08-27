package org.byteveda.flexiq.dashboard.auth.oauth.error;

/** Env-var configuration is invalid or incomplete (fail-fast on partial setup). */
public final class OAuthConfigError extends OAuthException {
    private static final long serialVersionUID = 1L;

    /**
     * A configuration that cannot be used as written.
     *
     * @param message which variable is missing or contradictory
     */
    public OAuthConfigError(String message) {
        super(message);
    }
}
