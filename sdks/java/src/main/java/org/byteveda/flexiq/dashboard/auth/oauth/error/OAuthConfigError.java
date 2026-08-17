package org.byteveda.flexiq.dashboard.auth.oauth.error;

/** Env-var configuration is invalid or incomplete (fail-fast on partial setup). */
public final class OAuthConfigError extends OAuthException {
    private static final long serialVersionUID = 1L;

    public OAuthConfigError(String message) {
        super(message);
    }
}
