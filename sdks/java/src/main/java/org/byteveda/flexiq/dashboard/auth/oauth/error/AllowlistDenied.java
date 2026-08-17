package org.byteveda.flexiq.dashboard.auth.oauth.error;

/** A verified identity was rejected by a configured domain/org allowlist. */
public final class AllowlistDenied extends OAuthException {
    private static final long serialVersionUID = 1L;

    public AllowlistDenied(String message) {
        super(message);
    }
}
