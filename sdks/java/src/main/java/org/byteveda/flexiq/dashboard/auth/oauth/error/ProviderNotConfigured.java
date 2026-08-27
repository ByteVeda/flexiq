package org.byteveda.flexiq.dashboard.auth.oauth.error;

/** A request referenced an OAuth slot that is not registered. */
public final class ProviderNotConfigured extends OAuthException {
    private static final long serialVersionUID = 1L;

    /**
     * A request for a slot no provider is registered under.
     *
     * @param message which slot was asked for
     */
    public ProviderNotConfigured(String message) {
        super(message);
    }
}
