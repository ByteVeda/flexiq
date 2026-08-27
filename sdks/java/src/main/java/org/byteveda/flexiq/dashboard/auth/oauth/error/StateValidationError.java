package org.byteveda.flexiq.dashboard.auth.oauth.error;

/** The callback state is missing, expired, replayed, or does not match the slot. */
public final class StateValidationError extends OAuthException {
    private static final long serialVersionUID = 1L;

    /**
     * A callback whose state does not stand up.
     *
     * @param message which check failed — missing, expired, replayed, or wrong slot
     */
    public StateValidationError(String message) {
        super(message);
    }
}
