package org.byteveda.flexiq.dashboard.oauth;

import org.byteveda.flexiq.dashboard.auth.oauth.model.OAuthState;
import org.byteveda.flexiq.dashboard.auth.oauth.model.ProviderIdentity;
import org.byteveda.flexiq.dashboard.auth.oauth.provider.OAuthProvider;

/**
 * A network-free {@link OAuthProvider} for flow/integration tests. It records the
 * state it saw, returns a canned identity, and can be told to fail either the
 * code exchange or the allowlist check.
 */
public final class FakeOAuthProvider implements OAuthProvider {
    private final String slot;
    private final String label;
    private final String type;
    private ProviderIdentity identity;
    private RuntimeException exchangeError;
    private RuntimeException allowlistError;

    public OAuthState lastState;
    public String lastRedirectUri;

    public FakeOAuthProvider(String slot, String label, String type) {
        this.slot = slot;
        this.label = label;
        this.type = type;
        this.identity = new ProviderIdentity(slot, "subject-1", "user@example.com", true, "Test User", null);
    }

    public FakeOAuthProvider identity(ProviderIdentity value) {
        this.identity = value;
        return this;
    }

    public FakeOAuthProvider failExchange(RuntimeException error) {
        this.exchangeError = error;
        return this;
    }

    public FakeOAuthProvider denyAllowlist(RuntimeException error) {
        this.allowlistError = error;
        return this;
    }

    @Override
    public String slot() {
        return slot;
    }

    @Override
    public String label() {
        return label;
    }

    @Override
    public String type() {
        return type;
    }

    @Override
    public String authorizationUrl(OAuthState state, String redirectUri) {
        this.lastState = state;
        this.lastRedirectUri = redirectUri;
        return "https://provider.example/authorize?state=" + state.state();
    }

    @Override
    public ProviderIdentity exchangeCode(String code, String codeVerifier, String redirectUri, String expectedNonce) {
        if (exchangeError != null) {
            throw exchangeError;
        }
        return identity;
    }

    @Override
    public void checkAllowlist(ProviderIdentity id) {
        if (allowlistError != null) {
            throw allowlistError;
        }
    }
}
