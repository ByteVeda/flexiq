package org.byteveda.flexiq.dashboard.oauth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.byteveda.flexiq.dashboard.InMemorySettings;
import org.byteveda.flexiq.dashboard.auth.AuthStore;
import org.byteveda.flexiq.dashboard.auth.Role;
import org.byteveda.flexiq.dashboard.auth.oauth.OAuthFlow;
import org.byteveda.flexiq.dashboard.auth.oauth.OAuthStateStore;
import org.byteveda.flexiq.dashboard.auth.oauth.config.OAuthConfig;
import org.byteveda.flexiq.dashboard.auth.oauth.error.AllowlistDenied;
import org.byteveda.flexiq.dashboard.auth.oauth.error.IdentityFetchError;
import org.byteveda.flexiq.dashboard.auth.oauth.error.ProviderNotConfigured;
import org.byteveda.flexiq.dashboard.auth.oauth.error.StateValidationError;
import org.byteveda.flexiq.dashboard.auth.oauth.provider.OAuthProvider;
import org.junit.jupiter.api.Test;

class OAuthFlowTest {

    private final InMemorySettings settings = new InMemorySettings();
    private final AuthStore authStore = new AuthStore(settings);
    private final OAuthConfig config = new OAuthConfig("https://dash.example", null, null, List.of(), true, List.of());
    private final FakeOAuthProvider fake = new FakeOAuthProvider("fake", "Fake", "oidc");
    private final OAuthFlow flow = flowWith(fake);

    private OAuthFlow flowWith(FakeOAuthProvider provider) {
        Map<String, OAuthProvider> providers = Map.of(provider.slot(), provider);
        return new OAuthFlow(authStore, config, new OAuthStateStore(settings), providers);
    }

    @Test
    void startStashesStateAndBuildsAuthorizeUrl() {
        String url = flow.start("fake", "/dashboard");
        assertTrue(url.startsWith("https://provider.example/authorize?state="));
        assertNotNull(fake.lastState);
        assertEquals("/dashboard", fake.lastState.nextUrl());
        assertEquals("https://dash.example/api/auth/oauth/callback/fake", fake.lastRedirectUri);
    }

    @Test
    void startSanitizesUnsafeNext() {
        flow.start("fake", "//evil.com");
        assertEquals("/", fake.lastState.nextUrl());
    }

    @Test
    void startRejectsUnknownProvider() {
        assertThrows(ProviderNotConfigured.class, () -> flow.start("missing", "/"));
    }

    @Test
    void handleCallbackLandsSessionAsViewerWithoutAllowlist() {
        flow.start("fake", "/next");
        String state = fake.lastState.state();
        OAuthFlow.CallbackResult result = flow.handleCallback("fake", "code", state, null);
        assertEquals("fake:subject-1", result.session().username());
        assertEquals(Role.VIEWER.wire(), result.session().role()); // admin comes only from the allowlist
        assertEquals("/next", result.nextUrl());
        assertTrue(authStore.getUser("fake:subject-1").isPresent());
    }

    @Test
    void handleCallbackStateIsSingleUse() {
        flow.start("fake", "/");
        String state = fake.lastState.state();
        flow.handleCallback("fake", "code", state, null);
        assertThrows(StateValidationError.class, () -> flow.handleCallback("fake", "code", state, null));
    }

    @Test
    void handleCallbackRejectsMissingParamsAndProviderError() {
        assertThrows(StateValidationError.class, () -> flow.handleCallback("fake", null, null, null));
        assertThrows(IdentityFetchError.class, () -> flow.handleCallback("fake", null, null, "access_denied"));
    }

    @Test
    void handleCallbackRejectsSlotMismatch() {
        flow.start("fake", "/");
        String state = fake.lastState.state();
        // A state minted for "fake" cannot be redeemed at a different slot.
        assertThrows(StateValidationError.class, () -> flow.handleCallback("other", "code", state, null));
    }

    @Test
    void handleCallbackPropagatesAllowlistDenial() {
        fake.denyAllowlist(new AllowlistDenied("outside allowlist"));
        flow.start("fake", "/");
        String state = fake.lastState.state();
        assertThrows(AllowlistDenied.class, () -> flow.handleCallback("fake", "code", state, null));
    }
}
