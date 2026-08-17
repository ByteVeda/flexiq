package org.byteveda.taskito.dashboard.oauth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.byteveda.taskito.dashboard.auth.oauth.config.OAuthConfig;
import org.byteveda.taskito.dashboard.auth.oauth.config.OAuthConfig.GitHubConfig;
import org.byteveda.taskito.dashboard.auth.oauth.config.OAuthConfig.GoogleConfig;
import org.byteveda.taskito.dashboard.auth.oauth.config.OAuthConfig.OidcConfig;
import org.byteveda.taskito.dashboard.auth.oauth.error.OAuthConfigError;
import org.junit.jupiter.api.Test;

class OAuthConfigTest {

    @Test
    void emptyWhenNothingConfigured() {
        assertTrue(OAuthConfig.fromEnv(Map.of()).isEmpty());
    }

    @Test
    void providerWithoutBaseUrlFailsFast() {
        Map<String, String> env =
                Map.of(OAuthConfig.ENV_GOOGLE_CLIENT_ID, "gid", OAuthConfig.ENV_GOOGLE_CLIENT_SECRET, "gsec");
        assertThrows(OAuthConfigError.class, () -> OAuthConfig.fromEnv(env));
    }

    @Test
    void googleWithoutSecretFails() {
        Map<String, String> env = Map.of(
                OAuthConfig.ENV_REDIRECT_BASE_URL, "https://dash.example",
                OAuthConfig.ENV_GOOGLE_CLIENT_ID, "gid");
        assertThrows(OAuthConfigError.class, () -> OAuthConfig.fromEnv(env));
    }

    @Test
    void parsesGoogleAndAdminEmails() {
        Map<String, String> env = Map.of(
                OAuthConfig.ENV_REDIRECT_BASE_URL, "https://dash.example/",
                OAuthConfig.ENV_GOOGLE_CLIENT_ID, "gid",
                OAuthConfig.ENV_GOOGLE_CLIENT_SECRET, "gsec",
                OAuthConfig.ENV_GOOGLE_ALLOWED_DOMAINS, "acme.com, corp.example",
                OAuthConfig.ENV_ADMIN_EMAILS, "boss@acme.com");
        OAuthConfig config = OAuthConfig.fromEnv(env).orElseThrow();
        assertTrue(config.isEnabled());
        assertEquals(List.of("acme.com", "corp.example"), config.google().allowedDomains());
        assertEquals(List.of("boss@acme.com"), config.adminEmails());
        // Trailing slash on the base URL is stripped before the callback path.
        assertEquals("https://dash.example/api/auth/oauth/callback/google", config.callbackUrl("google"));
    }

    @Test
    void parsesOidcSlot() {
        Map<String, String> env = Map.of(
                OAuthConfig.ENV_REDIRECT_BASE_URL,
                "https://dash.example",
                OAuthConfig.ENV_OIDC_PROVIDERS,
                "corp",
                "FLEXIQ_DASHBOARD_OAUTH_OIDC_CORP_CLIENT_ID",
                "cid",
                "FLEXIQ_DASHBOARD_OAUTH_OIDC_CORP_CLIENT_SECRET",
                "csec",
                "FLEXIQ_DASHBOARD_OAUTH_OIDC_CORP_DISCOVERY_URL",
                "https://idp.example/.well-known/openid-configuration");
        OAuthConfig config = OAuthConfig.fromEnv(env).orElseThrow();
        assertEquals(1, config.oidc().size());
        OidcConfig oidc = config.oidc().get(0);
        assertEquals("corp", oidc.slot());
        assertEquals("Corp", oidc.label()); // title-cased default
    }

    @Test
    void rejectsReservedAndMalformedOidcSlots() {
        assertThrows(
                OAuthConfigError.class,
                () -> new OidcConfig("google", "id", "sec", "https://idp/.well-known", List.of(), "G"));
        assertThrows(
                OAuthConfigError.class,
                () -> new OidcConfig("Bad Slot", "id", "sec", "https://idp/.well-known", List.of(), "x"));
    }

    @Test
    void redirectUrlAllowsHttpOnlyForLocalhost() {
        assertThrows(OAuthConfigError.class, () -> config("http://example.com"));
        assertFalse(config("http://localhost:8080").isEnabled()); // valid, just no providers
        assertFalse(config("https://example.com").isEnabled());
    }

    @Test
    void providersListingIsOrderedGoogleGithubOidc() {
        OAuthConfig config = new OAuthConfig(
                "https://dash.example",
                new GoogleConfig("gid", "gsec", List.of()),
                new GitHubConfig("ghid", "ghsec", List.of()),
                List.of(new OidcConfig("corp", "cid", "csec", "https://idp/.well-known", List.of(), "Corp")),
                true,
                List.of());
        List<String> slots = config.providersListing().stream()
                .map(p -> (String) p.get("slot"))
                .toList();
        assertEquals(List.of("google", "github", "corp"), slots);
    }

    private static OAuthConfig config(String baseUrl) {
        return new OAuthConfig(baseUrl, null, null, List.of(), true, List.of());
    }
}
