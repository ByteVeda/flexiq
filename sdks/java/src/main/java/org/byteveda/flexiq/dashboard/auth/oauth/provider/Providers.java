package org.byteveda.flexiq.dashboard.auth.oauth.provider;

import java.net.http.HttpClient;
import java.util.LinkedHashMap;
import java.util.Map;
import org.byteveda.flexiq.dashboard.auth.oauth.config.OAuthConfig;
import org.byteveda.flexiq.dashboard.auth.oauth.config.OAuthConfig.GitHubConfig;
import org.byteveda.flexiq.dashboard.auth.oauth.config.OAuthConfig.GoogleConfig;
import org.byteveda.flexiq.dashboard.auth.oauth.config.OAuthConfig.OidcConfig;
import org.byteveda.flexiq.dashboard.auth.oauth.config.OAuthConfig.ProviderConfig;

/**
 * Factory for the concrete provider implementations. Keeping it in this package
 * lets Google/GitHub/OIDC providers stay package-private while the orchestration
 * layer builds them by config type through one public entry point.
 */
public final class Providers {

    private Providers() {}

    /**
     * Instantiate one provider per configured slot, keyed by slot, in display order.
     *
     * @param config the parsed environment configuration
     * @param http the client each provider makes its token and userinfo calls on
     * @return the registry, keyed by slot
     */
    public static Map<String, OAuthProvider> build(OAuthConfig config, HttpClient http) {
        Map<String, OAuthProvider> registry = new LinkedHashMap<>();
        for (ProviderConfig entry : config.providers()) {
            registry.put(entry.slot(), build(entry, http));
        }
        return registry;
    }

    private static OAuthProvider build(ProviderConfig entry, HttpClient http) {
        if (entry instanceof GoogleConfig google) {
            return new GoogleProvider(google, http);
        }
        if (entry instanceof GitHubConfig github) {
            return new GitHubProvider(github, http);
        }
        if (entry instanceof OidcConfig oidc) {
            return new GenericOidcProvider(oidc, http);
        }
        throw new IllegalStateException("unknown provider config: " + entry);
    }
}
