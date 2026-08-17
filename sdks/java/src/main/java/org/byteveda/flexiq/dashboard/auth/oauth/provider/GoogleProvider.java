package org.byteveda.flexiq.dashboard.auth.oauth.provider;

import java.net.http.HttpClient;
import java.util.LinkedHashMap;
import java.util.Map;
import org.byteveda.flexiq.dashboard.auth.oauth.config.OAuthConfig.GoogleConfig;
import org.byteveda.flexiq.dashboard.auth.oauth.model.ProviderIdentity;

/** Google Sign-In over OIDC, with optional Google-Workspace domain allowlisting. */
final class GoogleProvider extends AbstractOidcProvider {
    private static final String DISCOVERY_URL = "https://accounts.google.com/.well-known/openid-configuration";

    private final GoogleConfig config;

    GoogleProvider(GoogleConfig config, HttpClient http) {
        super(http);
        this.config = config;
    }

    @Override
    public String slot() {
        return "google";
    }

    @Override
    public String label() {
        return "Google";
    }

    @Override
    public String type() {
        return "google";
    }

    @Override
    protected String clientId() {
        return config.clientId();
    }

    @Override
    protected String clientSecret() {
        return config.clientSecret();
    }

    @Override
    protected String discoveryUrl() {
        return DISCOVERY_URL;
    }

    @Override
    protected Map<String, String> extraAuthParams() {
        Map<String, String> params = new LinkedHashMap<>();
        params.put("prompt", "select_account");
        // A single allowed domain is passed as a UX hint so Google pre-selects the
        // right account; enforcement still happens in checkAllowlist.
        if (config.allowedDomains().size() == 1) {
            params.put("hd", config.allowedDomains().get(0));
        }
        return params;
    }

    @Override
    public void checkAllowlist(ProviderIdentity identity) {
        checkDomainAllowlist(config.allowedDomains(), identity);
    }
}
