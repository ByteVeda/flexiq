package org.byteveda.flexiq.dashboard.auth.oauth.provider;

import java.net.http.HttpClient;
import org.byteveda.flexiq.dashboard.auth.oauth.config.OAuthConfig.OidcConfig;
import org.byteveda.flexiq.dashboard.auth.oauth.model.ProviderIdentity;

/** A generic OIDC provider (Okta, Auth0, Keycloak, Entra, …) driven by discovery. */
final class GenericOidcProvider extends AbstractOidcProvider {
    private final OidcConfig config;

    GenericOidcProvider(OidcConfig config, HttpClient http) {
        super(http);
        this.config = config;
    }

    @Override
    public String slot() {
        return config.slot();
    }

    @Override
    public String label() {
        return config.label();
    }

    @Override
    public String type() {
        return "oidc";
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
        return config.discoveryUrl();
    }

    @Override
    public void checkAllowlist(ProviderIdentity identity) {
        checkDomainAllowlist(config.allowedDomains(), identity);
    }
}
