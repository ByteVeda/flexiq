package org.byteveda.flexiq.dashboard.auth.oauth;

import java.net.http.HttpClient;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.byteveda.flexiq.dashboard.auth.AuthStore;
import org.byteveda.flexiq.dashboard.auth.Session;
import org.byteveda.flexiq.dashboard.auth.User;
import org.byteveda.flexiq.dashboard.auth.oauth.config.OAuthConfig;
import org.byteveda.flexiq.dashboard.auth.oauth.error.IdentityFetchError;
import org.byteveda.flexiq.dashboard.auth.oauth.error.ProviderNotConfigured;
import org.byteveda.flexiq.dashboard.auth.oauth.error.StateValidationError;
import org.byteveda.flexiq.dashboard.auth.oauth.model.OAuthState;
import org.byteveda.flexiq.dashboard.auth.oauth.model.ProviderIdentity;
import org.byteveda.flexiq.dashboard.auth.oauth.provider.OAuthProvider;
import org.byteveda.flexiq.dashboard.auth.oauth.provider.Providers;
import org.byteveda.flexiq.logging.FlexiQLogger;
import org.jspecify.annotations.Nullable;

/**
 * The seam between the HTTP handler layer and the provider implementations. It
 * owns the provider registry, the state store, and the {@link AuthStore}
 * integration: handlers call {@link #start} to mint a redirect URL and
 * {@link #handleCallback} to land a session.
 */
public final class OAuthFlow {
    private static final FlexiQLogger LOG = FlexiQLogger.create("dashboard");

    private final AuthStore authStore;
    private final OAuthConfig config;
    private final OAuthStateStore stateStore;
    private final Map<String, OAuthProvider> providers;

    /**
     * A flow over one configured provider set.
     *
     * <p>Warns when providers are configured with no admin allowlist: every OAuth
     * login would then land the viewer role, leaving an OAuth-only deployment with
     * no administrator at all.
     *
     * @param authStore where the provisioned users and landed sessions live
     * @param config the parsed environment configuration
     * @param stateStore where the pending flows are stashed
     * @param providers the registry, keyed by slot; copied, and its iteration order
     *     is the login UI's display order
     */
    public OAuthFlow(
            AuthStore authStore, OAuthConfig config, OAuthStateStore stateStore, Map<String, OAuthProvider> providers) {
        this.authStore = authStore;
        this.config = config;
        this.stateStore = stateStore;
        this.providers = new LinkedHashMap<>(providers);
        if (!this.providers.isEmpty() && config.adminEmails().isEmpty()) {
            // OAuth users only ever get the viewer role without an allowlist, so an
            // OAuth-only deployment would silently have zero admins.
            LOG.warn("OAuth is configured without admin emails: every OAuth login gets the viewer role." + " Set "
                    + OAuthConfig.ENV_ADMIN_EMAILS + " (or OAuthConfig.adminEmails) to grant admin access.");
        }
    }

    /**
     * The landed session plus the sanitised post-login redirect target.
     *
     * @param session the session the completed flow landed
     * @param nextUrl the sanitised post-login redirect target, or {@code null} for the default
     */
    public record CallbackResult(Session session, @Nullable String nextUrl) {}

    /**
     * Instantiate one provider per configured slot, keyed by slot, in display order.
     *
     * @param config the parsed environment configuration
     * @param http the client each provider makes its token and userinfo calls on
     * @return the registry, keyed by slot
     */
    public static Map<String, OAuthProvider> buildProviders(OAuthConfig config, HttpClient http) {
        return Providers.build(config, http);
    }

    // ---- introspection -----------------------------------------------------

    /**
     * Whether password login is still offered alongside the providers.
     *
     * @return {@code false} when the deployment is OAuth-only
     */
    public boolean passwordAuthEnabled() {
        return config.passwordAuthEnabled();
    }

    /**
     * Whether a slot has a registered provider.
     *
     * @param slot the slot from the request path
     * @return whether it resolves
     */
    public boolean hasProvider(String slot) {
        return providers.containsKey(slot);
    }

    /**
     * Compact provider summary for the login UI (no secrets), in display order.
     *
     * @return one entry per provider: slot, label and type
     */
    public List<Map<String, Object>> providersListing() {
        List<Map<String, Object>> out = new ArrayList<>();
        for (OAuthProvider provider : providers.values()) {
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("slot", provider.slot());
            entry.put("label", provider.label());
            entry.put("type", provider.type());
            out.add(entry);
        }
        return out;
    }

    // ---- flow --------------------------------------------------------------

    /**
     * Mint a state row and return the provider's authorize URL. {@code nextUrl}
     * is sanitised against {@link UrlSafety#isSafeRedirect}, falling back to
     * {@code "/"}.
     *
     * @param slot which provider to start with
     * @param nextUrl where to land after login, or {@code null}; anything that is not
     *     a same-origin path is replaced with {@code "/"}
     * @return the provider's authorize URL to redirect the browser to
     */
    public String start(String slot, @Nullable String nextUrl) {
        OAuthProvider provider = requireProvider(slot);
        String safeNext = nextUrl != null && UrlSafety.isSafeRedirect(nextUrl) ? nextUrl : "/";
        OAuthState state = stateStore.create(slot, safeNext);
        return provider.authorizationUrl(state, config.callbackUrl(slot));
    }

    /**
     * Exchange {@code code} for an identity and create a session.
     *
     * @param slot which provider the callback route named
     * @param code the one-time code the provider returned, or {@code null}
     * @param stateToken the state the provider echoed back, or {@code null}
     * @param error the provider's own error parameter, or {@code null}
     * @return the landed session and its sanitised redirect target
     * @throws StateValidationError missing/expired/replayed state or a slot mismatch
     * @throws IdentityFetchError any token / userinfo / claim failure
     * @throws org.byteveda.flexiq.dashboard.auth.oauth.error.AllowlistDenied the
     *     identity is outside a configured allowlist
     * @throws ProviderNotConfigured the slot has no registered provider
     */
    public CallbackResult handleCallback(
            String slot, @Nullable String code, @Nullable String stateToken, @Nullable String error) {
        if (error != null && !error.isEmpty()) {
            throw new IdentityFetchError("provider returned error: " + error);
        }
        if (code == null || code.isEmpty() || stateToken == null || stateToken.isEmpty()) {
            throw new StateValidationError("missing code or state parameter");
        }
        Optional<OAuthState> row = stateStore.consume(stateToken);
        if (row.isEmpty()) {
            throw new StateValidationError("state is invalid, expired, or already used");
        }
        OAuthState state = row.get();
        // slot is the non-null callback route param; compare from it so a null
        // slot on a malformed-but-parsed state row is rejected, not an NPE.
        if (!slot.equals(state.slot())) {
            throw new StateValidationError("state slot does not match callback slot");
        }

        OAuthProvider provider = requireProvider(slot);
        ProviderIdentity identity =
                provider.exchangeCode(code, state.codeVerifier(), config.callbackUrl(slot), state.nonce());
        provider.checkAllowlist(identity);

        User user = authStore.getOrCreateOauthUser(
                identity.slot(),
                identity.subject(),
                identity.email(),
                identity.name(),
                identity.emailVerified(),
                config.adminEmails());
        Session session = authStore.createSession(user.username(), user.role());
        stateStore.pruneExpiredIfDue();
        return new CallbackResult(session, state.nextUrl());
    }

    private OAuthProvider requireProvider(String slot) {
        OAuthProvider provider = providers.get(slot);
        if (provider == null) {
            throw new ProviderNotConfigured("OAuth provider '" + slot + "' is not configured");
        }
        return provider;
    }
}
