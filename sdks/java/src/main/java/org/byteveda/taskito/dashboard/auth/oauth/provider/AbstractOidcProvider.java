package org.byteveda.taskito.dashboard.auth.oauth.provider;

import java.net.URI;
import java.net.http.HttpClient;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.byteveda.taskito.dashboard.auth.oauth.error.AllowlistDenied;
import org.byteveda.taskito.dashboard.auth.oauth.error.IdentityFetchError;
import org.byteveda.taskito.dashboard.auth.oauth.model.OAuthState;
import org.byteveda.taskito.dashboard.auth.oauth.model.ProviderIdentity;
import org.byteveda.taskito.dashboard.support.Json;
import org.jspecify.annotations.Nullable;

/**
 * Shared OIDC machinery for Google and generic OIDC providers: discovery-doc
 * caching, the authorize-URL builder, the token exchange, and id-token
 * validation.
 *
 * <p>Holds an {@link IdTokenValidator}; constructing a subclass therefore forces
 * the nimbus dependency to load, so its absence is caught at flow-build time.
 * All endpoints read from the discovery document are forced to https (http only
 * for localhost) — an SSRF guard against a poisoned discovery doc redirecting
 * the token/JWKS fetch at an internal address.
 */
abstract class AbstractOidcProvider implements OAuthProvider {
    private static final String SCOPE = "openid email profile";
    /** Hosts where a discovered endpoint may be reached over plain http (dev only). */
    private static final Set<String> LOCAL_HOSTS = Set.of("localhost", "127.0.0.1", "::1");

    private final HttpClient http;
    private final IdTokenValidator validator;
    private volatile @Nullable Map<String, Object> discovery;

    AbstractOidcProvider(HttpClient http) {
        this.http = http;
        this.validator = new IdTokenValidator();
    }

    protected abstract String clientId();

    protected abstract String clientSecret();

    protected abstract String discoveryUrl();

    /** Provider-specific authorize hints (e.g. Google's {@code prompt}/{@code hd}). */
    protected Map<String, String> extraAuthParams() {
        return Map.of();
    }

    @Override
    public final String authorizationUrl(OAuthState state, String redirectUri) {
        Map<String, String> params = new LinkedHashMap<>();
        params.put("response_type", "code");
        params.put("client_id", clientId());
        params.put("redirect_uri", redirectUri);
        params.put("scope", SCOPE);
        params.put("state", state.state());
        params.put("nonce", state.nonce());
        params.put("code_challenge", Pkce.challenge(state.codeVerifier()));
        params.put("code_challenge_method", Pkce.METHOD);
        params.putAll(extraAuthParams());
        return endpoint("authorization_endpoint") + "?" + OAuthHttp.formEncode(params);
    }

    @Override
    public final ProviderIdentity exchangeCode(
            String code, String codeVerifier, String redirectUri, String expectedNonce) {
        Map<String, Object> token = fetchToken(code, codeVerifier, redirectUri);
        Object idToken = token.get("id_token");
        if (!(idToken instanceof String rawIdToken) || rawIdToken.isEmpty()) {
            throw new IdentityFetchError("no id_token in token response");
        }
        Map<String, Object> claims = validator.validate(
                rawIdToken,
                endpoint("jwks_uri"),
                requiredField(discovery(), "issuer"),
                clientId(),
                expectedNonce,
                System.currentTimeMillis() / 1000);
        return new ProviderIdentity(
                slot(),
                String.valueOf(claims.get("sub")),
                stringClaim(claims, "email"),
                Boolean.TRUE.equals(claims.get("email_verified")),
                stringClaim(claims, "name"),
                stringClaim(claims, "picture"));
    }

    private Map<String, Object> fetchToken(String code, String codeVerifier, String redirectUri) {
        Map<String, String> form = new LinkedHashMap<>();
        form.put("grant_type", "authorization_code");
        form.put("code", code);
        form.put("redirect_uri", redirectUri);
        form.put("code_verifier", codeVerifier);
        form.put("client_id", clientId());
        form.put("client_secret", clientSecret());
        OAuthHttp.HttpResult result = OAuthHttp.postForm(http, endpoint("token_endpoint"), form, Map.of());
        if (result.status() >= 400) {
            throw new IdentityFetchError("token exchange failed with status " + result.status());
        }
        Map<String, Object> token = Json.parseMap(result.body());
        if (token == null) {
            throw new IdentityFetchError("token endpoint returned a non-JSON body");
        }
        return token;
    }

    private Map<String, Object> discovery() {
        Map<String, Object> local = discovery;
        if (local != null) {
            return local;
        }
        synchronized (this) {
            if (discovery == null) {
                OAuthHttp.HttpResult result = OAuthHttp.get(http, discoveryUrl(), Map.of());
                if (result.status() >= 400) {
                    throw new IdentityFetchError("discovery document fetch failed with status " + result.status());
                }
                Map<String, Object> parsed = Json.parseMap(result.body());
                if (parsed == null) {
                    throw new IdentityFetchError("discovery document is not a JSON object");
                }
                discovery = parsed;
            }
            return discovery;
        }
    }

    /** Read a discovered endpoint URL and force https on it (localhost may use http). */
    private String endpoint(String key) {
        return enforceHttps(requiredField(discovery(), key));
    }

    /**
     * Shared domain allowlist check (Google + generic OIDC). A non-empty
     * allowlist requires a verified email whose domain is listed.
     */
    protected static void checkDomainAllowlist(List<String> allowedDomains, ProviderIdentity identity) {
        if (allowedDomains.isEmpty()) {
            return;
        }
        if (identity.email() == null || !identity.emailVerified()) {
            throw new AllowlistDenied("a verified email is required for the domain check");
        }
        Set<String> allowed =
                allowedDomains.stream().map(d -> d.toLowerCase(Locale.ROOT)).collect(Collectors.toSet());
        String domain = emailDomain(identity.email());
        if (domain == null || !allowed.contains(domain)) {
            throw new AllowlistDenied("email domain is not in the allowed domains list");
        }
    }

    private static @Nullable String emailDomain(String email) {
        int at = email.lastIndexOf('@');
        if (at < 0 || at == email.length() - 1) {
            return null;
        }
        return email.substring(at + 1).toLowerCase(Locale.ROOT);
    }

    private static String enforceHttps(String url) {
        if (url == null || url.isEmpty()) {
            throw new IdentityFetchError("discovery document is missing a required endpoint");
        }
        URI uri;
        try {
            uri = URI.create(url);
        } catch (IllegalArgumentException e) {
            throw new IdentityFetchError("discovered endpoint is not a valid URL: " + url);
        }
        String scheme = uri.getScheme() == null ? "" : uri.getScheme().toLowerCase(Locale.ROOT);
        if (scheme.equals("https")) {
            return url;
        }
        if (scheme.equals("http") && LOCAL_HOSTS.contains(hostOf(uri))) {
            return url;
        }
        throw new IdentityFetchError("discovered endpoint must use https: " + url);
    }

    private static String hostOf(URI uri) {
        String host = uri.getHost();
        if (host == null) {
            return "";
        }
        if (host.startsWith("[") && host.endsWith("]")) {
            host = host.substring(1, host.length() - 1);
        }
        return host.toLowerCase(Locale.ROOT);
    }

    /** A discovery-document field the flow cannot proceed without. */
    private static String requiredField(Map<String, Object> map, String key) {
        String value = stringField(map, key);
        if (value == null) {
            throw new IdentityFetchError("OIDC discovery document has no '" + key + "'");
        }
        return value;
    }

    private static @Nullable String stringField(Map<String, Object> map, String key) {
        Object value = map.get(key);
        return value instanceof String s ? s : null;
    }

    private static @Nullable String stringClaim(Map<String, Object> claims, String key) {
        Object value = claims.get(key);
        return value instanceof String s ? s : null;
    }
}
