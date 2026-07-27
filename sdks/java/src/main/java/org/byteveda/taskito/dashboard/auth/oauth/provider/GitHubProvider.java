package org.byteveda.taskito.dashboard.auth.oauth.provider;

import java.net.http.HttpClient;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.byteveda.taskito.dashboard.auth.oauth.config.OAuthConfig.GitHubConfig;
import org.byteveda.taskito.dashboard.auth.oauth.error.AllowlistDenied;
import org.byteveda.taskito.dashboard.auth.oauth.error.IdentityFetchError;
import org.byteveda.taskito.dashboard.auth.oauth.model.OAuthState;
import org.byteveda.taskito.dashboard.auth.oauth.model.ProviderIdentity;
import org.byteveda.taskito.dashboard.support.Json;
import org.jspecify.annotations.Nullable;

/**
 * GitHub login over plain OAuth2 (GitHub does not implement OIDC, so there is no
 * id-token and no {@link IdTokenValidator}). PKCE is honoured. Org-membership
 * allowlisting is enforced inside {@link #exchangeCode} because it needs the
 * access token, leaving {@link #checkAllowlist} a no-op.
 */
final class GitHubProvider implements OAuthProvider {
    private static final String AUTHORIZE_URL = "https://github.com/login/oauth/authorize";
    private static final String TOKEN_URL = "https://github.com/login/oauth/access_token";
    private static final String API_BASE = "https://api.github.com";
    private static final String SCOPE = "read:user user:email";

    private final GitHubConfig config;
    private final HttpClient http;

    GitHubProvider(GitHubConfig config, HttpClient http) {
        this.config = config;
        this.http = http;
    }

    @Override
    public String slot() {
        return "github";
    }

    @Override
    public String label() {
        return "GitHub";
    }

    @Override
    public String type() {
        return "github";
    }

    @Override
    public String authorizationUrl(OAuthState state, String redirectUri) {
        // GitHub is not OIDC: nonce is unused. read:org is added so the membership
        // endpoint returns reliable results when an org allowlist is configured.
        String scope = config.allowedOrgs().isEmpty() ? SCOPE : SCOPE + " read:org";
        Map<String, String> params = new LinkedHashMap<>();
        params.put("client_id", config.clientId());
        params.put("redirect_uri", redirectUri);
        params.put("scope", scope);
        params.put("state", state.state());
        params.put("code_challenge", Pkce.challenge(state.codeVerifier()));
        params.put("code_challenge_method", Pkce.METHOD);
        params.put("allow_signup", "false");
        return AUTHORIZE_URL + "?" + OAuthHttp.formEncode(params);
    }

    @Override
    public ProviderIdentity exchangeCode(String code, String codeVerifier, String redirectUri, String expectedNonce) {
        String accessToken = fetchAccessToken(code, codeVerifier, redirectUri);
        Map<String, Object> user = fetchUser(accessToken);
        Object id = user.get("id");
        Object login = user.get("login");
        if (id == null || !(login instanceof String loginName) || loginName.isEmpty()) {
            throw new IdentityFetchError("GitHub /user response missing 'id' or 'login'");
        }
        String email = primaryVerifiedEmail(accessToken);
        // Membership needs the access token, so enforce it here (not checkAllowlist).
        verifyOrgMembership(accessToken, loginName);
        return new ProviderIdentity(
                slot(),
                String.valueOf(id),
                email,
                email != null,
                stringValue(user.get("name"), loginName),
                asString(user.get("avatar_url")));
    }

    private String fetchAccessToken(String code, String codeVerifier, String redirectUri) {
        Map<String, String> form = new LinkedHashMap<>();
        form.put("grant_type", "authorization_code");
        form.put("code", code);
        form.put("redirect_uri", redirectUri);
        form.put("code_verifier", codeVerifier);
        form.put("client_id", config.clientId());
        form.put("client_secret", config.clientSecret());
        OAuthHttp.HttpResult result = OAuthHttp.postForm(http, TOKEN_URL, form, Map.of());
        if (result.status() >= 400) {
            throw new IdentityFetchError("GitHub token exchange failed with status " + result.status());
        }
        Map<String, Object> token = Json.parseMap(result.body());
        Object accessToken = token == null ? null : token.get("access_token");
        if (!(accessToken instanceof String value) || value.isEmpty()) {
            throw new IdentityFetchError("no access_token in GitHub token response");
        }
        return value;
    }

    private Map<String, Object> fetchUser(String accessToken) {
        OAuthHttp.HttpResult result = OAuthHttp.get(http, API_BASE + "/user", apiHeaders(accessToken));
        if (result.status() != 200) {
            throw new IdentityFetchError("GitHub GET /user failed with status " + result.status());
        }
        Map<String, Object> user = Json.parseMap(result.body());
        if (user == null) {
            throw new IdentityFetchError("GitHub /user returned a non-JSON body");
        }
        return user;
    }

    /** The primary verified email, or {@code null} — an unverified email is never trusted. */
    private @Nullable String primaryVerifiedEmail(String accessToken) {
        OAuthHttp.HttpResult result = OAuthHttp.get(http, API_BASE + "/user/emails", apiHeaders(accessToken));
        if (result.status() != 200) {
            return null;
        }
        for (Map<String, Object> entry : Json.parseListOfObjects(result.body())) {
            if (Boolean.TRUE.equals(entry.get("primary")) && Boolean.TRUE.equals(entry.get("verified"))) {
                return asString(entry.get("email"));
            }
        }
        return null;
    }

    private void verifyOrgMembership(String accessToken, String login) {
        List<String> allowedOrgs = config.allowedOrgs();
        if (allowedOrgs.isEmpty()) {
            return;
        }
        for (String org : allowedOrgs) {
            String url = API_BASE + "/orgs/" + org + "/members/" + login;
            OAuthHttp.HttpResult result = OAuthHttp.get(http, url, apiHeaders(accessToken));
            if (result.status() == 204) {
                return;
            }
            // 404 = not a member; 302 = requester cannot see membership. Anything
            // else is an unexpected API error.
            if (result.status() != 404 && result.status() != 302) {
                throw new IdentityFetchError("GitHub org membership check failed with status " + result.status());
            }
        }
        throw new AllowlistDenied("user is not a member of any allowed GitHub org");
    }

    @Override
    public void checkAllowlist(ProviderIdentity identity) {
        // GitHub's org check runs inside exchangeCode; required by the contract.
    }

    private static Map<String, String> apiHeaders(String accessToken) {
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("Authorization", "Bearer " + accessToken);
        headers.put("Accept", "application/vnd.github+json");
        headers.put("X-GitHub-Api-Version", "2022-11-28");
        return headers;
    }

    private static String stringValue(@Nullable Object value, String fallback) {
        String text = asString(value);
        return text != null && !text.isEmpty() ? text : fallback;
    }

    private static @Nullable String asString(@Nullable Object value) {
        return value instanceof String s ? s : null;
    }
}
