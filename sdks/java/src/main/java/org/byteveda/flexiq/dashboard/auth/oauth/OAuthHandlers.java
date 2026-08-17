package org.byteveda.flexiq.dashboard.auth.oauth;

import com.sun.net.httpserver.HttpExchange;
import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.byteveda.flexiq.dashboard.auth.AuthStore;
import org.byteveda.flexiq.dashboard.auth.Cookies;
import org.byteveda.flexiq.dashboard.auth.oauth.error.AllowlistDenied;
import org.byteveda.flexiq.dashboard.auth.oauth.error.IdentityFetchError;
import org.byteveda.flexiq.dashboard.auth.oauth.error.ProviderNotConfigured;
import org.byteveda.flexiq.dashboard.auth.oauth.error.StateValidationError;
import org.byteveda.flexiq.dashboard.support.Http;
import org.jspecify.annotations.Nullable;

/**
 * The three public OAuth HTTP routes. Unlike the JSON API routes these emit 302
 * redirects (and set the session cookies on success), so they live outside the
 * JSON router and are dispatched directly.
 *
 * <p>Handled gracefully when OAuth is not configured (a {@code null} flow):
 * {@code /api/auth/providers} reports password-only, and the start/callback
 * routes report 404 {@code oauth_not_configured}.
 */
public final class OAuthHandlers {
    static final String PROVIDERS_PATH = "/api/auth/providers";
    static final String START_PREFIX = "/api/auth/oauth/start/";
    static final String CALLBACK_PREFIX = "/api/auth/oauth/callback/";

    private final @Nullable OAuthFlow flow;
    private final boolean secureCookies;

    public OAuthHandlers(@Nullable OAuthFlow flow, boolean secureCookies) {
        this.flow = flow;
        this.secureCookies = secureCookies;
    }

    /**
     * Serve an OAuth route. Returns {@code false} (so the caller falls through to
     * the normal API pipeline) when the path/method is not one of ours.
     */
    public boolean serve(HttpExchange exchange, String path, String method, Map<String, String> query)
            throws IOException {
        if (!"GET".equals(method)) {
            return false;
        }
        if (path.equals(PROVIDERS_PATH)) {
            providers(exchange);
            return true;
        }
        if (path.startsWith(START_PREFIX)) {
            start(exchange, slotFrom(path, START_PREFIX), query);
            return true;
        }
        if (path.startsWith(CALLBACK_PREFIX)) {
            callback(exchange, slotFrom(path, CALLBACK_PREFIX), query);
            return true;
        }
        return false;
    }

    private void providers(HttpExchange exchange) throws IOException {
        boolean passwordEnabled = flow == null || flow.passwordAuthEnabled();
        List<Map<String, Object>> listing = flow == null ? List.of() : flow.providersListing();
        Http.respondJson(exchange, 200, Map.of("password_enabled", passwordEnabled, "providers", listing));
    }

    private void start(HttpExchange exchange, String slot, Map<String, String> query) throws IOException {
        if (flow == null) {
            Http.respondError(exchange, 404, "oauth_not_configured");
            return;
        }
        try {
            redirect(exchange, flow.start(slot, query.get("next")), List.of());
        } catch (ProviderNotConfigured e) {
            Http.respondError(exchange, 404, "oauth_not_configured");
        }
    }

    private void callback(HttpExchange exchange, String slot, Map<String, String> query) throws IOException {
        if (flow == null) {
            Http.respondError(exchange, 404, "oauth_not_configured");
            return;
        }
        try {
            OAuthFlow.CallbackResult result =
                    flow.handleCallback(slot, query.get("code"), query.get("state"), query.get("error"));
            long ttl = AuthStore.DEFAULT_SESSION_TTL_SECONDS;
            List<String> cookies = List.of(
                    Cookies.sessionCookie(result.session().token(), secureCookies, ttl),
                    Cookies.csrfCookie(result.session().csrfToken(), secureCookies, ttl));
            String nextUrl = result.nextUrl();
            redirect(exchange, nextUrl == null ? "/" : nextUrl, cookies);
        } catch (ProviderNotConfigured e) {
            Http.respondError(exchange, 404, "oauth_not_configured");
        } catch (StateValidationError e) {
            redirect(exchange, "/login?error=oauth_state_invalid", List.of());
        } catch (AllowlistDenied e) {
            redirect(exchange, "/login?error=oauth_denied", List.of());
        } catch (IdentityFetchError e) {
            redirect(exchange, "/login?error=oauth_failed", List.of());
        }
    }

    private static void redirect(HttpExchange exchange, String location, List<String> setCookies) throws IOException {
        var headers = exchange.getResponseHeaders();
        headers.set("Location", location);
        headers.set("Cache-Control", "no-store");
        for (String cookie : setCookies) {
            headers.add("Set-Cookie", cookie);
        }
        exchange.sendResponseHeaders(302, -1);
    }

    private static String slotFrom(String path, String prefix) {
        return URLDecoder.decode(path.substring(prefix.length()), StandardCharsets.UTF_8);
    }
}
