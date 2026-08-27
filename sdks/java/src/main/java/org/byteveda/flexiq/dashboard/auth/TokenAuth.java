package org.byteveda.flexiq.dashboard.auth;

import com.sun.net.httpserver.HttpExchange;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.LinkedHashMap;
import java.util.Map;
import org.jspecify.annotations.Nullable;

/**
 * Legacy shared-token mode: a single bearer token gates {@code /api/*} and every
 * request runs as a fixed admin identity (no users, sessions, or CSRF). Kept for
 * back-compat with {@code --token}; the session flow is the default.
 *
 * <p>The token is accepted, in order, from {@code Authorization: Bearer},
 * {@code X-Flexiq-Token}, or the {@code flexiq_token} cookie. A {@code ?token=}
 * query param is deliberately NOT accepted here — query strings leak into access
 * logs, browser history, and the Referer header; it is only honoured once on a
 * page load to bootstrap the cookie.
 */
public final class TokenAuth {
    private static final long OPEN_COOKIE_MAX_AGE = 24 * 60 * 60;

    private final String token;

    /**
     * A gate over one shared token.
     *
     * @param token the token every request must present
     */
    public TokenAuth(String token) {
        this.token = token;
    }

    /**
     * The token the request carried.
     *
     * @param exchange the request
     * @return the token from {@code Authorization: Bearer}, {@code X-Flexiq-Token}
     *     or the cookie, in that order; {@code null} when none was sent
     */
    public @Nullable String presented(HttpExchange exchange) {
        String authorization = exchange.getRequestHeaders().getFirst("Authorization");
        if (authorization != null && authorization.startsWith("Bearer ")) {
            return authorization.substring("Bearer ".length()).trim();
        }
        String header = exchange.getRequestHeaders().getFirst("X-Flexiq-Token");
        if (header != null && !header.isEmpty()) {
            return header;
        }
        return Cookies.get(exchange, Cookies.LEGACY_TOKEN);
    }

    /**
     * Whether the presented token is the configured one.
     *
     * @param presented what the request carried, or {@code null}
     * @return whether they match, compared in constant time
     */
    public boolean matches(@Nullable String presented) {
        if (presented == null) {
            return false;
        }
        return MessageDigest.isEqual(
                token.getBytes(StandardCharsets.UTF_8), presented.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * A {@code Set-Cookie} that bootstraps the token cookie from a {@code ?token=}
     * page load, so the query string is not needed again.
     *
     * @param token the shared token
     * @param secure whether to add {@code Secure}; {@code false} for local HTTP
     * @return the header value
     */
    public static String openCookie(String token, boolean secure) {
        return Cookies.legacyTokenCookie(token, secure, OPEN_COOKIE_MAX_AGE);
    }

    /**
     * The {@code /api/auth/status} body for this mode.
     *
     * @return auth on, setup never required — there are no users to create
     */
    public static Map<String, Object> openStatus() {
        return Map.of("auth_enabled", true, "setup_required", false);
    }

    /**
     * The {@code /api/auth/whoami} body for this mode.
     *
     * @return the fixed admin identity every token-mode request runs as
     */
    public static Map<String, Object> openWhoami() {
        Map<String, Object> user = new LinkedHashMap<>();
        user.put("username", "viewer");
        user.put("role", Role.ADMIN.wire());
        user.put("created_at", 0);
        user.put("last_login_at", null);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("user", user);
        out.put("csrf_token", "open");
        out.put("expires_at", 0);
        return out;
    }
}
