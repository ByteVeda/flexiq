package org.byteveda.taskito.dashboard.auth;

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
 * {@code X-FlexiQ-Token}, or the {@code taskito_token} cookie. A {@code ?token=}
 * query param is deliberately NOT accepted here — query strings leak into access
 * logs, browser history, and the Referer header; it is only honoured once on a
 * page load to bootstrap the cookie.
 */
public final class TokenAuth {
    private static final long OPEN_COOKIE_MAX_AGE = 24 * 60 * 60;

    private final String token;

    public TokenAuth(String token) {
        this.token = token;
    }

    public @Nullable String presented(HttpExchange exchange) {
        String authorization = exchange.getRequestHeaders().getFirst("Authorization");
        if (authorization != null && authorization.startsWith("Bearer ")) {
            return authorization.substring("Bearer ".length()).trim();
        }
        String header = exchange.getRequestHeaders().getFirst("X-FlexiQ-Token");
        if (header != null && !header.isEmpty()) {
            return header;
        }
        return Cookies.get(exchange, Cookies.LEGACY_TOKEN);
    }

    public boolean matches(@Nullable String presented) {
        if (presented == null) {
            return false;
        }
        return MessageDigest.isEqual(
                token.getBytes(StandardCharsets.UTF_8), presented.getBytes(StandardCharsets.UTF_8));
    }

    public static String openCookie(String token, boolean secure) {
        return Cookies.legacyTokenCookie(token, secure, OPEN_COOKIE_MAX_AGE);
    }

    public static Map<String, Object> openStatus() {
        return Map.of("auth_enabled", true, "setup_required", false);
    }

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
