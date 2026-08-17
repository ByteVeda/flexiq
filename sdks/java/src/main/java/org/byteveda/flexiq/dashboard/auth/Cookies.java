package org.byteveda.flexiq.dashboard.auth;

import com.sun.net.httpserver.HttpExchange;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.jspecify.annotations.Nullable;

/**
 * Cookie parsing and {@code Set-Cookie} formatting. The JDK has no cookie
 * builder, so attributes are hand-formatted. Names and attributes match the
 * reference contract: {@code flexiq_session} (HttpOnly) and {@code flexiq_csrf}
 * (readable by the SPA), both {@code SameSite=Strict; Path=/}.
 */
public final class Cookies {
    public static final String SESSION = "flexiq_session";
    public static final String CSRF = "flexiq_csrf";
    public static final String CSRF_HEADER = "X-CSRF-Token";
    public static final String LEGACY_TOKEN = "flexiq_token";

    private Cookies() {}

    /** Parse the {@code Cookie} header(s); first value wins for duplicate names. */
    public static Map<String, String> parse(HttpExchange exchange) {
        List<String> headers = exchange.getRequestHeaders().getOrDefault("Cookie", Collections.emptyList());
        Map<String, String> out = new HashMap<>();
        for (String header : headers) {
            for (String pair : header.split(";")) {
                int eq = pair.indexOf('=');
                if (eq < 0) {
                    continue;
                }
                String name = pair.substring(0, eq).trim();
                String value = pair.substring(eq + 1).trim();
                if (!name.isEmpty()) {
                    out.putIfAbsent(name, value);
                }
            }
        }
        return out;
    }

    public static @Nullable String get(HttpExchange exchange, String name) {
        return parse(exchange).get(name);
    }

    public static String sessionCookie(String token, boolean secure, long maxAgeSeconds) {
        return format(SESSION, token, true, secure, maxAgeSeconds);
    }

    public static String csrfCookie(String csrf, boolean secure, long maxAgeSeconds) {
        return format(CSRF, csrf, false, secure, maxAgeSeconds);
    }

    public static String clearSession(boolean secure) {
        return format(SESSION, "", true, secure, 0);
    }

    public static String clearCsrf(boolean secure) {
        return format(CSRF, "", false, secure, 0);
    }

    public static String legacyTokenCookie(String token, boolean secure, long maxAgeSeconds) {
        return format(LEGACY_TOKEN, token, true, secure, maxAgeSeconds);
    }

    private static String format(String name, String value, boolean httpOnly, boolean secure, long maxAgeSeconds) {
        StringBuilder sb = new StringBuilder(name).append('=').append(value);
        if (httpOnly) {
            sb.append("; HttpOnly");
        }
        sb.append("; SameSite=Strict; Path=/");
        if (secure) {
            sb.append("; Secure");
        }
        sb.append("; Max-Age=").append(maxAgeSeconds);
        return sb.toString();
    }
}
