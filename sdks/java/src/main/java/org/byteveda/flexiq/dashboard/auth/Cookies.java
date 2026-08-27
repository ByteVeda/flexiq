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
    /** Name of the HttpOnly session cookie. */
    public static final String SESSION = "flexiq_session";

    /** Name of the CSRF cookie, deliberately readable by the SPA. */
    public static final String CSRF = "flexiq_csrf";

    /** Header the SPA echoes the CSRF cookie back in. */
    public static final String CSRF_HEADER = "X-CSRF-Token";

    /** Name of the legacy shared-token cookie. */
    public static final String LEGACY_TOKEN = "flexiq_token";

    private Cookies() {}

    /**
     * Parse the {@code Cookie} header(s); first value wins for duplicate names.
     *
     * @param exchange the request
     * @return the cookies by name; empty when the request carried none
     */
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

    /**
     * One cookie off the request.
     *
     * @param exchange the request
     * @param name the cookie's name
     * @return its value, or {@code null} when the request did not carry it
     */
    public static @Nullable String get(HttpExchange exchange, String name) {
        return parse(exchange).get(name);
    }

    /**
     * A {@code Set-Cookie} for the HttpOnly session cookie.
     *
     * @param token the session token
     * @param secure whether to add {@code Secure}; {@code false} for local HTTP
     * @param maxAgeSeconds how long the browser should keep it
     * @return the header value
     */
    public static String sessionCookie(String token, boolean secure, long maxAgeSeconds) {
        return format(SESSION, token, true, secure, maxAgeSeconds);
    }

    /**
     * A {@code Set-Cookie} for the CSRF cookie, which the SPA must be able to read.
     *
     * @param csrf the session's CSRF token
     * @param secure whether to add {@code Secure}; {@code false} for local HTTP
     * @param maxAgeSeconds how long the browser should keep it
     * @return the header value
     */
    public static String csrfCookie(String csrf, boolean secure, long maxAgeSeconds) {
        return format(CSRF, csrf, false, secure, maxAgeSeconds);
    }

    /**
     * A {@code Set-Cookie} that expires the session cookie.
     *
     * @param secure whether to add {@code Secure}; must match how it was set
     * @return the header value
     */
    public static String clearSession(boolean secure) {
        return format(SESSION, "", true, secure, 0);
    }

    /**
     * A {@code Set-Cookie} that expires the CSRF cookie.
     *
     * @param secure whether to add {@code Secure}; must match how it was set
     * @return the header value
     */
    public static String clearCsrf(boolean secure) {
        return format(CSRF, "", false, secure, 0);
    }

    /**
     * A {@code Set-Cookie} for the legacy shared-token cookie.
     *
     * @param token the shared token
     * @param secure whether to add {@code Secure}; {@code false} for local HTTP
     * @param maxAgeSeconds how long the browser should keep it
     * @return the header value
     */
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
