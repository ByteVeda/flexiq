package org.byteveda.flexiq.dashboard.auth;

import com.sun.net.httpserver.HttpExchange;
import java.util.Map;
import org.jspecify.annotations.Nullable;

/**
 * Per-request auth state: the resolved {@link Session} (null when
 * unauthenticated) plus the CSRF cookie and header used for double-submit
 * validation.
 */
public record RequestContext(@Nullable Session session, @Nullable String csrfCookie, @Nullable String csrfHeader) {

    public boolean authenticated() {
        return session != null;
    }

    /**
     * Double-submit CSRF check: the {@code flexiq_csrf} cookie must equal both
     * the {@code X-CSRF-Token} header and the token bound to the session. The
     * session binding defeats a pre-seeded cookie.
     */
    public boolean csrfValid() {
        return session != null
                && csrfCookie != null
                && !csrfCookie.isEmpty()
                && csrfHeader != null
                && csrfCookie.equals(csrfHeader)
                && csrfCookie.equals(session.csrfToken());
    }

    public static RequestContext build(HttpExchange exchange, AuthStore store) {
        Map<String, String> cookies = Cookies.parse(exchange);
        String sessionToken = cookies.get(Cookies.SESSION);
        @Nullable
        Session session =
                sessionToken == null ? null : store.getSession(sessionToken).orElse(null);
        String csrfCookie = cookies.get(Cookies.CSRF);
        String csrfHeader = exchange.getRequestHeaders().getFirst(Cookies.CSRF_HEADER);
        return new RequestContext(session, csrfCookie, csrfHeader);
    }

    /** Fixed open identity for legacy shared-token mode. */
    public static RequestContext open() {
        return new RequestContext(null, null, null);
    }
}
