package org.byteveda.flexiq.dashboard.auth;

import java.util.LinkedHashMap;
import java.util.Map;
import org.byteveda.flexiq.dashboard.support.DashboardError;

/**
 * The {@code /api/auth/*} session endpoints: status, setup, login, logout,
 * whoami, change-password. Handlers return the JSON body; the server applies
 * cookie side effects (setting them on login, clearing them on logout) and
 * redacts the raw session token from the login response.
 */
public final class AuthHandlers {
    private final AuthStore store;

    public AuthHandlers(AuthStore store) {
        this.store = store;
    }

    public Map<String, Object> status() {
        return Map.of("auth_enabled", true, "setup_required", store.countUsers() == 0);
    }

    public Map<String, Object> setup(Map<String, Object> body) {
        if (store.countUsers() != 0) {
            throw DashboardError.badRequest("setup already complete");
        }
        String username = requireField(body, "username");
        String password = requireField(body, "password");
        User user = store.createUser(username, password, Role.ADMIN);
        return Map.of("user", serializeUser(user));
    }

    public Map<String, Object> login(Map<String, Object> body) {
        if (store.countUsers() == 0) {
            throw DashboardError.badRequest("setup_required");
        }
        String username = requireField(body, "username");
        String password = requireField(body, "password");
        User user = store.authenticate(username, password);
        if (user == null) {
            throw DashboardError.badRequest("invalid_credentials");
        }
        Session session = store.createSession(user.username(), user.role());
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("user", serializeUser(user));
        out.put("session", serializeSessionWithToken(session));
        return out;
    }

    public Map<String, Object> logout(RequestContext ctx) {
        if (ctx.session() != null) {
            store.deleteSession(ctx.session().token());
        }
        return Map.of("ok", true);
    }

    public Map<String, Object> whoami(RequestContext ctx) {
        Session session = ctx.session();
        if (session == null) {
            throw DashboardError.unauthorized("not_authenticated");
        }
        User user = store.getUser(session.username()).orElse(null);
        if (user == null) {
            store.deleteSession(session.token());
            throw DashboardError.notFound("not_authenticated");
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("user", serializeUser(user));
        out.put("csrf_token", session.csrfToken());
        out.put("expires_at", session.expiresAt());
        return out;
    }

    public Map<String, Object> changePassword(RequestContext ctx, Map<String, Object> body) {
        Session session = ctx.session();
        if (session == null) {
            throw DashboardError.unauthorized("not_authenticated");
        }
        String oldPassword = requireField(body, "old_password");
        String newPassword = requireField(body, "new_password");
        User user = store.getUser(session.username()).orElse(null);
        if (user == null) {
            throw DashboardError.badRequest("not_authenticated");
        }
        if (!store.verifyPassword(user, oldPassword)) {
            throw DashboardError.badRequest("invalid_credentials");
        }
        store.updatePassword(session.username(), newPassword);
        return Map.of("ok", true);
    }

    // ---- serialization -----------------------------------------------------

    private static Map<String, Object> serializeUser(User user) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("username", user.username());
        m.put("role", user.role());
        m.put("created_at", user.createdAt());
        m.put("last_login_at", user.lastLoginAt());
        return m;
    }

    /** Login session body — includes the raw {@code token}, redacted by the server. */
    private static Map<String, Object> serializeSessionWithToken(Session session) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("username", session.username());
        m.put("role", session.role());
        m.put("expires_at", session.expiresAt());
        m.put("csrf_token", session.csrfToken());
        m.put("token", session.token());
        return m;
    }

    private static String requireField(Map<String, Object> body, String key) {
        Object value = body == null ? null : body.get(key);
        if (value instanceof String s && !s.isEmpty()) {
            return s;
        }
        throw DashboardError.badRequest("missing or empty field '" + key + "'");
    }
}
