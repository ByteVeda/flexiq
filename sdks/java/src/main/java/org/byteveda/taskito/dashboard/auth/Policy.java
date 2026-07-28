package org.byteveda.taskito.dashboard.auth;

import java.util.List;
import java.util.Set;
import org.byteveda.taskito.dashboard.support.DashboardError;

/**
 * The authorization gate: setup-required, authentication, CSRF, and RBAC.
 *
 * <p>RBAC is deliberately simple — every state-changing request is admin-only
 * except the self-service endpoints (logout, change-password); all reads are
 * allowed for any authenticated user. So admin vs viewer is purely write access.
 */
public final class Policy {
    private static final Set<String> PUBLIC_PATHS =
            Set.of("/api/auth/status", "/api/auth/login", "/api/auth/setup", "/api/auth/providers", "/health");

    private static final List<String> PUBLIC_PREFIXES = List.of("/api/auth/oauth/start/", "/api/auth/oauth/callback/");

    private static final Set<String> SELF_SERVICE_PATHS = Set.of("/api/auth/logout", "/api/auth/change-password");

    private static final Set<String> CSRF_EXEMPT_PATHS = Set.of("/api/auth/login", "/api/auth/setup");

    private Policy() {}

    public static boolean isPublicPath(String path) {
        if (PUBLIC_PATHS.contains(path)) {
            return true;
        }
        for (String prefix : PUBLIC_PREFIXES) {
            if (path.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    public static boolean isStateChanging(String method) {
        return "POST".equals(method) || "PUT".equals(method) || "DELETE".equals(method) || "PATCH".equals(method);
    }

    private static boolean isCsrfExempt(String path) {
        return CSRF_EXEMPT_PATHS.contains(path);
    }

    private static boolean requiresAdmin(String path, String method) {
        return isStateChanging(method)
                && path.startsWith("/api/")
                && !isPublicPath(path)
                && !SELF_SERVICE_PATHS.contains(path);
    }

    /** Enforce the gate for an {@code /api/} request; throws on denial. */
    public static void authorize(String path, String method, RequestContext ctx, AuthStore store) {
        if (!isPublicPath(path) && store.countUsers() == 0) {
            throw DashboardError.serviceUnavailable("setup_required");
        }
        if (isPublicPath(path)) {
            return;
        }
        Session session = ctx.session();
        if (session == null) {
            throw DashboardError.unauthorized("not_authenticated");
        }
        if (isStateChanging(method) && !isCsrfExempt(path) && !ctx.csrfValid()) {
            throw DashboardError.forbidden("csrf_failed");
        }
        if (requiresAdmin(path, method) && Role.orViewer(session.role()) != Role.ADMIN) {
            throw DashboardError.forbidden("forbidden");
        }
    }
}
