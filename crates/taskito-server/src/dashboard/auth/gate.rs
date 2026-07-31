//! Which requests need a session, a CSRF token, or the admin role.
//!
//! One classification table, applied in one middleware, is what keeps a new
//! route from silently landing outside the gate: anything under `/api/` that
//! is not explicitly public requires a session.

/// Paths that bypass the session check entirely.
const PUBLIC_PATHS: [&str; 7] = [
    "/api/auth/status",
    "/api/auth/login",
    "/api/auth/setup",
    "/api/auth/providers",
    "/health",
    "/readiness",
    "/metrics",
];

/// Prefixes that bypass it — the provider flow carries its slot in the path.
const PUBLIC_PREFIXES: [&str; 2] = ["/api/auth/oauth/start/", "/api/auth/oauth/callback/"];

/// Routes any authenticated user may call against their own session, so they
/// are not admin-gated even though they mutate.
const SELF_SERVICE_PATHS: [&str; 2] = ["/api/auth/logout", "/api/auth/change-password"];

/// Login and setup happen before a session exists, so they cannot carry a CSRF
/// token. Every other mutation must.
const CSRF_EXEMPT_PATHS: [&str; 2] = ["/api/auth/login", "/api/auth/setup"];

/// Whether `path` bypasses the session/CSRF gate.
pub fn is_public_path(path: &str) -> bool {
    PUBLIC_PATHS.contains(&path)
        || PUBLIC_PREFIXES
            .iter()
            .any(|prefix| path.starts_with(prefix))
}

/// Whether `method` changes state, and therefore needs CSRF protection.
pub fn is_state_changing(method: &str) -> bool {
    matches!(method, "POST" | "PUT" | "DELETE" | "PATCH")
}

/// Whether a request needs the `admin` role.
///
/// Every state-changing API route is admin-only; viewers keep read access and
/// their own account endpoints.
pub fn requires_admin(path: &str, method: &str) -> bool {
    is_state_changing(method)
        && path.starts_with("/api/")
        && !is_public_path(path)
        && !SELF_SERVICE_PATHS.contains(&path)
}

/// Whether a state-changing request may proceed without a CSRF token.
pub fn is_csrf_exempt(path: &str) -> bool {
    CSRF_EXEMPT_PATHS.contains(&path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn probe_and_login_paths_are_public() {
        for path in ["/health", "/readiness", "/metrics", "/api/auth/login"] {
            assert!(is_public_path(path), "{path} must be public");
        }
        assert!(is_public_path("/api/auth/oauth/callback/google"));
        assert!(!is_public_path("/api/jobs"));
        assert!(!is_public_path("/api/auth/whoami"));
    }

    #[test]
    fn every_mutating_api_route_is_admin_gated() {
        assert!(requires_admin("/api/jobs/abc/cancel", "POST"));
        assert!(requires_admin("/api/settings/key", "DELETE"));
        assert!(requires_admin("/api/webhooks", "POST"));
        assert!(!requires_admin("/api/jobs", "GET"));
    }

    #[test]
    fn self_service_and_public_mutations_are_not_admin_gated() {
        assert!(!requires_admin("/api/auth/logout", "POST"));
        assert!(!requires_admin("/api/auth/change-password", "POST"));
        assert!(!requires_admin("/api/auth/login", "POST"));
    }

    #[test]
    fn only_pre_session_routes_skip_csrf() {
        assert!(is_csrf_exempt("/api/auth/login"));
        assert!(is_csrf_exempt("/api/auth/setup"));
        assert!(!is_csrf_exempt("/api/auth/logout"));
        assert!(!is_csrf_exempt("/api/jobs/abc/cancel"));
    }

    #[test]
    fn state_changing_methods_are_recognised() {
        for method in ["POST", "PUT", "DELETE", "PATCH"] {
            assert!(is_state_changing(method));
        }
        for method in ["GET", "HEAD", "OPTIONS"] {
            assert!(!is_state_changing(method));
        }
    }
}
