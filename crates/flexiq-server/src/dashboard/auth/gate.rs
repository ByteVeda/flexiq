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

/// Paths whose *reads* are admin-only too, not only their mutations.
///
/// A token listing carries no secret and no hash, but it is an inventory of
/// credentials — which ones exist, what each may call, whose they are, and when
/// each lapses. That is a targeting map rather than operational data, and the
/// dashboard's token CRUD is admin-gated by contract, read included.
const ADMIN_READ_PREFIXES: [&str; 1] = ["/api/grpc-tokens"];

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
/// their own account endpoints — except under [`ADMIN_READ_PREFIXES`], where
/// reading is itself privileged.
pub fn requires_admin(path: &str, method: &str) -> bool {
    if is_public_path(path) {
        return false;
    }
    if ADMIN_READ_PREFIXES
        .iter()
        .any(|prefix| path.starts_with(prefix))
    {
        return true;
    }
    is_state_changing(method) && path.starts_with("/api/") && !SELF_SERVICE_PATHS.contains(&path)
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
        // Minting and revoking a gRPC credential are admin actions, and they are
        // admin actions because of the rule above rather than because of a line
        // naming them — which is the property worth pinning.
        assert!(requires_admin("/api/grpc-tokens", "POST"));
        assert!(requires_admin("/api/grpc-tokens/abc123", "DELETE"));
        assert!(!requires_admin("/api/jobs", "GET"));
    }

    /// Reading the token inventory is privileged too: it names every credential,
    /// what each may call and when it lapses.
    #[test]
    fn reading_the_token_inventory_needs_admin() {
        for path in [
            "/api/grpc-tokens",
            "/api/grpc-tokens/scopes",
            "/api/grpc-tokens/abc123",
        ] {
            assert!(requires_admin(path, "GET"), "{path} must be admin-only");
        }
        // And the rule is a prefix on this resource only — every other read
        // stays open to a viewer.
        assert!(!requires_admin("/api/jobs", "GET"));
        assert!(!requires_admin("/api/webhooks", "GET"));
        // A public path is never admin-gated, whatever else matches.
        assert!(!requires_admin("/health", "GET"));
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
