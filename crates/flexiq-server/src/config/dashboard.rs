//! Dashboard bind address, auth mode, and asset source.

use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::{bail, Result};

use crate::config::listen::resolve;
use crate::config::{flag, value, Env};
use crate::dashboard::auth::oauth::config::{from_env as oauth_from_env, OAuthConfig};

/// How the dashboard authenticates requests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthMode {
    /// No authentication. The auth endpoints report the mode and otherwise 404
    /// so the SPA hides its login affordances.
    Open,
    /// Session cookies, CSRF, and role checks.
    Session,
    /// No authentication, read-only: every caller is an anonymous viewer. For
    /// a public demo, where open mode would hand every visitor admin.
    Public,
}

/// Validated dashboard settings.
#[derive(Debug, Clone)]
pub struct DashboardConfig {
    /// Address the HTTP server binds.
    pub bind: SocketAddr,
    /// Authentication mode.
    pub auth: AuthMode,
    /// Serve the SPA from this directory instead of the embedded bundle.
    pub assets_dir: Option<PathBuf>,
    /// Bearer token accepted on `/metrics` and `/readiness`.
    pub metrics_token: Option<String>,
    /// Whether `/readiness` answers without a credential.
    pub public_readiness: bool,
    /// Whether session cookies carry `Secure`.
    pub secure_cookies: bool,
    /// First admin to create on start, as `(username, password)`.
    pub admin_bootstrap: Option<(String, String)>,
    /// Provider login configuration, when any provider is set up.
    pub oauth: Option<OAuthConfig>,
    /// Whether webhooks may target private addresses. Development only.
    pub allow_private_webhooks: bool,
    /// Same-origin trigger path a public dashboard's enqueue button posts to.
    pub demo_enqueue: Option<String>,
}

/// Parse the dashboard block, or `None` when the dashboard is disabled.
pub fn from_env(env: &Env, allow_insecure: bool) -> Result<Option<DashboardConfig>> {
    let Some(spec) = value(env, "FLEXIQ_DASHBOARD") else {
        return Ok(None);
    };
    let bind = resolve("FLEXIQ_DASHBOARD", &spec)?;
    let auth = auth_mode(env)?;

    // An unauthenticated dashboard exposes every operate action (cancel,
    // replay, purge, pause). Reachable from off-host, that has to be a
    // deliberate choice.
    if !bind.ip().is_loopback() && auth == AuthMode::Open && !allow_insecure {
        bail!(
            "FLEXIQ_DASHBOARD={spec} is reachable off-host with FLEXIQ_DASHBOARD_AUTH=off. \
             Set FLEXIQ_DASHBOARD_AUTH=session, bind loopback, or set \
             FLEXIQ_ALLOW_INSECURE=1 if the network already restricts access."
        );
    }

    Ok(Some(DashboardConfig {
        bind,
        auth,
        assets_dir: value(env, "FLEXIQ_DASHBOARD_ASSETS").map(PathBuf::from),
        metrics_token: value(env, "FLEXIQ_DASHBOARD_METRICS_TOKEN"),
        // Opt-in, because it publishes a little state — whether storage answers
        // and how many workers are registered — to anything that can reach the
        // port. An orchestrator probe carries no credential and cannot be given
        // one (probe headers are literal strings in a manifest), so the choice
        // is this or a readiness check that never runs.
        public_readiness: flag(env, "FLEXIQ_DASHBOARD_PUBLIC_READINESS", false),
        secure_cookies: !flag(env, "FLEXIQ_DASHBOARD_INSECURE_COOKIES", false),
        admin_bootstrap: admin_bootstrap(env),
        // Only meaningful with sessions: there is nothing to log in to
        // otherwise, and silently accepting the config would misrepresent it.
        oauth: match auth {
            AuthMode::Session => oauth_from_env(env)?,
            AuthMode::Open | AuthMode::Public => None,
        },
        allow_private_webhooks: flag(
            env,
            crate::dashboard::stores::url_safety::ALLOW_PRIVATE_VAR,
            false,
        ),
        demo_enqueue: demo_enqueue(env, auth)?,
    }))
}

/// The demo trigger path, checked to be one the browser resolves same-origin.
///
/// The SPA posts to it without asking, so an absolute or protocol-relative
/// URL would aim every visitor's click at a host the operator did not front.
fn demo_enqueue(env: &Env, auth: AuthMode) -> Result<Option<String>> {
    let Some(path) = value(env, "FLEXIQ_DASHBOARD_DEMO_ENQUEUE") else {
        return Ok(None);
    };
    if auth != AuthMode::Public {
        bail!("FLEXIQ_DASHBOARD_DEMO_ENQUEUE needs FLEXIQ_DASHBOARD_AUTH=public");
    }
    if !path.starts_with('/') || path.starts_with("//") || path.contains('\\') {
        bail!(
            "FLEXIQ_DASHBOARD_DEMO_ENQUEUE must be a same-origin path such as \
             /t/demo?token=…, got '{path}'"
        );
    }
    Ok(Some(path))
}

fn auth_mode(env: &Env) -> Result<AuthMode> {
    match value(env, "FLEXIQ_DASHBOARD_AUTH") {
        None => Ok(AuthMode::Open),
        Some(raw) => match raw.to_ascii_lowercase().as_str() {
            "off" | "open" | "none" => Ok(AuthMode::Open),
            "session" | "on" => Ok(AuthMode::Session),
            "public" => Ok(AuthMode::Public),
            other => {
                bail!("FLEXIQ_DASHBOARD_AUTH must be 'off', 'session' or 'public', got '{other}'")
            }
        },
    }
}

fn admin_bootstrap(env: &Env) -> Option<(String, String)> {
    let username = value(env, "FLEXIQ_DASHBOARD_ADMIN_USER")?;
    let password = value(env, "FLEXIQ_DASHBOARD_ADMIN_PASSWORD")?;
    Some((username, password))
}

/// Drop the bootstrap password from the live environment once it has been
/// read, so no later in-process read or child process sees it; the initial
/// block `/proc/<pid>/environ` shows keeps it. Mirrors the SDK dashboards.
pub fn scrub_bootstrap_password() {
    // Called once from `main`, before any thread that reads the environment
    // has been spawned.
    std::env::remove_var("FLEXIQ_DASHBOARD_ADMIN_PASSWORD");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn env(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(key, val)| (key.to_string(), val.to_string()))
            .collect()
    }

    #[test]
    fn readiness_is_gated_unless_a_deployment_opts_out() {
        let base = [("FLEXIQ_DASHBOARD", "127.0.0.1:8080")];
        let config = from_env(&env(&base), false)
            .expect("valid")
            .expect("configured");
        assert!(!config.public_readiness);

        let opted_out = from_env(
            &env(&[base[0], ("FLEXIQ_DASHBOARD_PUBLIC_READINESS", "1")]),
            false,
        )
        .expect("valid")
        .expect("configured");
        assert!(opted_out.public_readiness);
    }

    #[test]
    fn loopback_defaults_to_open_auth() {
        let config = from_env(&env(&[("FLEXIQ_DASHBOARD", "127.0.0.1:8080")]), false)
            .expect("valid")
            .expect("configured");
        assert_eq!(config.auth, AuthMode::Open);
        assert!(config.secure_cookies);
    }

    #[test]
    fn open_auth_off_host_is_refused() {
        let error = from_env(&env(&[("FLEXIQ_DASHBOARD", "0.0.0.0:8080")]), false)
            .expect_err("must refuse");
        assert!(error.to_string().contains("FLEXIQ_DASHBOARD_AUTH"));
    }

    #[test]
    fn session_auth_off_host_is_allowed() {
        let config = from_env(
            &env(&[
                ("FLEXIQ_DASHBOARD", "0.0.0.0:8080"),
                ("FLEXIQ_DASHBOARD_AUTH", "session"),
            ]),
            false,
        )
        .expect("valid")
        .expect("configured");
        assert_eq!(config.auth, AuthMode::Session);
    }

    #[test]
    fn the_insecure_override_is_honoured() {
        let config = from_env(&env(&[("FLEXIQ_DASHBOARD", "0.0.0.0:8080")]), true)
            .expect("valid")
            .expect("configured");
        assert_eq!(config.auth, AuthMode::Open);
    }

    #[test]
    fn an_unknown_auth_mode_is_rejected() {
        let error = from_env(
            &env(&[
                ("FLEXIQ_DASHBOARD", "127.0.0.1:8080"),
                ("FLEXIQ_DASHBOARD_AUTH", "basic"),
            ]),
            false,
        )
        .expect_err("must refuse");
        assert!(error
            .to_string()
            .contains("must be 'off', 'session' or 'public'"));
    }

    /// Read-only has nothing to protect from off-host callers, so it needs no
    /// insecure override.
    #[test]
    fn public_auth_off_host_is_allowed() {
        let config = from_env(
            &env(&[
                ("FLEXIQ_DASHBOARD", "0.0.0.0:8080"),
                ("FLEXIQ_DASHBOARD_AUTH", "public"),
            ]),
            false,
        )
        .expect("valid")
        .expect("configured");
        assert_eq!(config.auth, AuthMode::Public);
        assert!(config.oauth.is_none());
        assert!(config.demo_enqueue.is_none());
    }

    #[test]
    fn the_demo_enqueue_path_is_a_public_mode_same_origin_path() {
        let base = [
            ("FLEXIQ_DASHBOARD", "0.0.0.0:8080"),
            ("FLEXIQ_DASHBOARD_AUTH", "public"),
        ];
        let with = |path: &str| {
            from_env(
                &env(&[base[0], base[1], ("FLEXIQ_DASHBOARD_DEMO_ENQUEUE", path)]),
                false,
            )
        };

        let config = with("/t/demo?token=flexiq-public-demo")
            .expect("valid")
            .expect("configured");
        assert_eq!(
            config.demo_enqueue.as_deref(),
            Some("/t/demo?token=flexiq-public-demo")
        );

        for path in [
            "https://evil.example/t",
            "//evil.example/t",
            "/\\evil.example",
            "t/demo",
        ] {
            let error = with(path).expect_err(path);
            assert!(error.to_string().contains("same-origin"), "{path}: {error}");
        }

        // Anywhere else it would sit beside working write buttons.
        let error = from_env(
            &env(&[
                ("FLEXIQ_DASHBOARD", "127.0.0.1:8080"),
                ("FLEXIQ_DASHBOARD_DEMO_ENQUEUE", "/t/demo"),
            ]),
            false,
        )
        .expect_err("open mode");
        assert!(error.to_string().contains("FLEXIQ_DASHBOARD_AUTH=public"));
    }

    #[test]
    fn bootstrap_needs_both_halves() {
        let config = from_env(
            &env(&[
                ("FLEXIQ_DASHBOARD", "127.0.0.1:8080"),
                ("FLEXIQ_DASHBOARD_ADMIN_USER", "admin"),
            ]),
            false,
        )
        .expect("valid")
        .expect("configured");
        assert!(config.admin_bootstrap.is_none());
    }
}
