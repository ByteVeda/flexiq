//! Environment-only configuration.
//!
//! The server takes no required flags: everything is read from the process
//! environment so one image is configured entirely by the deployment. Parsing
//! works over a plain map — `Config::from_env` collects the real environment,
//! and tests build a map without touching global state.

pub mod backend;
pub mod dashboard;
pub mod grpc;
pub mod listen;
pub mod webhook;

use std::collections::HashMap;

use anyhow::{bail, Context, Result};

use crate::config::dashboard::DashboardConfig;
use crate::config::grpc::GrpcConfig;
use crate::config::listen::AttachConfig;
use crate::config::webhook::WebhookConfig;

/// Environment variables as a lookup map.
pub type Env = HashMap<String, String>;

/// Fully validated server configuration.
#[derive(Debug, Clone)]
pub struct Config {
    /// Storage connection string. `None` only in a webhook-only deployment,
    /// which reads no jobs and so needs no database.
    pub dsn: Option<String>,
    /// Explicit backend name, when the DSN scheme is not enough.
    pub backend: Option<String>,
    /// Tenant namespace the scheduler is scoped to.
    pub namespace: Option<String>,
    /// Queues the scheduler consumes.
    pub queues: Vec<String>,
    /// Dispatch concurrency. `None` sizes it from the capacity attached
    /// executors advertise.
    pub workers: Option<usize>,
    /// Whether this process runs retention and cleanup.
    pub maintenance: bool,
    /// Where executors attach, and what they must present. `None` disables the
    /// listener.
    pub attach: Option<AttachConfig>,
    /// Where the dashboard listens. `None` disables it.
    pub dashboard: Option<DashboardConfig>,
    /// Where the sidecar-injecting admission webhook listens. `None` disables
    /// it.
    pub webhook: Option<WebhookConfig>,
    /// Where the `flexiq.v1` gRPC door listens. `None` disables it.
    pub grpc: Option<GrpcConfig>,
    /// Whether opening storage applies pending schema changes. Off for a
    /// deployment whose database credentials do not permit DDL at runtime; the
    /// schema must then be applied out of band before the server starts.
    pub auto_migrate: bool,
}

impl Config {
    /// Read and validate configuration from the process environment.
    pub fn from_env() -> Result<Self> {
        Self::from_map(&std::env::vars().collect())
    }

    /// Read and validate configuration from an explicit map.
    pub fn from_map(env: &Env) -> Result<Self> {
        let allow_insecure = flag(env, "FLEXIQ_ALLOW_INSECURE", false);
        // Read ahead of the struct: the gRPC role is the one whose validity
        // depends on it, and it needs the parsed value to say so.
        let namespace = value(env, "FLEXIQ_NAMESPACE");

        let config = Self {
            dsn: value(env, "FLEXIQ_DSN"),
            backend: value(env, "FLEXIQ_BACKEND"),
            queues: queues(env),
            workers: usize_value(env, "FLEXIQ_WORKERS")?,
            maintenance: flag(env, "FLEXIQ_MAINTENANCE", true),
            auto_migrate: flag(env, "FLEXIQ_AUTO_MIGRATE", true),
            attach: listen::from_env(env)?,
            dashboard: dashboard::from_env(env, allow_insecure)?,
            webhook: webhook::from_env(env)?,
            grpc: grpc::from_env(env, namespace.as_deref())?,
            namespace,
        };

        if config.attach.is_none()
            && config.dashboard.is_none()
            && config.webhook.is_none()
            && config.grpc.is_none()
        {
            bail!(
                "nothing to run: set FLEXIQ_LISTEN (executor attach), \
                 FLEXIQ_DASHBOARD (dashboard), FLEXIQ_WEBHOOK_LISTEN (sidecar \
                 injection), FLEXIQ_GRPC_LISTEN (the gRPC door), or any combination"
            );
        }
        // The webhook is the one role that touches no storage — it rewrites pod
        // specs and never reads a job — so it alone may run without a DSN.
        if config.dsn.is_none()
            && (config.attach.is_some() || config.dashboard.is_some() || config.grpc.is_some())
        {
            bail!(
                "FLEXIQ_DSN is required — the storage connection string. Only a \
                 webhook-only deployment (FLEXIQ_WEBHOOK_LISTEN alone) can omit it."
            );
        }
        Ok(config)
    }

    /// The DSN, for the roles that cannot run without one.
    pub fn require_dsn(&self) -> Result<&str> {
        self.dsn
            .as_deref()
            .context("FLEXIQ_DSN is required — the storage connection string")
    }
}

/// A set environment variable, with surrounding whitespace trimmed. Empty
/// values read as unset so `FOO=` in a compose file means "off".
pub fn value(env: &Env, key: &str) -> Option<String> {
    env.get(key)
        .map(|raw| raw.trim().to_string())
        .filter(|trimmed| !trimmed.is_empty())
}

/// A boolean variable. `1`/`true`/`on`/`yes` enable, `0`/`false`/`off`/`no`
/// disable, anything else falls back to `default`.
pub fn flag(env: &Env, key: &str, default: bool) -> bool {
    match value(env, key) {
        None => default,
        Some(raw) => match raw.to_ascii_lowercase().as_str() {
            "1" | "true" | "on" | "yes" => true,
            "0" | "false" | "off" | "no" => false,
            _ => default,
        },
    }
}

fn usize_value(env: &Env, key: &str) -> Result<Option<usize>> {
    match value(env, key) {
        None => Ok(None),
        Some(raw) => {
            let parsed: usize = raw
                .parse()
                .with_context(|| format!("{key} must be a positive integer, got '{raw}'"))?;
            if parsed == 0 {
                bail!("{key} must be greater than zero");
            }
            Ok(Some(parsed))
        }
    }
}

fn queues(env: &Env) -> Vec<String> {
    match value(env, "FLEXIQ_QUEUES") {
        None => vec!["default".to_string()],
        Some(raw) => {
            let parsed: Vec<String> = raw
                .split(',')
                .map(str::trim)
                .filter(|name| !name.is_empty())
                .map(str::to_string)
                .collect();
            if parsed.is_empty() {
                vec!["default".to_string()]
            } else {
                parsed
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(pairs: &[(&str, &str)]) -> Env {
        pairs
            .iter()
            .map(|(key, val)| (key.to_string(), val.to_string()))
            .collect()
    }

    #[test]
    fn dsn_is_required() {
        let error = Config::from_map(&env(&[("FLEXIQ_DASHBOARD", "127.0.0.1:8080")]))
            .expect_err("must reject a missing DSN");
        assert!(error.to_string().contains("FLEXIQ_DSN"));
    }

    #[test]
    fn an_attach_listener_still_needs_a_dsn() {
        let error = Config::from_map(&env(&[("FLEXIQ_LISTEN", "127.0.0.1:7777")]))
            .expect_err("must reject a missing DSN");
        assert!(error.to_string().contains("FLEXIQ_DSN"));
    }

    #[test]
    fn defaults_fill_in_around_the_dsn() {
        let config = Config::from_map(&env(&[
            ("FLEXIQ_DSN", ":memory:"),
            ("FLEXIQ_DASHBOARD", "127.0.0.1:8080"),
        ]))
        .expect("valid config");
        assert_eq!(config.queues, vec!["default".to_string()]);
        assert!(config.maintenance);
        assert!(config.workers.is_none());
        assert!(config.attach.is_none());
    }

    #[test]
    fn queues_split_on_commas_and_ignore_blanks() {
        let config = Config::from_map(&env(&[
            ("FLEXIQ_DSN", ":memory:"),
            ("FLEXIQ_QUEUES", " high , ,default "),
            ("FLEXIQ_DASHBOARD", "127.0.0.1:8080"),
        ]))
        .expect("valid config");
        assert_eq!(
            config.queues,
            vec!["high".to_string(), "default".to_string()]
        );
    }

    #[test]
    fn an_empty_value_reads_as_unset() {
        let config = Config::from_map(&env(&[
            ("FLEXIQ_DSN", ":memory:"),
            ("FLEXIQ_NAMESPACE", "   "),
            ("FLEXIQ_DASHBOARD", "127.0.0.1:8080"),
        ]))
        .expect("valid config");
        assert!(config.namespace.is_none());
    }

    #[test]
    fn a_config_that_runs_nothing_is_rejected() {
        let error = Config::from_map(&env(&[("FLEXIQ_DSN", ":memory:")])).expect_err("must reject");
        assert!(error.to_string().contains("nothing to run"));
    }

    #[test]
    fn worker_count_must_be_a_positive_integer() {
        for bad in ["0", "-1", "many"] {
            let error = Config::from_map(&env(&[
                ("FLEXIQ_DSN", ":memory:"),
                ("FLEXIQ_DASHBOARD", "127.0.0.1:8080"),
                ("FLEXIQ_WORKERS", bad),
            ]))
            .expect_err("must reject");
            assert!(error.to_string().contains("FLEXIQ_WORKERS"));
        }
    }

    #[cfg(feature = "grpc")]
    #[test]
    fn the_grpc_door_is_a_role_on_its_own() {
        let config = Config::from_map(&env(&[
            ("FLEXIQ_DSN", ":memory:"),
            ("FLEXIQ_NAMESPACE", "prod"),
            ("FLEXIQ_GRPC_LISTEN", "127.0.0.1:50051"),
        ]))
        .expect("a gRPC-only deployment is a deployment");
        assert!(config.grpc.is_some());
        assert!(config.attach.is_none());
        assert!(config.dashboard.is_none());
    }

    #[cfg(feature = "grpc")]
    #[test]
    fn the_grpc_door_still_needs_a_dsn() {
        let error = Config::from_map(&env(&[
            ("FLEXIQ_NAMESPACE", "prod"),
            ("FLEXIQ_GRPC_LISTEN", "127.0.0.1:50051"),
        ]))
        .expect_err("must reject a missing DSN");
        assert!(error.to_string().contains("FLEXIQ_DSN"));
    }
}
