//! Where the trigger listener binds, and the definitions it serves.
//!
//! Two variables: `FLEXIQ_TRIGGER_LISTEN` turns the role on, and
//! `FLEXIQ_TRIGGERS_FILE` names the JSON file of definitions, which is
//! required — a listener with nothing to answer would only ever say `404`.
//!
//! **A namespace is mandatory**, for the reason the gRPC door's is: `None`
//! means different things to different `Storage` methods, and a public door
//! must enqueue into exactly one tenant.
//!
//! The listener speaks plain HTTP. Webhook senders call HTTPS URLs, so it is
//! meant to sit behind an ingress or load balancer that terminates TLS — the
//! same arrangement the dashboard has.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{bail, Context, Result};

use crate::config::listen::resolve;
use crate::config::{value, Env};
use crate::trigger::definition::{self, Trigger};

/// The variable that enables the role.
pub const LISTEN_VAR: &str = "FLEXIQ_TRIGGER_LISTEN";
/// The variable naming the definitions file.
pub const FILE_VAR: &str = "FLEXIQ_TRIGGERS_FILE";

/// The trigger listener's address, namespace and definitions.
#[derive(Debug, Clone)]
pub struct TriggerConfig {
    /// Address the HTTP server binds.
    pub bind: SocketAddr,
    /// The one namespace every trigger enqueues into.
    pub namespace: String,
    /// Where the definitions were read from, for the startup log.
    pub file: PathBuf,
    /// The validated definitions.
    pub triggers: Arc<[Trigger]>,
}

/// Parse the trigger block, or `None` when the role is disabled.
pub fn from_env(env: &Env, namespace: Option<&str>) -> Result<Option<TriggerConfig>> {
    let Some(spec) = value(env, LISTEN_VAR) else {
        return Ok(None);
    };
    let bind = resolve(LISTEN_VAR, &spec)?;

    let Some(namespace) = namespace else {
        bail!(
            "{LISTEN_VAR} needs FLEXIQ_NAMESPACE: every trigger enqueues into one \
             namespace, and this process must be told which"
        );
    };

    let file = value(env, FILE_VAR).map(PathBuf::from).with_context(|| {
        format!("{FILE_VAR} is required when {LISTEN_VAR} is set — the JSON file of trigger definitions")
    })?;
    let text = std::fs::read_to_string(&file)
        .with_context(|| format!("{FILE_VAR}={} could not be read", file.display()))?;
    let triggers = definition::parse(&text, env)
        .with_context(|| format!("{FILE_VAR}={} was refused", file.display()))?;

    Ok(Some(TriggerConfig {
        bind,
        namespace: namespace.to_string(),
        file,
        triggers: triggers.into(),
    }))
}

/// Remove every verifier secret from the process environment once the
/// definitions are parsed, so no later in-process read or child process sees
/// them.
pub fn scrub_trigger_secrets(config: &TriggerConfig) {
    // Called once from `main`, before any thread that reads the environment
    // has been spawned.
    for var in config
        .triggers
        .iter()
        .filter_map(|trigger| trigger.secret_env.as_ref())
    {
        std::env::remove_var(var);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DEFINITIONS: &str = r#"{"triggers": [{
        "name": "orders", "path": "/t/orders", "task": "orders.ingest",
        "rate_limit": "10/s",
        "auth": {"kind": "github", "secret_env": "HOOK_SECRET"}
    }]}"#;

    /// A definitions file removed again when the test ends.
    struct Definitions(PathBuf);

    impl Definitions {
        fn path(&self) -> &std::path::Path {
            &self.0
        }
    }

    impl Drop for Definitions {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    fn file(contents: &str) -> Definitions {
        let path =
            std::env::temp_dir().join(format!("flexiq-triggers-{}.json", uuid::Uuid::new_v4()));
        std::fs::write(&path, contents).expect("written");
        Definitions(path)
    }

    fn env(pairs: &[(&str, &str)]) -> Env {
        pairs
            .iter()
            .map(|(key, val)| (key.to_string(), val.to_string()))
            .collect()
    }

    #[test]
    fn disabled_without_the_listen_variable() {
        assert!(from_env(&env(&[]), Some("prod")).expect("valid").is_none());
    }

    #[test]
    fn reads_and_validates_the_file() {
        let definitions = file(DEFINITIONS);
        let path = definitions.path().to_string_lossy().to_string();
        let config = from_env(
            &env(&[
                (LISTEN_VAR, "127.0.0.1:8090"),
                (FILE_VAR, &path),
                ("HOOK_SECRET", "a-secret-long-enough"),
            ]),
            Some("prod"),
        )
        .expect("valid")
        .expect("enabled");
        assert_eq!(config.namespace, "prod");
        assert_eq!(config.triggers.len(), 1);
    }

    #[test]
    fn a_namespace_and_a_file_are_both_required() {
        let error =
            from_env(&env(&[(LISTEN_VAR, "127.0.0.1:8090")]), None).expect_err("must refuse");
        assert!(error.to_string().contains("FLEXIQ_NAMESPACE"), "{error}");

        let error = from_env(&env(&[(LISTEN_VAR, "127.0.0.1:8090")]), Some("prod"))
            .expect_err("must refuse");
        assert!(error.to_string().contains(FILE_VAR), "{error}");
    }

    #[test]
    fn a_trigger_only_deployment_is_a_deployment_that_needs_a_dsn() {
        let definitions = file(DEFINITIONS);
        let path = definitions.path().to_string_lossy().to_string();
        let mut pairs = vec![
            (LISTEN_VAR, "127.0.0.1:8090"),
            (FILE_VAR, path.as_str()),
            ("HOOK_SECRET", "a-secret-long-enough"),
            ("FLEXIQ_NAMESPACE", "prod"),
        ];
        let error = crate::config::Config::from_map(&env(&pairs)).expect_err("must refuse");
        assert!(error.to_string().contains("FLEXIQ_DSN"), "{error}");

        pairs.push(("FLEXIQ_DSN", ":memory:"));
        let config = crate::config::Config::from_map(&env(&pairs)).expect("a role on its own");
        assert!(config.triggers.is_some());
        assert!(config.dashboard.is_none());
    }

    #[test]
    fn a_bad_file_stops_the_boot() {
        let definitions = file(DEFINITIONS);
        let path = definitions.path().to_string_lossy().to_string();
        // HOOK_SECRET unset: the definition names a secret that is not there.
        let error = from_env(
            &env(&[(LISTEN_VAR, "127.0.0.1:8090"), (FILE_VAR, &path)]),
            Some("prod"),
        )
        .expect_err("must refuse");
        assert!(format!("{error:#}").contains("HOOK_SECRET"), "{error:#}");
    }
}
