//! Where job lifecycle events are sent.
//!
//! Two variables: `FLEXIQ_EVENTS_FILE` names the JSON document of sinks (the
//! shape `flexiq_core::events::EventsConfig` parses, shared with every SDK),
//! and `FLEXIQ_EVENTS_DRAIN` bounds how long shutdown spends delivering what
//! is still buffered.
//!
//! Events are not a role: a deployment that sets only these has nothing that
//! would ever emit, so they do not satisfy the "at least one role" check.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use flexiq_core::events::SinkConfig;
use flexiq_core::EventsConfig;

use crate::config::push::seconds;
use crate::config::{value, Env};

/// The variable naming the events document.
pub const FILE_VAR: &str = "FLEXIQ_EVENTS_FILE";
/// Seconds shutdown spends delivering buffered events before dropping them.
pub const DRAIN_VAR: &str = "FLEXIQ_EVENTS_DRAIN";

/// Five seconds.
const DEFAULT_DRAIN: Duration = Duration::from_secs(5);

/// The parsed events document and its shutdown budget.
#[derive(Debug, Clone)]
pub struct EventsSettings {
    /// Where the document was read from, for boot errors and the startup log.
    pub file: PathBuf,
    /// The validated document.
    pub config: EventsConfig,
    /// How long shutdown waits for buffered events to be delivered.
    pub drain: Duration,
}

/// Parse the events block, or `None` when no document is named.
pub fn from_env(env: &Env) -> Result<Option<EventsSettings>> {
    let Some(file) = value(env, FILE_VAR).map(PathBuf::from) else {
        return Ok(None);
    };
    let text = std::fs::read_to_string(&file)
        .with_context(|| format!("{FILE_VAR}={} could not be read", file.display()))?;
    let config = EventsConfig::parse(&text)
        .with_context(|| format!("{FILE_VAR}={} was refused", file.display()))?;
    let drain = seconds(
        env,
        DRAIN_VAR,
        DEFAULT_DRAIN,
        "a zero drain has expired before shutdown starts, so every buffered event is \
         dropped rather than delivered. Use 1 for a near-immediate drain that still flushes",
    )?;
    Ok(Some(EventsSettings {
        file,
        config,
        drain,
    }))
}

/// Every variable the document names a sink secret by: bearer tokens, HMAC
/// secrets, and Redis URLs, which can carry a password.
pub fn secret_vars(settings: &EventsSettings) -> Vec<&str> {
    settings
        .config
        .sinks
        .iter()
        .flat_map(|sink| match sink {
            SinkConfig::Http(http) => vec![
                http.bearer_token_env.as_deref(),
                http.hmac_secret_env.as_deref(),
            ],
            SinkConfig::RedisStreams(redis) => vec![Some(redis.url_env.as_str())],
        })
        .flatten()
        .collect()
}

/// Remove every sink secret from the process environment once the hub has
/// read it, so no later in-process read or child process sees it. The
/// initial environment block, which `/proc/<pid>/environ` shows, keeps it.
pub fn scrub_event_secrets(settings: &EventsSettings) {
    // Called once from `main`, after the hub has built its sinks and before
    // any role is spawned. The sink threads already exist, but they read the
    // environment only while building (on this thread) and are parked on an
    // empty channel until the first event, which no role has emitted yet.
    for var in secret_vars(settings) {
        std::env::remove_var(var);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DOCUMENT: &str = r#"{"sinks": [{
        "kind": "http", "name": "warehouse", "url": "https://events.example.com/in",
        "allow": ["events.example.com"]
    }]}"#;

    /// An events document removed again when the test ends.
    struct Document(PathBuf);

    impl Document {
        fn path(&self) -> String {
            self.0.to_string_lossy().to_string()
        }
    }

    impl Drop for Document {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    fn file(contents: &str) -> Document {
        let path =
            std::env::temp_dir().join(format!("flexiq-events-{}.json", uuid::Uuid::new_v4()));
        std::fs::write(&path, contents).expect("written");
        Document(path)
    }

    fn env(pairs: &[(&str, &str)]) -> Env {
        pairs
            .iter()
            .map(|(key, val)| (key.to_string(), val.to_string()))
            .collect()
    }

    #[test]
    fn disabled_without_the_file_variable() {
        assert!(from_env(&env(&[])).expect("valid").is_none());
    }

    #[test]
    fn reads_the_document_with_a_default_drain() {
        let document = file(DOCUMENT);
        let settings = from_env(&env(&[(FILE_VAR, &document.path())]))
            .expect("valid")
            .expect("enabled");
        assert_eq!(settings.config.sinks.len(), 1);
        assert_eq!(settings.drain, Duration::from_secs(5));
        assert_eq!(settings.file, PathBuf::from(document.path()));
    }

    #[test]
    fn the_drain_is_whole_seconds_and_never_zero() {
        let document = file(DOCUMENT);
        let path = document.path();
        let settings = from_env(&env(&[(FILE_VAR, &path), (DRAIN_VAR, "12")]))
            .expect("valid")
            .expect("enabled");
        assert_eq!(settings.drain, Duration::from_secs(12));

        for bad in ["0", "soon", "1.5"] {
            let error =
                from_env(&env(&[(FILE_VAR, &path), (DRAIN_VAR, bad)])).expect_err("must refuse");
            assert!(format!("{error:#}").contains(DRAIN_VAR), "{error:#}");
        }
    }

    #[test]
    fn a_missing_file_names_its_path() {
        let path =
            std::env::temp_dir().join(format!("flexiq-absent-{}.json", uuid::Uuid::new_v4()));
        let path = path.to_string_lossy().to_string();
        let error = from_env(&env(&[(FILE_VAR, &path)])).expect_err("must refuse");
        let message = format!("{error:#}");
        assert!(message.contains(FILE_VAR), "{message}");
        assert!(message.contains(&path), "{message}");
    }

    #[test]
    fn a_bad_document_names_its_path_and_the_reason() {
        // An HTTP sink with no allowlist is refused by the core parser.
        let document = file(
            r#"{"sinks": [{"kind": "http", "name": "w", "url": "https://e.example.com", "allow": []}]}"#,
        );
        let error = from_env(&env(&[(FILE_VAR, &document.path())])).expect_err("must refuse");
        let message = format!("{error:#}");
        assert!(message.contains(&document.path()), "{message}");
        assert!(message.contains("allow"), "{message}");
    }

    #[test]
    fn events_alone_are_not_a_deployment() {
        let document = file(DOCUMENT);
        let error = crate::config::Config::from_map(&env(&[
            ("FLEXIQ_DSN", ":memory:"),
            (FILE_VAR, &document.path()),
        ]))
        .expect_err("must refuse");
        assert!(error.to_string().contains("nothing to run"), "{error}");

        let config = crate::config::Config::from_map(&env(&[
            ("FLEXIQ_DSN", ":memory:"),
            ("FLEXIQ_DASHBOARD", "127.0.0.1:8080"),
            (FILE_VAR, &document.path()),
        ]))
        .expect("events beside a role");
        assert!(config.events.is_some());
    }

    /// The only test here that touches the real process environment, as
    /// scrubbing acts on `std::env` directly. The names are unique to it so no
    /// other test can observe them.
    #[test]
    fn every_sink_secret_is_scrubbed_from_the_environment() {
        const BEARER: &str = "FLEXIQ_TEST_EVENTS_SCRUB_BEARER";
        const HMAC: &str = "FLEXIQ_TEST_EVENTS_SCRUB_HMAC";
        const REDIS: &str = "FLEXIQ_TEST_EVENTS_SCRUB_REDIS_URL";
        let document = file(&format!(
            r#"{{"sinks": [
                {{"kind": "http", "name": "w", "url": "https://events.example.com/in",
                  "allow": ["events.example.com"],
                  "bearer_token_env": "{BEARER}", "hmac_secret_env": "{HMAC}"}},
                {{"kind": "redis_streams", "name": "s", "url_env": "{REDIS}", "stream": "flexiq:events"}}
            ]}}"#
        ));
        let settings = from_env(&env(&[(FILE_VAR, &document.path())]))
            .expect("valid")
            .expect("enabled");
        assert_eq!(secret_vars(&settings), vec![BEARER, HMAC, REDIS]);

        for var in [BEARER, HMAC, REDIS] {
            std::env::set_var(var, "redis://:hunter2@cache:6379");
        }
        scrub_event_secrets(&settings);
        for var in [BEARER, HMAC, REDIS] {
            assert!(std::env::var(var).is_err(), "{var} survived the scrub");
        }
    }
}
