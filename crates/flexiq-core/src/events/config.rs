//! The egress configuration document, shared by every runtime.
//!
//! One JSON shape, parsed here, so the server's `FLEXIQ_EVENTS_FILE` and each
//! SDK's worker option are held to the same rules. Unknown fields are refused
//! everywhere: a misspelt `include_payload` that silently read as `false` is
//! harmless, but a misspelt `allow` that silently read as empty is not.

use std::collections::HashSet;

use serde::Deserialize;

use super::event::{EventType, JobEvent, DEFAULT_SOURCE};

// Every public type here is `#[non_exhaustive]`: new sink kinds and settings
// are planned, and the document is built by `EventsConfig::parse`, never by a
// struct literal outside this crate.

/// Events a sink buffers before it starts dropping, when unset.
pub const DEFAULT_BUFFER: usize = 10_000;
/// Delivery attempts per batch, first included, when unset.
pub const DEFAULT_MAX_ATTEMPTS: u32 = 5;
/// Stream entries a Redis sink keeps (approximately), when unset.
pub const DEFAULT_STREAM_MAX_LEN: u64 = 100_000;
/// Largest batch a sink may be configured to send.
pub const MAX_BATCH: usize = 1_000;

/// The whole document.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct EventsConfig {
    /// CloudEvents `source` stamped on every event.
    #[serde(default = "default_source")]
    pub source: String,
    /// Where events go. At least one.
    pub sinks: Vec<SinkConfig>,
}

fn default_source() -> String {
    DEFAULT_SOURCE.to_string()
}

/// One destination.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[non_exhaustive]
pub enum SinkConfig {
    /// CloudEvents over HTTP.
    Http(HttpSinkConfig),
    /// `XADD` to a Redis stream.
    RedisStreams(RedisSinkConfig),
}

/// Settings every sink kind shares.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct Delivery {
    /// Events buffered before new ones are dropped.
    #[serde(default = "default_buffer")]
    pub buffer: usize,
    /// Attempts per batch before it is dropped, first included.
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
    /// Events sent per request (HTTP) or pipeline (Redis).
    #[serde(default = "default_max_batch")]
    pub max_batch: usize,
}

impl Default for Delivery {
    fn default() -> Self {
        Self {
            buffer: DEFAULT_BUFFER,
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            max_batch: 1,
        }
    }
}

fn default_buffer() -> usize {
    DEFAULT_BUFFER
}
fn default_max_attempts() -> u32 {
    DEFAULT_MAX_ATTEMPTS
}
fn default_max_batch() -> usize {
    1
}

/// A CloudEvents HTTP sink.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct HttpSinkConfig {
    /// Metrics label and log name; unique in the document.
    pub name: String,
    /// Where events are POSTed. `https`, or `http` to loopback only.
    pub url: String,
    /// Hosts and CIDRs the URL may resolve to. Required and non-empty — the
    /// same deny-by-default allowlist push dispatch uses.
    pub allow: Vec<String>,
    /// Permit a loopback destination (and cleartext `http` to it). For tests
    /// and sidecars.
    #[serde(default)]
    pub allow_loopback: bool,
    /// Environment variable holding a bearer token.
    #[serde(default)]
    pub bearer_token_env: Option<String>,
    /// Environment variable holding the HMAC signing secret.
    #[serde(default)]
    pub hmac_secret_env: Option<String>,
    /// Per-request budget, milliseconds.
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    /// Connect budget, milliseconds.
    #[serde(default = "default_connect_timeout_ms")]
    pub connect_timeout_ms: u64,
    /// Which events this sink receives.
    #[serde(default)]
    pub filter: Filter,
    /// Send job payloads. Off by default: arguments can be personal data.
    #[serde(default)]
    pub include_payload: bool,
    /// Buffering, retries and batching.
    #[serde(default)]
    pub delivery: Delivery,
}

fn default_timeout_ms() -> u64 {
    10_000
}
fn default_connect_timeout_ms() -> u64 {
    5_000
}

/// A Redis Streams sink.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct RedisSinkConfig {
    /// Metrics label and log name; unique in the document.
    pub name: String,
    /// Environment variable holding the `redis://` URL. An environment
    /// variable, not a literal, because the URL carries the password.
    pub url_env: String,
    /// Stream key events are appended to.
    pub stream: String,
    /// Approximate cap on the stream's length (`MAXLEN ~`).
    #[serde(default = "default_stream_max_len")]
    pub max_len: u64,
    /// Which events this sink receives.
    #[serde(default)]
    pub filter: Filter,
    /// Send job payloads. Off by default: arguments can be personal data.
    #[serde(default)]
    pub include_payload: bool,
    /// Buffering, retries and batching.
    #[serde(default)]
    pub delivery: Delivery,
}

fn default_stream_max_len() -> u64 {
    DEFAULT_STREAM_MAX_LEN
}

/// Which events a sink receives. Each list is an allowlist; an empty or absent
/// one admits everything, and an event must pass all four.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct Filter {
    /// Namespaces; `default` names the default namespace.
    #[serde(default)]
    pub namespaces: HashSet<String>,
    /// Queue names.
    #[serde(default)]
    pub queues: HashSet<String>,
    /// Task names.
    #[serde(default)]
    pub tasks: HashSet<String>,
    /// Event types by short name, e.g. `job.dead`.
    #[serde(default)]
    pub types: HashSet<String>,
}

impl Filter {
    /// Whether an event with these attributes passes.
    pub fn admits(&self, event: &JobEvent) -> bool {
        admits(&self.types, event.event_type.as_str())
            && admits(&self.namespaces, event.namespace_label())
            && admits(&self.queues, &event.queue)
            && admits(&self.tasks, &event.task_name)
    }
}

fn admits(set: &HashSet<String>, value: &str) -> bool {
    set.is_empty() || set.contains(value)
}

/// Why a configuration document was refused.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum EventsConfigError {
    /// Not JSON, or not this shape.
    #[error("events config is not valid: {0}")]
    Parse(String),
    /// `source` is empty; CloudEvents requires a non-empty one.
    #[error("events config source must not be empty")]
    EmptySource,
    /// No sinks.
    #[error("events config names no sinks")]
    NoSinks,
    /// A sink name is empty or reused.
    #[error("events sink name '{0}' is empty or not unique")]
    SinkName(String),
    /// A filter names an event type that does not exist.
    #[error("events sink '{sink}': unknown event type '{name}'")]
    UnknownType {
        /// The sink.
        sink: String,
        /// The unknown type.
        name: String,
    },
    /// A numeric setting is out of range.
    #[error("events sink '{sink}': {message}")]
    Invalid {
        /// The sink.
        sink: String,
        /// What is wrong.
        message: String,
    },
    /// The sink kind is not compiled into this build.
    #[error("events sink '{sink}': kind '{kind}' needs a build with the `{feature}` feature")]
    NotCompiled {
        /// The sink.
        sink: String,
        /// The configured kind.
        kind: &'static str,
        /// The cargo feature it needs.
        feature: &'static str,
    },
    /// A sink could not be built from its settings.
    #[error("events sink '{sink}': {message}")]
    Sink {
        /// The sink.
        sink: String,
        /// What went wrong.
        message: String,
    },
}

impl EventsConfig {
    /// Parse and validate a document.
    pub fn parse(json: &str) -> Result<Self, EventsConfigError> {
        let config: Self =
            serde_json::from_str(json).map_err(|e| EventsConfigError::Parse(e.to_string()))?;
        config.validate()?;
        Ok(config)
    }

    /// The rules `parse` enforces, re-run by hub start because the fields are
    /// public and a document can be built without parsing.
    pub(crate) fn validate(&self) -> Result<(), EventsConfigError> {
        if self.source.trim().is_empty() {
            return Err(EventsConfigError::EmptySource);
        }
        if self.sinks.is_empty() {
            return Err(EventsConfigError::NoSinks);
        }
        let mut names = HashSet::new();
        for sink in &self.sinks {
            let name = sink.name();
            if name.trim().is_empty() || !names.insert(name) {
                return Err(EventsConfigError::SinkName(name.to_string()));
            }
            let invalid = |message: &str| EventsConfigError::Invalid {
                sink: name.to_string(),
                message: message.to_string(),
            };
            if let Some(unknown) = sink
                .filter()
                .types
                .iter()
                .find(|t| EventType::parse(t).is_none())
            {
                return Err(EventsConfigError::UnknownType {
                    sink: name.to_string(),
                    name: unknown.clone(),
                });
            }
            let delivery = sink.delivery();
            if delivery.buffer == 0 {
                return Err(invalid("delivery.buffer must be at least 1"));
            }
            if delivery.max_attempts == 0 {
                return Err(invalid("delivery.max_attempts must be at least 1"));
            }
            if delivery.max_batch == 0 || delivery.max_batch > MAX_BATCH {
                return Err(invalid(&format!(
                    "delivery.max_batch must be between 1 and {MAX_BATCH}"
                )));
            }
            match sink {
                SinkConfig::Http(http) => {
                    if http.allow.is_empty() {
                        return Err(invalid("allow must name at least one host or network"));
                    }
                    if http.timeout_ms == 0 || http.connect_timeout_ms == 0 {
                        return Err(invalid("timeouts must be at least 1 ms"));
                    }
                }
                SinkConfig::RedisStreams(redis) => {
                    if redis.url_env.trim().is_empty() {
                        return Err(invalid("url_env must not be empty"));
                    }
                    if redis.stream.trim().is_empty() {
                        return Err(invalid("stream must not be empty"));
                    }
                    if redis.max_len == 0 {
                        return Err(invalid("max_len must be at least 1"));
                    }
                }
            }
        }
        Ok(())
    }
}

impl SinkConfig {
    /// The `kind` the document names this sink by, e.g. `redis_streams`.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Http(_) => "http",
            Self::RedisStreams(_) => "redis_streams",
        }
    }

    /// The sink's name.
    pub fn name(&self) -> &str {
        match self {
            Self::Http(c) => &c.name,
            Self::RedisStreams(c) => &c.name,
        }
    }

    /// The sink's filter.
    pub fn filter(&self) -> &Filter {
        match self {
            Self::Http(c) => &c.filter,
            Self::RedisStreams(c) => &c.filter,
        }
    }

    /// Whether the sink sends payloads.
    pub fn include_payload(&self) -> bool {
        match self {
            Self::Http(c) => c.include_payload,
            Self::RedisStreams(c) => c.include_payload,
        }
    }

    /// The sink's delivery settings.
    pub fn delivery(&self) -> &Delivery {
        match self {
            Self::Http(c) => &c.delivery,
            Self::RedisStreams(c) => &c.delivery,
        }
    }

    /// Every environment variable the sink reads a secret from: a bearer
    /// token, an HMAC secret, or a Redis URL, which can carry a password.
    /// Here, not in a caller, so a new kind cannot forget to list its own.
    pub fn secret_env_vars(&self) -> Vec<&str> {
        match self {
            Self::Http(c) => [c.bearer_token_env.as_deref(), c.hmac_secret_env.as_deref()]
                .into_iter()
                .flatten()
                .collect(),
            Self::RedisStreams(c) => vec![c.url_env.as_str()],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HTTP: &str = r#"{"sinks":[{"kind":"http","name":"a","url":"https://e.example.com/","allow":["e.example.com"]}]}"#;

    #[test]
    fn minimal_http_document_takes_defaults() {
        let config = EventsConfig::parse(HTTP).unwrap();
        assert_eq!(config.source, "/flexiq");
        let sink = &config.sinks[0];
        assert!(!sink.include_payload());
        assert_eq!(sink.delivery().buffer, DEFAULT_BUFFER);
        assert_eq!(sink.delivery().max_batch, 1);
    }

    #[test]
    fn unknown_fields_are_refused_at_every_level() {
        for doc in [
            r#"{"sinks":[],"extra":1}"#,
            r#"{"sinks":[{"kind":"http","name":"a","url":"u","allow":["h"],"alow":["h"]}]}"#,
            r#"{"sinks":[{"kind":"http","name":"a","url":"u","allow":["h"],"filter":{"type":["job.dead"]}}]}"#,
            r#"{"sinks":[{"kind":"http","name":"a","url":"u","allow":["h"],"delivery":{"bufer":1}}]}"#,
            r#"{"sinks":[{"kind":"redis_streams","name":"r","url_env":"R","stream":"s","maxlen":1}]}"#,
            r#"{"sinks":[{"kind":"kafka","name":"a"}]}"#,
        ] {
            // "unknown": an internally tagged enum can swallow a variant's
            // `deny_unknown_fields`, so prove the refusal is for the stray key.
            let error = EventsConfig::parse(doc);
            assert!(
                matches!(&error, Err(EventsConfigError::Parse(m)) if m.contains("unknown")),
                "{doc}: {error:?}"
            );
        }
    }

    #[test]
    fn validation_refuses_what_would_fail_open() {
        let cases = [
            (r#"{"sinks":[]}"#, "no sinks"),
            (
                r#"{"source":" ","sinks":[{"kind":"http","name":"a","url":"u","allow":["h"]}]}"#,
                "source must not be empty",
            ),
            (
                r#"{"sinks":[{"kind":"http","name":"a","url":"u","allow":[]}]}"#,
                "allow",
            ),
            (
                r#"{"sinks":[{"kind":"http","name":"a","url":"u","allow":["h"],"filter":{"types":["job.boom"]}}]}"#,
                "unknown event type",
            ),
            (
                r#"{"sinks":[{"kind":"http","name":"a","url":"u","allow":["h"]},{"kind":"http","name":"a","url":"u","allow":["h"]}]}"#,
                "not unique",
            ),
            (
                r#"{"sinks":[{"kind":"http","name":"a","url":"u","allow":["h"],"delivery":{"buffer":0}}]}"#,
                "buffer",
            ),
            (
                r#"{"sinks":[{"kind":"redis_streams","name":"r","url_env":"R","stream":"s","max_len":0}]}"#,
                "max_len",
            ),
            (
                r#"{"sinks":[{"kind":"redis_streams","name":"r","url_env":" ","stream":"s"}]}"#,
                "url_env",
            ),
        ];
        for (doc, needle) in cases {
            let error = EventsConfig::parse(doc).unwrap_err().to_string();
            assert!(error.contains(needle), "{doc}: {error}");
        }
    }

    #[test]
    fn filter_lists_are_anded_and_empty_admits_all() {
        let doc = r#"{"sinks":[{"kind":"http","name":"a","url":"u","allow":["h"],
            "filter":{"types":["job.dead"],"namespaces":["default"]}}]}"#;
        let config = EventsConfig::parse(doc).unwrap();
        let filter = config.sinks[0].filter();
        let dead = JobEvent::new(EventType::JobDead, "j", None, "q", "t");
        assert!(filter.admits(&dead));
        let other_ns = JobEvent::new(EventType::JobDead, "j", Some("acme".into()), "q", "t");
        assert!(!filter.admits(&other_ns));
        let done = JobEvent::new(EventType::JobCompleted, "j", None, "q", "t");
        assert!(!filter.admits(&done));
        assert!(Filter::default().admits(&done));
    }
}
