//! The trigger definitions file: parsed once at boot, refused whole if any
//! part of it is wrong.
//!
//! The file holds configuration and never a secret. Each verifier names the
//! environment variable its secret lives in, so the file can sit in a
//! ConfigMap or a repository while the secret stays in a Secret — and a
//! definition can be printed with `{:?}` without leaking what it verifies with.
//!
//! What a trigger may enqueue is fixed here and nowhere else: the task, the
//! queue, the priority, the retry budget. A request supplies values for the
//! arguments and nothing more, so a caller holding a valid signature still
//! cannot aim a job at a different task, queue or namespace.

use std::collections::{BTreeMap, HashSet};

use anyhow::{bail, Context, Result};
use flexiq_core::RateLimitConfig;
use serde::Deserialize;

use crate::config::{value, Env};
use crate::trigger::auth::{
    Encoding, GoogleOidc, HeaderHmac, Key, SecretLocation, SharedSecret, StandardWebhooks, Stripe,
    Twilio, Verifier, DEFAULT_TOLERANCE_SECS,
};
use crate::trigger::mapping::{Mapping, Selector};
use crate::trigger::object_store::Provider;

/// Largest body a trigger may be configured to accept. Webhook payloads are
/// kilobytes; a body past this is a job payload that belongs in object
/// storage, with the trigger carrying its key.
pub const MAX_BODY_BYTES_CEILING: usize = 10 * 1024 * 1024;

/// Body limit when a definition names none.
pub const DEFAULT_MAX_BODY_BYTES: usize = 256 * 1024;

/// Retries when a definition names none — the SDKs' default.
const DEFAULT_MAX_RETRIES: i32 = 3;

/// Execution timeout when a definition names none — the producer door's.
const DEFAULT_TIMEOUT_SECS: u64 = 300;

/// Shortest secret accepted for the kinds whose secret the operator invents.
/// The provider-issued ones (GitHub, Stripe, Twilio) are the provider's to size.
const MIN_SECRET_LEN: usize = 16;

/// The one path the listener answers itself.
pub const HEALTH_PATH: &str = "/healthz";

/// One validated trigger.
#[derive(Debug, Clone)]
pub struct Trigger {
    /// Unique name: the rate-limit bucket, the metrics label, the log field.
    pub name: String,
    /// The URL path it answers on.
    pub path: String,
    /// The task every job it enqueues runs.
    pub task: String,
    /// The queue every job it enqueues lands in.
    pub queue: String,
    /// Dispatch priority of its jobs.
    pub priority: i32,
    /// Retry budget of its jobs.
    pub max_retries: i32,
    /// Execution timeout of its jobs, in milliseconds.
    pub timeout_ms: i64,
    /// The bucket every accepted request draws one token from.
    pub rate: RateLimitConfig,
    /// How a request proves its origin.
    pub verifier: Verifier,
    /// The environment variable the verifier's secret was read from — a name,
    /// not a secret, kept so `main` can scrub the variable once it is read.
    /// `None` for a verifier that checks against published keys instead.
    pub secret_env: Option<String>,
    /// How a request becomes arguments.
    pub mapping: Mapping,
    /// Where a delivery's identity comes from, so a redelivery deduplicates.
    pub unique_key: Option<Selector>,
    /// Largest body accepted, in bytes.
    pub max_body_bytes: usize,
    /// What sends to it, and so what shape its body arrives in.
    pub source: Source,
}

/// What a trigger's requests come from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    /// Any HTTP sender; the body is mapped as it arrives.
    Http,
    /// An object-store eventing platform; the body is unwrapped into one
    /// plain event per object first, and the mapping addresses those.
    ObjectStore(Provider),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawFile {
    triggers: Vec<RawTrigger>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawTrigger {
    name: String,
    path: String,
    task: String,
    #[serde(default = "default_queue")]
    queue: String,
    /// Not defaulted: a public URL that enqueues without a bound is a public
    /// URL anyone can fill a queue through, so the operator must choose one.
    rate_limit: String,
    auth: RawAuth,
    /// `None` maps the whole body as the one positional argument; `Some(vec![])`
    /// maps none.
    #[serde(default)]
    args: Option<Vec<Selector>>,
    #[serde(default)]
    kwargs: BTreeMap<String, Selector>,
    #[serde(default)]
    unique_key: Option<Selector>,
    #[serde(default)]
    priority: i32,
    #[serde(default = "default_max_retries")]
    max_retries: i32,
    #[serde(default = "default_timeout_secs")]
    timeout_secs: u64,
    #[serde(default = "default_max_body_bytes")]
    max_body_bytes: usize,
    #[serde(default)]
    kind: RawKind,
    #[serde(default)]
    provider: Option<Provider>,
}

#[derive(Deserialize, Default, Clone, Copy)]
#[serde(rename_all = "snake_case")]
enum RawKind {
    #[default]
    Http,
    ObjectStore,
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum RawAuth {
    SharedSecret {
        secret_env: String,
        #[serde(default)]
        header: Option<String>,
        #[serde(default)]
        prefix: String,
        #[serde(default)]
        query: Option<String>,
    },
    HmacSha256 {
        secret_env: String,
        header: String,
        #[serde(default)]
        prefix: String,
        #[serde(default)]
        encoding: RawEncoding,
    },
    Github {
        secret_env: String,
    },
    Stripe {
        secret_env: String,
        #[serde(default = "default_tolerance")]
        tolerance_secs: i64,
    },
    StandardWebhooks {
        secret_env: String,
        #[serde(default = "default_tolerance")]
        tolerance_secs: i64,
    },
    Twilio {
        secret_env: String,
        public_url: String,
    },
    GoogleOidc {
        audience: String,
        service_account: String,
    },
}

#[derive(Deserialize, Default, Clone, Copy)]
#[serde(rename_all = "snake_case")]
enum RawEncoding {
    #[default]
    Hex,
    Base64,
}

impl RawAuth {
    fn secret_env(&self) -> Option<&str> {
        match self {
            Self::SharedSecret { secret_env, .. }
            | Self::HmacSha256 { secret_env, .. }
            | Self::Github { secret_env }
            | Self::Stripe { secret_env, .. }
            | Self::StandardWebhooks { secret_env, .. }
            | Self::Twilio { secret_env, .. } => Some(secret_env),
            Self::GoogleOidc { .. } => None,
        }
    }
}

fn default_queue() -> String {
    "default".to_string()
}

fn default_max_retries() -> i32 {
    DEFAULT_MAX_RETRIES
}

fn default_timeout_secs() -> u64 {
    DEFAULT_TIMEOUT_SECS
}

fn default_max_body_bytes() -> usize {
    DEFAULT_MAX_BODY_BYTES
}

fn default_tolerance() -> i64 {
    DEFAULT_TOLERANCE_SECS
}

/// Parse and validate a definitions file, resolving secrets from `env`.
pub fn parse(text: &str, env: &Env) -> Result<Vec<Trigger>> {
    let raw: RawFile =
        serde_json::from_str(text).context("the triggers file is not a valid definition")?;
    if raw.triggers.is_empty() {
        bail!("the triggers file defines no triggers");
    }

    let mut names = HashSet::new();
    let mut paths = HashSet::new();
    let mut triggers = Vec::with_capacity(raw.triggers.len());
    for definition in raw.triggers {
        let name = definition.name.clone();
        let trigger = validate(definition, env).with_context(|| format!("trigger {name:?}"))?;
        if !names.insert(trigger.name.clone()) {
            bail!("trigger name {:?} is defined twice", trigger.name);
        }
        if !paths.insert(trigger.path.clone()) {
            bail!("trigger path {:?} is claimed twice", trigger.path);
        }
        triggers.push(trigger);
    }
    Ok(triggers)
}

fn validate(raw: RawTrigger, env: &Env) -> Result<Trigger> {
    check_name(&raw.name)?;
    check_path(&raw.path)?;
    if raw.task.trim().is_empty() {
        bail!("task must not be empty");
    }
    if raw.queue.trim().is_empty() {
        bail!("queue must not be empty");
    }
    let rate = RateLimitConfig::parse(&raw.rate_limit).with_context(|| {
        format!(
            "rate_limit {:?} is not a rate: write <count>/<s|m|h> with a count of at least 1",
            raw.rate_limit
        )
    })?;
    if raw.max_retries < 0 {
        bail!("max_retries must not be negative");
    }
    if raw.timeout_secs == 0 {
        bail!("timeout_secs must be greater than zero");
    }
    let timeout_ms = i64::try_from(raw.timeout_secs)
        .ok()
        .and_then(|secs| secs.checked_mul(1000))
        .context("timeout_secs is too large")?;
    if raw.max_body_bytes == 0 || raw.max_body_bytes > MAX_BODY_BYTES_CEILING {
        bail!("max_body_bytes must be between 1 and {MAX_BODY_BYTES_CEILING}");
    }

    let source = match (raw.kind, raw.provider) {
        (RawKind::Http, None) => Source::Http,
        (RawKind::ObjectStore, Some(provider)) => Source::ObjectStore(provider),
        (RawKind::Http, Some(_)) => bail!("provider applies only to kind object_store"),
        (RawKind::ObjectStore, None) => {
            bail!("kind object_store needs a provider: s3, gcs or azure")
        }
    };
    // An event platform redelivers until it hears a 2xx, and every event
    // carries an id, so an object-store trigger deduplicates on it unless told
    // otherwise. Without this, one slow answer is two jobs for one upload.
    let unique_key = match (&source, raw.unique_key) {
        (_, Some(selector)) => Some(selector),
        (Source::ObjectStore(_), None) => Some(Selector::Body {
            pointer: "/id".to_string(),
            optional: false,
        }),
        (Source::Http, None) => None,
    };

    let args = raw.args.unwrap_or_else(|| vec![Selector::whole_body()]);
    for selector in args
        .iter()
        .chain(raw.kwargs.values())
        .chain(unique_key.iter())
    {
        selector.validate().map_err(anyhow::Error::msg)?;
    }
    if raw.kwargs.keys().any(|name| name.is_empty()) {
        bail!("a keyword argument needs a name");
    }

    Ok(Trigger {
        secret_env: raw.auth.secret_env().map(str::to_string),
        verifier: verifier(raw.auth, env)?,
        name: raw.name,
        path: raw.path,
        task: raw.task,
        queue: raw.queue,
        priority: raw.priority,
        max_retries: raw.max_retries,
        timeout_ms,
        rate,
        mapping: Mapping {
            args,
            kwargs: raw.kwargs,
        },
        unique_key,
        max_body_bytes: raw.max_body_bytes,
        source,
    })
}

/// A name is a label value and a storage key, so it stays boring.
fn check_name(name: &str) -> Result<()> {
    let boring = |c: char| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.');
    if name.is_empty() || name.len() > 64 || !name.chars().all(boring) {
        bail!("name must be 1-64 characters of letters, digits, '-', '_' or '.'");
    }
    Ok(())
}

fn check_path(path: &str) -> Result<()> {
    let clean = |c: char| c.is_ascii_graphic() && !matches!(c, '?' | '#' | '%');
    if !path.starts_with('/') || path.len() > 256 || !path.chars().all(clean) {
        bail!(
            "path {path:?} must start with '/', and hold no whitespace, '?', '#' or \
             percent-encoding"
        );
    }
    if path == HEALTH_PATH {
        bail!("path {HEALTH_PATH} is the listener's own health check");
    }
    Ok(())
}

fn verifier(raw: RawAuth, env: &Env) -> Result<Verifier> {
    Ok(match raw {
        RawAuth::SharedSecret {
            secret_env,
            header,
            prefix,
            query,
        } => {
            let secret = secret(env, &secret_env, MIN_SECRET_LEN)?;
            let location = match (header, query) {
                (Some(name), None) => {
                    check_header(&name)?;
                    SecretLocation::Header { name, prefix }
                }
                (None, Some(name)) if !name.is_empty() && prefix.is_empty() => {
                    SecretLocation::Query { name }
                }
                _ => bail!(
                    "shared_secret takes exactly one of header (with an optional prefix) or query"
                ),
            };
            Verifier::SharedSecret(SharedSecret::new(location, secret))
        }
        RawAuth::HmacSha256 {
            secret_env,
            header,
            prefix,
            encoding,
        } => {
            check_header(&header)?;
            let secret = secret(env, &secret_env, MIN_SECRET_LEN)?;
            let encoding = match encoding {
                RawEncoding::Hex => Encoding::Hex,
                RawEncoding::Base64 => Encoding::Base64,
            };
            Verifier::HeaderHmac(HeaderHmac::new(
                header,
                prefix,
                encoding,
                Key::new(secret.into_bytes()),
            ))
        }
        RawAuth::Github { secret_env } => {
            let secret = secret(env, &secret_env, 1)?;
            Verifier::HeaderHmac(HeaderHmac::github(Key::new(secret.into_bytes())))
        }
        RawAuth::Stripe {
            secret_env,
            tolerance_secs,
        } => {
            check_tolerance(tolerance_secs)?;
            Verifier::Stripe(Stripe::new(&secret(env, &secret_env, 1)?, tolerance_secs))
        }
        RawAuth::StandardWebhooks {
            secret_env,
            tolerance_secs,
        } => {
            check_tolerance(tolerance_secs)?;
            let secret = secret(env, &secret_env, 1)?;
            Verifier::StandardWebhooks(
                StandardWebhooks::new(&secret, tolerance_secs).map_err(anyhow::Error::msg)?,
            )
        }
        RawAuth::Twilio {
            secret_env,
            public_url,
        } => {
            let parsed = url::Url::parse(&public_url)
                .with_context(|| format!("public_url {public_url:?} is not a URL"))?;
            if !matches!(parsed.scheme(), "https" | "http")
                || parsed.query().is_some()
                || parsed.fragment().is_some()
            {
                bail!(
                    "public_url must be an http(s) URL with no query or fragment — the \
                     request's own query is appended when the signature is checked"
                );
            }
            Verifier::Twilio(Twilio::new(&secret(env, &secret_env, 1)?, public_url))
        }
        RawAuth::GoogleOidc {
            audience,
            service_account,
        } => {
            if audience.trim().is_empty() {
                bail!(
                    "google_oidc needs the audience the push subscription was given — by \
                     default, the push endpoint URL"
                );
            }
            if !service_account.contains('@') {
                bail!(
                    "google_oidc needs the email of the service account the push \
                     subscription authenticates as"
                );
            }
            Verifier::GoogleOidc(GoogleOidc::new(audience, service_account))
        }
    })
}

/// The secret in `var`, which must be set and at least `min_len` long.
///
/// The variable's name is in the error; its value never is.
fn secret(env: &Env, var: &str, min_len: usize) -> Result<String> {
    if var.is_empty() {
        bail!("secret_env must name an environment variable");
    }
    let secret =
        value(env, var).with_context(|| format!("secret_env names {var}, which is not set"))?;
    if secret.chars().count() < min_len {
        bail!("the secret in {var} must be at least {min_len} characters");
    }
    Ok(secret)
}

fn check_header(name: &str) -> Result<()> {
    axum::http::HeaderName::try_from(name)
        .map(|_| ())
        .with_context(|| format!("{name:?} is not a valid header name"))
}

fn check_tolerance(seconds: i64) -> Result<()> {
    if !(1..=3600).contains(&seconds) {
        bail!("tolerance_secs must be between 1 and 3600");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    const SECRET: &str = "a-secret-long-enough";

    fn env() -> Env {
        Env::from([("HOOK_SECRET".to_string(), SECRET.to_string())])
    }

    fn minimal() -> serde_json::Value {
        json!({
            "name": "orders",
            "path": "/t/orders",
            "task": "orders.ingest",
            "rate_limit": "10/s",
            "auth": {"kind": "github", "secret_env": "HOOK_SECRET"}
        })
    }

    fn parse_one(trigger: serde_json::Value) -> Result<Trigger> {
        let file = json!({ "triggers": [trigger] }).to_string();
        parse(&file, &env()).map(|mut all| all.remove(0))
    }

    fn refusal(trigger: serde_json::Value) -> String {
        format!("{:#}", parse_one(trigger).expect_err("must refuse"))
    }

    fn with(field: &str, value: serde_json::Value) -> serde_json::Value {
        let mut trigger = minimal();
        trigger[field] = value;
        trigger
    }

    #[test]
    fn defaults_fill_in_around_the_required_fields() {
        let trigger = parse_one(minimal()).expect("valid");
        assert_eq!(trigger.queue, "default");
        assert_eq!(trigger.max_retries, DEFAULT_MAX_RETRIES);
        assert_eq!(trigger.timeout_ms, 300_000);
        assert_eq!(trigger.max_body_bytes, DEFAULT_MAX_BODY_BYTES);
        assert_eq!(trigger.mapping.args, vec![Selector::whole_body()]);
        assert!(trigger.mapping.kwargs.is_empty());
        assert_eq!(trigger.verifier.kind(), "github");
    }

    #[test]
    fn a_rate_limit_is_mandatory() {
        let mut trigger = minimal();
        trigger
            .as_object_mut()
            .expect("an object")
            .remove("rate_limit");
        assert!(refusal(trigger).contains("rate_limit"));
        assert!(refusal(with("rate_limit", json!("0/s"))).contains("rate_limit"));
    }

    #[test]
    fn an_unknown_field_is_refused_not_ignored() {
        // A misspelt `queue` would otherwise enqueue into "default".
        assert!(refusal(with("queu", json!("payments"))).contains("queu"));
    }

    #[test]
    fn there_is_no_anonymous_kind() {
        let error = refusal(with("auth", json!({"kind": "none"})));
        assert!(error.contains("none"), "{error}");
    }

    #[test]
    fn a_missing_or_short_secret_is_refused_by_name_only() {
        let error = refusal(with(
            "auth",
            json!({"kind": "github", "secret_env": "NOT_SET"}),
        ));
        assert!(error.contains("NOT_SET"), "{error}");

        let short = parse(
            &json!({"triggers": [with("auth", json!({
                "kind": "shared_secret", "secret_env": "SHORT", "header": "x-token"
            }))]})
            .to_string(),
            &Env::from([("SHORT".to_string(), "tiny".to_string())]),
        )
        .expect_err("must refuse");
        let message = format!("{short:#}");
        assert!(message.contains("SHORT"), "{message}");
        assert!(!message.contains("tiny"), "a secret leaked: {message}");
    }

    #[test]
    fn a_shared_secret_lives_in_exactly_one_place() {
        let both = json!({
            "kind": "shared_secret", "secret_env": "HOOK_SECRET",
            "header": "x-token", "query": "token"
        });
        assert!(refusal(with("auth", both)).contains("exactly one"));
        let query = json!({"kind": "shared_secret", "secret_env": "HOOK_SECRET", "query": "token"});
        assert!(parse_one(with("auth", query)).is_ok());
    }

    #[test]
    fn paths_and_names_are_checked() {
        for bad in ["t/orders", "/t/a b", "/t?x", "/healthz", "/t/%2F"] {
            assert!(parse_one(with("path", json!(bad))).is_err(), "{bad}");
        }
        for bad in ["", "has space", "slash/name"] {
            assert!(parse_one(with("name", json!(bad))).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn names_and_paths_are_unique() {
        let mut second = minimal();
        second["path"] = json!("/t/other");
        let file = json!({ "triggers": [minimal(), second] }).to_string();
        let error = format!("{:#}", parse(&file, &env()).expect_err("must refuse"));
        assert!(error.contains("defined twice"), "{error}");

        let mut third = minimal();
        third["name"] = json!("other");
        let file = json!({ "triggers": [minimal(), third] }).to_string();
        let error = format!("{:#}", parse(&file, &env()).expect_err("must refuse"));
        assert!(error.contains("claimed twice"), "{error}");
    }

    #[test]
    fn a_twilio_url_carries_no_query() {
        let auth =
            |url: &str| json!({"kind": "twilio", "secret_env": "HOOK_SECRET", "public_url": url});
        assert!(parse_one(with("auth", auth("https://hooks.example.com/t/sms"))).is_ok());
        assert!(parse_one(with("auth", auth("https://hooks.example.com/t/sms?a=1"))).is_err());
        assert!(parse_one(with("auth", auth("ftp://hooks.example.com/"))).is_err());
    }

    #[test]
    fn an_empty_file_and_a_bad_pointer_are_refused() {
        assert!(parse(r#"{"triggers": []}"#, &env()).is_err());
        let bad = with("kwargs", json!({"id": {"from": "body", "pointer": "id"}}));
        assert!(refusal(bad).contains("JSON Pointer"));
    }

    #[test]
    fn an_object_store_trigger_keys_on_the_event_id_by_default() {
        let mut trigger = with("kind", json!("object_store"));
        trigger["provider"] = json!("s3");
        let parsed = parse_one(trigger.clone()).expect("valid");
        assert_eq!(parsed.source, Source::ObjectStore(Provider::S3));
        assert_eq!(
            parsed.unique_key,
            Some(Selector::Body {
                pointer: "/id".into(),
                optional: false
            })
        );

        trigger["unique_key"] = json!({"from": "body", "pointer": "/key"});
        let parsed = parse_one(trigger).expect("valid");
        assert_eq!(
            parsed.unique_key,
            Some(Selector::Body {
                pointer: "/key".into(),
                optional: false
            })
        );

        assert!(parse_one(minimal()).expect("valid").unique_key.is_none());
    }

    #[test]
    fn kind_and_provider_go_together() {
        assert!(refusal(with("kind", json!("object_store"))).contains("provider"));
        assert!(refusal(with("provider", json!("gcs"))).contains("object_store"));
        let mut unknown = with("kind", json!("object_store"));
        unknown["provider"] = json!("dropbox");
        assert!(parse_one(unknown).is_err());
    }

    #[test]
    fn a_google_oidc_verifier_needs_no_secret_but_both_identities() {
        let auth = json!({
            "kind": "google_oidc",
            "audience": "https://hooks.example.com/t/gcs",
            "service_account": "pusher@p.iam.gserviceaccount.com"
        });
        let parsed = parse_one(with("auth", auth.clone())).expect("valid");
        assert_eq!(parsed.verifier.kind(), "google_oidc");
        assert_eq!(parsed.secret_env, None);

        let mut no_account = auth.clone();
        no_account["service_account"] = json!("not-an-email");
        assert!(refusal(with("auth", no_account)).contains("service account"));
        let mut no_audience = auth;
        no_audience["audience"] = json!(" ");
        assert!(refusal(with("auth", no_audience)).contains("audience"));
    }

    #[test]
    fn explicit_empty_args_map_nothing() {
        let trigger = parse_one(with("args", json!([]))).expect("valid");
        assert!(trigger.mapping.args.is_empty());
    }

    #[test]
    fn a_definition_prints_without_its_secret() {
        let printed = format!("{:?}", parse_one(minimal()).expect("valid"));
        assert!(!printed.contains(SECRET), "{printed}");
    }
}
