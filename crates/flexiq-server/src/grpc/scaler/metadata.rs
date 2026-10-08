//! A `ScaledObject` trigger's metadata, read into what the scaler acts on.
//!
//! KEDA hands the trigger's `metadata:` map over verbatim as
//! `ScaledObjectRef.scalerMetadata`, with one rewrite: a `<key>FromEnv` entry
//! arrives holding the value of the named variable from the scale target's
//! environment, still under `<key>FromEnv`.

use std::collections::HashMap;

use crate::grpc::status::WireError;
use crate::scaling::TARGET_QUEUE_DEPTH;

/// The queue to measure; absent means every queue in the namespace.
pub const QUEUE: &str = "queue";
/// The per-replica queue depth the HPA aims for.
pub const TARGET: &str = "targetQueueDepth";
/// The depth at or below which the target counts as idle.
pub const ACTIVATION: &str = "activationQueueDepth";
/// A token written into the trigger itself.
pub const TOKEN: &str = "token";
/// A token KEDA resolved from the scale target's environment.
pub const TOKEN_FROM_ENV: &str = "tokenFromEnv";

/// What one scaled object asks of the scaler.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Query {
    /// The queue measured, or `None` for the whole namespace.
    pub queue: Option<String>,
    /// [`TARGET`], defaulting to [`TARGET_QUEUE_DEPTH`].
    pub target: i64,
    /// [`ACTIVATION`], defaulting to 0.
    pub activation: i64,
}

impl Query {
    /// Read the scaling keys out of `metadata`, refusing a malformed one by
    /// name. Unknown keys are KEDA's or the operator's, and are ignored.
    pub fn parse(metadata: &HashMap<String, String>) -> Result<Self, WireError> {
        let queue = match metadata.get(QUEUE) {
            Some(queue) if queue.is_empty() => {
                return Err(WireError::invalid_request(format!(
                    "{QUEUE} must not be empty; omit it to measure every queue"
                )))
            }
            queue => queue.cloned(),
        };
        Ok(Self {
            queue,
            target: integer(metadata, TARGET, 1)?.unwrap_or(TARGET_QUEUE_DEPTH),
            activation: integer(metadata, ACTIVATION, 0)?.unwrap_or(0),
        })
    }

    /// The metric this query reports, `flexiq-<queue>` or `flexiq-all`.
    ///
    /// KEDA prefixes it (`s0-…`) and hands it to the HPA as an external metric
    /// name, so it is kept to lowercase alphanumerics and `-`.
    pub fn metric_name(&self) -> String {
        let Some(queue) = &self.queue else {
            return "flexiq-all".to_string();
        };
        let safe: String = queue
            .chars()
            .map(|c| c.to_ascii_lowercase())
            .map(|c| {
                if c.is_ascii_lowercase() || c.is_ascii_digit() {
                    c
                } else {
                    '-'
                }
            })
            .collect();
        format!("flexiq-{safe}")
    }
}

/// The credential carried in `metadata`: [`TOKEN`] first, then
/// [`TOKEN_FROM_ENV`]. An empty value is no credential.
pub fn token(metadata: &HashMap<String, String>) -> Option<&str> {
    [TOKEN, TOKEN_FROM_ENV]
        .into_iter()
        .filter_map(|key| metadata.get(key))
        .map(String::as_str)
        .find(|value| !value.is_empty())
}

/// An optional integer key, at least `floor`.
fn integer(
    metadata: &HashMap<String, String>,
    key: &str,
    floor: i64,
) -> Result<Option<i64>, WireError> {
    let Some(raw) = metadata.get(key) else {
        return Ok(None);
    };
    match raw.trim().parse::<i64>() {
        Ok(value) if value >= floor => Ok(Some(value)),
        _ => Err(WireError::invalid_request(format!(
            "{key} must be an integer of at least {floor}, got {raw:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::Code;

    fn map(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect()
    }

    #[test]
    fn every_key_is_optional() {
        let query = Query::parse(&HashMap::new()).expect("valid");
        assert_eq!(
            query,
            Query {
                queue: None,
                target: TARGET_QUEUE_DEPTH,
                activation: 0,
            }
        );
        assert_eq!(query.metric_name(), "flexiq-all");
    }

    #[test]
    fn the_keys_override_the_defaults() {
        let query = Query::parse(&map(&[
            (QUEUE, "emails"),
            (TARGET, "25"),
            (ACTIVATION, "3"),
        ]))
        .expect("valid");
        assert_eq!(query.queue.as_deref(), Some("emails"));
        assert_eq!((query.target, query.activation), (25, 3));
    }

    #[test]
    fn a_bad_number_is_refused_by_name() {
        for (key, value) in [
            (TARGET, "0"),
            (TARGET, "-1"),
            (TARGET, "ten"),
            (TARGET, ""),
            (ACTIVATION, "-1"),
            (ACTIVATION, "1.5"),
        ] {
            let error = Query::parse(&map(&[(key, value)])).expect_err(value);
            assert_eq!(error.code(), Code::InvalidArgument);
            assert!(error.message().contains(key), "{}", error.message());
        }
    }

    #[test]
    fn an_empty_queue_is_refused() {
        let error = Query::parse(&map(&[(QUEUE, "")])).expect_err("empty");
        assert_eq!(error.code(), Code::InvalidArgument);
    }

    #[test]
    fn the_metric_name_is_dns_safe() {
        let query = Query::parse(&map(&[(QUEUE, "Emails_EU.v2")])).expect("valid");
        assert_eq!(query.metric_name(), "flexiq-emails-eu-v2");
    }

    #[test]
    fn the_token_comes_from_either_key() {
        assert_eq!(token(&map(&[(TOKEN, "a")])), Some("a"));
        assert_eq!(token(&map(&[(TOKEN_FROM_ENV, "b")])), Some("b"));
        assert_eq!(
            token(&map(&[(TOKEN, "a"), (TOKEN_FROM_ENV, "b")])),
            Some("a")
        );
        assert_eq!(
            token(&map(&[(TOKEN, ""), (TOKEN_FROM_ENV, "b")])),
            Some("b")
        );
        assert_eq!(token(&HashMap::new()), None);
    }
}
