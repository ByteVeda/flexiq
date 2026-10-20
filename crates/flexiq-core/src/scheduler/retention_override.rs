//! Retention windows set from a dashboard, beside the ones set in code.
//!
//! One document per namespace. `source` is the toggle: `code` applies the
//! windows the worker was started with, `dashboard` applies the stored ones.
//! Flipping back to `code` keeps the stored windows, so the editor reopens on
//! what was last saved. The elected cleaner reads it every sweep, so a change
//! applies within one cleanup interval and needs no restart.

use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::scheduler::retention::{RetentionConfig, DEFAULT_NAMESPACE};
use crate::storage::Storage;

/// Settings-key prefix of the override document, one key per namespace. Under
/// the reserved `retention:` prefix, so the generic settings API can neither
/// list nor write it.
pub const RETENTION_OVERRIDE_PREFIX: &str = "retention:override:";

/// Shortest window a dashboard may set. A window is "delete rows older than
/// this", so a slip of the unit (`0`, or minutes for days) would empty a table
/// on the next sweep; code can still set anything.
pub const MIN_OVERRIDE_WINDOW_MS: i64 = 3_600_000;

/// Which windows the cleaner applies.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetentionSource {
    /// The windows the worker was configured with in code.
    #[default]
    Code,
    /// The windows stored from the dashboard.
    Dashboard,
}

/// The stored override for one namespace.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetentionOverride {
    /// Which windows apply.
    pub source: RetentionSource,
    /// The dashboard's windows, kept while `source` is `code`. `null` keeps a
    /// table forever.
    pub windows: RetentionConfig,
    /// When it was last saved, in Unix milliseconds; `null` when never saved.
    pub updated_at: Option<i64>,
}

impl RetentionOverride {
    /// The windows the cleaner should apply instead of the code config, if any.
    pub fn active_windows(&self) -> Option<RetentionConfig> {
        (self.source == RetentionSource::Dashboard).then(|| self.windows.sanitized())
    }

    /// Parse and validate a dashboard's `PUT` body, stamping `now`.
    ///
    /// The error is a message for the caller — a shell answers it as a `400` —
    /// rather than a storage error.
    pub fn from_request_json(body: &str, now: i64) -> std::result::Result<Self, String> {
        let request: OverrideRequest =
            serde_json::from_str(body).map_err(|error| format!("invalid body: {error}"))?;
        let windows = request.windows.into_config();
        for (field, window) in windows_by_field(&windows) {
            if let Some(ms) = window {
                if ms < MIN_OVERRIDE_WINDOW_MS {
                    return Err(format!(
                        "{field} must be at least {MIN_OVERRIDE_WINDOW_MS} ms (one hour), \
                         or null to keep the table forever"
                    ));
                }
            }
        }
        Ok(Self {
            source: request.source,
            windows,
            updated_at: Some(now),
        })
    }
}

/// A `PUT` body. Strict, so a misspelt table is refused instead of silently
/// read as "keep forever".
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OverrideRequest {
    source: RetentionSource,
    #[serde(default)]
    windows: WindowsRequest,
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct WindowsRequest {
    archived_jobs_ttl_ms: Option<i64>,
    dead_letter_ttl_ms: Option<i64>,
    task_logs_ttl_ms: Option<i64>,
    task_metrics_ttl_ms: Option<i64>,
    job_errors_ttl_ms: Option<i64>,
}

impl WindowsRequest {
    fn into_config(self) -> RetentionConfig {
        RetentionConfig {
            archived_jobs_ttl_ms: self.archived_jobs_ttl_ms,
            dead_letter_ttl_ms: self.dead_letter_ttl_ms,
            task_logs_ttl_ms: self.task_logs_ttl_ms,
            task_metrics_ttl_ms: self.task_metrics_ttl_ms,
            job_errors_ttl_ms: self.job_errors_ttl_ms,
        }
    }
}

fn windows_by_field(windows: &RetentionConfig) -> [(&'static str, Option<i64>); 5] {
    [
        ("archived_jobs_ttl_ms", windows.archived_jobs_ttl_ms),
        ("dead_letter_ttl_ms", windows.dead_letter_ttl_ms),
        ("task_logs_ttl_ms", windows.task_logs_ttl_ms),
        ("task_metrics_ttl_ms", windows.task_metrics_ttl_ms),
        ("job_errors_ttl_ms", windows.job_errors_ttl_ms),
    ]
}

/// Settings key of the override for `namespace`.
pub fn retention_override_key(namespace: Option<&str>) -> String {
    format!(
        "{RETENTION_OVERRIDE_PREFIX}{}",
        namespace.unwrap_or(DEFAULT_NAMESPACE)
    )
}

/// The stored override, or `None` when none was ever saved.
///
/// An unparseable document is an error, not "no override": reading it as
/// absent would silently swap a dashboard's windows for the code config.
pub fn read_retention_override<S: Storage>(
    storage: &S,
    namespace: Option<&str>,
) -> Result<Option<RetentionOverride>> {
    storage
        .get_setting(&retention_override_key(namespace))?
        .map(|raw| serde_json::from_str(&raw).map_err(Into::into))
        .transpose()
}

/// The stored override as JSON, or the default (`code`, no windows, never
/// saved) when there is none — the document a dashboard's editor opens on.
pub fn read_retention_override_json<S: Storage>(
    storage: &S,
    namespace: Option<&str>,
) -> Result<String> {
    let document = read_retention_override(storage, namespace)?.unwrap_or_default();
    Ok(serde_json::to_string(&document)?)
}

/// Store `document` as the override for `namespace`.
pub fn write_retention_override<S: Storage>(
    storage: &S,
    namespace: Option<&str>,
    document: &RetentionOverride,
) -> Result<()> {
    let json = serde_json::to_string(document)?;
    storage.set_setting(&retention_override_key(namespace), &json)
}

/// Drop the override for `namespace`: the code config applies and the editor
/// forgets its windows. True when one existed.
pub fn clear_retention_override<S: Storage>(storage: &S, namespace: Option<&str>) -> Result<bool> {
    storage.delete_setting(&retention_override_key(namespace))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::settings::is_reserved_setting_key;

    const DAY_MS: i64 = 86_400_000;

    #[test]
    fn the_key_is_namespaced_and_reserved() {
        assert_eq!(
            retention_override_key(Some("tenant-a")),
            "retention:override:tenant-a"
        );
        assert_eq!(retention_override_key(None), "retention:override:default");
        assert!(is_reserved_setting_key(&retention_override_key(None)));
    }

    #[test]
    fn only_the_dashboard_source_replaces_the_code_config() {
        let mut document = RetentionOverride {
            source: RetentionSource::Code,
            windows: RetentionConfig::uniform(DAY_MS),
            updated_at: Some(1),
        };
        assert_eq!(document.active_windows(), None);

        document.source = RetentionSource::Dashboard;
        assert_eq!(
            document.active_windows(),
            Some(RetentionConfig::uniform(DAY_MS))
        );
    }

    #[test]
    fn a_request_parses_with_absent_tables_kept_forever() {
        let document = RetentionOverride::from_request_json(
            r#"{"source":"dashboard","windows":{"task_logs_ttl_ms":86400000}}"#,
            42,
        )
        .expect("valid");
        assert_eq!(document.source, RetentionSource::Dashboard);
        assert_eq!(document.windows.task_logs_ttl_ms, Some(DAY_MS));
        assert_eq!(document.windows.dead_letter_ttl_ms, None);
        assert_eq!(document.updated_at, Some(42));
    }

    #[test]
    fn a_request_is_refused_when_it_would_empty_a_table_or_misnames_one() {
        for body in [
            r#"{"source":"dashboard","windows":{"task_logs_ttl_ms":0}}"#,
            r#"{"source":"dashboard","windows":{"task_logs_ttl_ms":-5}}"#,
            r#"{"source":"dashboard","windows":{"task_log_ttl_ms":86400000}}"#,
            r#"{"source":"code","extra":true}"#,
            r#"{"source":"worker"}"#,
            r#"{"windows":{}}"#,
        ] {
            assert!(
                RetentionOverride::from_request_json(body, 0).is_err(),
                "{body}"
            );
        }
    }

    #[test]
    fn the_default_document_reads_as_code_never_saved() {
        let json = serde_json::to_value(RetentionOverride::default()).expect("encode");
        assert_eq!(json["source"], "code");
        assert_eq!(json["updated_at"], serde_json::Value::Null);
        assert_eq!(
            json["windows"]["dead_letter_ttl_ms"],
            serde_json::Value::Null
        );
    }
}
