//! Per-table retention windows.
//!
//! How long each history table keeps a row before auto-cleanup deletes it.
//! Named per *table*, never `result_ttl_ms` — that name already means the
//! per-entry TTL a single job or DLQ entry can carry, which is honored
//! independently of any window here.

use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::storage::{RetentionCounts, RetentionCutoffs, Storage};

const DAY_MS: i64 = 86_400_000;

/// Namespace label used when a queue runs unnamespaced.
pub const DEFAULT_NAMESPACE: &str = "default";

/// Settings-key prefix under which the elected retention leader publishes the
/// windows it applies, one key per namespace. Dashboards read it to echo the
/// live policy; the shells treat the prefix as reserved so it can neither be
/// listed as an editable setting nor spoofed through the settings API.
pub const RETENTION_SETTING_PREFIX: &str = "retention:effective:";

/// Recommended default windows, applied when a queue configures no retention at
/// all. Ordered by the invariant
/// `task_logs ≤ archived_jobs = job_errors = task_metrics ≤ dead_letter`:
/// logs churn hardest and are worth least (shortest); the DLQ is the only copy
/// of a payload a human must act on, so it is deleted last (longest).
pub const DEFAULT_ARCHIVED_JOBS_TTL_MS: i64 = 7 * DAY_MS;
/// Default `dead_letter` window: 30 days, in milliseconds.
pub const DEFAULT_DEAD_LETTER_TTL_MS: i64 = 30 * DAY_MS;
/// Default `task_logs` window: 3 days, in milliseconds.
pub const DEFAULT_TASK_LOGS_TTL_MS: i64 = 3 * DAY_MS;
/// Default `task_metrics` window: 7 days, in milliseconds.
pub const DEFAULT_TASK_METRICS_TTL_MS: i64 = 7 * DAY_MS;
/// Default `job_errors` window: 7 days, in milliseconds.
pub const DEFAULT_JOB_ERRORS_TTL_MS: i64 = 7 * DAY_MS;

/// Retention window per history table, in milliseconds. `None` keeps a table
/// forever.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct RetentionConfig {
    /// Terminal jobs (`archived_jobs`) — the artifact `get_job` reads after
    /// completion. Covers every terminal status.
    pub archived_jobs_ttl_ms: Option<i64>,
    /// Dead-letter entries. The only copy of a payload a human must act on, so
    /// deliberately the longest-lived by default.
    pub dead_letter_ttl_ms: Option<i64>,
    /// Task logs — highest write volume, lowest per-row value.
    pub task_logs_ttl_ms: Option<i64>,
    /// Task metrics — feeds the dashboard charts.
    pub task_metrics_ttl_ms: Option<i64>,
    /// Per-attempt job errors.
    pub job_errors_ttl_ms: Option<i64>,
}

impl RetentionConfig {
    /// Every table set to the same window. This is exactly what the deprecated
    /// queue-wide `result_ttl_ms` meant, so the legacy knob maps onto it losslessly.
    pub fn uniform(ttl_ms: i64) -> Self {
        Self {
            archived_jobs_ttl_ms: Some(ttl_ms),
            dead_letter_ttl_ms: Some(ttl_ms),
            task_logs_ttl_ms: Some(ttl_ms),
            task_metrics_ttl_ms: Some(ttl_ms),
            job_errors_ttl_ms: Some(ttl_ms),
        }
    }

    /// The recommended default windows, applied when a queue sets no retention.
    /// See the module-level `DEFAULT_*_TTL_MS` constants and their invariant.
    pub fn recommended() -> Self {
        Self {
            archived_jobs_ttl_ms: Some(DEFAULT_ARCHIVED_JOBS_TTL_MS),
            dead_letter_ttl_ms: Some(DEFAULT_DEAD_LETTER_TTL_MS),
            task_logs_ttl_ms: Some(DEFAULT_TASK_LOGS_TTL_MS),
            task_metrics_ttl_ms: Some(DEFAULT_TASK_METRICS_TTL_MS),
            job_errors_ttl_ms: Some(DEFAULT_JOB_ERRORS_TTL_MS),
        }
    }

    /// True when no table has a window — auto-cleanup only runs the per-entry
    /// TTL sweeps.
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }

    /// A copy with every negative window dropped to `None`. A negative window
    /// would invert the cutoff into the future (`now - (-ttl)`) and match every
    /// row, so it is treated as "no window" rather than a mass delete.
    pub fn sanitized(&self) -> Self {
        let keep = |ttl: Option<i64>| ttl.filter(|t| *t >= 0);
        Self {
            archived_jobs_ttl_ms: keep(self.archived_jobs_ttl_ms),
            dead_letter_ttl_ms: keep(self.dead_letter_ttl_ms),
            task_logs_ttl_ms: keep(self.task_logs_ttl_ms),
            task_metrics_ttl_ms: keep(self.task_metrics_ttl_ms),
            job_errors_ttl_ms: keep(self.job_errors_ttl_ms),
        }
    }

    /// The epoch-ms cutoffs a purge deletes below, one per table, for `now` —
    /// exactly the `now - ttl` the cleaner hands each purge. An absent window
    /// stays `None` (only the per-entry TTL is swept). Sanitizes first so a
    /// negative window can never invert into a future cutoff.
    pub fn cutoffs(&self, now: i64) -> RetentionCutoffs {
        let sanitized = self.sanitized();
        let cutoff = |ttl: Option<i64>| ttl.map(|t| now.saturating_sub(t));
        RetentionCutoffs {
            archived_jobs: cutoff(sanitized.archived_jobs_ttl_ms),
            dead_letter: cutoff(sanitized.dead_letter_ttl_ms),
            task_logs: cutoff(sanitized.task_logs_ttl_ms),
            task_metrics: cutoff(sanitized.task_metrics_ttl_ms),
            job_errors: cutoff(sanitized.job_errors_ttl_ms),
        }
    }
}

/// The retention windows a config actually applies. An explicit `retention`
/// wins (an empty one disables retention); otherwise the deprecated queue-wide
/// `result_ttl_ms` maps onto every table. With neither set, retention falls back
/// to the recommended defaults. Negative windows are dropped on both paths.
///
/// Single source of truth for [`crate::scheduler::SchedulerConfig::effective_retention`]
/// and the retention dry-run, so the preview can never diverge from the sweep.
pub fn resolve_effective_retention(
    retention: Option<&RetentionConfig>,
    result_ttl_ms: Option<i64>,
) -> RetentionConfig {
    if let Some(retention) = retention {
        return retention.sanitized();
    }
    match result_ttl_ms {
        Some(ttl) if ttl >= 0 => RetentionConfig::uniform(ttl),
        // A negative legacy TTL is set-but-invalid: disable rather than invert
        // the cutoff or silently upgrade to the recommended windows.
        Some(_) => RetentionConfig::default(),
        None => RetentionConfig::recommended(),
    }
}

/// True when retention runs on the recommended defaults because nothing was
/// configured — no per-table map and no legacy `result_ttl_ms`.
pub fn is_retention_defaulted(
    retention: Option<&RetentionConfig>,
    result_ttl_ms: Option<i64>,
) -> bool {
    retention.is_none() && result_ttl_ms.is_none()
}

/// What the elected retention leader is actually applying, as published for
/// dashboards to echo. Windows are milliseconds; a `null` window keeps that
/// table forever.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectiveRetention {
    /// False when no table has a window — only per-entry TTLs are swept.
    pub enabled: bool,
    /// True when the windows are the recommended defaults because the operator
    /// configured nothing.
    pub defaulted: bool,
    /// Namespace the windows apply to. The purges are not queue-scoped, so they
    /// cover every queue in this namespace.
    pub namespace: String,
    /// When the leader last published this document, in Unix milliseconds.
    pub reported_at: i64,
    /// The windows themselves.
    pub windows: RetentionConfig,
}

/// Settings key holding the published windows for `namespace`.
pub fn retention_setting_key(namespace: Option<&str>) -> String {
    format!(
        "{RETENTION_SETTING_PREFIX}{}",
        namespace.unwrap_or(DEFAULT_NAMESPACE)
    )
}

/// Publish the windows this leader applies so any dashboard can echo the live
/// policy instead of guessing at the defaults.
pub fn publish_effective_retention<S: Storage>(
    storage: &S,
    namespace: Option<&str>,
    snapshot: &EffectiveRetention,
) -> Result<()> {
    let json = serde_json::to_string(snapshot)?;
    storage.set_setting(&retention_setting_key(namespace), &json)
}

/// The windows last published for `namespace`, or `None` when no leader has
/// reported yet — the state a dashboard sees before the first cleanup sweep.
/// An unparseable document reads as unreported rather than failing the caller.
pub fn read_effective_retention<S: Storage>(
    storage: &S,
    namespace: Option<&str>,
) -> Result<Option<EffectiveRetention>> {
    let Some(raw) = storage.get_setting(&retention_setting_key(namespace))? else {
        return Ok(None);
    };
    // Unparseable reads as unreported, but say so: during a rolling upgrade that
    // is a schema mismatch, not "no leader has swept yet".
    Ok(serde_json::from_str(&raw)
        .inspect_err(|error| {
            log::warn!(
                "published retention document for {} is unparseable: {error}",
                namespace.unwrap_or(DEFAULT_NAMESPACE)
            )
        })
        .ok())
}

/// The published document for `namespace` as JSON, re-encoded from the parsed
/// form so a shell never forwards a malformed body. `None` when unreported.
pub fn read_effective_retention_json<S: Storage>(
    storage: &S,
    namespace: Option<&str>,
) -> Result<Option<String>> {
    read_effective_retention(storage, namespace)?
        .map(|snapshot| serde_json::to_string(&snapshot).map_err(Into::into))
        .transpose()
}

/// What a retention purge would delete right now, without deleting anything.
/// Counts are a point-in-time snapshot taken at `reference_time`; a `null`
/// window keeps its table forever and its count reflects per-entry TTLs only.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetentionPreview {
    /// False when no table has a window — only per-entry TTLs would be swept.
    pub enabled: bool,
    /// True when the windows are the recommended defaults, configured by no one.
    pub defaulted: bool,
    /// Namespace the windows cover. The purges are not queue-scoped.
    pub namespace: String,
    /// The `now` the snapshot was taken at, in Unix milliseconds.
    pub reference_time: i64,
    /// The effective windows the counts were computed against.
    pub windows: RetentionConfig,
    /// Rows each table's purge would remove.
    pub counts: RetentionCounts,
    /// Total rows across every table.
    pub total: u64,
}

/// Count what a retention purge would delete for the given config, without
/// deleting. Resolves the effective windows exactly as the cleaner does
/// ([`resolve_effective_retention`]), derives the same cutoffs, and counts the
/// matching rows in each table.
pub fn dry_run<S: Storage>(
    storage: &S,
    retention: Option<&RetentionConfig>,
    result_ttl_ms: Option<i64>,
    namespace: Option<&str>,
    now: i64,
) -> Result<RetentionPreview> {
    let windows = resolve_effective_retention(retention, result_ttl_ms);
    let counts = storage.count_expired_rows(&windows.cutoffs(now), now)?;
    Ok(RetentionPreview {
        enabled: !windows.is_empty(),
        defaulted: is_retention_defaulted(retention, result_ttl_ms),
        namespace: namespace.unwrap_or(DEFAULT_NAMESPACE).to_string(),
        reference_time: now,
        windows,
        total: counts.total(),
        counts,
    })
}

/// [`dry_run`] serialized to the JSON document a shell forwards. See
/// `BINDING_CONTRACT.md`.
pub fn dry_run_json<S: Storage>(
    storage: &S,
    retention: Option<&RetentionConfig>,
    result_ttl_ms: Option<i64>,
    namespace: Option<&str>,
    now: i64,
) -> Result<String> {
    Ok(serde_json::to_string(&dry_run(
        storage,
        retention,
        result_ttl_ms,
        namespace,
        now,
    )?)?)
}

/// [`dry_run`] against the policy the elected cleaner *reported* for this
/// namespace, falling back to the recommended defaults when no cleaner has
/// swept yet. For shells whose queue handle carries no retention config
/// (retention is a worker option there): the reported document is the policy
/// that actually governs the deletes, so the preview follows it rather than
/// assuming the defaults.
pub fn dry_run_reported<S: Storage>(
    storage: &S,
    namespace: Option<&str>,
    now: i64,
) -> Result<RetentionPreview> {
    let Some(published) = read_effective_retention(storage, namespace)? else {
        return dry_run(storage, None, None, namespace, now);
    };
    let windows = published.windows.sanitized();
    let counts = storage.count_expired_rows(&windows.cutoffs(now), now)?;
    Ok(RetentionPreview {
        enabled: !windows.is_empty(),
        // The reporter knows whether its windows were the defaults; carry that
        // through so the preview labels itself the same way the echo does.
        defaulted: published.defaulted,
        namespace: namespace.unwrap_or(DEFAULT_NAMESPACE).to_string(),
        reference_time: now,
        windows,
        total: counts.total(),
        counts,
    })
}

/// [`dry_run_reported`] serialized to the JSON document a shell forwards.
pub fn dry_run_reported_json<S: Storage>(
    storage: &S,
    namespace: Option<&str>,
    now: i64,
) -> Result<String> {
    Ok(serde_json::to_string(&dry_run_reported(
        storage, namespace, now,
    )?)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recommended_respects_the_ordering_invariant() {
        let r = RetentionConfig::recommended();
        let logs = r.task_logs_ttl_ms.unwrap();
        let archived = r.archived_jobs_ttl_ms.unwrap();
        let errors = r.job_errors_ttl_ms.unwrap();
        let metrics = r.task_metrics_ttl_ms.unwrap();
        let dlq = r.dead_letter_ttl_ms.unwrap();

        // task_logs ≤ archived_jobs = job_errors = task_metrics ≤ dead_letter.
        assert!(logs <= archived, "logs must not outlive their archived job");
        assert_eq!(archived, errors, "job_errors backs its archived job");
        assert_eq!(archived, metrics, "metrics must outlive their charted job");
        assert!(
            archived <= dlq,
            "the DLQ is the longest-lived, deliberately"
        );
        assert!(!r.is_empty(), "the recommended windows are not empty");
    }

    #[test]
    fn setting_key_is_namespaced() {
        assert_eq!(
            retention_setting_key(Some("tenant-a")),
            "retention:effective:tenant-a"
        );
        // An unnamespaced queue publishes under the same label the cleanup
        // announcement uses, so the two always name the same thing.
        assert_eq!(
            retention_setting_key(None),
            "retention:effective:default".to_string()
        );
    }

    #[test]
    fn published_document_round_trips() {
        let snapshot = EffectiveRetention {
            enabled: true,
            defaulted: false,
            namespace: DEFAULT_NAMESPACE.to_string(),
            reported_at: 1_753_200_000_000,
            windows: RetentionConfig {
                archived_jobs_ttl_ms: Some(DAY_MS),
                ..RetentionConfig::default()
            },
        };

        let json = serde_json::to_string(&snapshot).unwrap();
        assert_eq!(
            serde_json::from_str::<EffectiveRetention>(&json).unwrap(),
            snapshot
        );
        // A window that keeps its table forever must survive as an explicit
        // null — a shell reads absent and null identically.
        assert!(json.contains("\"dead_letter_ttl_ms\":null"));
    }

    #[test]
    fn cutoffs_map_windows_and_keep_none() {
        let cfg = RetentionConfig {
            archived_jobs_ttl_ms: Some(1000),
            task_logs_ttl_ms: None,
            ..RetentionConfig::default()
        };
        let c = cfg.cutoffs(10_000);
        assert_eq!(c.archived_jobs, Some(9_000), "cutoff = now - ttl");
        assert_eq!(c.task_logs, None, "an absent window stays None");
    }

    #[test]
    fn cutoffs_drop_a_negative_window() {
        let cfg = RetentionConfig {
            dead_letter_ttl_ms: Some(-5),
            ..RetentionConfig::default()
        };
        assert_eq!(
            cfg.cutoffs(10_000).dead_letter,
            None,
            "a negative window sanitizes to no window, never a future cutoff"
        );
    }

    #[test]
    fn dry_run_with_no_config_is_defaulted_and_enabled() {
        let storage = crate::storage::sqlite::SqliteStorage::in_memory().unwrap();
        let preview = dry_run(&storage, None, None, None, 1_753_200_000_000).unwrap();
        assert!(preview.enabled, "recommended defaults enable retention");
        assert!(preview.defaulted, "nothing configured → defaulted");
        assert_eq!(preview.namespace, DEFAULT_NAMESPACE);
        assert_eq!(preview.reference_time, 1_753_200_000_000);
        assert!(preview.windows.archived_jobs_ttl_ms.is_some());
        assert_eq!(preview.total, 0, "an empty store has nothing to purge");
    }

    #[test]
    fn dry_run_reported_follows_the_published_policy() {
        let storage = crate::storage::sqlite::SqliteStorage::in_memory().unwrap();

        // Unreported → falls back to the recommended defaults.
        let preview = dry_run_reported(&storage, None, 1_753_200_000_000).unwrap();
        assert!(preview.defaulted, "no report yet → defaults");

        // A cleaner published explicit windows → the preview follows them.
        let published = EffectiveRetention {
            enabled: true,
            defaulted: false,
            namespace: DEFAULT_NAMESPACE.to_string(),
            reported_at: 1_753_100_000_000,
            windows: RetentionConfig {
                task_logs_ttl_ms: Some(3_600_000),
                ..RetentionConfig::default()
            },
        };
        publish_effective_retention(&storage, None, &published).unwrap();

        let preview = dry_run_reported(&storage, None, 1_753_200_000_000).unwrap();
        assert!(!preview.defaulted, "published policy is not the defaults");
        assert_eq!(preview.windows.task_logs_ttl_ms, Some(3_600_000));
        assert_eq!(
            preview.windows.archived_jobs_ttl_ms, None,
            "tables the report leaves out keep forever"
        );
    }

    #[test]
    fn dry_run_with_empty_config_is_disabled_not_defaulted() {
        let storage = crate::storage::sqlite::SqliteStorage::in_memory().unwrap();
        let preview = dry_run(
            &storage,
            Some(&RetentionConfig::default()),
            None,
            None,
            1_753_200_000_000,
        )
        .unwrap();
        assert!(
            !preview.enabled,
            "an explicit empty config disables the windows"
        );
        assert!(
            !preview.defaulted,
            "an explicit config is the operator's own choice"
        );
    }
}
