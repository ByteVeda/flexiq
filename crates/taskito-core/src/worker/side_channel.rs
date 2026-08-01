//! The storage an attached executor does not have.
//!
//! An executor exists so the app image needs no database credentials, which
//! leaves five job-scoped operations with nowhere to go: progress, task logs,
//! published partials, the dashboard's middleware toggles, and job metadata.
//! The scheduler *does* hold the connection, so the executor asks it instead of
//! reaching for a database of its own.
//!
//! This module is the scheduler's half of that arrangement: the narrow surface
//! [`RemoteDispatcher`](super::remote::RemoteDispatcher) needs, plus the
//! storage-backed implementation a real deployment installs. It is a trait
//! rather than an `Arc<dyn Storage>` so the dispatcher can be tested against a
//! fake, and so the settings key a toggle list lives under is spelled in one
//! place instead of at every call site.

use std::collections::HashMap;
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant};

use crate::storage::{Storage, StorageBackend};

/// How long a resolved toggle list is reused before it is re-read.
///
/// Matches the SDK worker caches (`_MW_CHAIN_TTL` in the Python shell): a
/// dashboard toggle is rare and a dispatch is hot, so a bounded staleness
/// window is worth far more than a settings read per job.
const DISABLE_CACHE_TTL: Duration = Duration::from_secs(5);

/// What the scheduler does on an executor's behalf, because the executor has no
/// storage of its own.
///
/// Every method is infallible by design: a task that only wanted to report
/// progress must not fail because the database was briefly unhappy, so a
/// failure is logged and dropped rather than propagated to the job.
pub trait SideChannel: Send + Sync + 'static {
    /// Record a running job's progress (0-100).
    fn update_progress(&self, job_id: &str, progress: i32);

    /// Append one structured log line. `extra` is pre-encoded JSON; a published
    /// partial arrives here as level `result`.
    fn write_task_log(
        &self,
        job_id: &str,
        task_name: &str,
        level: &str,
        message: &str,
        extra: Option<&str>,
        namespace: Option<&str>,
    );

    /// Middleware the operator has disabled for `task_name`, for attaching to a
    /// dispatch frame.
    fn disabled_middleware(&self, task_name: &str) -> Vec<String>;
}

/// The [`SideChannel`] a real deployment installs: straight through to storage.
pub struct StorageSideChannel {
    storage: StorageBackend,
    /// Resolved toggle lists, with the instant each was read. Bounded by the
    /// number of distinct task names, which is the app's own vocabulary.
    disables: Mutex<HashMap<String, (Vec<String>, Instant)>>,
}

impl StorageSideChannel {
    /// Wrap the scheduler's storage.
    pub fn new(storage: StorageBackend) -> Self {
        Self {
            storage,
            disables: Mutex::new(HashMap::new()),
        }
    }

    /// The settings key holding `task_name`'s disable list.
    ///
    /// Kept identical to the key every dashboard writes
    /// (`middleware:disabled:<task_name>`), which is already reserved in
    /// [`crate::settings::RESERVED_SETTING_PREFIXES`] so a generic settings API
    /// can neither read nor forge one.
    fn disable_key(task_name: &str) -> String {
        format!("middleware:disabled:{task_name}")
    }

    /// Read and decode the disable list, treating every failure as "nothing
    /// disabled" — the same non-fatal stance the SDK workers take, because a
    /// settings blip must not silently change which middleware runs.
    fn read_disabled(&self, task_name: &str) -> Vec<String> {
        let raw = match self.storage.get_setting(&Self::disable_key(task_name)) {
            Ok(Some(raw)) => raw,
            Ok(None) => return Vec::new(),
            Err(error) => {
                log::warn!(
                    "[taskito] could not read the middleware disable list for '{task_name}': \
                     {error}; dispatching with none disabled"
                );
                return Vec::new();
            }
        };
        serde_json::from_str::<Vec<String>>(&raw).unwrap_or_else(|error| {
            log::warn!(
                "[taskito] the middleware disable list for '{task_name}' is not a JSON array of \
                 names ({error}); dispatching with none disabled"
            );
            Vec::new()
        })
    }
}

/// Recover a guard from a poisoned lock instead of cascading the panic. The
/// state behind it is a cache, which stays safe to read.
fn recover<T>(poisoned: PoisonError<T>) -> T {
    poisoned.into_inner()
}

impl SideChannel for StorageSideChannel {
    fn update_progress(&self, job_id: &str, progress: i32) {
        if let Err(error) = self.storage.update_progress(job_id, progress) {
            log::warn!("[taskito] could not record progress for job {job_id}: {error}");
        }
    }

    fn write_task_log(
        &self,
        job_id: &str,
        task_name: &str,
        level: &str,
        message: &str,
        extra: Option<&str>,
        namespace: Option<&str>,
    ) {
        let written = self
            .storage
            .write_task_log(job_id, task_name, level, message, extra, namespace);
        if let Err(error) = written {
            log::warn!("[taskito] could not write a task log for job {job_id}: {error}");
        }
    }

    fn disabled_middleware(&self, task_name: &str) -> Vec<String> {
        {
            let cache = self.disables.lock().unwrap_or_else(recover);
            if let Some((disabled, read_at)) = cache.get(task_name) {
                if read_at.elapsed() < DISABLE_CACHE_TTL {
                    return disabled.clone();
                }
            }
        }

        // Read outside the lock: a slow settings backend would otherwise stall
        // every other task's dispatch behind this one.
        let disabled = self.read_disabled(task_name);
        self.disables
            .lock()
            .unwrap_or_else(recover)
            .insert(task_name.to_string(), (disabled.clone(), Instant::now()));
        disabled
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::sqlite::SqliteStorage;

    fn channel() -> StorageSideChannel {
        StorageSideChannel::new(StorageBackend::Sqlite(
            SqliteStorage::in_memory().expect("in-memory storage"),
        ))
    }

    #[test]
    fn an_unset_toggle_list_is_empty_rather_than_an_error() {
        assert!(channel().disabled_middleware("resize").is_empty());
    }

    #[test]
    fn a_stored_toggle_list_is_decoded_and_then_cached() {
        let channel = channel();
        channel
            .storage
            .set_setting(
                &StorageSideChannel::disable_key("resize"),
                r#"["tracing","app.mw.Audit"]"#,
            )
            .expect("set");

        assert_eq!(
            channel.disabled_middleware("resize"),
            ["tracing", "app.mw.Audit"]
        );

        // Within the TTL the cached list is reused, so a dispatch costs no
        // settings read.
        channel
            .storage
            .set_setting(&StorageSideChannel::disable_key("resize"), r#"[]"#)
            .expect("set");
        assert_eq!(
            channel.disabled_middleware("resize"),
            ["tracing", "app.mw.Audit"]
        );
    }

    #[test]
    fn a_malformed_toggle_list_disables_nothing() {
        // Failing open matters more than failing loud here: the alternative is
        // silently running a chain the operator thinks they turned off, or
        // failing every job for a bad settings row.
        let channel = channel();
        channel
            .storage
            .set_setting(&StorageSideChannel::disable_key("resize"), "not json")
            .expect("set");
        assert!(channel.disabled_middleware("resize").is_empty());
    }

    #[test]
    fn the_toggle_key_matches_what_a_dashboard_writes() {
        assert_eq!(
            StorageSideChannel::disable_key("resize"),
            "middleware:disabled:resize"
        );
        assert!(crate::settings::is_reserved_setting_key(
            &StorageSideChannel::disable_key("resize")
        ));
    }
}
