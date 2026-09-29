//! Keeping the audit table from growing forever.
//!
//! The window is the server's, not a queue's: the shells never write this
//! table, so it has no place in the per-queue retention a shell configures.
//! Each listener prunes its own namespace on a timer. The delete is
//! idempotent, so replicas sharing a database need no election — two of them
//! pruning the same window just find nothing left the second time.

use std::time::Duration;

use flexiq_core::job::now_millis;
use flexiq_core::{Storage, StorageBackend};

use crate::runtime::shutdown::Shutdown;

/// How often the window is enforced. A record lives at most this much past
/// its window, which against a window of days is noise.
pub const PRUNE_EVERY: Duration = Duration::from_secs(3_600);

/// Prune `namespace`'s records older than `window`, now and every
/// [`PRUNE_EVERY`] until `shutdown`.
pub fn start(storage: StorageBackend, namespace: String, window: Duration, shutdown: Shutdown) {
    tokio::spawn(async move {
        let mut ticks = tokio::time::interval(PRUNE_EVERY);
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                () = shutdown.wait() => return,
                _ = ticks.tick() => {}
            }
            prune(&storage, &namespace, window).await;
        }
    });
}

/// One pass: delete what the window has passed, and say so.
async fn prune(storage: &StorageBackend, namespace: &str, window: Duration) {
    let cutoff = cutoff(now_millis(), window);
    let (storage, scope) = (storage.clone(), namespace.to_string());
    match tokio::task::spawn_blocking(move || storage.purge_audit(&scope, cutoff)).await {
        Ok(Ok(0)) => {}
        Ok(Ok(removed)) => {
            log::info!(
                "audit: pruned {removed} record(s) past the retention window in '{namespace}'"
            );
        }
        Ok(Err(error)) => log::warn!("audit: pruning '{namespace}' failed: {error}"),
        Err(error) => log::error!("audit: the prune task failed to run: {error}"),
    }
}

/// The instant a record must be newer than to survive. Saturating, so an
/// absurd window keeps everything rather than wrapping into deleting it.
fn cutoff(now_ms: i64, window: Duration) -> i64 {
    let window_ms = i64::try_from(window.as_millis()).unwrap_or(i64::MAX);
    now_ms.saturating_sub(window_ms)
}

#[cfg(test)]
mod tests {
    use super::*;
    use flexiq_core::storage::sqlite::SqliteStorage;
    use flexiq_core::{AuditFilter, AuditRecord};

    const DAY: Duration = Duration::from_secs(86_400);

    fn record(namespace: &str, id: &str, at_ms: i64) -> AuditRecord {
        AuditRecord {
            id: id.to_string(),
            namespace: namespace.to_string(),
            at_ms,
            token_id: "tok".to_string(),
            principal: "ci".to_string(),
            operation: "flexiq.v1.ProducerService/Enqueue".to_string(),
            target_kind: None,
            target: None,
            outcome: "OK".to_string(),
        }
    }

    #[test]
    fn the_cutoff_is_the_window_before_now() {
        assert_eq!(cutoff(10 * 86_400_000, DAY), 9 * 86_400_000);
    }

    #[test]
    fn an_absurd_window_keeps_everything() {
        assert!(cutoff(1_000, Duration::MAX) < 0, "before every record");
    }

    #[tokio::test]
    async fn a_pass_prunes_only_this_namespace_past_the_window() {
        let storage = StorageBackend::Sqlite(SqliteStorage::in_memory().expect("sqlite"));
        let now = now_millis();
        let old = now - 2 * 86_400_000;
        storage
            .append_audit(&[
                record("prod", "old", old),
                record("prod", "new", now),
                record("staging", "theirs", old),
            ])
            .expect("append");

        prune(&storage, "prod", DAY).await;

        let left = |namespace: &str| -> Vec<String> {
            storage
                .list_audit_after(namespace, &AuditFilter::default(), 100, None)
                .expect("list")
                .into_iter()
                .map(|r| r.id)
                .collect()
        };
        assert_eq!(left("prod"), ["new"]);
        assert_eq!(
            left("staging"),
            ["theirs"],
            "another namespace keeps its trail"
        );
    }
}
