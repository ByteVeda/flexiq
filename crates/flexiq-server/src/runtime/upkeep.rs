//! Periodic cleanup of the rows the dashboard leaves behind.
//!
//! Expired sessions and abandoned OAuth state are inert but not free: they are
//! settings rows, and `list_settings` walks all of them. A start-up sweep only
//! helps a process that restarts, so a long-lived server needs a cadence.

use std::thread::{self, JoinHandle};
use std::time::Duration;

use flexiq_core::StorageBackend;

use crate::dashboard::auth::oauth::state as oauth_state;
use crate::dashboard::auth::store;
use crate::runtime::shutdown::Shutdown;

/// How often the sweep runs. Sessions live 24h and OAuth state 5 minutes, so
/// this only has to keep the tables from accumulating, not be prompt.
const INTERVAL: Duration = Duration::from_secs(15 * 60);

/// How often the loop wakes to notice shutdown between sweeps.
const TICK: Duration = Duration::from_secs(1);

/// Start the sweep loop. Returns a handle to join at shutdown.
pub fn spawn(storage: StorageBackend, shutdown: Shutdown) -> JoinHandle<()> {
    thread::Builder::new()
        .name("flexiq-dashboard-upkeep".to_string())
        .spawn(move || {
            // Sweep once at start too: whatever expired while the process was
            // down is already stale.
            sweep(&storage);
            let mut waited = Duration::ZERO;
            while !shutdown.is_triggered() {
                thread::sleep(TICK);
                waited += TICK;
                if waited >= INTERVAL {
                    waited = Duration::ZERO;
                    sweep(&storage);
                }
            }
        })
        .expect("spawning the upkeep thread cannot fail with a valid name")
}

/// One pass. Failures are logged and the loop continues — a storage blip must
/// not take the sweep down for the life of the process.
fn sweep(storage: &StorageBackend) {
    match store::prune_expired_sessions(storage) {
        Ok(0) => {}
        Ok(removed) => log::info!("[flexiq] pruned {removed} expired dashboard session(s)"),
        Err(error) => log::warn!("pruning expired sessions failed: {error}"),
    }
    match oauth_state::prune_expired(storage) {
        Ok(0) => {}
        Ok(removed) => log::info!("[flexiq] pruned {removed} abandoned OAuth login(s)"),
        Err(error) => log::warn!("pruning abandoned OAuth state failed: {error}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flexiq_core::{SqliteStorage, Storage};

    fn storage() -> StorageBackend {
        StorageBackend::Sqlite(SqliteStorage::new(":memory:").expect("in-memory SQLite"))
    }

    #[test]
    fn a_sweep_removes_expired_rows_and_leaves_live_ones() {
        let storage = storage();

        // An expired session and an abandoned login, written as the stores do.
        storage
            .set_setting(
                "auth:session:stale",
                r#"{"username":"ops","role":"admin","created_at":0,"expires_at":1,"csrf_token":"c"}"#,
            )
            .expect("seed a session");
        storage
            .set_setting(
                "auth:oauth_state:stale",
                r#"{"slot":"google","nonce":"n","code_verifier":"v","next_url":"/","created_at":0,"expires_at":1}"#,
            )
            .expect("seed an oauth state");
        storage
            .set_setting("dashboard:branding", "{}")
            .expect("seed an unrelated setting");

        sweep(&storage);

        assert!(storage
            .get_setting("auth:session:stale")
            .expect("read")
            .is_none());
        assert!(storage
            .get_setting("auth:oauth_state:stale")
            .expect("read")
            .is_none());
        assert!(
            storage
                .get_setting("dashboard:branding")
                .expect("read")
                .is_some(),
            "the sweep must touch only what it owns"
        );
    }

    #[test]
    fn the_loop_stops_on_shutdown() {
        let shutdown = Shutdown::default();
        let handle = spawn(storage(), shutdown.clone());
        shutdown.trigger();
        // Bounded by one tick; a hang here would hang every deploy.
        handle.join().expect("the upkeep thread exits");
    }
}
