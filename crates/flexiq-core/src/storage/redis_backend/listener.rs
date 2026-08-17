//! Redis `BLPOP`-based wake listener for push-dispatch.
//!
//! Entirely behind the `push-dispatch` feature. The default (feature-off)
//! build never compiles this module.
//!
//! A dedicated blocking connection loops on `BLPOP <notify-key> 1`. The 1s
//! timeout keeps shutdown responsive (the loop re-checks the forward channel
//! between blocks). Each popped sentinel forwards a unit wake into the
//! scheduler's [`crate::scheduler::wake::WakeSource::Channel`]. Connection
//! errors back off before reconnecting.

use std::time::Duration;

use tokio::sync::mpsc;

use super::RedisStorage;

/// `BLPOP` block timeout. Short enough to notice a dropped forward channel and
/// stop promptly on shutdown.
const BLPOP_TIMEOUT_SECS: f64 = 1.0;

/// Backoff after a connection error before reconnecting.
const RECONNECT_BACKOFF: Duration = Duration::from_millis(500);

/// Spawn the Redis wake listener and return the receiver end for the
/// scheduler's [`crate::scheduler::wake::WakeSource::Channel`].
pub fn spawn(storage: RedisStorage) -> mpsc::Receiver<()> {
    let (tx, rx) = mpsc::channel(1);
    let key = storage.notify_key();

    tokio::task::spawn_blocking(move || {
        loop {
            let mut conn = match storage.client().get_connection() {
                Ok(c) => c,
                Err(e) => {
                    log::warn!("push-dispatch: redis listener connect failed: {e}");
                    std::thread::sleep(RECONNECT_BACKOFF);
                    if tx.is_closed() {
                        break;
                    }
                    continue;
                }
            };

            loop {
                if tx.is_closed() {
                    return;
                }
                let popped: redis::RedisResult<Option<(String, i64)>> = redis::cmd("BLPOP")
                    .arg(&key)
                    .arg(BLPOP_TIMEOUT_SECS)
                    .query(&mut conn);

                match popped {
                    // Timed out with no element — just re-check shutdown.
                    Ok(None) => continue,
                    Ok(Some(_)) => match tx.try_send(()) {
                        Ok(()) => {}
                        Err(mpsc::error::TrySendError::Full(_)) => {}
                        Err(mpsc::error::TrySendError::Closed(_)) => return,
                    },
                    Err(e) => {
                        log::warn!("push-dispatch: redis BLPOP failed: {e}");
                        std::thread::sleep(RECONNECT_BACKOFF);
                        break; // reconnect
                    }
                }
            }
        }
    });

    rx
}
