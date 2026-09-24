//! Redis pub/sub wake listener for push-dispatch.
//!
//! Entirely behind the `push-dispatch` feature. The default (feature-off)
//! build never compiles this module.
//!
//! A dedicated blocking connection `SUBSCRIBE`s to the notify channel of every
//! queue the scheduler serves; each message forwards a unit wake into the
//! scheduler's [`crate::scheduler::wake::WakeSource::Channel`]. Pub/sub is a
//! broadcast, so every scheduler serving a queue wakes — a consumed signal
//! (a list pop) would wake one, possibly one that cannot take the job. A read
//! timeout bounds each wait so the loop re-checks the forward channel and
//! stops promptly on shutdown. Errors back off exponentially (capped at
//! [`MAX_BACKOFF`]), then reconnect and resubscribe; a Redis that never lets
//! the listener subscribe (ACL without `@pubsub`, a pub/sub-less proxy) logs
//! one `error!` per failure streak, not one per retry, while dispatch runs on
//! the fallback poll. Every successful subscribe forwards one wake, so a
//! message published while no subscription existed is drained on arrival.

use std::time::Duration;

use tokio::sync::mpsc;

use super::RedisStorage;

/// Socket read deadline, and so the cadence at which an idle listener notices
/// a dropped forward channel. Also bounds a half-dead connection's read.
const READ_TIMEOUT: Duration = Duration::from_secs(1);

/// First backoff after an error; doubles per consecutive failure.
const INITIAL_BACKOFF: Duration = Duration::from_millis(500);

/// Backoff ceiling: a Redis that rejects SUBSCRIBE outright is retried at this
/// cadence rather than hammered every half second forever.
const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// Longest single sleep inside a backoff, so a dropped forward channel is
/// noticed — and shutdown proceeds — within this bound even at [`MAX_BACKOFF`].
const SHUTDOWN_CHECK: Duration = Duration::from_millis(250);

/// Connect deadline for the listener's connection. A black-holed connect would
/// otherwise block the blocking task — and so `Runtime::drop` — for the OS
/// TCP timeout (minutes).
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);

/// Spawn the Redis wake listener for `queues` and return the receiver end for
/// the scheduler's [`crate::scheduler::wake::WakeSource::Channel`].
///
/// The task ends once that receiver is dropped (the push loop returned),
/// within one read timeout; every other wait in the loop is bounded too.
pub fn spawn(storage: RedisStorage, queues: &[String]) -> mpsc::Receiver<()> {
    let (tx, rx) = mpsc::channel(1);
    let channels: Vec<String> = queues.iter().map(|q| storage.notify_channel(q)).collect();

    tokio::task::spawn_blocking(move || {
        let mut backoff = Backoff::new();
        while !tx.is_closed() {
            let mut conn = match connect(&storage) {
                Ok(c) => c,
                Err(e) => {
                    backoff.fail("connect", &e, &tx);
                    continue;
                }
            };
            let mut pubsub = conn.as_pubsub();
            if let Err(e) = pubsub.subscribe(&channels) {
                backoff.fail("SUBSCRIBE", &e, &tx);
                continue;
            }
            backoff.reset();
            // Pub/sub keeps nothing for an absent subscriber: wake once so a
            // publish before this (re)subscribe — startup or a reconnect gap —
            // is still drained now rather than at the fallback.
            let _ = tx.try_send(());

            while !tx.is_closed() {
                match pubsub.get_message() {
                    Ok(_) => match tx.try_send(()) {
                        Ok(()) => {}
                        Err(mpsc::error::TrySendError::Full(_)) => {}
                        Err(mpsc::error::TrySendError::Closed(_)) => return,
                    },
                    // Idle past the read deadline — just re-check shutdown.
                    Err(e) if e.is_timeout() => {}
                    Err(e) => {
                        backoff.fail("pub/sub read", &e, &tx);
                        break; // reconnect and resubscribe
                    }
                }
            }
        }
    });

    rx
}

/// Open the listener's dedicated connection with both deadlines set.
fn connect(storage: &RedisStorage) -> redis::RedisResult<redis::Connection> {
    let conn = storage
        .client()
        .get_connection_with_timeout(CONNECT_TIMEOUT)?;
    conn.set_read_timeout(Some(READ_TIMEOUT))?;
    Ok(conn)
}

/// Exponential reconnect backoff that logs once per failure streak.
struct Backoff {
    delay: Duration,
    /// Whether this streak's `error!` was already logged.
    reported: bool,
}

impl Backoff {
    fn new() -> Self {
        Self {
            delay: INITIAL_BACKOFF,
            reported: false,
        }
    }

    /// A subscribe succeeded: the next failure starts a fresh streak.
    fn reset(&mut self) {
        *self = Self::new();
    }

    /// Record a failed `step`, then sleep out the current delay.
    fn fail(&mut self, step: &str, err: &redis::RedisError, tx: &mpsc::Sender<()>) {
        if self.reported {
            log::debug!("push-dispatch: redis listener {step} failed again: {err}");
        } else {
            self.reported = true;
            log::error!(
                "push-dispatch: redis listener {step} failed: {err}; dispatch falls back to \
                 polling and the listener keeps retrying (up to every {}s). If this Redis \
                 cannot SUBSCRIBE, turn push dispatch off (push_dispatch = false, or \
                 FLEXIQ_PUSH_DISPATCH=false on the server)",
                MAX_BACKOFF.as_secs()
            );
        }
        let delay = self.advance();
        sleep_unless_closed(delay, tx);
    }

    /// The delay to sleep now; doubles the next one up to [`MAX_BACKOFF`].
    fn advance(&mut self) -> Duration {
        let now = self.delay;
        self.delay = (self.delay * 2).min(MAX_BACKOFF);
        now
    }
}

/// Sleep `total` in [`SHUTDOWN_CHECK`] slices, returning early once the push
/// loop is gone so a failing Redis cannot stretch shutdown by a backoff.
fn sleep_unless_closed(total: Duration, tx: &mpsc::Sender<()>) {
    let deadline = std::time::Instant::now() + total;
    while !tx.is_closed() {
        let left = deadline.saturating_duration_since(std::time::Instant::now());
        if left.is_zero() {
            return;
        }
        std::thread::sleep(left.min(SHUTDOWN_CHECK));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_to_the_cap_and_resets() {
        let mut b = Backoff::new();
        let delays: Vec<Duration> = (0..9).map(|_| b.advance()).collect();
        assert_eq!(delays[0], INITIAL_BACKOFF);
        assert_eq!(delays[1], INITIAL_BACKOFF * 2);
        assert!(delays.windows(2).all(|w| w[1] >= w[0]));
        assert_eq!(*delays.last().unwrap(), MAX_BACKOFF);
        b.reset();
        assert_eq!(b.advance(), INITIAL_BACKOFF);
    }

    #[test]
    fn a_max_backoff_sleep_ends_when_the_push_loop_goes() {
        let (tx, rx) = mpsc::channel::<()>(1);
        let dropper = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(100));
            drop(rx);
        });
        let started = std::time::Instant::now();
        sleep_unless_closed(MAX_BACKOFF, &tx);
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "slept {:?} after the receiver dropped",
            started.elapsed()
        );
        dropper.join().unwrap();
    }
}
