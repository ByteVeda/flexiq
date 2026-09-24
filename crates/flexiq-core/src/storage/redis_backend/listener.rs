//! Redis pub/sub wake listener for push-dispatch.
//!
//! Entirely behind the `push-dispatch` feature. The default (feature-off)
//! build never compiles this module.
//!
//! A dedicated blocking connection `SUBSCRIBE`s to the notify channel of every
//! queue the scheduler serves, in its namespace; each message forwards its
//! payload — the enqueued job's `scheduled_at` in ms, `None` when it does not
//! parse — into the scheduler's
//! [`crate::scheduler::wake::WakeSource::Channel`]. Pub/sub is a
//! broadcast, so every scheduler serving a queue wakes — a consumed signal
//! (a list pop) would wake one, possibly one that cannot take the job. A read
//! timeout bounds each wait so the loop re-checks the forward channel and
//! stops promptly on shutdown. Errors back off exponentially (capped at
//! 30 s), then reconnect and resubscribe; a Redis that never lets
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

/// Messages buffered for the push loop, which folds all of them into one wake.
/// Deep enough that a delayed job's deadline survives a burst of ready wakes;
/// one dropped when full only waits for the fallback timer.
const FORWARD_BACKLOG: usize = 64;

/// Spawn the Redis wake listener for `queues` in `namespace` and return the
/// receiver end for the scheduler's [`crate::scheduler::wake::WakeSource::Channel`].
///
/// The task ends once that receiver is dropped (the push loop returned),
/// within one read timeout; every other wait in the loop is bounded too.
pub fn spawn(
    storage: RedisStorage,
    namespace: Option<&str>,
    queues: &[String],
) -> mpsc::Receiver<Option<i64>> {
    let (tx, rx) = mpsc::channel(FORWARD_BACKLOG);
    let channels: Vec<String> = queues
        .iter()
        .map(|q| storage.notify_channel(namespace, q))
        .collect();

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
            let _ = tx.try_send(None);

            while !tx.is_closed() {
                match pubsub.get_message() {
                    Ok(msg) => match tx.try_send(announced_at(&msg)) {
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

/// The `scheduled_at` a notify message carries. Anything that is not a decimal
/// `i64` — a foreign publisher, an older build's payload — is a plain wake.
fn announced_at(msg: &redis::Msg) -> Option<i64> {
    msg.get_payload::<String>().ok()?.trim().parse().ok()
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
    fn fail(&mut self, step: &str, err: &redis::RedisError, tx: &mpsc::Sender<Option<i64>>) {
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
fn sleep_unless_closed(total: Duration, tx: &mpsc::Sender<Option<i64>>) {
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

    fn message(payload: &[u8]) -> redis::Msg {
        let value = redis::Value::Array(vec![
            redis::Value::BulkString(b"message".to_vec()),
            redis::Value::BulkString(b"chan".to_vec()),
            redis::Value::BulkString(payload.to_vec()),
        ]);
        redis::Msg::from_owned_value(value).unwrap()
    }

    /// A deadline payload is forwarded; anything else is a plain wake, never
    /// an error or a dropped signal.
    #[test]
    fn announced_at_parses_deadlines_and_tolerates_junk() {
        assert_eq!(
            announced_at(&message(b"1790000000000")),
            Some(1_790_000_000_000)
        );
        assert_eq!(announced_at(&message(b"-5")), Some(-5));
        assert_eq!(announced_at(&message(b"")), None);
        assert_eq!(announced_at(&message(b"ready")), None);
        assert_eq!(announced_at(&message(b"99999999999999999999999")), None);
        assert_eq!(announced_at(&message(&[0xff, 0xfe])), None);
    }

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
        let (tx, rx) = mpsc::channel::<Option<i64>>(1);
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
