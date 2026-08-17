//! Failed-login throttling.
//!
//! Password verification is deliberately expensive, which makes the login
//! endpoint both a guessing oracle and a cheap way to burn every blocking
//! thread the server has. A fixed window per identity bounds both.
//!
//! In-memory on purpose: a shared counter in storage would turn every login
//! attempt into writes on the hot path, and a restart clearing the window is
//! an acceptable trade for a control that only has to blunt automation.

use std::collections::HashMap;
use std::convert::Infallible;
use std::net::{IpAddr, SocketAddr};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use axum::extract::{ConnectInfo, FromRequestParts};
use axum::http::request::Parts;

/// Failures tolerated within one window before the identity is locked out.
const MAX_FAILURES: u32 = 10;

/// How long that window lasts, and therefore how long a lockout lasts.
const WINDOW: Duration = Duration::from_secs(5 * 60);

/// Entries older than this are dropped when the map is swept, so a spray of
/// one-off usernames cannot grow it without bound.
const SWEEP_AFTER: Duration = WINDOW;

/// The peer's address, when the server was started with connect info.
///
/// `Option<ConnectInfo<_>>` is not an extractor in axum, and a hard
/// `ConnectInfo` would make every handler using it fail under `oneshot` in
/// tests — so this reads the extension and shrugs when it is absent.
#[derive(Debug, Clone, Copy)]
pub struct ClientAddr(pub Option<IpAddr>);

impl ClientAddr {
    /// The address as a string, for use in a throttle key.
    pub fn label(&self) -> Option<String> {
        self.0.map(|address| address.to_string())
    }
}

impl<S: Sync> FromRequestParts<S> for ClientAddr {
    type Rejection = Infallible;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        Ok(Self(
            parts
                .extensions
                .get::<ConnectInfo<SocketAddr>>()
                .map(|ConnectInfo(peer)| peer.ip()),
        ))
    }
}

#[derive(Debug)]
struct Window {
    started: Instant,
    failures: u32,
}

/// Per-identity failed-attempt counters.
#[derive(Debug, Default)]
pub struct LoginThrottle {
    windows: Mutex<HashMap<String, Window>>,
}

impl LoginThrottle {
    /// Whether `key` may attempt a login now, or how long it must wait.
    pub fn check(&self, key: &str) -> Result<(), Duration> {
        let windows = self.windows.lock().unwrap_or_else(|e| e.into_inner());
        let Some(window) = windows.get(key) else {
            return Ok(());
        };
        let elapsed = window.started.elapsed();
        if window.failures >= MAX_FAILURES && elapsed < WINDOW {
            return Err(WINDOW - elapsed);
        }
        Ok(())
    }

    /// Count one failed attempt for `key`.
    pub fn record_failure(&self, key: &str) {
        let mut windows = self.windows.lock().unwrap_or_else(|e| e.into_inner());
        sweep(&mut windows);
        match windows.get_mut(key) {
            // A window that has run its course starts over rather than
            // keeping an identity locked out forever.
            Some(window) if window.started.elapsed() < WINDOW => window.failures += 1,
            _ => {
                windows.insert(
                    key.to_string(),
                    Window {
                        started: Instant::now(),
                        failures: 1,
                    },
                );
            }
        }
    }

    /// Forget `key`'s failures — a successful login clears the slate.
    pub fn clear(&self, key: &str) {
        self.windows
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(key);
    }

    /// Identity key for an attempt: the client address when the server knows
    /// it, so one account being probed does not lock out its real owner from
    /// elsewhere, plus the username so one address cannot spray accounts.
    pub fn key(client: Option<&str>, username: &str) -> String {
        match client {
            Some(client) => format!("{client}|{username}"),
            None => username.to_string(),
        }
    }
}

fn sweep(windows: &mut HashMap<String, Window>) {
    windows.retain(|_, window| window.started.elapsed() < SWEEP_AFTER);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_fresh_identity_may_attempt() {
        let throttle = LoginThrottle::default();
        throttle.check("someone").expect("no failures yet");
    }

    #[test]
    fn the_limit_locks_out_and_reports_a_wait() {
        let throttle = LoginThrottle::default();
        for _ in 0..MAX_FAILURES {
            throttle.check("someone").expect("still under the limit");
            throttle.record_failure("someone");
        }
        let wait = throttle.check("someone").expect_err("must be locked out");
        assert!(wait <= WINDOW && wait > Duration::ZERO);
    }

    #[test]
    fn a_success_clears_the_counter() {
        let throttle = LoginThrottle::default();
        for _ in 0..MAX_FAILURES {
            throttle.record_failure("someone");
        }
        assert!(throttle.check("someone").is_err());

        throttle.clear("someone");
        throttle.check("someone").expect("cleared");
    }

    #[test]
    fn identities_are_counted_separately() {
        let throttle = LoginThrottle::default();
        for _ in 0..MAX_FAILURES {
            throttle.record_failure("someone");
        }
        throttle.check("someone-else").expect("unrelated identity");
    }

    #[test]
    fn a_missing_connect_info_is_not_a_failure() {
        assert!(ClientAddr(None).label().is_none());
        assert_eq!(
            ClientAddr(Some("10.0.0.1".parse().expect("valid"))).label(),
            Some("10.0.0.1".to_string())
        );
    }

    #[test]
    fn the_key_separates_client_and_username() {
        assert_eq!(LoginThrottle::key(Some("10.0.0.1"), "ops"), "10.0.0.1|ops");
        assert_eq!(LoginThrottle::key(None, "ops"), "ops");
        // A different client probing the same account gets its own budget.
        assert_ne!(
            LoginThrottle::key(Some("10.0.0.1"), "ops"),
            LoginThrottle::key(Some("10.0.0.2"), "ops")
        );
    }
}
