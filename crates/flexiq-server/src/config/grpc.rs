//! Where the `flexiq.v1` gRPC door listens, and the three things it refuses
//! to start without.
//!
//! The address is parsed by the same [`crate::config::listen`] machinery the
//! attach listener uses, so `unix:/run/flexiq-grpc.sock` and a bare `:50051`
//! mean here exactly what they mean there.
//!
//! **A namespace is mandatory.** `None` means three different things inside
//! `Storage` — only the NULL rows to a dequeue, *every* namespace to an
//! id-addressed read, no filter at all to a listing — so a wire that could
//! express it would put a value with three meanings one bug away from
//! `get_job`. The gRPC role therefore serves exactly one namespace, the
//! process's own, and refuses to start without one (design doc §5.2, D11).
//!
//! **A credential is mandatory everywhere, and it is not configured here.**
//! Callers present a stored API token (#717), minted with
//! `flexiq-server token create` or from the dashboard, so there is no token
//! variable to set and no bind that is exempt from presenting one — loopback and
//! Unix sockets included. That is why this module no longer refuses a
//! non-loopback bind: there is no longer such a thing as an uncredentialled
//! listener to refuse.
//!
//! **TLS is not terminated here**, same posture as attach: transport security
//! belongs to a sidecar proxy or a service mesh, and a variable that looks like
//! it encrypts the connection but does not is worse than no variable.

use std::time::Duration;

use anyhow::{bail, Context, Result};

use crate::config::listen::{parse, ListenAddress};
use crate::config::{value, Env};

/// The variable that turns the role on, named once.
pub const LISTEN_VAR: &str = "FLEXIQ_GRPC_LISTEN";

/// How long one executor's attach stream may live, in seconds.
pub const STREAM_MAX_AGE_VAR: &str = "FLEXIQ_GRPC_EXECUTOR_STREAM_MAX_AGE";

/// How often the listener pings an idle connection, in seconds.
pub const KEEPALIVE_INTERVAL_VAR: &str = "FLEXIQ_GRPC_KEEPALIVE_INTERVAL";

/// How long a single call may run before the listener gives up, in seconds.
pub const REQUEST_TIMEOUT_VAR: &str = "FLEXIQ_GRPC_REQUEST_TIMEOUT";

/// How many calls one connection may have in flight at once.
pub const MAX_CONCURRENT_REQUESTS_VAR: &str = "FLEXIQ_GRPC_MAX_CONCURRENT_REQUESTS";

/// One minute.
///
/// This is an HTTP/2 ping, not a TCP keepalive: the listener serves a socket it
/// bound itself, and tonic applies `tcp_keepalive` only to a socket it binds, so
/// the TCP-level setting would be accepted and silently ignored. An attach
/// stream can sit idle for far longer than a producer call, and a peer that
/// vanished mid-stream is otherwise invisible until the rotation deadline.
const DEFAULT_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(60);

/// The share of [`DEFAULT_KEEPALIVE_INTERVAL`] a ping has to come back in.
///
/// Derived rather than configured: an interval and its timeout have to stay in
/// a ratio to mean anything, and two variables that must agree are one variable
/// plus a way to get it wrong.
const KEEPALIVE_TIMEOUT_DIVISOR: u32 = 3;

/// The floor under the derived keepalive timeout.
///
/// A very short interval would otherwise derive a timeout smaller than one
/// round trip on a slow link, and every ping would fail.
const MIN_KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(5);

/// Thirty seconds.
///
/// Every RPC on this door is unary and answered out of storage; none is a
/// long-running operation. The deadline races the *response future*, not the
/// response body, so a bidirectional `Attach` stream — which returns its
/// response as soon as it has spawned its workers — is unaffected however long
/// it then lives.
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Calls in flight per connection.
///
/// The producer door answers out of a bounded storage pool, so an unbounded
/// number of concurrent calls does not buy throughput — it buys a queue in
/// front of the pool that no one can see. Per *connection*, so one noisy client
/// cannot crowd out another's.
const DEFAULT_MAX_CONCURRENT_REQUESTS: usize = 256;

/// Thirty minutes.
///
/// A stream cannot be load balanced once it has started, so it has to end for a
/// replacement to be placed somewhere else. It also has to end rarely: a
/// rotation drains the executor first, which costs a brief window in which it
/// is matched no new work, and paying that every few minutes would be worse
/// than the imbalance it fixes.
const DEFAULT_STREAM_MAX_AGE: Duration = Duration::from_secs(1_800);

/// Variables an earlier build honoured that this one does not read at all.
///
/// A leftover value is not harmless: it is a credential the operator believes
/// is in force. Same reasoning as [`UNHONOURED_TLS_VARS`] — a variable that
/// looks like a security control and does nothing is worse than no variable —
/// and the Helm chart refuses the matching value at template time, but a
/// Compose, systemd or bare-environment deployment has no such gate.
const RETIRED_VARS: [(&str, &str); 1] = [(
    "FLEXIQ_GRPC_TOKEN",
    "the gRPC door no longer takes a shared secret; callers present a scoped API \
     token stored in the database. Unset it and mint one with `flexiq-server token \
     create --name <name> --scope produce`, or from the dashboard.",
)];

/// TLS variables the gRPC listener does not terminate. Accepting them would let
/// an operator believe the connection is encrypted when it is not.
const UNHONOURED_TLS_VARS: [&str; 2] = ["FLEXIQ_GRPC_TLS_CERT", "FLEXIQ_GRPC_TLS_KEY"];

/// The gRPC listener's address and the one namespace it serves.
#[derive(Debug, Clone)]
pub struct GrpcConfig {
    /// Address producers dial.
    pub listen: ListenAddress,
    /// Tenant namespace every call is scoped to. Non-empty by construction.
    pub namespace: String,
    /// How long one executor's attach stream lives before the scheduler drains
    /// it and closes it. Zero leaves streams unbounded.
    pub executor_stream_max_age: Duration,
    /// How often an idle connection is pinged. Zero disables the ping.
    pub keepalive_interval: Duration,
    /// How long one call may take before the listener answers
    /// `DEADLINE_EXCEEDED`. Zero leaves calls unbounded.
    pub request_timeout: Duration,
    /// Calls one connection may have in flight. Zero leaves it unbounded.
    pub max_concurrent_requests: usize,
}

impl GrpcConfig {
    /// A listener on `listen` serving `namespace`, with every tunable at its
    /// default.
    ///
    /// [`from_env`] is the only production constructor; this exists so a caller
    /// that already knows the two values it cannot default — the address, and
    /// the namespace that is non-empty by construction — does not restate the
    /// tunables, and so adding one is not an edit in every test.
    pub fn new(listen: ListenAddress, namespace: impl Into<String>) -> Self {
        Self {
            listen,
            namespace: namespace.into(),
            executor_stream_max_age: DEFAULT_STREAM_MAX_AGE,
            keepalive_interval: DEFAULT_KEEPALIVE_INTERVAL,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            max_concurrent_requests: DEFAULT_MAX_CONCURRENT_REQUESTS,
        }
    }

    /// How long a keepalive ping has to be answered, or `None` when pings are
    /// off.
    ///
    /// See [`KEEPALIVE_TIMEOUT_DIVISOR`] for why this is derived.
    pub fn keepalive_timeout(&self) -> Option<Duration> {
        (!self.keepalive_interval.is_zero()).then(|| {
            (self.keepalive_interval / KEEPALIVE_TIMEOUT_DIVISOR).max(MIN_KEEPALIVE_TIMEOUT)
        })
    }
}

/// Read a whole number of seconds, or `default` when the variable is unset.
///
/// Zero is never rejected here: each of these variables reads it as "off", and
/// a deployment that wants one off has no other way to say so.
fn seconds(env: &Env, key: &str, default: Duration) -> Result<Duration> {
    match value(env, key) {
        None => Ok(default),
        Some(raw) => Ok(Duration::from_secs(raw.parse().with_context(|| {
            format!("{key} must be a whole number of seconds, got '{raw}'")
        })?)),
    }
}

/// Parse the gRPC block, or `None` when the role is disabled.
///
/// `namespace` is the already-parsed `FLEXIQ_NAMESPACE`, because the rule that
/// makes it mandatory is a property of this role and of no other.
pub fn from_env(env: &Env, namespace: Option<&str>) -> Result<Option<GrpcConfig>> {
    let Some(spec) = value(env, LISTEN_VAR) else {
        return Ok(None);
    };

    // A binary without the feature has no gRPC server to start. Ignoring the
    // variable would leave a deployment that looks configured and serves
    // nothing on the port its clients dial.
    if !cfg!(feature = "grpc") {
        bail!(
            "{LISTEN_VAR} is set, but this binary was built without the `grpc` \
             cargo feature and has no gRPC server to start. Rebuild with \
             `--features grpc`, or unset {LISTEN_VAR}."
        );
    }

    for (name, guidance) in RETIRED_VARS {
        if value(env, name).is_some() {
            bail!("{name} is set, but this build does not read it — {guidance}");
        }
    }

    for name in UNHONOURED_TLS_VARS {
        if value(env, name).is_some() {
            bail!(
                "{name} is set, but this build does not terminate TLS on the gRPC \
                 listener. Terminate TLS in a proxy in front of it, rather than \
                 running with a security control that does nothing."
            );
        }
    }

    let Some(namespace) = namespace.filter(|name| !name.is_empty()) else {
        bail!(
            "{LISTEN_VAR} requires FLEXIQ_NAMESPACE. An unset namespace means \
             'every namespace' to a read and 'only the unnamespaced rows' to a \
             dequeue, so the gRPC door serves one named namespace and never the \
             ambiguous one. Set FLEXIQ_NAMESPACE to the namespace your producers \
             enqueue into."
        );
    };

    let listen = parse(LISTEN_VAR, &spec)?;

    // Zero is honoured rather than rejected: an operator behind a proxy that
    // already recycles connections has a reason to turn this off, and the
    // alternative is a variable that cannot express what they want.
    let executor_stream_max_age = seconds(env, STREAM_MAX_AGE_VAR, DEFAULT_STREAM_MAX_AGE)?;
    let keepalive_interval = seconds(env, KEEPALIVE_INTERVAL_VAR, DEFAULT_KEEPALIVE_INTERVAL)?;
    let request_timeout = seconds(env, REQUEST_TIMEOUT_VAR, DEFAULT_REQUEST_TIMEOUT)?;

    let max_concurrent_requests = match value(env, MAX_CONCURRENT_REQUESTS_VAR) {
        None => DEFAULT_MAX_CONCURRENT_REQUESTS,
        Some(raw) => raw.parse().with_context(|| {
            format!("{MAX_CONCURRENT_REQUESTS_VAR} must be a whole number, got '{raw}'")
        })?,
    };

    Ok(Some(GrpcConfig {
        executor_stream_max_age,
        keepalive_interval,
        request_timeout,
        max_concurrent_requests,
        ..GrpcConfig::new(listen, namespace)
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(pairs: &[(&str, &str)]) -> Env {
        pairs
            .iter()
            .map(|(key, val)| (key.to_string(), val.to_string()))
            .collect()
    }

    #[test]
    fn no_listen_variable_disables_the_role() {
        assert!(from_env(&env(&[]), Some("prod")).expect("valid").is_none());
    }

    #[cfg(feature = "grpc")]
    #[test]
    fn a_tcp_address_and_a_namespace_are_enough() {
        let config = from_env(&env(&[(LISTEN_VAR, "127.0.0.1:50051")]), Some("prod"))
            .expect("valid")
            .expect("configured");
        assert_eq!(
            config.listen,
            ListenAddress::Tcp("127.0.0.1:50051".parse().unwrap())
        );
        assert_eq!(config.namespace, "prod");
    }

    #[cfg(feature = "grpc")]
    #[test]
    fn a_bare_port_binds_loopback() {
        let config = from_env(&env(&[(LISTEN_VAR, ":50051")]), Some("prod"))
            .expect("valid")
            .expect("configured");
        assert_eq!(
            config.listen,
            ListenAddress::Tcp("127.0.0.1:50051".parse().unwrap())
        );
    }

    #[cfg(feature = "grpc")]
    #[test]
    fn executor_streams_rotate_by_default() {
        let config = from_env(&env(&[(LISTEN_VAR, ":50051")]), Some("prod"))
            .expect("valid")
            .expect("configured");
        assert_eq!(config.executor_stream_max_age, DEFAULT_STREAM_MAX_AGE);
    }

    #[cfg(feature = "grpc")]
    #[test]
    fn a_zero_stream_age_leaves_streams_unbounded() {
        // Honoured, not refused: a deployment behind a proxy that already
        // recycles connections has a reason to turn this off.
        let config = from_env(
            &env(&[(LISTEN_VAR, ":50051"), (STREAM_MAX_AGE_VAR, "0")]),
            Some("prod"),
        )
        .expect("valid")
        .expect("configured");
        assert_eq!(config.executor_stream_max_age, Duration::ZERO);
    }

    #[cfg(feature = "grpc")]
    #[test]
    fn a_stream_age_that_is_not_seconds_names_its_variable() {
        let error = from_env(
            &env(&[(LISTEN_VAR, ":50051"), (STREAM_MAX_AGE_VAR, "30m")]),
            Some("prod"),
        )
        .expect_err("must refuse");
        assert!(
            error.to_string().contains(STREAM_MAX_AGE_VAR),
            "unexpected message: {error}"
        );
    }

    #[cfg(feature = "grpc")]
    #[test]
    fn idle_connections_are_pinged_by_default() {
        let config = from_env(&env(&[(LISTEN_VAR, ":50051")]), Some("prod"))
            .expect("valid")
            .expect("configured");
        assert_eq!(config.keepalive_interval, DEFAULT_KEEPALIVE_INTERVAL);
        assert_eq!(config.keepalive_timeout(), Some(Duration::from_secs(20)));
    }

    #[cfg(feature = "grpc")]
    #[test]
    fn a_zero_keepalive_interval_turns_pings_off() {
        let config = from_env(
            &env(&[(LISTEN_VAR, ":50051"), (KEEPALIVE_INTERVAL_VAR, "0")]),
            Some("prod"),
        )
        .expect("valid")
        .expect("configured");
        assert_eq!(config.keepalive_interval, Duration::ZERO);
        assert_eq!(config.keepalive_timeout(), None);
    }

    /// The derived timeout has a floor, or a short interval would derive one
    /// smaller than a round trip and every ping would fail.
    #[cfg(feature = "grpc")]
    #[test]
    fn a_short_keepalive_interval_still_derives_a_usable_timeout() {
        let config = from_env(
            &env(&[(LISTEN_VAR, ":50051"), (KEEPALIVE_INTERVAL_VAR, "6")]),
            Some("prod"),
        )
        .expect("valid")
        .expect("configured");
        assert_eq!(config.keepalive_timeout(), Some(MIN_KEEPALIVE_TIMEOUT));
    }

    #[cfg(feature = "grpc")]
    #[test]
    fn calls_are_deadlined_and_capped_by_default() {
        let config = from_env(&env(&[(LISTEN_VAR, ":50051")]), Some("prod"))
            .expect("valid")
            .expect("configured");
        assert_eq!(config.request_timeout, DEFAULT_REQUEST_TIMEOUT);
        assert_eq!(
            config.max_concurrent_requests,
            DEFAULT_MAX_CONCURRENT_REQUESTS
        );
    }

    #[cfg(feature = "grpc")]
    #[test]
    fn zero_turns_the_deadline_and_the_cap_off() {
        let config = from_env(
            &env(&[
                (LISTEN_VAR, ":50051"),
                (REQUEST_TIMEOUT_VAR, "0"),
                (MAX_CONCURRENT_REQUESTS_VAR, "0"),
            ]),
            Some("prod"),
        )
        .expect("valid")
        .expect("configured");
        assert_eq!(config.request_timeout, Duration::ZERO);
        assert_eq!(config.max_concurrent_requests, 0);
    }

    /// Every tunable's parse failure has to name the variable, or an operator
    /// reads "invalid digit found in string" and has no idea which one.
    #[cfg(feature = "grpc")]
    #[test]
    fn a_tunable_that_is_not_a_number_names_its_variable() {
        for var in [
            STREAM_MAX_AGE_VAR,
            KEEPALIVE_INTERVAL_VAR,
            REQUEST_TIMEOUT_VAR,
            MAX_CONCURRENT_REQUESTS_VAR,
        ] {
            let error = from_env(&env(&[(LISTEN_VAR, ":50051"), (var, "lots")]), Some("prod"))
                .expect_err("must refuse");
            assert!(
                error.to_string().contains(var),
                "unexpected message for {var}: {error}"
            );
        }
    }

    /// #716 refused this bind unless `FLEXIQ_GRPC_TOKEN` was set. There is no
    /// such variable now: every call presents a stored token, including on
    /// loopback, so a public bind is no longer the thing that decides whether
    /// the door has a credential.
    #[cfg(feature = "grpc")]
    #[test]
    fn a_non_loopback_bind_needs_no_variable_to_unlock_it() {
        for spec in ["0.0.0.0:50051", "[::]:50051"] {
            let config = from_env(&env(&[(LISTEN_VAR, spec)]), Some("prod"))
                .expect("a public bind is credentialled by the token store")
                .expect("configured");
            assert_eq!(config.namespace, "prod");
        }
    }

    #[cfg(all(unix, feature = "grpc"))]
    #[test]
    fn a_unix_socket_parses_the_same_way_a_tcp_address_does() {
        // The socket is created 0660 inside a private directory, so the
        // filesystem is a second boundary in front of the credential — not a
        // replacement for it. A Unix bind presents a token like any other.
        assert!(from_env(
            &env(&[(LISTEN_VAR, "unix:/run/flexiq-grpc.sock")]),
            Some("prod")
        )
        .is_ok());
    }

    #[cfg(all(unix, feature = "grpc"))]
    #[test]
    fn a_unix_socket_parses_through_the_shared_machinery() {
        let config = from_env(
            &env(&[(LISTEN_VAR, "unix:/run/flexiq-grpc.sock")]),
            Some("prod"),
        )
        .expect("valid")
        .expect("configured");
        assert_eq!(
            config.listen,
            ListenAddress::Unix(std::path::PathBuf::from("/run/flexiq-grpc.sock"))
        );
    }

    #[cfg(feature = "grpc")]
    #[test]
    fn a_bad_address_names_the_variable_that_carried_it() {
        let error = from_env(&env(&[(LISTEN_VAR, "not-an-address")]), Some("prod"))
            .expect_err("must refuse");
        assert!(
            error.to_string().contains(LISTEN_VAR),
            "unexpected message: {error}"
        );
    }

    #[cfg(feature = "grpc")]
    #[test]
    fn no_namespace_refuses_to_start() {
        for namespace in [None, Some("")] {
            let error = from_env(&env(&[(LISTEN_VAR, "127.0.0.1:50051")]), namespace)
                .expect_err("must refuse an unnamespaced gRPC door");
            assert!(
                error.to_string().contains("FLEXIQ_NAMESPACE"),
                "unexpected message: {error}"
            );
        }
    }

    /// A deployment upgrading from #716 still sets the old variable. Starting
    /// quietly would leave the operator believing it credentials the door.
    #[cfg(feature = "grpc")]
    #[test]
    fn a_retired_variable_fails_loudly() {
        for (name, _) in RETIRED_VARS {
            let error = from_env(
                &env(&[(LISTEN_VAR, "127.0.0.1:50051"), (name, "0123456789abcdef")]),
                Some("prod"),
            )
            .expect_err("must refuse a variable this build does not read");
            let message = error.to_string();
            assert!(message.contains(name), "unexpected message: {message}");
            assert!(
                message.contains("flexiq-server token create"),
                "the message must say what to do instead: {message}"
            );
        }
    }

    #[cfg(feature = "grpc")]
    #[test]
    fn an_unhonoured_tls_variable_fails_loudly() {
        for name in UNHONOURED_TLS_VARS {
            let error = from_env(
                &env(&[(LISTEN_VAR, "127.0.0.1:50051"), (name, "/certs/x")]),
                Some("prod"),
            )
            .expect_err("must refuse");
            assert!(error.to_string().contains(name));
        }
    }

    /// The other side of the same coin: a build with no gRPC server must not
    /// quietly accept the variable that asks for one.
    #[cfg(not(feature = "grpc"))]
    #[test]
    fn a_build_without_the_feature_refuses_the_variable() {
        let error = from_env(&env(&[(LISTEN_VAR, "127.0.0.1:50051")]), Some("prod"))
            .expect_err("must refuse");
        assert!(
            error.to_string().contains("`grpc` cargo feature"),
            "unexpected message: {error}"
        );
    }
}
