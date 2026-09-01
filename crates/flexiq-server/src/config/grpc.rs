//! Where the `flexiq.v1` gRPC door listens, and the two things it refuses to
//! start without.
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
//! **TLS is not terminated here**, same posture as attach: transport security
//! belongs to a sidecar proxy or a service mesh, and a variable that looks like
//! it encrypts the connection but does not is worse than no variable.

use anyhow::{bail, Result};

use crate::config::listen::{parse, ListenAddress};
use crate::config::{value, Env};

/// The variable that turns the role on, named once.
pub const LISTEN_VAR: &str = "FLEXIQ_GRPC_LISTEN";

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
    if let ListenAddress::Tcp(addr) = &listen {
        if !addr.ip().is_loopback() {
            bail!(
                "{LISTEN_VAR}={spec} binds a non-loopback address, and this door \
                 accepts unauthenticated enqueues: nothing on it asks for a \
                 credential yet. Bind loopback (127.0.0.1:50051) or use a Unix \
                 socket (unix:/run/flexiq-grpc.sock), where the filesystem mode \
                 is the boundary. This refusal is lifted by the release that \
                 adds a gRPC credential."
            );
        }
    }

    Ok(Some(GrpcConfig {
        listen,
        namespace: namespace.to_string(),
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
    fn a_non_loopback_bind_refuses_while_the_door_has_no_credential() {
        // The same shape as the attach listener's refusal, and for a stronger
        // reason: attach at least has a token to offer, and this door does not
        // yet. An enqueue port open to the network with no credential is not a
        // thing to ship and warn about.
        for spec in ["0.0.0.0:50051", "[::]:50051", "1.2.3.4:50051"] {
            let error = from_env(&env(&[(LISTEN_VAR, spec)]), Some("prod"))
                .expect_err("must refuse an unauthenticated public bind");
            let message = error.to_string();
            assert!(
                message.contains(LISTEN_VAR),
                "unexpected message: {message}"
            );
            assert!(
                message.contains("loopback"),
                "the message must say what to do instead: {message}"
            );
        }
    }

    #[cfg(all(unix, feature = "grpc"))]
    #[test]
    fn a_unix_socket_is_not_caught_by_the_loopback_refusal() {
        // The filesystem mode is the boundary there, as it already is for
        // attach: the socket is created 0660 inside a private directory.
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
