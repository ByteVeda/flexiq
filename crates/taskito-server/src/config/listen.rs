//! Where executors attach, and the guards that keep that port from being a
//! remote code-dispatch hole.
//!
//! An attach connection receives jobs, so the listener is deliberately harder
//! to expose than the dashboard: there is no insecure escape hatch. Until the
//! handshake carries a credential (S4), a non-loopback bind is refused and a
//! configured token is reported as unhonoured rather than silently ignored.

use std::net::{SocketAddr, ToSocketAddrs};
use std::path::PathBuf;

use anyhow::{bail, Context, Result};

use crate::config::{value, Env};

/// Credential variables that only take effect once the handshake carries them.
/// Accepting them now would let an operator believe attach is authenticated.
const UNHONOURED_CREDENTIAL_VARS: [&str; 3] = [
    "TASKITO_ATTACH_TOKEN",
    "TASKITO_LISTEN_TLS_CERT",
    "TASKITO_LISTEN_TLS_KEY",
];

/// Address executors dial to attach.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttachListen {
    /// TCP, for an executor in another container.
    Tcp(SocketAddr),
    /// Unix domain socket, the same-pod sidecar case.
    #[cfg(unix)]
    Unix(PathBuf),
}

impl std::fmt::Display for AttachListen {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Tcp(addr) => write!(f, "tcp://{addr}"),
            #[cfg(unix)]
            Self::Unix(path) => write!(f, "unix:{}", path.display()),
        }
    }
}

/// Parse `TASKITO_LISTEN`, or `None` when the listener is disabled.
pub fn from_env(env: &Env) -> Result<Option<AttachListen>> {
    let Some(spec) = value(env, "TASKITO_LISTEN") else {
        return Ok(None);
    };
    for name in UNHONOURED_CREDENTIAL_VARS {
        if value(env, name).is_some() {
            bail!(
                "{name} is set, but this build does not yet verify attach credentials \
                 (executor attach auth is a follow-up phase). Unset it rather than \
                 running with a security control that does nothing."
            );
        }
    }
    let listen = parse(&spec)?;
    if let AttachListen::Tcp(addr) = &listen {
        if !addr.ip().is_loopback() {
            bail!(
                "TASKITO_LISTEN={spec} binds a non-loopback address, and an attach port \
                 dispatches code. Until the handshake is authenticated, bind loopback \
                 (127.0.0.1) or a Unix socket (unix:/run/taskito.sock) and reach it \
                 through the pod network."
            );
        }
    }
    Ok(Some(listen))
}

/// Parse one listen spec: `unix:/path`, `host:port`, or `:port`.
pub fn parse(spec: &str) -> Result<AttachListen> {
    if let Some(path) = spec.strip_prefix("unix:") {
        #[cfg(unix)]
        {
            if path.is_empty() {
                bail!("TASKITO_LISTEN=unix: needs a socket path, e.g. unix:/run/taskito.sock");
            }
            return Ok(AttachListen::Unix(PathBuf::from(path)));
        }
        #[cfg(not(unix))]
        bail!("Unix socket listeners are not supported on this platform");
    }
    Ok(AttachListen::Tcp(resolve(spec)?))
}

/// Resolve `host:port` to a single socket address. A bare `:port` binds
/// loopback rather than every interface — the safe reading of an ambiguous
/// value.
pub fn resolve(spec: &str) -> Result<SocketAddr> {
    let normalised = if let Some(port) = spec.strip_prefix(':') {
        format!("127.0.0.1:{port}")
    } else {
        spec.to_string()
    };
    normalised
        .to_socket_addrs()
        .with_context(|| format!("'{spec}' is not a valid host:port"))?
        .next()
        .with_context(|| format!("'{spec}' resolved to no address"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn env(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(key, val)| (key.to_string(), val.to_string()))
            .collect()
    }

    #[test]
    fn loopback_tcp_is_accepted() {
        let listen = from_env(&env(&[("TASKITO_LISTEN", "127.0.0.1:7777")]))
            .expect("valid")
            .expect("configured");
        assert_eq!(listen, AttachListen::Tcp("127.0.0.1:7777".parse().unwrap()));
    }

    #[test]
    fn non_loopback_tcp_refuses_to_start() {
        let error = from_env(&env(&[("TASKITO_LISTEN", "0.0.0.0:7777")])).expect_err("must refuse");
        assert!(error.to_string().contains("dispatches code"));
    }

    #[test]
    fn a_bare_port_binds_loopback() {
        let listen = from_env(&env(&[("TASKITO_LISTEN", ":7777")]))
            .expect("valid")
            .expect("configured");
        assert_eq!(listen, AttachListen::Tcp("127.0.0.1:7777".parse().unwrap()));
    }

    #[cfg(unix)]
    #[test]
    fn unix_sockets_skip_the_loopback_check() {
        let listen = from_env(&env(&[("TASKITO_LISTEN", "unix:/run/taskito.sock")]))
            .expect("valid")
            .expect("configured");
        assert_eq!(
            listen,
            AttachListen::Unix(PathBuf::from("/run/taskito.sock"))
        );
    }

    #[test]
    fn an_unhonoured_credential_variable_fails_loudly() {
        let error = from_env(&env(&[
            ("TASKITO_LISTEN", "127.0.0.1:7777"),
            ("TASKITO_ATTACH_TOKEN", "s3cret"),
        ]))
        .expect_err("must refuse");
        assert!(error.to_string().contains("TASKITO_ATTACH_TOKEN"));
    }

    #[test]
    fn no_listen_variable_disables_the_listener() {
        assert!(from_env(&env(&[])).expect("valid").is_none());
    }
}
