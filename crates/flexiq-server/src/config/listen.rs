//! Where executors attach, and the guards that keep that port from being a
//! remote code-dispatch hole.
//!
//! [`ListenAddress`] and [`parse`] are the shared half: every role that binds a
//! port takes its address through them, so `unix:` and a bare `:port` mean the
//! same thing whichever variable named them. Everything else here belongs to
//! attach alone.
//!
//! An attach connection receives jobs, so the listener is deliberately harder
//! to expose than the dashboard: there is no insecure escape hatch. A bind
//! reachable off-host requires `FLEXIQ_ATTACH_TOKEN`, and the token is a
//! bearer credential, not transport security — for production, terminate mTLS
//! in front of the listener (sidecar proxy or service mesh) and keep the token
//! as the second factor.

use std::net::{SocketAddr, ToSocketAddrs};
use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use flexiq_core::Secret;

use crate::config::{value, Env};

/// Shortest token accepted. Long enough that a token generated the documented
/// way passes and a hand-typed word does not.
const MIN_TOKEN_LEN: usize = 16;

/// TLS variables the listener does not yet terminate. Accepting them would let
/// an operator believe the connection is encrypted when it is not.
const UNHONOURED_TLS_VARS: [&str; 2] = ["FLEXIQ_LISTEN_TLS_CERT", "FLEXIQ_LISTEN_TLS_KEY"];

/// The attach listener's address and credential.
#[derive(Debug, Clone)]
pub struct AttachConfig {
    /// Address executors dial.
    pub listen: ListenAddress,
    /// Secret an executor must present in its `hello`.
    pub token: Option<Secret>,
}

/// An address a role binds: either half of one `FLEXIQ_*_LISTEN` variable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ListenAddress {
    /// TCP, for a peer in another container.
    Tcp(SocketAddr),
    /// Unix domain socket, the same-pod sidecar case.
    #[cfg(unix)]
    Unix(PathBuf),
}

impl std::fmt::Display for ListenAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Tcp(addr) => write!(f, "tcp://{addr}"),
            #[cfg(unix)]
            Self::Unix(path) => write!(f, "unix:{}", path.display()),
        }
    }
}

/// Parse the attach block, or `None` when the listener is disabled.
pub fn from_env(env: &Env) -> Result<Option<AttachConfig>> {
    let Some(spec) = value(env, "FLEXIQ_LISTEN") else {
        return Ok(None);
    };
    for name in UNHONOURED_TLS_VARS {
        if value(env, name).is_some() {
            bail!(
                "{name} is set, but this build does not terminate TLS on the attach \
                 listener. Terminate mTLS in a proxy in front of it and authenticate \
                 executors with FLEXIQ_ATTACH_TOKEN, rather than running with a \
                 security control that does nothing."
            );
        }
    }

    let listen = parse("FLEXIQ_LISTEN", &spec)?;
    let token = token(env)?;
    if let ListenAddress::Tcp(addr) = &listen {
        if !addr.ip().is_loopback() && token.is_none() {
            bail!(
                "FLEXIQ_LISTEN={spec} binds a non-loopback address, and an attach port \
                 dispatches code. Set FLEXIQ_ATTACH_TOKEN, bind loopback (127.0.0.1), \
                 or use a Unix socket (unix:/run/flexiq.sock)."
            );
        }
    }
    Ok(Some(AttachConfig { listen, token }))
}

/// Parse `FLEXIQ_ATTACH_TOKEN`, rejecting one too short to be a secret.
fn token(env: &Env) -> Result<Option<Secret>> {
    let Some(raw) = value(env, "FLEXIQ_ATTACH_TOKEN") else {
        return Ok(None);
    };
    let token = Secret::new(raw);
    if token.len() < MIN_TOKEN_LEN {
        bail!(
            "FLEXIQ_ATTACH_TOKEN must be at least {MIN_TOKEN_LEN} characters — a \
             guessable one is not a control. Generate it with `openssl rand -base64 32`."
        );
    }
    Ok(Some(token))
}

/// Remove the token from the process environment once it is parsed, so a task
/// body or a crash dump cannot read it back.
pub fn scrub_attach_token() {
    // Called once from `main`, before any thread that reads the environment
    // has been spawned.
    std::env::remove_var("FLEXIQ_ATTACH_TOKEN");
}

/// Parse one listen spec: `unix:/path`, `host:port`, or `:port`.
///
/// `var` is the variable the spec came from, so a role that is not attach
/// still reports the name the operator actually set.
pub fn parse(var: &str, spec: &str) -> Result<ListenAddress> {
    if let Some(path) = spec.strip_prefix("unix:") {
        #[cfg(unix)]
        {
            if path.is_empty() {
                bail!("{var}=unix: needs a socket path, e.g. unix:/run/flexiq.sock");
            }
            return Ok(ListenAddress::Unix(PathBuf::from(path)));
        }
        #[cfg(not(unix))]
        bail!("{var} asks for a Unix socket listener, which this platform does not support");
    }
    Ok(ListenAddress::Tcp(resolve(var, spec)?))
}

/// Resolve `host:port` to a single socket address. A bare `:port` binds
/// loopback rather than every interface — the safe reading of an ambiguous
/// value.
pub fn resolve(var: &str, spec: &str) -> Result<SocketAddr> {
    let normalised = if let Some(port) = spec.strip_prefix(':') {
        format!("127.0.0.1:{port}")
    } else {
        spec.to_string()
    };
    normalised
        .to_socket_addrs()
        .with_context(|| format!("{var}='{spec}' is not a valid host:port"))?
        .next()
        .with_context(|| format!("{var}='{spec}' resolved to no address"))
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

    /// Long enough to pass the length floor.
    const TOKEN: &str = "0123456789abcdef0123";

    fn attach(pairs: &[(&str, &str)]) -> AttachConfig {
        from_env(&env(pairs)).expect("valid").expect("configured")
    }

    #[test]
    fn loopback_tcp_is_accepted() {
        let config = attach(&[("FLEXIQ_LISTEN", "127.0.0.1:7777")]);
        assert_eq!(
            config.listen,
            ListenAddress::Tcp("127.0.0.1:7777".parse().unwrap())
        );
        assert!(config.token.is_none());
    }

    #[test]
    fn non_loopback_tcp_without_a_token_refuses_to_start() {
        let error = from_env(&env(&[("FLEXIQ_LISTEN", "0.0.0.0:7777")])).expect_err("must refuse");
        assert!(error.to_string().contains("FLEXIQ_ATTACH_TOKEN"));
    }

    #[test]
    fn a_token_unlocks_a_non_loopback_bind() {
        let config = attach(&[
            ("FLEXIQ_LISTEN", "0.0.0.0:7777"),
            ("FLEXIQ_ATTACH_TOKEN", TOKEN),
        ]);
        assert_eq!(
            config.listen,
            ListenAddress::Tcp("0.0.0.0:7777".parse().unwrap())
        );
        assert!(config
            .token
            .expect("the token must be honoured")
            .matches(&Secret::new(TOKEN)));
    }

    #[test]
    fn a_short_token_is_rejected() {
        let error = from_env(&env(&[
            ("FLEXIQ_LISTEN", "0.0.0.0:7777"),
            ("FLEXIQ_ATTACH_TOKEN", "s3cret"),
        ]))
        .expect_err("must refuse");
        assert!(error.to_string().contains("at least"));
    }

    #[test]
    fn a_token_is_honoured_on_a_loopback_bind_too() {
        let config = attach(&[
            ("FLEXIQ_LISTEN", "127.0.0.1:7777"),
            ("FLEXIQ_ATTACH_TOKEN", TOKEN),
        ]);
        assert!(config.token.is_some());
    }

    #[test]
    fn a_bare_port_binds_loopback() {
        let config = attach(&[("FLEXIQ_LISTEN", ":7777")]);
        assert_eq!(
            config.listen,
            ListenAddress::Tcp("127.0.0.1:7777".parse().unwrap())
        );
    }

    #[cfg(unix)]
    #[test]
    fn unix_sockets_skip_the_loopback_check() {
        let config = attach(&[("FLEXIQ_LISTEN", "unix:/run/flexiq.sock")]);
        assert_eq!(
            config.listen,
            ListenAddress::Unix(PathBuf::from("/run/flexiq.sock"))
        );
    }

    #[test]
    fn an_unhonoured_tls_variable_fails_loudly() {
        for name in UNHONOURED_TLS_VARS {
            let error = from_env(&env(&[
                ("FLEXIQ_LISTEN", "127.0.0.1:7777"),
                (name, "/certs/x"),
            ]))
            .expect_err("must refuse");
            assert!(error.to_string().contains(name));
        }
    }

    #[test]
    fn no_listen_variable_disables_the_listener() {
        assert!(from_env(&env(&[])).expect("valid").is_none());
    }
}
