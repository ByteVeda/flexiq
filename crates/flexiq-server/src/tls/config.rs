//! Which key material a listener terminates TLS with, read from the
//! environment.
//!
//! One parser for every listener that carries a credential, so the refusals
//! below mean the same thing on each: a half-configured security control fails
//! at boot rather than serving in the clear, or failing at the first handshake,
//! with the operator believing otherwise.

use std::path::PathBuf;

use anyhow::{bail, Result};

use crate::config::listen::ListenAddress;
use crate::config::{value, Env};

/// A listener's certificate, key and, for mTLS, the CAs its clients' own
/// certificates must chain to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsFiles {
    /// PEM certificate chain the listener presents.
    pub cert: PathBuf,
    /// PEM private key for `cert`.
    pub key: PathBuf,
    /// PEM bundle of client CAs. When set, a client without a certificate that
    /// chains to one of them is refused during the handshake — before any
    /// credential it carries is read.
    pub client_ca: Option<PathBuf>,
}

impl TlsFiles {
    /// Every file, for the reload watch.
    pub fn paths(&self) -> Vec<PathBuf> {
        let mut paths = vec![self.cert.clone(), self.key.clone()];
        paths.extend(self.client_ca.clone());
        paths
    }
}

/// Parse `{prefix}_TLS_CERT`, `{prefix}_TLS_KEY` and `{prefix}_TLS_CLIENT_CA`
/// for a listener bound to `listen`. `None` when none of them is set.
pub fn from_env(env: &Env, prefix: &str, listen: &ListenAddress) -> Result<Option<TlsFiles>> {
    let cert_var = format!("{prefix}_TLS_CERT");
    let key_var = format!("{prefix}_TLS_KEY");
    let ca_var = format!("{prefix}_TLS_CLIENT_CA");

    let cert = path(env, &cert_var)?;
    let key = path(env, &key_var)?;
    let client_ca = path(env, &ca_var)?;

    let (cert, key) = match (cert, key) {
        (None, None) => {
            if client_ca.is_some() {
                bail!(
                    "{ca_var} is set without {cert_var} and {key_var}. Verifying client \
                     certificates needs a TLS listener to verify them on — set the \
                     listener's own certificate and key too."
                );
            }
            return Ok(None);
        }
        (Some(cert), Some(key)) => (cert, key),
        (Some(_), None) => bail!("{cert_var} is set without {key_var}; set both or neither"),
        (None, Some(_)) => bail!("{key_var} is set without {cert_var}; set both or neither"),
    };

    #[cfg(unix)]
    if let ListenAddress::Unix(socket) = listen {
        bail!(
            "{cert_var} is set, but the listener is the Unix socket unix:{}. A local \
             socket never leaves this host and its file mode is the access control; \
             bind TCP to terminate TLS, or unset the TLS variables.",
            socket.display()
        );
    }
    #[cfg(not(unix))]
    let _ = listen;

    Ok(Some(TlsFiles {
        cert,
        key,
        client_ca,
    }))
}

/// A path that must name a readable file when set, so a typo fails at boot
/// with the variable's name rather than at load with only the path.
fn path(env: &Env, var: &str) -> Result<Option<PathBuf>> {
    let Some(raw) = value(env, var) else {
        return Ok(None);
    };
    let path = PathBuf::from(raw);
    if !path.is_file() {
        bail!("{var}={} is not a readable file", path.display());
    }
    Ok(Some(path))
}

#[cfg(test)]
mod tests {
    use super::*;

    const PREFIX: &str = "FLEXIQ_TEST";

    fn fixture(name: &str) -> String {
        format!(
            "{}/../flexiq-core/tests/fixtures/tls/{name}",
            env!("CARGO_MANIFEST_DIR")
        )
    }

    fn env(pairs: &[(&str, String)]) -> Env {
        pairs
            .iter()
            .map(|(key, val)| (key.to_string(), val.clone()))
            .collect()
    }

    fn tcp() -> ListenAddress {
        ListenAddress::Tcp("127.0.0.1:50051".parse().unwrap())
    }

    #[test]
    fn nothing_set_is_plaintext() {
        assert!(from_env(&env(&[]), PREFIX, &tcp())
            .expect("valid")
            .is_none());
    }

    #[test]
    fn a_pair_parses_and_the_client_ca_is_optional() {
        let files = from_env(
            &env(&[
                ("FLEXIQ_TEST_TLS_CERT", fixture("server.pem")),
                ("FLEXIQ_TEST_TLS_KEY", fixture("server-key.pem")),
            ]),
            PREFIX,
            &tcp(),
        )
        .expect("valid")
        .expect("configured");
        assert!(files.client_ca.is_none());
        assert_eq!(files.paths().len(), 2);

        let files = from_env(
            &env(&[
                ("FLEXIQ_TEST_TLS_CERT", fixture("server.pem")),
                ("FLEXIQ_TEST_TLS_KEY", fixture("server-key.pem")),
                ("FLEXIQ_TEST_TLS_CLIENT_CA", fixture("ca.pem")),
            ]),
            PREFIX,
            &tcp(),
        )
        .expect("valid")
        .expect("configured");
        assert_eq!(files.paths().len(), 3);
    }

    #[test]
    fn half_a_pair_is_refused() {
        for (set, missing) in [
            ("FLEXIQ_TEST_TLS_CERT", "FLEXIQ_TEST_TLS_KEY"),
            ("FLEXIQ_TEST_TLS_KEY", "FLEXIQ_TEST_TLS_CERT"),
        ] {
            let error = from_env(&env(&[(set, fixture("server.pem"))]), PREFIX, &tcp())
                .expect_err("must refuse");
            assert!(error.to_string().contains(missing), "{error}");
        }
    }

    #[test]
    fn a_client_ca_without_a_server_pair_is_refused() {
        let error = from_env(
            &env(&[("FLEXIQ_TEST_TLS_CLIENT_CA", fixture("ca.pem"))]),
            PREFIX,
            &tcp(),
        )
        .expect_err("must refuse");
        assert!(
            error.to_string().contains("FLEXIQ_TEST_TLS_CERT"),
            "{error}"
        );
    }

    #[test]
    fn a_missing_file_is_refused_by_its_variable() {
        let error = from_env(
            &env(&[
                ("FLEXIQ_TEST_TLS_CERT", "/no/such/cert.pem".to_string()),
                ("FLEXIQ_TEST_TLS_KEY", fixture("server-key.pem")),
            ]),
            PREFIX,
            &tcp(),
        )
        .expect_err("must refuse");
        assert!(
            error.to_string().contains("FLEXIQ_TEST_TLS_CERT"),
            "{error}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn tls_on_a_unix_socket_is_refused() {
        let error = from_env(
            &env(&[
                ("FLEXIQ_TEST_TLS_CERT", fixture("server.pem")),
                ("FLEXIQ_TEST_TLS_KEY", fixture("server-key.pem")),
            ]),
            PREFIX,
            &ListenAddress::Unix("/run/flexiq.sock".into()),
        )
        .expect_err("must refuse");
        assert!(error.to_string().contains("Unix socket"), "{error}");
    }
}
