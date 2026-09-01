//! Where the admission webhook listens, and the certificate it serves.
//!
//! Kubernetes speaks to a mutating webhook over HTTPS and no other way, so
//! unlike the dashboard there is no plaintext mode to fall back to: a bind
//! without a key pair is rejected rather than served in the clear.
//!
//! The pair is loaded at startup and then re-read whenever the files change on
//! disk, because cert-manager renews a Secret in place: the pod keeps running,
//! the mounted files are swapped underneath it, and a server that read them
//! once would serve an expired certificate until something restarted it. See
//! [`crate::webhook::serve`] for the watch.

use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::{bail, Context, Result};

use crate::config::listen::resolve;
use crate::config::{value, Env};

/// The admission webhook's bind address and serving certificate.
#[derive(Debug, Clone)]
pub struct WebhookConfig {
    /// Address the HTTPS server binds.
    pub bind: SocketAddr,
    /// PEM certificate chain presented to the API server.
    pub cert: PathBuf,
    /// PEM private key for `cert`.
    pub key: PathBuf,
}

/// Parse the webhook block, or `None` when the webhook is disabled.
pub fn from_env(env: &Env) -> Result<Option<WebhookConfig>> {
    let Some(spec) = value(env, "FLEXIQ_WEBHOOK_LISTEN") else {
        return Ok(None);
    };
    let bind = resolve("FLEXIQ_WEBHOOK_LISTEN", &spec)?;

    let cert = required_path(env, "FLEXIQ_WEBHOOK_TLS_CERT")?;
    let key = required_path(env, "FLEXIQ_WEBHOOK_TLS_KEY")?;

    Ok(Some(WebhookConfig { bind, cert, key }))
}

/// A TLS path that must be set, because Kubernetes will not talk to a webhook
/// over plaintext and a half-configured one should fail at boot rather than at
/// the first pod creation.
fn required_path(env: &Env, key: &str) -> Result<PathBuf> {
    let raw = value(env, key).with_context(|| {
        format!(
            "{key} is required when FLEXIQ_WEBHOOK_LISTEN is set — the API server \
             only calls a webhook over HTTPS, so there is no plaintext mode"
        )
    })?;
    let path = PathBuf::from(raw);
    if !path.is_file() {
        bail!("{key}={} is not a readable file", path.display());
    }
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn env(pairs: &[(&str, &str)]) -> Env {
        pairs
            .iter()
            .map(|(key, val)| (key.to_string(), val.to_string()))
            .collect()
    }

    /// A real file on disk, since the parser checks readability.
    fn touch(label: &str) -> PathBuf {
        let path =
            std::env::temp_dir().join(format!("flexiq-webhook-{label}-{}.pem", std::process::id()));
        let mut file = std::fs::File::create(&path).expect("create");
        file.write_all(b"not really a certificate").expect("write");
        path
    }

    #[test]
    fn no_listen_variable_disables_the_webhook() {
        assert!(from_env(&env(&[])).expect("valid").is_none());
    }

    #[test]
    fn a_listen_address_needs_both_tls_paths() {
        for present in ["FLEXIQ_WEBHOOK_TLS_CERT", "FLEXIQ_WEBHOOK_TLS_KEY"] {
            let file = touch("partial");
            let error = from_env(&env(&[
                ("FLEXIQ_WEBHOOK_LISTEN", "0.0.0.0:9443"),
                (present, file.to_str().expect("utf-8")),
            ]))
            .expect_err("must refuse a half-configured webhook");
            assert!(error.to_string().contains("HTTPS"));
        }
    }

    #[test]
    fn a_missing_certificate_file_is_rejected() {
        let key = touch("key-only");
        let error = from_env(&env(&[
            ("FLEXIQ_WEBHOOK_LISTEN", "0.0.0.0:9443"),
            ("FLEXIQ_WEBHOOK_TLS_CERT", "/no/such/cert.pem"),
            ("FLEXIQ_WEBHOOK_TLS_KEY", key.to_str().expect("utf-8")),
        ]))
        .expect_err("must refuse an unreadable path");
        assert!(error.to_string().contains("not a readable file"));
    }

    #[test]
    fn a_full_block_parses() {
        let cert = touch("cert");
        let key = touch("key");
        let config = from_env(&env(&[
            ("FLEXIQ_WEBHOOK_LISTEN", "0.0.0.0:9443"),
            ("FLEXIQ_WEBHOOK_TLS_CERT", cert.to_str().expect("utf-8")),
            ("FLEXIQ_WEBHOOK_TLS_KEY", key.to_str().expect("utf-8")),
        ]))
        .expect("valid")
        .expect("configured");
        assert_eq!(config.bind, "0.0.0.0:9443".parse().unwrap());
        assert_eq!(config.cert, cert);
        assert_eq!(config.key, key);
    }

    #[test]
    fn a_bare_port_binds_loopback() {
        let cert = touch("bare-cert");
        let key = touch("bare-key");
        let config = from_env(&env(&[
            ("FLEXIQ_WEBHOOK_LISTEN", ":9443"),
            ("FLEXIQ_WEBHOOK_TLS_CERT", cert.to_str().expect("utf-8")),
            ("FLEXIQ_WEBHOOK_TLS_KEY", key.to_str().expect("utf-8")),
        ]))
        .expect("valid")
        .expect("configured");
        assert_eq!(config.bind, "127.0.0.1:9443".parse().unwrap());
    }
}
