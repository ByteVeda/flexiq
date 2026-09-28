//! Build a listener's rustls config from its files, and swap it on rotation.

use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::{Context, Result};
use flexiq_core::worker::tls::{provider, read_certs, read_key, read_roots};
use rustls::server::WebPkiClientVerifier;
use rustls::ServerConfig;

use crate::runtime::shutdown::Shutdown;
use crate::tls::config::TlsFiles;
use crate::tls::watch::watch_files;

/// A server config from `files`, offering `alpn` in preference order.
///
/// With a client CA every client must present a certificate chaining to it;
/// that is on top of whatever credential the listener then reads, never
/// instead of it.
pub fn server_config(files: &TlsFiles, alpn: &[&[u8]]) -> Result<Arc<ServerConfig>> {
    let certs = read_certs(&files.cert)?;
    let key = read_key(&files.key)?;
    let builder = ServerConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .context("TLS could not be configured")?;
    let builder = match &files.client_ca {
        Some(ca) => {
            let roots = Arc::new(read_roots(ca)?);
            let verifier = WebPkiClientVerifier::builder_with_provider(roots, provider())
                .build()
                .with_context(|| format!("{} cannot verify client certificates", ca.display()))?;
            builder.with_client_cert_verifier(verifier)
        }
        None => builder.with_no_client_auth(),
    };
    let mut config = builder.with_single_cert(certs, key).with_context(|| {
        format!(
            "the certificate {} and key {} do not form a usable pair",
            files.cert.display(),
            files.key.display()
        )
    })?;
    config.alpn_protocols = alpn.iter().map(|protocol| protocol.to_vec()).collect();
    Ok(Arc::new(config))
}

/// A listener's current server config, replaced whole when its files change.
///
/// Each new connection takes the config in force when it arrives; a live one
/// keeps the session it negotiated. Cloning shares the one slot.
#[derive(Clone)]
pub struct ServerTls {
    files: TlsFiles,
    alpn: Vec<Vec<u8>>,
    current: Arc<RwLock<Arc<ServerConfig>>>,
}

impl ServerTls {
    /// Load `files` now, so a bad pair fails the boot rather than the first
    /// handshake.
    pub fn load(files: TlsFiles, alpn: &[&[u8]]) -> Result<Self> {
        let config = server_config(&files, alpn)?;
        Ok(Self {
            files,
            alpn: alpn.iter().map(|protocol| protocol.to_vec()).collect(),
            current: Arc::new(RwLock::new(config)),
        })
    }

    /// The config a connection accepted now should use.
    pub fn current(&self) -> Arc<ServerConfig> {
        // A writer that panicked mid-swap still left a whole `Arc` behind.
        let slot = self.current.read().unwrap_or_else(|p| p.into_inner());
        Arc::clone(&slot)
    }

    /// Re-read the files. On failure the previous config stays in force.
    pub fn reload(&self) -> Result<()> {
        let alpn: Vec<&[u8]> = self.alpn.iter().map(Vec::as_slice).collect();
        let config = server_config(&self.files, &alpn)?;
        *self.current.write().unwrap_or_else(|p| p.into_inner()) = config;
        Ok(())
    }

    /// Reload whenever the files' content changes, until `shutdown`. Must be
    /// called inside the runtime.
    pub fn spawn_watch(&self, label: &'static str, poll: Duration, shutdown: Shutdown) {
        let tls = self.clone();
        tokio::spawn(watch_files(
            label,
            self.files.paths(),
            poll,
            shutdown,
            move || {
                let tls = tls.clone();
                // The PEM readers are blocking `std::fs`; off the runtime so a
                // slow mount cannot stall a handshake task on the same worker.
                async move { tokio::task::spawn_blocking(move || tls.reload()).await? }
            },
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn fixture(name: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../flexiq-core/tests/fixtures/tls")
            .join(name)
    }

    fn files(cert: &str, key: &str, client_ca: Option<&str>) -> TlsFiles {
        TlsFiles {
            cert: fixture(cert),
            key: fixture(key),
            client_ca: client_ca.map(fixture),
        }
    }

    #[test]
    fn a_pair_loads_with_its_alpn() {
        let config =
            server_config(&files("server.pem", "server-key.pem", None), &[b"h2"]).expect("config");
        assert_eq!(config.alpn_protocols, vec![b"h2".to_vec()]);
    }

    #[test]
    fn a_client_ca_loads() {
        server_config(&files("server.pem", "server-key.pem", Some("ca.pem")), &[]).expect("config");
    }

    #[test]
    fn a_key_for_another_certificate_is_refused() {
        let error = server_config(&files("server.pem", "client-key.pem", None), &[])
            .expect_err("must refuse");
        assert!(
            format!("{error:#}").contains("do not form a usable pair"),
            "{error:#}"
        );
    }

    #[test]
    fn a_client_ca_that_is_not_pem_is_refused() {
        let error = server_config(
            &files("server.pem", "server-key.pem", Some("server-key.pem")),
            &[],
        )
        .expect_err("must refuse");
        assert!(format!("{error:#}").contains("server-key.pem"), "{error:#}");
    }

    #[test]
    fn a_failed_reload_keeps_the_previous_config() {
        let dir = std::env::temp_dir().join(format!("flexiq-tls-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).expect("dir");
        let cert = dir.join("tls.crt");
        let key = dir.join("tls.key");
        std::fs::copy(fixture("server.pem"), &cert).expect("cert");
        std::fs::copy(fixture("server-key.pem"), &key).expect("key");
        let tls = ServerTls::load(
            TlsFiles {
                cert: cert.clone(),
                key: key.clone(),
                client_ca: None,
            },
            &[],
        )
        .expect("load");
        let before = tls.current();

        std::fs::write(&cert, b"half-written").expect("corrupt");
        assert!(tls.reload().is_err());
        assert!(Arc::ptr_eq(&before, &tls.current()));

        std::fs::copy(fixture("server-rotated.pem"), &cert).expect("cert");
        std::fs::copy(fixture("server-rotated-key.pem"), &key).expect("key");
        tls.reload().expect("reload");
        assert!(!Arc::ptr_eq(&before, &tls.current()));
        let _ = std::fs::remove_dir_all(dir);
    }
}
