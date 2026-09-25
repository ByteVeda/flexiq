//! TLS for the broker sinks.
//!
//! The workspace compiles rustls with both the ring and the aws-lc-rs
//! providers, and with two compiled in `ClientConfig::builder()` has no
//! default to pick and panics. So the provider is always named here.

use std::sync::Arc;

use rustls::{ClientConfig, RootCertStore};
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::CertificateDer;

/// A client config trusting `ca_file`'s certificates, or the bundled web
/// roots when there is none.
pub(crate) fn client_config(ca_file: Option<&str>) -> Result<ClientConfig, String> {
    let roots = match ca_file {
        Some(path) => roots_from_file(path)?,
        None => RootCertStore {
            roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
        },
    };
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    Ok(ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| format!("TLS could not be configured: {e}"))?
        .with_root_certificates(roots)
        .with_no_client_auth())
}

fn roots_from_file(path: &str) -> Result<RootCertStore, String> {
    let unreadable = |e: rustls_pki_types::pem::Error| format!("ca_file '{path}': {e}");
    let mut roots = RootCertStore::empty();
    for cert in CertificateDer::pem_file_iter(path).map_err(unreadable)? {
        roots
            .add(cert.map_err(unreadable)?)
            .map_err(|e| format!("ca_file '{path}': {e}"))?;
    }
    if roots.is_empty() {
        return Err(format!("ca_file '{path}' holds no certificates"));
    }
    Ok(roots)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn web_roots_build_without_a_default_provider() {
        // `ClientConfig::builder()` would panic here in a workspace build.
        client_config(None).unwrap();
    }

    #[test]
    fn a_missing_or_empty_ca_file_is_refused() {
        let error = client_config(Some("/nonexistent/ca.pem")).unwrap_err();
        assert!(error.contains("/nonexistent/ca.pem"), "{error}");

        let file = tempfile::NamedTempFile::new().unwrap();
        let path = file.path().to_str().unwrap();
        let error = client_config(Some(path)).unwrap_err();
        assert!(error.contains("holds no certificates"), "{error}");
    }
}
