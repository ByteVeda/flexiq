//! Dial the address an executor was pointed at.
//!
//! The listener parses the same grammar on the bind side
//! (`flexiq-server`'s `config::listen`), so the two stay readable against each
//! other: whatever `FLEXIQ_LISTEN` accepts, `FLEXIQ_ATTACH` dials. Every SDK
//! shares this rather than reimplementing the grammar in its own language,
//! where `unix:` support would inevitably drift.

use std::io;
use std::net::{TcpStream, ToSocketAddrs};
#[cfg(unix)]
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::time::Duration;

#[cfg(unix)]
use super::transport::UnixTransport;
use super::transport::{TcpTransport, Transport};

/// Where an executor attaches.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttachAddress {
    /// TCP, for a scheduler in another container or host.
    Tcp(String),
    /// TCP under TLS, for a scheduler that terminates it itself.
    Tls(String),
    /// Unix domain socket, the same-pod sidecar case.
    #[cfg(unix)]
    Unix(PathBuf),
}

/// Client-side TLS material for a `tls://` attach address.
///
/// Paths rather than bytes: they are what an operator configures, and they are
/// not secrets the way the attach token is. An empty value trusts the bundled
/// web roots and presents no client certificate.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AttachTls {
    /// PEM bundle of the CAs the scheduler's certificate must chain to.
    pub ca: Option<PathBuf>,
    /// PEM client certificate chain, for a scheduler that requires mTLS.
    pub cert: Option<PathBuf>,
    /// PEM private key for `cert`.
    pub key: Option<PathBuf>,
}

impl AttachTls {
    /// Whether nothing is configured.
    pub fn is_empty(&self) -> bool {
        self.ca.is_none() && self.cert.is_none() && self.key.is_none()
    }
}

impl std::fmt::Display for AttachAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Tcp(target) => write!(f, "tcp://{target}"),
            Self::Tls(target) => write!(f, "tls://{target}"),
            #[cfg(unix)]
            Self::Unix(path) => write!(f, "unix:{}", path.display()),
        }
    }
}

impl AttachAddress {
    /// Parse one attach spec: `unix:/path`, `tls://host:port`,
    /// `tcp://host:port`, `host:port`, or `:port`.
    ///
    /// A bare `:port` means loopback, matching the listener's reading of the
    /// same ambiguous value.
    pub fn parse(spec: &str) -> io::Result<Self> {
        let spec = spec.trim();
        if spec.is_empty() {
            return Err(invalid("an attach address must not be empty"));
        }

        if let Some(path) = spec.strip_prefix("unix:") {
            #[cfg(unix)]
            {
                if path.is_empty() {
                    return Err(invalid(
                        "a unix attach address needs a socket path, e.g. unix:/run/flexiq.sock",
                    ));
                }
                return Ok(Self::Unix(std::path::PathBuf::from(path)));
            }
            #[cfg(not(unix))]
            {
                let _ = path;
                return Err(invalid(
                    "unix socket attach addresses are not supported on this platform",
                ));
            }
        }

        // The listener prints itself as `tcp://host:port`, so an operator who
        // copies that line out of the logs must get a working address back.
        let (target, tls) = match spec.strip_prefix("tls://") {
            Some(target) => (target, true),
            None => (spec.strip_prefix("tcp://").unwrap_or(spec), false),
        };
        let target = match target.strip_prefix(':') {
            Some(port) => format!("127.0.0.1:{port}"),
            None => target.to_string(),
        };
        if !target.contains(':') {
            return Err(invalid(format!(
                "'{spec}' has no port — an attach address looks like host:port or \
                 unix:/run/flexiq.sock"
            )));
        }
        Ok(if tls {
            Self::Tls(target)
        } else {
            Self::Tcp(target)
        })
    }

    /// Open a connection to this address.
    ///
    /// `timeout` bounds the TCP connect so an unreachable scheduler fails
    /// promptly instead of sitting in the platform's default retry window,
    /// which can be minutes.
    pub fn connect(&self, timeout: Duration) -> io::Result<Box<dyn Transport>> {
        self.connect_with(timeout, None)
    }

    /// Open a connection, with `tls` as the client's TLS material.
    ///
    /// TLS material beside a plaintext address is refused rather than ignored:
    /// an executor configured with a CA believes its connection is encrypted,
    /// and quietly dialling in the clear would make that belief false. On a
    /// `tls://` address `timeout` bounds the handshake as well as the connect.
    pub fn connect_with(
        &self,
        timeout: Duration,
        tls: Option<&AttachTls>,
    ) -> io::Result<Box<dyn Transport>> {
        let tls = tls.filter(|tls| !tls.is_empty());
        match self {
            Self::Tcp(target) => {
                if tls.is_some() {
                    return Err(invalid(format!(
                        "TLS options are set, but 'tcp://{target}' is a plaintext address — \
                         dial tls://{target} instead"
                    )));
                }
                Ok(Box::new(TcpTransport::new(dial_tcp(target, timeout)?)?))
            }
            Self::Tls(target) => connect_tls(target, timeout, tls),
            #[cfg(unix)]
            Self::Unix(path) => {
                if tls.is_some() {
                    return Err(invalid(format!(
                        "TLS options are set, but unix:{} is a local socket that TLS does not \
                         apply to",
                        path.display()
                    )));
                }
                // No connect timeout exists for a Unix socket, and none is
                // needed: the peer is on this host, so a connect either
                // succeeds or fails at once.
                let stream = UnixStream::connect(path)?;
                Ok(Box::new(UnixTransport::new(stream)))
            }
        }
    }
}

/// Connect to the first resolved address of `target` that answers.
fn dial_tcp(target: &str, timeout: Duration) -> io::Result<TcpStream> {
    let addresses = target
        .to_socket_addrs()
        .map_err(|error| invalid(format!("'{target}' is not a valid host:port: {error}")))?;
    // Every resolved address is tried, not just the first: a dual-stack
    // scheduler resolves to both an AAAA and an A record, and a host that
    // cannot route one still reaches the other. The last failure is what gets
    // reported.
    let mut last_error = None;
    for address in addresses {
        match TcpStream::connect_timeout(&address, timeout) {
            Ok(stream) => return Ok(stream),
            Err(error) => last_error = Some(error),
        }
    }
    Err(last_error.unwrap_or_else(|| invalid(format!("'{target}' resolved to no address"))))
}

#[cfg(feature = "attach-tls")]
fn connect_tls(
    target: &str,
    timeout: Duration,
    tls: Option<&AttachTls>,
) -> io::Result<Box<dyn Transport>> {
    let config = super::tls::client_config(tls.unwrap_or(&AttachTls::default()))?;
    let name = server_name(target)?;
    let socket = dial_tcp(target, timeout)?;
    Ok(Box::new(super::tls::TlsTransport::connect(
        socket, config, name, timeout,
    )?))
}

#[cfg(not(feature = "attach-tls"))]
fn connect_tls(
    target: &str,
    _timeout: Duration,
    _tls: Option<&AttachTls>,
) -> io::Result<Box<dyn Transport>> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        format!("tls://{target} needs TLS support, which this build was compiled without"),
    ))
}

/// The name the scheduler's certificate must carry: the host half of
/// `host:port`, with an IPv6 literal's brackets removed.
#[cfg(feature = "attach-tls")]
fn server_name(target: &str) -> io::Result<rustls::pki_types::ServerName<'static>> {
    let host = target
        .rsplit_once(':')
        .map_or(target, |(host, _)| host)
        .trim_start_matches('[')
        .trim_end_matches(']');
    rustls::pki_types::ServerName::try_from(host.to_string())
        .map_err(|error| invalid(format!("'{host}' is not a valid TLS server name: {error}")))
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_host_and_port_parses_as_tcp() {
        assert_eq!(
            AttachAddress::parse("scheduler:7749").expect("parse"),
            AttachAddress::Tcp("scheduler:7749".to_string())
        );
    }

    #[test]
    fn the_tcp_scheme_the_listener_prints_is_accepted() {
        // The listener logs `attach listener on tcp://127.0.0.1:7749`; pasting
        // that back must work.
        assert_eq!(
            AttachAddress::parse("tcp://127.0.0.1:7749").expect("parse"),
            AttachAddress::Tcp("127.0.0.1:7749".to_string())
        );
    }

    #[test]
    fn a_bare_port_means_loopback() {
        assert_eq!(
            AttachAddress::parse(":7749").expect("parse"),
            AttachAddress::Tcp("127.0.0.1:7749".to_string())
        );
    }

    #[test]
    fn surrounding_whitespace_is_ignored() {
        // Shell heredocs and Kubernetes manifests both leak trailing newlines.
        assert_eq!(
            AttachAddress::parse("  127.0.0.1:7749\n").expect("parse"),
            AttachAddress::Tcp("127.0.0.1:7749".to_string())
        );
    }

    #[test]
    fn an_address_without_a_port_is_rejected_with_the_shape_it_wanted() {
        let error = AttachAddress::parse("scheduler").expect_err("must be rejected");
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(
            error.to_string().contains("host:port"),
            "the message must show the expected shape: {error}"
        );
    }

    #[test]
    fn an_empty_address_is_rejected() {
        assert!(AttachAddress::parse("").is_err());
        assert!(AttachAddress::parse("   ").is_err());
    }

    #[cfg(unix)]
    #[test]
    fn a_unix_path_parses_and_prints_back() {
        let address = AttachAddress::parse("unix:/run/flexiq.sock").expect("parse");
        assert_eq!(
            address,
            AttachAddress::Unix(std::path::PathBuf::from("/run/flexiq.sock"))
        );
        assert_eq!(address.to_string(), "unix:/run/flexiq.sock");
    }

    #[cfg(unix)]
    #[test]
    fn a_unix_scheme_without_a_path_is_rejected() {
        let error = AttachAddress::parse("unix:").expect_err("must be rejected");
        assert!(error.to_string().contains("socket path"), "{error}");
    }

    #[test]
    fn the_tls_scheme_parses_and_prints_back() {
        let address = AttachAddress::parse("tls://scheduler:7749").expect("parse");
        assert_eq!(address, AttachAddress::Tls("scheduler:7749".to_string()));
        assert_eq!(address.to_string(), "tls://scheduler:7749");
        assert_eq!(
            AttachAddress::parse("tls://:7749").expect("parse"),
            AttachAddress::Tls("127.0.0.1:7749".to_string())
        );
    }

    #[test]
    fn tls_material_beside_a_plaintext_address_is_refused() {
        let tls = AttachTls {
            ca: Some(PathBuf::from("/certs/ca.pem")),
            ..AttachTls::default()
        };
        let error = AttachAddress::parse("127.0.0.1:1")
            .expect("parse")
            .connect_with(Duration::from_millis(100), Some(&tls))
            .err()
            .expect("must refuse rather than dial in the clear");
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(error.to_string().contains("tls://127.0.0.1:1"), "{error}");
    }

    #[cfg(unix)]
    #[test]
    fn tls_material_beside_a_unix_socket_is_refused() {
        let tls = AttachTls {
            ca: Some(PathBuf::from("/certs/ca.pem")),
            ..AttachTls::default()
        };
        let error = AttachAddress::parse("unix:/run/flexiq.sock")
            .expect("parse")
            .connect_with(Duration::from_millis(100), Some(&tls))
            .err()
            .expect("must refuse");
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn empty_tls_material_is_no_tls_material() {
        // A shell passes an all-`None` value when nothing is configured; that
        // must not make a plaintext dial fail.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let port = listener.local_addr().expect("addr").port();
        let address = AttachAddress::parse(&format!(":{port}")).expect("parse");
        address
            .connect_with(Duration::from_secs(5), Some(&AttachTls::default()))
            .expect("plaintext dial");
    }

    #[cfg(feature = "attach-tls")]
    #[test]
    fn a_tls_address_dials_a_tls_listener() {
        use std::io::{Read, Write};
        use std::sync::Arc;

        let fixture = |name: &str| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures/tls")
                .join(name)
        };
        let server = Arc::new(
            rustls::ServerConfig::builder_with_provider(super::super::tls::provider())
                .with_safe_default_protocol_versions()
                .expect("versions")
                .with_no_client_auth()
                .with_single_cert(
                    super::super::tls::read_certs(&fixture("server.pem")).expect("certs"),
                    super::super::tls::read_key(&fixture("server-key.pem")).expect("key"),
                )
                .expect("server config"),
        );
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let port = listener.local_addr().expect("addr").port();
        std::thread::spawn(move || {
            let (socket, _) = listener.accept().expect("accept");
            let transport =
                super::super::tls::TlsTransport::accept(socket, server, Duration::from_secs(5))
                    .expect("accept");
            let (_, mut write, _) = Box::new(transport).split().expect("split");
            write.write_all(b"!").expect("write");
        });

        // `localhost`, so the name checked is the certificate's DNS entry.
        let address = AttachAddress::parse(&format!("tls://localhost:{port}")).expect("parse");
        let tls = AttachTls {
            ca: Some(fixture("ca.pem")),
            ..AttachTls::default()
        };
        let transport = address
            .connect_with(Duration::from_secs(5), Some(&tls))
            .expect("dial");
        assert!(transport.peer().starts_with("tls:"));
        let (mut read, _, _) = transport.split().expect("split");
        let mut byte = [0u8; 1];
        read.read_exact(&mut byte).expect("read");
        assert_eq!(&byte, b"!");
    }

    #[test]
    fn connecting_to_a_closed_port_fails_rather_than_hanging() {
        // Port 1 on loopback: reserved, and nothing listens there.
        let address = AttachAddress::parse("127.0.0.1:1").expect("parse");
        assert!(address.connect(Duration::from_millis(500)).is_err());
    }

    #[test]
    fn a_dialed_address_round_trips_through_a_real_listener() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let port = listener.local_addr().expect("addr").port();
        let accepting = std::thread::spawn(move || listener.accept().expect("accept"));

        let address = AttachAddress::parse(&format!(":{port}")).expect("parse");
        let transport = address.connect(Duration::from_secs(5)).expect("connect");
        assert!(transport.peer().starts_with("tcp:"));

        let _ = accepting.join();
    }
}
