//! Dial the address an executor was pointed at.
//!
//! The listener parses the same grammar on the bind side
//! (`taskito-server`'s `config::listen`), so the two stay readable against each
//! other: whatever `TASKITO_LISTEN` accepts, `TASKITO_ATTACH` dials. Every SDK
//! shares this rather than reimplementing the grammar in its own language,
//! where `unix:` support would inevitably drift.

use std::io;
use std::net::{TcpStream, ToSocketAddrs};
#[cfg(unix)]
use std::os::unix::net::UnixStream;
use std::time::Duration;

#[cfg(unix)]
use super::transport::UnixTransport;
use super::transport::{TcpTransport, Transport};

/// Where an executor attaches.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttachAddress {
    /// TCP, for a scheduler in another container or host.
    Tcp(String),
    /// Unix domain socket, the same-pod sidecar case.
    #[cfg(unix)]
    Unix(std::path::PathBuf),
}

impl std::fmt::Display for AttachAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Tcp(target) => write!(f, "tcp://{target}"),
            #[cfg(unix)]
            Self::Unix(path) => write!(f, "unix:{}", path.display()),
        }
    }
}

impl AttachAddress {
    /// Parse one attach spec: `unix:/path`, `tcp://host:port`, `host:port`, or
    /// `:port`.
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
                        "a unix attach address needs a socket path, e.g. unix:/run/taskito.sock",
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
        let target = spec.strip_prefix("tcp://").unwrap_or(spec);
        let target = match target.strip_prefix(':') {
            Some(port) => format!("127.0.0.1:{port}"),
            None => target.to_string(),
        };
        if !target.contains(':') {
            return Err(invalid(format!(
                "'{spec}' has no port — an attach address looks like host:port or \
                 unix:/run/taskito.sock"
            )));
        }
        Ok(Self::Tcp(target))
    }

    /// Open a connection to this address.
    ///
    /// `timeout` bounds the TCP connect so an unreachable scheduler fails
    /// promptly instead of sitting in the platform's default retry window,
    /// which can be minutes.
    pub fn connect(&self, timeout: Duration) -> io::Result<Box<dyn Transport>> {
        match self {
            Self::Tcp(target) => {
                let addresses = target.to_socket_addrs().map_err(|error| {
                    invalid(format!("'{target}' is not a valid host:port: {error}"))
                })?;
                // Every resolved address is tried, not just the first: a
                // dual-stack scheduler resolves to both an AAAA and an A
                // record, and a host that cannot route one still reaches the
                // other. The last failure is what gets reported.
                let mut last_error = None;
                for address in addresses {
                    match TcpStream::connect_timeout(&address, timeout) {
                        Ok(stream) => return Ok(Box::new(TcpTransport::new(stream)?)),
                        Err(error) => last_error = Some(error),
                    }
                }
                Err(last_error
                    .unwrap_or_else(|| invalid(format!("'{target}' resolved to no address"))))
            }
            #[cfg(unix)]
            Self::Unix(path) => {
                // No connect timeout exists for a Unix socket, and none is
                // needed: the peer is on this host, so a connect either
                // succeeds or fails at once.
                let stream = UnixStream::connect(path)?;
                Ok(Box::new(UnixTransport::new(stream)))
            }
        }
    }
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
        let address = AttachAddress::parse("unix:/run/taskito.sock").expect("parse");
        assert_eq!(
            address,
            AttachAddress::Unix(std::path::PathBuf::from("/run/taskito.sock"))
        );
        assert_eq!(address.to_string(), "unix:/run/taskito.sock");
    }

    #[cfg(unix)]
    #[test]
    fn a_unix_scheme_without_a_path_is_rejected() {
        let error = AttachAddress::parse("unix:").expect_err("must be rejected");
        assert!(error.to_string().contains("socket path"), "{error}");
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
