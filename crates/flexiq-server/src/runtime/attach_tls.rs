//! An accepted attach connection whose TLS handshake has not run yet.
//!
//! The accept loop must never block on a peer, so it cannot handshake. This
//! defers the handshake to [`Transport::split`], which the dispatcher calls on
//! the connection's own handshake thread — the thread that already bounds how
//! long an unauthenticated peer may hold it.

use std::io;
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;

use flexiq_core::worker::transport::{Connection, ReadHalf, WriteHalf};
use flexiq_core::worker::TlsTransport;
use flexiq_core::Transport;
use rustls::ServerConfig;

/// How long a peer has to finish its TLS handshake, before the frame
/// protocol's own `hello` deadline starts.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// A TCP connection to be handshaken with the config in force when it was
/// accepted.
pub struct AcceptingTls {
    socket: TcpStream,
    config: Arc<ServerConfig>,
}

impl AcceptingTls {
    /// Wrap an accepted socket.
    pub fn new(socket: TcpStream, config: Arc<ServerConfig>) -> Self {
        Self { socket, config }
    }
}

impl Transport for AcceptingTls {
    fn split(self: Box<Self>) -> io::Result<(ReadHalf, WriteHalf, Connection)> {
        // The listener made the socket non-blocking to poll `accept`; an
        // accepted socket inherits that on some platforms, and the handshake
        // and the frame protocol both expect blocking reads.
        self.socket.set_nonblocking(false)?;
        let tls = TlsTransport::accept(self.socket, self.config, HANDSHAKE_TIMEOUT)?;
        Box::new(tls).split()
    }

    fn peer(&self) -> String {
        match self.socket.peer_addr() {
            Ok(addr) => format!("tls:{addr}"),
            Err(_) => "tls:unknown".to_string(),
        }
    }
}
