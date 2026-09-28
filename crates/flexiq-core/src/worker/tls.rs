//! The attach frame protocol over TLS.
//!
//! [`Transport`] hands out owned read and write halves used from two threads at
//! once, and a rustls connection is one state machine for both directions. The
//! obvious wrapper — the whole stream behind a lock — deadlocks: an idle reader
//! blocked in `read` holds the lock, and a dispatch can never be written.
//!
//! So the socket is cloned into two file descriptors and only the rustls state
//! is shared. The reader blocks on *its* descriptor with the lock released, and
//! takes the lock only to hand rustls the bytes that arrived. The writer takes
//! it to encrypt and send. Timeouts and `close` act on the real socket, so they
//! mean exactly what they mean on [`TcpTransport`](super::TcpTransport).

use std::io::{self, BufReader, Read, Write};
use std::net::TcpStream;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use rustls::pki_types::ServerName;
use rustls::{ClientConfig, ClientConnection, Connection, ServerConfig, ServerConnection};

use super::transport::{Connection as Controls, ReadHalf, Transport, WriteHalf};

/// Bytes read off the socket per `read` call. One maximum-size TLS record.
const RECORD_BYTES: usize = 16 * 1024 + 256;

/// A TCP connection that has completed a TLS handshake.
pub struct TlsTransport {
    socket: TcpStream,
    tls: Connection,
}

impl TlsTransport {
    /// Handshake as the client, verifying the peer as `server_name`.
    ///
    /// `timeout` bounds the whole handshake, so a peer that accepts the TCP
    /// connection and then says nothing cannot hold the dial forever.
    pub fn connect(
        socket: TcpStream,
        config: Arc<ClientConfig>,
        server_name: ServerName<'static>,
        timeout: Duration,
    ) -> io::Result<Self> {
        let tls = ClientConnection::new(config, server_name).map_err(tls_error)?;
        Self::handshake(socket, tls.into(), timeout)
    }

    /// Handshake as the server. `timeout` bounds it, as on the client side: an
    /// unauthenticated peer must not pin a handshake thread.
    pub fn accept(
        socket: TcpStream,
        config: Arc<ServerConfig>,
        timeout: Duration,
    ) -> io::Result<Self> {
        let tls = ServerConnection::new(config).map_err(tls_error)?;
        Self::handshake(socket, tls.into(), timeout)
    }

    fn handshake(
        mut socket: TcpStream,
        mut tls: Connection,
        timeout: Duration,
    ) -> io::Result<Self> {
        socket.set_nodelay(true)?;
        let deadline = Instant::now() + timeout;
        while tls.is_handshaking() {
            let left = deadline
                .checked_duration_since(Instant::now())
                .filter(|left| !left.is_zero())
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::TimedOut, "TLS handshake timed out")
                })?;
            socket.set_read_timeout(Some(left))?;
            socket.set_write_timeout(Some(left))?;
            tls.complete_io(&mut socket)?;
        }
        // The client's last flight can still be buffered once the state machine
        // says it is done.
        while tls.wants_write() {
            tls.write_tls(&mut socket)?;
        }
        // Back to a plain socket's defaults; the attach handshake sets its own.
        socket.set_read_timeout(None)?;
        socket.set_write_timeout(None)?;
        Ok(Self { socket, tls })
    }
}

impl Transport for TlsTransport {
    fn split(self: Box<Self>) -> io::Result<(ReadHalf, WriteHalf, Controls)> {
        let tls = Arc::new(Mutex::new(self.tls));
        let reader = TlsReader {
            socket: self.socket.try_clone()?,
            tls: Arc::clone(&tls),
            pending: Vec::new(),
            eof: false,
        };
        let control = Arc::new(self.socket.try_clone()?);
        let writer_control = Arc::clone(&control);
        let closer = Arc::clone(&control);
        let writer = TlsWriter {
            socket: self.socket,
            tls,
        };
        Ok((
            Box::new(BufReader::new(reader)),
            Box::new(writer),
            Controls::new(
                move |timeout| control.set_read_timeout(timeout),
                move |timeout| writer_control.set_write_timeout(timeout),
                move || {
                    let _ = closer.shutdown(std::net::Shutdown::Both);
                },
            ),
        ))
    }

    fn peer(&self) -> String {
        match self.socket.peer_addr() {
            Ok(addr) => format!("tls:{addr}"),
            Err(_) => "tls:unknown".to_string(),
        }
    }
}

/// Plaintext out of the shared session, ciphertext off this half's descriptor.
struct TlsReader {
    socket: TcpStream,
    tls: Arc<Mutex<Connection>>,
    /// Ciphertext read off the socket that rustls has not taken yet. It stops
    /// taking bytes while undelivered plaintext is buffered.
    pending: Vec<u8>,
    /// The socket reported end of stream; rustls has been told.
    eof: bool,
}

impl Read for TlsReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        loop {
            {
                let mut tls = lock(&self.tls);
                match tls.reader().read(buf) {
                    Ok(n) => return Ok(n),
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                    Err(error) => return Err(error),
                }
                if !self.pending.is_empty() {
                    let taken = tls.read_tls(&mut self.pending.as_slice())?;
                    if taken == 0 {
                        // Nothing buffered to deliver and nothing accepted:
                        // looping would spin forever on the same bytes.
                        return Err(io::Error::other("TLS session stopped accepting data"));
                    }
                    self.pending.drain(..taken);
                    self.process(&mut tls)?;
                    continue;
                }
                if self.eof {
                    // Told rustls, and it still has nothing: the peer closed
                    // without a close_notify.
                    return Err(io::ErrorKind::UnexpectedEof.into());
                }
            }

            // Blocks without the lock, so the writer is free meanwhile. A read
            // timeout set through `Connection` surfaces here as the platform's
            // `WouldBlock`, the same as on a plain socket.
            let mut record = [0u8; RECORD_BYTES];
            let n = self.socket.read(&mut record)?;
            if n == 0 {
                self.eof = true;
                let mut tls = lock(&self.tls);
                tls.read_tls(&mut io::empty())?;
                self.process(&mut tls)?;
            } else {
                self.pending.extend_from_slice(&record[..n]);
            }
        }
    }
}

impl TlsReader {
    /// Decrypt what rustls has taken, and send whatever that obliges it to —
    /// an alert, a key update — before anyone waits on the reply.
    fn process(&self, tls: &mut MutexGuard<'_, Connection>) -> io::Result<()> {
        let outcome = tls.process_new_packets();
        flush(tls, &mut &self.socket)?;
        outcome.map(|_| ()).map_err(tls_error)
    }
}

/// Plaintext into the shared session, ciphertext onto this half's descriptor.
struct TlsWriter {
    socket: TcpStream,
    tls: Arc<Mutex<Connection>>,
}

impl Write for TlsWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut tls = lock(&self.tls);
        let n = tls.writer().write(buf)?;
        flush(&mut tls, &mut self.socket)?;
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        let mut tls = lock(&self.tls);
        tls.writer().flush()?;
        flush(&mut tls, &mut self.socket)
    }
}

/// Send every encrypted byte rustls is holding.
fn flush(tls: &mut Connection, socket: &mut impl Write) -> io::Result<()> {
    while tls.wants_write() {
        tls.write_tls(socket)?;
    }
    Ok(())
}

/// The session lock. A panic on the other half leaves the rustls state
/// mid-operation, so the connection is unusable either way; reporting that as
/// an I/O error would need every caller to handle a case the other half's
/// panic already surfaces, so the state is taken as it is.
fn lock(tls: &Mutex<Connection>) -> MutexGuard<'_, Connection> {
    tls.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn tls_error(error: rustls::Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;
    use std::path::PathBuf;

    use rustls::pki_types::pem::PemObject;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};
    use rustls::RootCertStore;

    fn fixture(name: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/tls")
            .join(name)
    }

    fn certs(name: &str) -> Vec<CertificateDer<'static>> {
        CertificateDer::pem_file_iter(fixture(name))
            .expect("read")
            .collect::<Result<_, _>>()
            .expect("parse")
    }

    fn key(name: &str) -> PrivateKeyDer<'static> {
        PrivateKeyDer::from_pem_file(fixture(name)).expect("key")
    }

    fn provider() -> Arc<rustls::crypto::CryptoProvider> {
        Arc::new(rustls::crypto::ring::default_provider())
    }

    fn server_config() -> Arc<ServerConfig> {
        Arc::new(
            ServerConfig::builder_with_provider(provider())
                .with_safe_default_protocol_versions()
                .expect("versions")
                .with_no_client_auth()
                .with_single_cert(certs("server.pem"), key("server-key.pem"))
                .expect("server config"),
        )
    }

    fn client_config() -> Arc<ClientConfig> {
        let mut roots = RootCertStore::empty();
        for cert in certs("ca.pem") {
            roots.add(cert).expect("root");
        }
        Arc::new(
            ClientConfig::builder_with_provider(provider())
                .with_safe_default_protocol_versions()
                .expect("versions")
                .with_root_certificates(roots)
                .with_no_client_auth(),
        )
    }

    /// A handshaken client and server over loopback.
    fn pair() -> (Box<dyn Transport>, Box<dyn Transport>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        let server = std::thread::spawn(move || {
            let (socket, _) = listener.accept().expect("accept");
            TlsTransport::accept(socket, server_config(), Duration::from_secs(5))
                .expect("accept tls")
        });
        let socket = TcpStream::connect(addr).expect("connect");
        let name = ServerName::try_from("localhost").expect("name");
        let client = TlsTransport::connect(socket, client_config(), name, Duration::from_secs(5))
            .expect("client tls");
        let server = server.join().expect("server thread");
        (Box::new(client), Box::new(server))
    }

    #[test]
    fn bytes_cross_in_both_directions() {
        let (client, server) = pair();
        assert!(client.peer().starts_with("tls:127.0.0.1:"));
        let (mut client_read, mut client_write, _) = client.split().expect("split");
        let (mut server_read, mut server_write, _) = server.split().expect("split");

        client_write.write_all(b"hello\n").expect("write");
        let mut line = String::new();
        std::io::BufRead::read_line(&mut server_read, &mut line).expect("read");
        assert_eq!(line, "hello\n");

        server_write.write_all(b"world\n").expect("write");
        line.clear();
        std::io::BufRead::read_line(&mut client_read, &mut line).expect("read");
        assert_eq!(line, "world\n");
    }

    /// The reason the halves share only the session: a reader parked in `read`
    /// must not stop the writer on the same connection.
    #[test]
    fn a_blocked_reader_does_not_block_the_writer() {
        let (client, server) = pair();
        let (mut client_read, mut client_write, _) = client.split().expect("split");
        let (mut server_read, mut server_write, _) = server.split().expect("split");

        let parked = std::thread::spawn(move || {
            let mut byte = [0u8; 1];
            client_read.read_exact(&mut byte).expect("read");
            byte[0]
        });
        std::thread::sleep(Duration::from_millis(100));

        // Larger than one record, so it takes several writes under the lock.
        let payload = vec![7u8; 100_000];
        client_write
            .write_all(&payload)
            .expect("write while the reader is parked");
        client_write.flush().expect("flush");
        let mut received = vec![0u8; payload.len()];
        server_read.read_exact(&mut received).expect("read");
        assert_eq!(received, payload);

        server_write.write_all(&[42]).expect("wake the reader");
        assert_eq!(parked.join().expect("reader"), 42);
    }

    #[test]
    fn a_read_timeout_surfaces_like_a_plain_socket() {
        let (client, _server) = pair();
        let (mut client_read, _, controls) = client.split().expect("split");
        controls
            .set_read_timeout(Some(Duration::from_millis(50)))
            .expect("timeout");
        let error = client_read
            .read(&mut [0u8; 1])
            .expect_err("nothing was sent");
        assert!(
            matches!(
                error.kind(),
                io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
            ),
            "{error:?}"
        );
    }

    #[test]
    fn close_unblocks_a_parked_reader() {
        let (client, _server) = pair();
        let (mut client_read, _, controls) = client.split().expect("split");
        let parked = std::thread::spawn(move || client_read.read(&mut [0u8; 1]));
        std::thread::sleep(Duration::from_millis(100));
        controls.close();
        let outcome = parked.join().expect("reader");
        assert!(matches!(outcome, Ok(0) | Err(_)), "{outcome:?}");
    }

    #[test]
    fn a_peer_that_never_speaks_times_the_handshake_out() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        let _silent = TcpStream::connect(addr).expect("connect");
        let (socket, _) = listener.accept().expect("accept");
        let started = Instant::now();
        assert!(TlsTransport::accept(socket, server_config(), Duration::from_millis(200)).is_err());
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    #[test]
    fn an_untrusted_server_is_refused() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        std::thread::spawn(move || {
            let (socket, _) = listener.accept().expect("accept");
            let _ = TlsTransport::accept(socket, server_config(), Duration::from_secs(5));
        });
        let mut roots = RootCertStore::empty();
        for cert in certs("rogue-ca.pem") {
            roots.add(cert).expect("root");
        }
        let config = Arc::new(
            ClientConfig::builder_with_provider(provider())
                .with_safe_default_protocol_versions()
                .expect("versions")
                .with_root_certificates(roots)
                .with_no_client_auth(),
        );
        let socket = TcpStream::connect(addr).expect("connect");
        let name = ServerName::try_from("localhost").expect("name");
        let error = TlsTransport::connect(socket, config, name, Duration::from_secs(5))
            .err()
            .expect("an untrusted chain must fail the handshake");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{error}");
    }
}
