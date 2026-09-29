//! The gRPC listener's TLS accept path.
//!
//! tonic can terminate TLS itself, but it takes its config once, at build
//! time, and a certificate rotation would then need a restart. So the listener
//! handshakes each connection here, against whichever config [`ServerTls`]
//! holds when the connection arrives, and hands tonic the finished streams.

use std::io;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Semaphore};
use tokio_rustls::server::TlsStream;
use tokio_rustls::TlsAcceptor;
use tokio_stream::wrappers::ReceiverStream;

use crate::runtime::shutdown::Shutdown;
use crate::tls::ServerTls;

/// How long a peer has to finish its handshake. A peer that opens a socket and
/// says nothing must not hold a slot for longer than a real client needs.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// Handshakes allowed to run at once. Each is unauthenticated, so without a cap
/// a connect flood would spawn tasks without bound; past it, a connection is
/// dropped rather than queued. The attach listener caps its handshakes the same
/// way.
const MAX_PENDING_HANDSHAKES: usize = 64;

/// How long the accept loop backs off after an accept error — out of file
/// descriptors, most likely — rather than spinning on it.
const ACCEPT_BACKOFF: Duration = Duration::from_millis(100);

/// The handshaken connections accepted on `listener`, until `shutdown`.
///
/// Each handshake runs in its own task, so one slow peer never delays the
/// next accept. A failed handshake is logged and dropped: tonic never sees
/// the connection, and so never reads a credential off it.
pub fn incoming(
    listener: TcpListener,
    tls: ServerTls,
    shutdown: Shutdown,
) -> ReceiverStream<io::Result<TlsStream<TcpStream>>> {
    let (sender, receiver) = mpsc::channel(MAX_PENDING_HANDSHAKES);
    let slots = Arc::new(Semaphore::new(MAX_PENDING_HANDSHAKES));
    tokio::spawn(async move {
        loop {
            let accepted = tokio::select! {
                () = shutdown.wait() => return,
                accepted = listener.accept() => accepted,
            };
            let (socket, peer) = match accepted {
                Ok(accepted) => accepted,
                Err(error) => {
                    log::warn!("gRPC accept failed: {error}");
                    tokio::time::sleep(ACCEPT_BACKOFF).await;
                    continue;
                }
            };
            let Ok(slot) = Arc::clone(&slots).try_acquire_owned() else {
                log::warn!(
                    "gRPC connection from {peer} dropped: {MAX_PENDING_HANDSHAKES} TLS \
                     handshakes already pending"
                );
                continue;
            };
            let acceptor = TlsAcceptor::from(tls.current());
            let sender = sender.clone();
            tokio::spawn(async move {
                let handshake = tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(socket));
                let outcome = handshake.await;
                drop(slot);
                match outcome {
                    Ok(Ok(stream)) => {
                        // Closed only once the listener has stopped serving.
                        let _ = sender.send(Ok(stream)).await;
                    }
                    Ok(Err(error)) => log::warn!("TLS handshake from {peer} failed: {error}"),
                    Err(_) => log::warn!(
                        "TLS handshake from {peer} did not finish within {HANDSHAKE_TIMEOUT:?}"
                    ),
                }
            });
        }
    });
    ReceiverStream::new(receiver)
}
