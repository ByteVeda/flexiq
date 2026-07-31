//! Accept executor connections and hand them to the `RemoteDispatcher`.
//!
//! Binding and accepting are deliberately the caller's job in core, so this is
//! where they live. Accepts run on a blocking thread — the handshake is
//! blocking too — and each connection gets its own thread so one silent peer
//! cannot hold the accept loop for a whole `handshake_timeout`.

use std::io;
use std::net::TcpListener;
#[cfg(unix)]
use std::os::unix::net::UnixListener;
#[cfg(unix)]
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use anyhow::{Context, Result};
use taskito_core::worker::TcpTransport;
#[cfg(unix)]
use taskito_core::worker::UnixTransport;
use taskito_core::{RemoteDispatcher, Transport};

use crate::config::listen::AttachListen;
use crate::runtime::scheduler::SchedulerSupervisor;
use crate::runtime::shutdown::Shutdown;

/// How long the accept loop sleeps between polls when no peer is waiting.
const ACCEPT_POLL: Duration = Duration::from_millis(100);

/// A running attach listener.
pub struct ListenerHandle {
    accept_thread: JoinHandle<()>,
    bound: Option<std::net::SocketAddr>,
    #[cfg(unix)]
    socket_path: Option<PathBuf>,
}

impl ListenerHandle {
    /// Address actually bound, for a TCP listener. Resolves the ephemeral port
    /// when the configured address asked for one.
    pub fn local_addr(&self) -> Option<std::net::SocketAddr> {
        self.bound
    }

    /// Wait for the accept loop to finish and clean up the socket file. The
    /// caller must have triggered shutdown first.
    pub fn join(self) {
        if self.accept_thread.join().is_err() {
            log::error!("the attach accept loop panicked");
        }
        #[cfg(unix)]
        if let Some(path) = self.socket_path {
            let _ = std::fs::remove_file(path);
        }
    }
}

/// Bind `address` and start accepting executor attachments.
pub fn spawn(
    address: AttachListen,
    dispatcher: RemoteDispatcher,
    supervisor: Arc<SchedulerSupervisor>,
    shutdown: Shutdown,
) -> Result<ListenerHandle> {
    match address {
        AttachListen::Tcp(addr) => {
            let listener = TcpListener::bind(addr)
                .with_context(|| format!("failed to bind the attach listener on {addr}"))?;
            listener.set_nonblocking(true)?;
            // Report what was bound, not what was asked for: port 0 resolves
            // to an ephemeral port only the listener knows.
            let bound = listener.local_addr().unwrap_or(addr);
            log::info!("[taskito] attach listener on tcp://{bound}");
            Ok(ListenerHandle {
                accept_thread: accept_loop(
                    move || match listener.accept() {
                        Ok((stream, _)) => Ok(Some(Box::new(TcpTransport::new(stream)?) as _)),
                        Err(error) => Err(error),
                    },
                    dispatcher,
                    supervisor,
                    shutdown,
                ),
                bound: Some(bound),
                #[cfg(unix)]
                socket_path: None,
            })
        }
        #[cfg(unix)]
        AttachListen::Unix(path) => {
            let listener = bind_unix(&path)?;
            listener.set_nonblocking(true)?;
            log::info!("[taskito] attach listener on unix:{}", path.display());
            Ok(ListenerHandle {
                accept_thread: accept_loop(
                    move || match listener.accept() {
                        Ok((stream, _)) => Ok(Some(
                            Box::new(UnixTransport::new(stream)) as Box<dyn Transport>
                        )),
                        Err(error) => Err(error),
                    },
                    dispatcher,
                    supervisor,
                    shutdown,
                ),
                bound: None,
                socket_path: Some(path),
            })
        }
    }
}

/// Bind a Unix socket, clearing a stale file left behind by a crashed process.
/// A path that still has a live listener behind it is left alone — that is a
/// misconfiguration, not stale state.
#[cfg(unix)]
fn bind_unix(path: &Path) -> Result<UnixListener> {
    match UnixListener::bind(path) {
        Ok(listener) => Ok(listener),
        Err(error) if error.kind() == io::ErrorKind::AddrInUse => {
            if std::os::unix::net::UnixStream::connect(path).is_ok() {
                anyhow::bail!("another process is already listening on {}", path.display());
            }
            std::fs::remove_file(path).with_context(|| {
                format!("failed to remove the stale socket at {}", path.display())
            })?;
            UnixListener::bind(path).with_context(|| format!("failed to bind {}", path.display()))
        }
        Err(error) => Err(error)
            .with_context(|| format!("failed to bind the attach listener on {}", path.display())),
    }
}

/// Poll `accept` until shutdown, attaching each connection on its own thread.
///
/// The listener is non-blocking so the loop can observe shutdown; a blocking
/// `accept` would only return when some peer happened to connect.
fn accept_loop(
    accept: impl FnMut() -> io::Result<Option<Box<dyn Transport>>> + Send + 'static,
    dispatcher: RemoteDispatcher,
    supervisor: Arc<SchedulerSupervisor>,
    shutdown: Shutdown,
) -> JoinHandle<()> {
    let mut accept = accept;
    thread::Builder::new()
        .name("taskito-attach-accept".to_string())
        .spawn(move || {
            let mut handshakes: Vec<JoinHandle<()>> = Vec::new();
            while !shutdown.is_triggered() {
                match accept() {
                    Ok(Some(transport)) => {
                        // Reap finished handshakes so a reconnect loop cannot
                        // grow this vector for the life of the process.
                        handshakes.retain(|handle| !handle.is_finished());
                        handshakes.push(spawn_handshake(
                            transport,
                            dispatcher.clone(),
                            supervisor.clone(),
                        ));
                    }
                    Ok(None) => {}
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        thread::sleep(ACCEPT_POLL);
                    }
                    Err(error) => {
                        log::warn!("attach accept failed: {error}");
                        thread::sleep(ACCEPT_POLL);
                    }
                }
            }
            // Bounded by `handshake_timeout`, so this cannot hang shutdown.
            for handle in handshakes {
                let _ = handle.join();
            }
        })
        .expect("spawning the accept thread cannot fail with a valid name")
}

/// Run one handshake off the accept loop, then start the scheduler if this is
/// the first executor to attach.
fn spawn_handshake(
    transport: Box<dyn Transport>,
    dispatcher: RemoteDispatcher,
    supervisor: Arc<SchedulerSupervisor>,
) -> JoinHandle<()> {
    thread::Builder::new()
        .name("taskito-attach-handshake".to_string())
        .spawn(move || {
            let peer = transport.peer();
            match dispatcher.attach(transport) {
                Ok(executor_id) => {
                    if let Err(error) = supervisor.ensure_started() {
                        log::error!(
                            "executor {executor_id} attached but the scheduler failed to start: {error}"
                        );
                    }
                }
                // Never log the frame itself: an attach failure may carry
                // credential material once the handshake is authenticated.
                Err(error) => log::warn!("attach from {peer} rejected: {error}"),
            }
        })
        .expect("spawning a handshake thread cannot fail with a valid name")
}
