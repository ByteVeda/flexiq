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

/// Handshakes allowed to run at once. Every unauthenticated peer holds a thread
/// for up to `handshake_timeout`, so without a cap a connect flood would spawn
/// threads without bound. Past it, connections are dropped rather than queued.
const MAX_PENDING_HANDSHAKES: usize = 64;

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

/// Owner and group may connect; nobody else. `bind` would otherwise leave the
/// socket at `0777 & ~umask` — 0755 under the usual 022, which denies *write*
/// to the group and so admits only the binding uid, since `connect(2)` needs
/// write permission. A sidecar rarely shares a uid with the app container but
/// can always be given a shared group (`fsGroup`), so the group bit is what
/// makes a same-pod attach configurable at all. `other` is dropped outright:
/// under a lax umask 0777 would let anything on the host attach.
#[cfg(unix)]
const SOCKET_MODE: u32 = 0o660;

/// Bind a Unix socket that is never reachable at a wider mode than
/// [`SOCKET_MODE`].
///
/// `UnixListener::bind` binds and listens in one call, so a chmod afterwards
/// leaves a window where the socket is already accepting at `0777 & ~umask` —
/// world-writable under a permissive umask, on a port that dispatches code. So
/// the bind happens on a scratch path nobody is told about, the mode is
/// narrowed there, and only then is it `rename`d into place. `rename` is atomic
/// within a directory and keeps the inode, so the listener carries on serving
/// under its new name and no peer ever sees the wide mode. A connect that lands
/// before the rename gets `ENOENT` and retries, which is the executor's normal
/// scheduler-not-up-yet path.
///
/// A stale file left by a crashed process is replaced; a path with a live
/// listener behind it is left alone, because that is a misconfiguration rather
/// than stale state.
#[cfg(unix)]
fn bind_unix(path: &Path) -> Result<UnixListener> {
    use std::os::unix::fs::PermissionsExt;

    if std::os::unix::net::UnixStream::connect(path).is_ok() {
        anyhow::bail!("another process is already listening on {}", path.display());
    }

    // Same directory as the destination: `rename` is only atomic within one
    // filesystem, and a socket the executor can reach has to land exactly here.
    let staging = staging_path(path);
    let _ = std::fs::remove_file(&staging);
    let listener = UnixListener::bind(&staging).with_context(|| {
        format!(
            "failed to bind the attach listener on {}",
            staging.display()
        )
    })?;

    if let Err(error) =
        std::fs::set_permissions(&staging, std::fs::Permissions::from_mode(SOCKET_MODE))
    {
        let _ = std::fs::remove_file(&staging);
        return Err(error).with_context(|| {
            format!(
                "failed to set mode {SOCKET_MODE:o} on the attach socket at {}",
                staging.display()
            )
        });
    }

    // Replaces a stale socket in one step, so there is no moment where the
    // path is missing for an executor that is already retrying.
    if let Err(error) = std::fs::rename(&staging, path) {
        let _ = std::fs::remove_file(&staging);
        return Err(error).with_context(|| {
            format!(
                "failed to move the attach socket into place at {}",
                path.display()
            )
        });
    }
    Ok(listener)
}

/// Scratch path the socket is bound at before it is named. `.` prefixed so a
/// directory listing reads it as transient, and pid-suffixed so two servers
/// racing on one directory cannot clobber each other's staging file.
#[cfg(unix)]
fn staging_path(path: &Path) -> PathBuf {
    let name = path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| "attach.sock".to_string());
    let staging = format!(".{name}.{}.tmp", std::process::id());
    match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.join(staging),
        _ => PathBuf::from(staging),
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
                        if handshakes.len() >= MAX_PENDING_HANDSHAKES {
                            log::warn!(
                                "attach from {} dropped: {MAX_PENDING_HANDSHAKES} handshakes \
                                 already pending",
                                transport.peer()
                            );
                            continue;
                        }
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

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    /// A unique socket path under the temp dir, removed if a prior run left one.
    fn socket_path(label: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "taskito-attach-{label}-{}.sock",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        path
    }

    fn mode_of(path: &Path) -> u32 {
        std::fs::metadata(path)
            .expect("the socket must exist")
            .permissions()
            .mode()
            & 0o777
    }

    #[test]
    fn a_bound_socket_admits_the_group_and_nobody_else() {
        let path = socket_path("mode");
        let _listener = bind_unix(&path).expect("bind");
        // Not the umask-derived 0755: the group needs write to connect(2), and
        // `other` must not have it under any umask.
        assert_eq!(mode_of(&path), SOCKET_MODE);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn replacing_a_stale_socket_restores_the_mode() {
        let path = socket_path("stale");
        // A socket file with no listener behind it, as a crash would leave.
        drop(UnixListener::bind(&path).expect("first bind"));
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o777)).expect("loosen");

        let _listener = bind_unix(&path).expect("rebind over the stale socket");
        assert_eq!(mode_of(&path), SOCKET_MODE);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn a_live_listener_is_not_clobbered() {
        let path = socket_path("live");
        let _first = bind_unix(&path).expect("first bind");
        let error = bind_unix(&path).expect_err("a live listener must not be replaced");
        assert!(error.to_string().contains("already listening"));
        let _ = std::fs::remove_file(&path);
    }

    /// The reason for the staging path: under a permissive umask, a
    /// chmod-after-bind would leave the socket accepting at 0777 until the
    /// chmod landed, on a port that dispatches code.
    #[test]
    fn the_socket_is_never_visible_at_a_wider_mode() {
        let path = socket_path("umask");
        let previous = unsafe { libc_umask(0) };
        let _listener = bind_unix(&path);
        unsafe { libc_umask(previous) };

        let _listener = _listener.expect("bind under umask 0");
        assert_eq!(mode_of(&path), SOCKET_MODE);
        let _ = std::fs::remove_file(&path);
    }

    /// No staging file survives a successful bind.
    #[test]
    fn the_staging_path_is_consumed_by_the_rename() {
        let path = socket_path("staging");
        let _listener = bind_unix(&path).expect("bind");
        assert!(
            !staging_path(&path).exists(),
            "the staging socket must be renamed, not copied"
        );
        let _ = std::fs::remove_file(&path);
    }

    extern "C" {
        #[link_name = "umask"]
        fn libc_umask(mask: u32) -> u32;
    }
}
