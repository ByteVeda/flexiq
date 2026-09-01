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
use flexiq_core::worker::TcpTransport;
#[cfg(unix)]
use flexiq_core::worker::UnixTransport;
use flexiq_core::{RemoteDispatcher, Transport};

use crate::config::listen::ListenAddress;
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
    address: ListenAddress,
    dispatcher: RemoteDispatcher,
    supervisor: Arc<SchedulerSupervisor>,
    shutdown: Shutdown,
) -> Result<ListenerHandle> {
    match address {
        ListenAddress::Tcp(addr) => {
            let listener = TcpListener::bind(addr)
                .with_context(|| format!("failed to bind the attach listener on {addr}"))?;
            listener.set_nonblocking(true)?;
            // Report what was bound, not what was asked for: port 0 resolves
            // to an ephemeral port only the listener knows.
            let bound = listener.local_addr().unwrap_or(addr);
            log::info!("[flexiq] attach listener on tcp://{bound}");
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
        ListenAddress::Unix(path) => {
            let listener = bind_unix(&path)?;
            listener.set_nonblocking(true)?;
            log::info!("[flexiq] attach listener on unix:{}", path.display());
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

/// Mode of the directory the socket is bound in before it is named. Reaching a
/// path requires search permission on every directory above it, so `0700` here
/// is what makes the socket unreachable to anyone else *while* it exists at
/// whatever mode `bind` chose.
#[cfg(unix)]
const STAGING_DIR_MODE: u32 = 0o700;

/// Bind a Unix socket that no other user can reach before its mode is narrowed
/// to [`SOCKET_MODE`].
///
/// `UnixListener::bind` binds and listens in one call and honours the umask, so
/// a chmod afterwards always leaves a window where the socket is accepting at
/// `0777 & ~umask` — world-writable under a permissive umask, on a port that
/// dispatches code. Renaming from a scratch *path* shortens that window but
/// does not close it: the scratch path sits in the same directory and its name
/// is predictable.
///
/// So the bind happens inside a `0700` directory instead. Path resolution needs
/// search permission on each component, so no other uid can reach the socket
/// there however wide its own mode is; the mode is narrowed, and only then is it
/// `rename`d into place. `rename` is atomic within a filesystem and keeps the
/// inode, so the listener carries on serving under its new name. A connect that
/// lands before the rename gets `ENOENT` and retries, which is the executor's
/// ordinary scheduler-not-up-yet path.
///
/// A stale file left by a crashed process is replaced; a path with a live
/// listener behind it is left alone, because that is a misconfiguration rather
/// than stale state.
#[cfg(unix)]
fn bind_unix(path: &Path) -> Result<UnixListener> {
    if std::os::unix::net::UnixStream::connect(path).is_ok() {
        anyhow::bail!("another process is already listening on {}", path.display());
    }

    // Beside the destination: `rename` is only atomic within one filesystem,
    // and the socket has to land exactly where the executor was told to dial.
    let staging = StagingDir::create(path)?;
    let socket = staging.socket(path);

    let listener = UnixListener::bind(&socket).with_context(|| {
        format!(
            "failed to bind the attach listener in {}",
            staging.display()
        )
    })?;
    restrict(&socket)?;

    // Replaces a stale socket in one step, so the path is never missing for an
    // executor that is already retrying.
    std::fs::rename(&socket, path).with_context(|| {
        format!(
            "failed to move the attach socket into place at {}",
            path.display()
        )
    })?;
    Ok(listener)
}

/// Narrow a socket to [`SOCKET_MODE`].
#[cfg(unix)]
fn restrict(socket: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    std::fs::set_permissions(socket, std::fs::Permissions::from_mode(SOCKET_MODE)).with_context(
        || {
            format!(
                "failed to set mode {SOCKET_MODE:o} on the attach socket at {}",
                socket.display()
            )
        },
    )
}

/// A private directory holding the socket until it is named, removed on drop so
/// no error path can leave one behind.
#[cfg(unix)]
struct StagingDir(PathBuf);

#[cfg(unix)]
impl StagingDir {
    /// Create the directory beside `destination`, at [`STAGING_DIR_MODE`].
    fn create(destination: &Path) -> Result<Self> {
        use std::os::unix::fs::DirBuilderExt;

        let path = Self::path_for(destination);
        // A crashed predecessor with the same pid is the only way this exists.
        let _ = std::fs::remove_dir_all(&path);
        std::fs::DirBuilder::new()
            .mode(STAGING_DIR_MODE)
            .create(&path)
            .with_context(|| {
                format!("failed to create the staging directory {}", path.display())
            })?;

        // `mkdir` masks its mode with the umask, so a pathological umask could
        // strip the owner's own search bit and make the directory unusable.
        // Setting it again is unmasked, and nothing is inside yet to expose.
        let staging = Self(path);
        std::fs::set_permissions(
            &staging.0,
            std::os::unix::fs::PermissionsExt::from_mode(STAGING_DIR_MODE),
        )
        .with_context(|| {
            format!(
                "failed to set mode {STAGING_DIR_MODE:o} on {}",
                staging.0.display()
            )
        })?;
        Ok(staging)
    }

    /// Where the socket is bound inside it. The final name is reused so a log
    /// line naming the staging path still reads sensibly.
    fn socket(&self, destination: &Path) -> PathBuf {
        self.0.join(
            destination
                .file_name()
                .unwrap_or_else(|| std::ffi::OsStr::new("attach.sock")),
        )
    }

    fn display(&self) -> std::path::Display<'_> {
        self.0.display()
    }

    /// `.` prefixed so a listing reads it as transient, pid-suffixed so two
    /// servers sharing a directory cannot collide.
    fn path_for(destination: &Path) -> PathBuf {
        let name = destination
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_else(|| "attach.sock".to_string());
        let staging = format!(".{name}.{}.staging", std::process::id());
        match destination.parent() {
            Some(parent) if !parent.as_os_str().is_empty() => parent.join(staging),
            _ => PathBuf::from(staging),
        }
    }
}

#[cfg(unix)]
impl Drop for StagingDir {
    fn drop(&mut self) {
        // Empty after a successful rename; still holding the socket if an error
        // path bailed before it.
        let _ = std::fs::remove_dir_all(&self.0);
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
        .name("flexiq-attach-accept".to_string())
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
        .name("flexiq-attach-handshake".to_string())
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
        let path =
            std::env::temp_dir().join(format!("flexiq-attach-{label}-{}.sock", std::process::id()));
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

    /// The reason for the staging directory: under a permissive umask, a
    /// chmod-after-bind would leave the socket accepting at 0777 until the
    /// chmod landed, on a port that dispatches code.
    #[test]
    fn the_socket_lands_narrow_under_a_permissive_umask() {
        let path = socket_path("umask");
        let previous = unsafe { libc_umask(0) };
        let bound = bind_unix(&path);
        unsafe { libc_umask(previous) };

        let _listener = bound.expect("bind under umask 0");
        assert_eq!(mode_of(&path), SOCKET_MODE);
        let _ = std::fs::remove_file(&path);
    }

    /// What actually closes the window: while the socket exists at whatever
    /// mode `bind` chose, it sits behind a directory no other uid can search.
    #[test]
    fn the_staging_directory_is_private_even_under_a_permissive_umask() {
        let path = socket_path("staging-mode");
        let previous = unsafe { libc_umask(0) };
        let staging = StagingDir::create(&path).expect("create");
        unsafe { libc_umask(previous) };

        assert_eq!(mode_of(&staging.0), STAGING_DIR_MODE);
    }

    /// No staging directory survives a successful bind.
    #[test]
    fn the_staging_directory_is_cleaned_up() {
        let path = socket_path("staging");
        let _listener = bind_unix(&path).expect("bind");
        assert!(
            !StagingDir::path_for(&path).exists(),
            "the staging directory must not outlive the bind"
        );
        let _ = std::fs::remove_file(&path);
    }

    /// And none survives a failure either — `Drop` runs on every path out.
    #[test]
    fn a_failed_bind_leaves_no_staging_directory() {
        // A destination whose parent does not exist: the staging directory
        // cannot be created there, so nothing is left anywhere.
        let path = std::env::temp_dir()
            .join(format!("flexiq-missing-{}", std::process::id()))
            .join("attach.sock");
        assert!(bind_unix(&path).is_err());
        assert!(!StagingDir::path_for(&path).exists());
    }

    extern "C" {
        #[link_name = "umask"]
        fn libc_umask(mask: u32) -> u32;
    }
}
