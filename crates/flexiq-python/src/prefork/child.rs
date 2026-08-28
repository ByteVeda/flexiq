//! Child process handle — spawn, write jobs, read results.
//!
//! A child is split into three halves after spawning, handed back together as
//! a [`SpawnedChild`]:
//! - `ChildWriter`: sends frames to the child's stdin (owned by dispatch thread)
//! - `ChildReader`: reads frames from the child's stdout (owned by reader thread)
//! - `ChildProcess`: holds the process handle for lifecycle management
//!
//! The frames themselves are the shared worker protocol, so a pipe child and a
//! socket-attached executor speak the same wire format.

use std::io::BufReader;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

use flexiq_core::worker::protocol::{
    ExecutorMessage, FrameReader, FrameWriter, SchedulerMessage, CAP_STEPS, PROTOCOL_VERSION,
};

/// Identity this pool announces in `hello_ack`. Informational — it only ever
/// reaches the child's logs.
const SCHEDULER_ID: &str = "prefork";

use crate::py_step::CLAIM_OWNER_ENV;

/// Writer half — sends frames to the child process via stdin.
pub type ChildWriter = FrameWriter<ChildStdin>;

/// Reader half — reads frames from the child process via stdout.
pub type ChildReader = FrameReader<BufReader<ChildStdout>>;

/// Process handle for lifecycle management.
pub struct ChildProcess {
    process: Child,
}

impl ChildProcess {
    /// Check if the child process is still alive.
    pub fn is_alive(&mut self) -> bool {
        matches!(self.process.try_wait(), Ok(None))
    }

    /// `SIGKILL` the child and reap the zombie.
    ///
    /// Both calls are best-effort: the child may have already exited (e.g.
    /// crashed) between the watchdog's deadline scan and this call, in which
    /// case `kill` returns `EPERM`/`ESRCH` and `wait` returns immediately.
    /// After this returns, `is_alive()` is guaranteed to be `false`, so the
    /// dispatcher's respawn path will pick the slot up on the next job.
    pub fn kill_and_reap(&mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();
    }

    /// Wait for the child to exit, with a timeout. Kills if it doesn't exit in time.
    pub fn wait_or_kill(&mut self, timeout: std::time::Duration) {
        let start = std::time::Instant::now();
        loop {
            match self.process.try_wait() {
                Ok(Some(_)) => return,
                Ok(None) if start.elapsed() >= timeout => {
                    let _ = self.process.kill();
                    let _ = self.process.wait();
                    return;
                }
                Ok(None) => std::thread::sleep(std::time::Duration::from_millis(100)),
                Err(_) => return,
            }
        }
    }
}

/// A child that has completed its handshake, split into the halves the pool's
/// threads own.
pub struct SpawnedChild {
    /// Sends frames to the child's stdin. Owned by the dispatch thread, and
    /// borrowed by the cancel router and the step relay.
    pub writer: ChildWriter,
    /// Reads frames from the child's stdout. Owned by the reader thread.
    pub reader: ChildReader,
    /// Process handle, for the watchdog and the shutdown path.
    pub process: ChildProcess,
    /// Whether the child claimed [`CAP_STEPS`] in its `hello`.
    ///
    /// A pool pays a snapshot read per dispatch only for a child that says it
    /// will use one — the same negotiation the socket hop makes, one level
    /// down.
    pub steps: bool,
}

/// Spawn a child worker process and complete the `hello`/`hello_ack` handshake.
///
/// `steps` is what this pool can offer: `true` only when it relays durable
/// steps to a scheduler that advertised a step store. The ack is sent even on a
/// version mismatch so both sides can log both versions — `FLEXIQ_PYTHON` lets
/// the child run from a different interpreter, so a mismatched flexiq install is
/// reachable in practice.
pub fn spawn_child(
    python: &str,
    app_path: &str,
    claim_owner: Option<&str>,
    steps: bool,
) -> Result<SpawnedChild, String> {
    let mut command = Command::new(python);
    command
        .args(["-m", "flexiq.prefork", app_path])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());

    // The claim owner travels on the spawn, never on a frame. This same frame
    // format also crosses a socket to an attached executor, and an owner an
    // executor fills in is an owner it can forge — after a reclaim, a stale
    // executor naming the *current* owner would write straight into the live
    // attempt's step sequence. A private spawn cannot be spoofed that way.
    // Removed rather than left alone when there is none, so an inherited value
    // can never stand in for a claim this process does not hold.
    match claim_owner {
        Some(owner) => command.env(CLAIM_OWNER_ENV, owner),
        None => command.env_remove(CLAIM_OWNER_ENV),
    };

    let mut process = command
        .spawn()
        .map_err(|e| format!("failed to spawn child: {e}"))?;

    let stdin = process.stdin.take().expect("stdin should be piped");
    let stdout = process.stdout.take().expect("stdout should be piped");

    let mut reader = ChildReader::new(BufReader::new(stdout));
    let mut writer = ChildWriter::new(stdin);
    // `Child::drop` does not terminate the process, so every failure below has
    // to reap it or the child survives as an orphan the restart path never
    // notices (it is only ever reached via `is_alive()` on a live handle).
    let mut child = ChildProcess { process };

    match handshake(&mut reader, &mut writer, steps) {
        Ok(claimed_steps) => Ok(SpawnedChild {
            writer,
            reader,
            process: child,
            steps: claimed_steps,
        }),
        Err(e) => {
            child.kill_and_reap();
            Err(e)
        }
    }
}

/// Read the child's `hello`, acknowledge it, and check the protocol version.
///
/// Returns whether the child claimed [`CAP_STEPS`].
fn handshake(
    reader: &mut ChildReader,
    writer: &mut ChildWriter,
    steps: bool,
) -> Result<bool, String> {
    let hello = reader
        .read::<ExecutorMessage>()
        .map_err(|e| format!("child handshake failed: {e}"))?
        .0;
    let ExecutorMessage::Hello {
        sdk,
        version,
        protocol_version,
        capabilities,
        ..
    } = hello
    else {
        return Err("child sent a non-hello frame before the handshake completed".into());
    };

    writer
        .write_header(&SchedulerMessage::HelloAck {
            scheduler_id: SCHEDULER_ID.to_string(),
            protocol_version: PROTOCOL_VERSION,
            // A child of an in-process worker holds real storage and writes its
            // own progress, logs and steps, so it is told nothing: there is
            // nothing for this pool to do on its behalf. Under an executor the
            // pool relays, and `steps` is what the scheduler said it can apply.
            capabilities: if steps {
                vec![CAP_STEPS.to_string()]
            } else {
                Vec::new()
            },
        })
        .map_err(|e| format!("failed to acknowledge child handshake: {e}"))?;

    if protocol_version != PROTOCOL_VERSION {
        return Err(format!(
            "child speaks worker protocol {protocol_version}, we speak {PROTOCOL_VERSION} \
             (child is {sdk} {version}; check FLEXIQ_PYTHON points at the same install)"
        ));
    }

    Ok(capabilities.iter().any(|cap| cap == CAP_STEPS))
}
