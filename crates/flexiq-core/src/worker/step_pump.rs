//! Applying an attached executor's step commits, off the reader thread.
//!
//! A step commit is a database write, and the thread it would otherwise happen
//! on is the one carrying every other job's results back from that executor.
//! Applying inline would put a finished job's outcome behind an unrelated job's
//! step, so the writes are handed to a small pool instead and the reader returns
//! to reading immediately.
//!
//! The pool is deliberately small and its queue bounded. A step commit is one
//! indexed write, and an unbounded queue would only convert a slow database into
//! unbounded memory — so a full queue is *answered*, retryably, rather than
//! waited on. The executor's own retry is then the backpressure.

use std::sync::Arc;
use std::thread::{self, JoinHandle};

use crossbeam_channel::{Receiver, Sender, TrySendError};

use super::protocol::SchedulerMessage;
use super::side_channel::SideChannel;
use crate::error::QueueError;
use crate::step::classify_step_failure;
use crate::storage::records::{NewJobStep, SleepOutcome, StepCommit, StepKind};

/// One step commit an executor asked the scheduler to apply on its behalf.
///
/// `owner`, `attempt` and `namespace` are the scheduler's, resolved from the
/// dispatch it recorded — never off the frame. That is the whole point of
/// routing the write through here: the executor says *which* step, and the
/// scheduler says who is entitled to write it.
pub(super) struct StepWrite {
    /// Job the step runs inside.
    pub job_id: String,
    /// Position in the job's step sequence.
    pub seq: i32,
    /// Identity of the step within the job.
    pub step_key: String,
    /// Whether this commits a value or a deadline.
    pub kind: StepKind,
    /// Candidate deadline, for a [`StepKind::Sleep`].
    pub wake_at: Option<i64>,
    /// Encoded result, for a [`StepKind::Run`]. Empty for a sleep.
    pub result: Vec<u8>,
    /// Worker id the execution claim was won under.
    pub owner: String,
    /// `retry_count` the job carried when it was dispatched.
    pub attempt: i32,
    /// Namespace the job was dispatched in.
    pub namespace: Option<String>,
    /// Where the answer goes — a closure so this module never has to know what
    /// an executor connection is.
    pub reply: Box<dyn FnOnce(SchedulerMessage) + Send>,
}

/// A small pool of threads applying step commits.
pub(super) struct StepPump {
    /// `None` only during [`Drop`]: dropping the last sender is what ends the
    /// workers' loops.
    tx: Option<Sender<StepWrite>>,
    handles: Vec<JoinHandle<()>>,
}

impl StepPump {
    /// Start `workers` threads draining a queue of `capacity` writes.
    pub(super) fn start(sink: Arc<dyn SideChannel>, workers: usize, capacity: usize) -> Self {
        let (tx, rx) = crossbeam_channel::bounded(capacity.max(1));
        let handles = (0..workers.max(1))
            .map(|n| spawn_worker(n, Arc::clone(&sink), rx.clone()))
            .collect();
        Self {
            tx: Some(tx),
            handles,
        }
    }

    /// Queue a write, answering immediately if there is no room for it.
    ///
    /// Never blocks: the caller is the connection's reader thread, and parking
    /// it would stall every other job on that connection behind one slow write.
    pub(super) fn submit(&self, write: StepWrite) {
        let Some(tx) = self.tx.as_ref() else {
            return;
        };
        if let Err(TrySendError::Full(write) | TrySendError::Disconnected(write)) =
            tx.try_send(write)
        {
            let StepWrite {
                job_id, seq, reply, ..
            } = write;
            // Retryable, and honestly so: nothing was written, so the replay
            // re-runs the step under the same idempotency key.
            reply(refusal(
                job_id,
                seq,
                QueueError::Other(
                    "the scheduler's step queue is full; the step was not committed".to_string(),
                ),
            ));
        }
    }
}

impl Drop for StepPump {
    fn drop(&mut self) {
        drop(self.tx.take());
        for handle in self.handles.drain(..) {
            if handle.join().is_err() {
                log::error!("[flexiq] a step-commit thread panicked");
            }
        }
    }
}

/// One drain thread: apply writes until the queue disconnects.
fn spawn_worker(n: usize, sink: Arc<dyn SideChannel>, rx: Receiver<StepWrite>) -> JoinHandle<()> {
    thread::Builder::new()
        .name(format!("flexiq-steps-{n}"))
        .spawn(move || {
            for write in rx {
                apply(sink.as_ref(), write);
            }
        })
        .expect("spawning a step-commit thread cannot fail with a valid name")
}

/// Apply one write and answer it. Every path answers exactly once — an
/// executor blocked on an ack that never comes waits out its whole timeout.
fn apply(sink: &dyn SideChannel, write: StepWrite) {
    let StepWrite {
        job_id,
        seq,
        step_key,
        kind,
        wake_at,
        result,
        owner,
        attempt,
        namespace,
        reply,
    } = write;

    let step = NewJobStep {
        job_id: &job_id,
        seq,
        step_key: &step_key,
        kind,
        // A sleep commits no bytes; a run always commits the ones it framed,
        // and an empty result is a result rather than a missing one.
        result: matches!(kind, StepKind::Run).then_some(result.as_slice()),
    };

    let answer = match kind {
        StepKind::Run => sink
            .record_step(&step, &owner, attempt, namespace.as_deref())
            .map(|commit| Settled {
                already: matches!(commit, StepCommit::AlreadyCommitted),
                wake_at: None,
            }),
        StepKind::Sleep => match wake_at {
            Some(candidate) => sink
                .sleep_job(&step, &owner, attempt, candidate, namespace.as_deref())
                .map(|outcome| Settled {
                    already: matches!(outcome, SleepOutcome::AlreadySleeping { .. }),
                    // The deadline storage settled on, which on a replay is not
                    // the one this commit proposed.
                    wake_at: Some(outcome.wake_at()),
                }),
            // A malformed frame, and no retry makes it well-formed.
            None => Err(QueueError::Config(format!(
                "the sleep commit for step '{step_key}' of job {job_id} carries no deadline"
            ))),
        },
    };

    reply(match answer {
        Ok(settled) => SchedulerMessage::StepAck {
            job_id,
            seq,
            ok: true,
            already: settled.already,
            wake_at: settled.wake_at,
            error: None,
            failure: None,
        },
        Err(error) => refusal(job_id, seq, error),
    });
}

/// What a successful commit settled on.
struct Settled {
    already: bool,
    wake_at: Option<i64>,
}

/// A refusal, carrying the message *and* what the executor should do about it.
///
/// Shared with `remote.rs`, which refuses a commit before it ever reaches the
/// queue — an unadvertised store, a job the sender is not running — and has to
/// classify those the same way.
///
/// The classification is made here because only this side saw the error, and
/// the split matters: retrying a permanently-bad commit burns the job's whole
/// retry budget reproducing it, and dead-lettering a transient one throws away
/// work over a blip.
pub(super) fn refusal(job_id: String, seq: i32, error: QueueError) -> SchedulerMessage {
    let failure = classify_step_failure(&error);
    SchedulerMessage::StepAck {
        job_id,
        seq,
        ok: false,
        already: false,
        wake_at: None,
        error: Some(error.to_string()),
        failure: Some(failure),
    }
}
