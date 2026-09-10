//! Durable steps for a running task.
//!
//! A step runs once per durable run and is memoized after that, so a retry
//! resumes rather than repeats. Every write is fenced on `(owner, attempt,
//! epoch)` — the triple the scheduler minted for this dispatch — which is why
//! this shell brings its own pool: core's reference pool discards all three
//! before a handler could see them.
//!
//! The session is held in a thread-local rather than passed as a parameter,
//! because a macro-expanded body is the caller's own function and cannot grow
//! an argument it did not declare. Every handler runs on its own blocking
//! thread, so the thread-local is per-dispatch; the pool clears it afterwards,
//! since a blocking thread is reused.

use std::cell::RefCell;

use flexiq_core::{
    Job, QueueError, StepLimits, StepSession, StepSleep, StorageBackend, StorageSteps, TaskError,
};

use crate::{Abort, Outcome};

/// An open step session, fenced on this dispatch.
pub(crate) type Session = StepSession<StorageSteps<StorageBackend>>;

thread_local! {
    /// The session for the dispatch this thread is running, if any.
    static CURRENT: RefCell<Option<Session>> = const { RefCell::new(None) };
}

/// Open a session for `job`, fenced on the triple this dispatch won.
///
/// `with_epoch` is the term no other shell sets: Python, Node and Java each
/// reach steps through an FFI class that has to be one concrete non-generic
/// type, and each fences on `(owner, attempt)` alone. Nothing here needs that
/// erasure, so the third term is wired.
pub(crate) fn open(
    storage: &StorageBackend,
    job: &Job,
    owner: &str,
    epoch: Option<i64>,
) -> Result<Session, QueueError> {
    let store = StorageSteps::new(storage.clone(), owner, job.retry_count).with_epoch(epoch);
    StepSession::open(store, job, StepLimits::default())
}

/// Install `session` for the duration of one dispatch on this thread.
pub(crate) fn install(session: Session) {
    CURRENT.with(|cell| *cell.borrow_mut() = Some(session));
}

/// Take the session back, so a reused blocking thread starts clean.
pub(crate) fn take() -> Option<Session> {
    CURRENT.with(|cell| cell.borrow_mut().take())
}

/// Run `f` against this dispatch's session.
///
/// The session is taken out for the duration rather than borrowed, so a step
/// opened *inside* another step's body finds nothing and is refused by name —
/// where a `RefCell` borrow would panic and take the pool thread with it.
fn with_session<R>(what: &str, f: impl FnOnce(&mut Session) -> R) -> Outcome<R> {
    let Some(mut session) = take() else {
        return Err(Abort::Fail(TaskError::fatal(format!(
            "`{what}` was called outside a running task, or inside another step's body: \
             a step session belongs to one dispatch and cannot nest"
        ))));
    };
    let out = f(&mut session);
    install(session);
    Ok(out)
}

impl crate::StepHandle {
    /// Run `body` once per durable run, memoizing what it returns.
    ///
    /// On a retry the body does not run: its recorded value is decoded and
    /// returned instead. That is the point — a step that charged a card must
    /// not charge it twice because something after it failed.
    pub fn run<T, F>(&mut self, name: &str, body: F) -> Outcome<T>
    where
        T: serde::Serialize + serde::de::DeserializeOwned,
        F: FnOnce() -> Outcome<T>,
    {
        self.run_step(name, None, body)
    }

    /// [`run`](Self::run), with an explicit identity.
    ///
    /// Two steps of the same name in one run are told apart by their order;
    /// a key tells them apart by what they are about, which survives a body
    /// that runs them in a different order.
    pub fn run_keyed<T, F>(&mut self, name: &str, key: &str, body: F) -> Outcome<T>
    where
        T: serde::Serialize + serde::de::DeserializeOwned,
        F: FnOnce() -> Outcome<T>,
    {
        self.run_step(name, Some(key), body)
    }

    /// The shared body of [`run`](Self::run) and [`run_keyed`](Self::run_keyed).
    fn run_step<T, F>(&mut self, name: &str, key: Option<&str>, body: F) -> Outcome<T>
    where
        T: serde::Serialize + serde::de::DeserializeOwned,
        F: FnOnce() -> Outcome<T>,
    {
        // The body's own failure has to travel out intact. `StepSession::run`
        // wants a `QueueError`, which cannot carry an `Abort`, so the abort is
        // set aside here and re-raised below — and the sentinel error it
        // returns in its place still refuses the commit, which is what matters.
        let mut aborted: Option<Abort> = None;

        let committed = with_session(name, |session| {
            session.run(name, key, |_idempotency_key| match body() {
                Ok(value) => crate::encode::to_wire(&value)
                    .map(|wire| flexiq_core::wire::encode_result(&wire))
                    .map_err(|e| QueueError::Other(e.to_string())),
                Err(abort) => {
                    aborted = Some(abort);
                    Err(QueueError::Other(BODY_FAILED.to_string()))
                }
            })
        })?;

        if let Some(abort) = aborted {
            return Err(abort);
        }

        let bytes = committed
            .map_err(|e| Abort::Fail(TaskError::retryable(format!("step `{name}` failed: {e}"))))?;

        crate::decode::decode_result(&bytes).map_err(|e| {
            Abort::Fail(TaskError::fatal(format!(
                "step `{name}` recorded a value this build cannot read: {e}"
            )))
        })
    }

    /// End this attempt and resume after `ms` milliseconds.
    ///
    /// Not a `thread::sleep`: the job is rescheduled and its claim released, so
    /// the worker's slot is free while it waits. The attempt ends here — code
    /// after this call runs on the *next* attempt, from the top, with every
    /// committed step memoized.
    pub fn sleep_ms(&mut self, name: &str, ms: i64) -> Outcome<()> {
        self.sleep(name, |session| session.sleep_for(Some(name), None, ms))
    }

    /// [`sleep_ms`](Self::sleep_ms), to an absolute Unix-millisecond deadline.
    pub fn sleep_until(&mut self, name: &str, wake_at: i64) -> Outcome<()> {
        self.sleep(name, |session| {
            session.sleep_until(Some(name), None, wake_at)
        })
    }

    /// The shared body of the two sleeps.
    fn sleep(
        &mut self,
        name: &str,
        f: impl FnOnce(&mut Session) -> Result<StepSleep, QueueError>,
    ) -> Outcome<()> {
        let outcome = with_session(name, f)?;
        match outcome {
            Err(e) => Err(Abort::Fail(TaskError::retryable(format!(
                "step `{name}` could not sleep: {e}"
            )))),
            // The deadline has already passed, which is what a replay sees once
            // the sleep is over. Treating this as an abort would make a woken
            // job sleep forever.
            Ok(StepSleep::Elapsed { .. }) => Ok(()),
            Ok(sleep @ StepSleep::Sleeping { .. }) => Err(Abort::Sleep(sleep)),
        }
    }
}

/// The sentinel a failed step body returns so the commit is refused.
///
/// Never surfaces: the real [`Abort`] is re-raised in its place.
const BODY_FAILED: &str = "the step body failed";
