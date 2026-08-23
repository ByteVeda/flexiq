//! One attempt's step store: the rules of [`StepSequence`] driven against a
//! [`Storage`].
//!
//! The core never looks inside a step's bytes. Encoding is the queue's
//! serializer and codec chain, which lives in the SDK shell — the session
//! commits exactly what it is handed and returns exactly what it read, which is
//! what makes an encrypting codec work here with no extra plumbing.

use super::{PendingStep, StepDecision, StepLimits, StepSequence};
use crate::error::{QueueError, Result};
use crate::job::Job;
use crate::storage::records::{NewJobStep, StepCommit};
use crate::storage::Storage;

/// Durable inline steps for one attempt of one job.
///
/// Reads the job's committed steps **once**, at construction (§5.1), then
/// answers every `step.run` from that snapshot. A step that already ran returns
/// its stored bytes and the closure never runs; a new one is committed under the
/// `(owner, attempt)` fence the storage layer requires.
pub struct StepSession<S: Storage> {
    storage: S,
    job_id: String,
    namespace: Option<String>,
    owner: String,
    attempt: i32,
    limits: StepLimits,
    sequence: StepSequence,
}

impl<S: Storage> StepSession<S> {
    /// Load the job's committed steps and open a session over them.
    ///
    /// `owner` is the worker id the execution claim was won with — never
    /// something the running code asserts about itself. The attempt is the
    /// job's own `retry_count`, so a step written by a superseded attempt is
    /// refused by the fence rather than landing in the live attempt's sequence.
    ///
    /// A backend without a step store fails here. It must never degrade to "no
    /// steps recorded": that answer re-runs a charge.
    pub fn load(storage: S, job: &Job, owner: &str, limits: StepLimits) -> Result<Self> {
        if !storage.supports_steps() {
            return Err(QueueError::Config(format!(
                "job {} uses durable steps, which this storage backend does not implement",
                job.id
            )));
        }
        let namespace = job.namespace.clone();
        let recorded = storage.get_job_steps(&job.id, namespace.as_deref())?;
        let sequence = StepSequence::new(job.id.clone(), recorded)?;
        Ok(Self {
            storage,
            job_id: job.id.clone(),
            namespace,
            owner: owner.to_string(),
            attempt: job.retry_count,
            limits,
            sequence,
        })
    }

    /// Run one step, or return what it returned last time.
    ///
    /// `body` produces the step's result **already encoded** — post serializer,
    /// post codec — because those are the bytes that get stored and the bytes
    /// the caps are measured on. It is not called at all on a memo hit.
    pub fn run<F>(&mut self, name: &str, key: Option<&str>, body: F) -> Result<Vec<u8>>
    where
        F: FnOnce() -> Result<Vec<u8>>,
    {
        match self.begin_run(name, key)? {
            StepDecision::Memoized { result, .. } => Ok(result),
            StepDecision::Run(pending) => {
                let encoded = body()?;
                self.commit_run(&pending, &encoded)?;
                Ok(encoded)
            }
        }
    }

    /// Decide what this step must do, without running anything.
    ///
    /// The split form, for a shell whose closure cannot cross into Rust: call
    /// this, run the closure where it lives, then
    /// [`commit_run`](Self::commit_run).
    pub fn begin_run(&mut self, name: &str, key: Option<&str>) -> Result<StepDecision> {
        self.sequence.begin_run(name, key)
    }

    /// Commit the result of the step [`begin_run`](Self::begin_run) handed out.
    pub fn commit_run(&mut self, pending: &PendingStep, encoded: &[u8]) -> Result<StepCommit> {
        self.check_caps(pending, encoded)?;
        let step = NewJobStep {
            job_id: &self.job_id,
            seq: pending.seq(),
            step_key: pending.step_key(),
            kind: pending.kind(),
            result: Some(encoded),
        };
        let commit = self.storage.record_step_result(
            &step,
            &self.owner,
            self.attempt,
            &self.limits,
            self.namespace.as_deref(),
        )?;
        // Only after storage has it: a failed commit leaves the sequence where
        // it was, and the attempt ends there anyway.
        self.sequence.commit(pending, encoded.len())?;
        Ok(commit)
    }

    /// Close the attempt out, warning if its code no longer runs steps that are
    /// recorded for it (§3.4).
    ///
    /// Not a failure: those side effects already happened, and failing a job
    /// whose code legitimately shortened would be worse than a value nobody
    /// reads. The rows are dropped with the job.
    pub fn finish(&self) {
        let orphaned = self.sequence.orphaned_tail();
        if orphaned.is_empty() {
            return;
        }
        log::warn!(
            "job {} has {} recorded step(s) its code no longer runs: [{}]. \
             Recorded: [{}]; this attempt ran: [{}].",
            self.job_id,
            orphaned.len(),
            orphaned.join(", "),
            self.sequence.recorded_keys().join(", "),
            self.sequence.issued_keys().join(", "),
        );
    }

    /// The recorded sequence and this attempt's walk through it.
    pub fn sequence(&self) -> &StepSequence {
        &self.sequence
    }

    /// The caps this session enforces.
    pub fn limits(&self) -> StepLimits {
        self.limits
    }

    /// Refuse an over-cap commit before the round trip, so the error can name
    /// the step and the number that failed (§4.2).
    ///
    /// Storage checks the same three caps inside its own transaction — this one
    /// is the good message, that one is the check that holds.
    fn check_caps(&self, pending: &PendingStep, encoded: &[u8]) -> Result<()> {
        let limits = self.limits.clamped();
        let too_large = |limit: &str, actual: usize, allowed: usize| {
            Err(QueueError::StepLimitExceeded {
                step_key: pending.step_key().to_string(),
                limit: limit.to_string(),
                actual: actual as u64,
                allowed: allowed as u64,
            })
        };

        if encoded.len() > limits.max_step_bytes {
            return too_large("step bytes", encoded.len(), limits.max_step_bytes);
        }
        let steps = self.sequence.committed_steps() + 1;
        if steps > limits.max_steps {
            return too_large("step count", steps, limits.max_steps);
        }
        let bytes = self.sequence.committed_bytes() + encoded.len();
        if bytes > limits.max_total_bytes {
            return too_large("total bytes", bytes, limits.max_total_bytes);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use super::*;
    use crate::job::{now_millis, NewJob};
    use crate::storage::records::JobStep;
    use crate::storage::sqlite::SqliteStorage;

    /// A job claimed by `worker-1`, ready for a step commit.
    fn claimed_job(storage: &SqliteStorage, task: &str) -> Job {
        let new_job = NewJob {
            queue: "default".to_string(),
            task_name: task.to_string(),
            payload: vec![],
            priority: 0,
            scheduled_at: now_millis(),
            max_retries: 3,
            timeout_ms: 300_000,
            unique_key: None,
            metadata: None,
            notes: None,
            depends_on: vec![],
            expires_at: None,
            result_ttl_ms: None,
            namespace: None,
            debounce_key: None,
        };
        let job = storage.enqueue(new_job).unwrap();
        storage
            .dequeue("default", now_millis() + 1000, None)
            .unwrap();
        assert!(storage.claim_execution(&job.id, "worker-1").unwrap());
        storage.get_job(&job.id, None).unwrap().unwrap()
    }

    fn open(storage: &SqliteStorage, job: &Job) -> StepSession<SqliteStorage> {
        StepSession::load(storage.clone(), job, "worker-1", StepLimits::default()).unwrap()
    }

    #[test]
    fn a_memoized_step_never_runs_its_closure() {
        let storage = SqliteStorage::in_memory().unwrap();
        let job = claimed_job(&storage, "charge_card");

        let mut first = open(&storage, &job);
        assert_eq!(
            first
                .run("charge", None, || Ok(b"receipt-1".to_vec()))
                .unwrap(),
            b"receipt-1"
        );

        // The attempt crashed after the step; a new session replays it.
        let mut second = open(&storage, &job);
        let ran = Cell::new(false);
        let replayed = second
            .run("charge", None, || {
                ran.set(true);
                Ok(b"receipt-2".to_vec())
            })
            .unwrap();
        assert_eq!(replayed, b"receipt-1", "the memo must win over a fresh run");
        assert!(!ran.get(), "a memoized step must not run its closure");
        assert_eq!(storage.get_job_steps(&job.id, None).unwrap().len(), 1);
    }

    #[test]
    fn a_new_step_is_committed_at_the_next_position() {
        let storage = SqliteStorage::in_memory().unwrap();
        let job = claimed_job(&storage, "checkout");
        let mut session = open(&storage, &job);

        session.run("charge", None, || Ok(b"a".to_vec())).unwrap();
        session.run("receipt", None, || Ok(b"b".to_vec())).unwrap();

        let stored = storage.get_job_steps(&job.id, None).unwrap();
        let keys: Vec<&str> = stored.iter().map(|step| step.step_key.as_str()).collect();
        assert_eq!(keys, vec!["charge#0", "receipt#0"]);
        assert_eq!(stored[0].seq, 0);
        assert_eq!(stored[1].seq, 1);
    }

    #[test]
    fn an_encoded_result_is_stored_verbatim() {
        // The core never looks inside the bytes: whatever the queue's codec
        // produced is what lands in the row. Asserting on the row, not on a
        // round trip, is what proves an encrypting codec leaks nothing.
        let storage = SqliteStorage::in_memory().unwrap();
        let job = claimed_job(&storage, "encrypted");
        let ciphertext = b"\x00\x9f\x01ENCRYPTED\xff\xfe".to_vec();

        let mut session = open(&storage, &job);
        session
            .run("charge", None, || Ok(ciphertext.clone()))
            .unwrap();

        let stored: Vec<JobStep> = storage.get_job_steps(&job.id, None).unwrap();
        assert_eq!(stored[0].result.as_deref(), Some(ciphertext.as_slice()));
        // And the memo hands the same bytes back for the shell to decode.
        let mut replay = open(&storage, &job);
        assert_eq!(
            replay.run("charge", None, || Ok(vec![])).unwrap(),
            ciphertext
        );
    }

    #[test]
    fn an_over_cap_result_names_the_step() {
        let storage = SqliteStorage::in_memory().unwrap();
        let job = claimed_job(&storage, "render");
        let limits = StepLimits {
            max_step_bytes: 8,
            ..StepLimits::default()
        };
        let mut session = StepSession::load(storage.clone(), &job, "worker-1", limits).unwrap();

        let err = session
            .run("render", None, || Ok(vec![7u8; 64]))
            .unwrap_err();
        match &err {
            QueueError::StepLimitExceeded {
                step_key,
                limit,
                actual,
                allowed,
            } => assert_eq!(
                (step_key.as_str(), limit.as_str(), *actual, *allowed),
                ("render#0", "step bytes", 64, 8)
            ),
            other => panic!("{other}"),
        }
        assert!(
            storage.get_job_steps(&job.id, None).unwrap().is_empty(),
            "an over-cap step must not be committed"
        );
    }

    #[test]
    fn the_step_count_cap_bites_before_the_round_trip() {
        let storage = SqliteStorage::in_memory().unwrap();
        let job = claimed_job(&storage, "loop_task");
        let limits = StepLimits {
            max_steps: 1,
            ..StepLimits::default()
        };
        let mut session = StepSession::load(storage.clone(), &job, "worker-1", limits).unwrap();

        session.run("noop", None, || Ok(vec![])).unwrap();
        let err = session.run("noop", None, || Ok(vec![])).unwrap_err();
        assert!(
            matches!(&err, QueueError::StepLimitExceeded { limit, step_key, .. }
                if limit == "step count" && step_key == "noop#1"),
            "{err}"
        );
    }

    #[test]
    fn the_total_byte_cap_counts_what_is_already_stored() {
        let storage = SqliteStorage::in_memory().unwrap();
        let job = claimed_job(&storage, "big_task");
        let limits = StepLimits {
            max_total_bytes: 10,
            ..StepLimits::default()
        };
        let mut session = StepSession::load(storage.clone(), &job, "worker-1", limits).unwrap();

        session.run("a", None, || Ok(vec![1u8; 6])).unwrap();
        let err = session.run("b", None, || Ok(vec![1u8; 6])).unwrap_err();
        assert!(
            matches!(&err, QueueError::StepLimitExceeded { limit, actual, allowed, .. }
                if limit == "total bytes" && *actual == 12 && *allowed == 10),
            "{err}"
        );
    }

    #[test]
    fn a_diverged_sequence_fails_before_the_closure_runs() {
        let storage = SqliteStorage::in_memory().unwrap();
        let job = claimed_job(&storage, "deployed_task");
        let mut first = open(&storage, &job);
        first.run("charge", None, || Ok(vec![])).unwrap();

        // The deploy renamed the step; the next attempt asks for a different one.
        let mut second = open(&storage, &job);
        let ran = Cell::new(false);
        let err = second
            .run("audit", None, || {
                ran.set(true);
                Ok(vec![])
            })
            .unwrap_err();

        assert!(
            !ran.get(),
            "a divergence must be caught before the closure runs"
        );
        assert!(matches!(err, QueueError::StepSequenceDiverged(_)), "{err}");
        assert!(
            !super::super::classify_step_failure(&err).should_retry(),
            "a divergence reproduces itself on every attempt"
        );
    }

    #[test]
    fn a_step_write_from_a_superseded_attempt_is_refused() {
        let storage = SqliteStorage::in_memory().unwrap();
        let job = claimed_job(&storage, "reclaimed");
        let mut session = open(&storage, &job);

        // Another worker takes the job over mid-attempt.
        assert!(storage
            .reclaim_execution(&job.id, "worker-1", "worker-2")
            .unwrap());

        let err = session.run("charge", None, || Ok(vec![])).unwrap_err();
        assert!(matches!(err, QueueError::ClaimLost(_)), "{err}");
        assert_eq!(
            super::super::classify_step_failure(&err),
            super::super::StepFailure::Superseded
        );
    }
}
