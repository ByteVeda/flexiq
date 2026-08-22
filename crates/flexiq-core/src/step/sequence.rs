//! One attempt's walk through a job's step sequence.
//!
//! Pure: it holds the snapshot read at attempt start and decides, for each
//! step the code asks for, whether the value is already known, whether new
//! ground has been reached, or whether the code has changed underneath the
//! recorded sequence. Nothing here touches storage.

use std::collections::{HashMap, HashSet};

use super::key::{abbreviate, StepKey};
use crate::error::{QueueError, Result, StepDivergence};
use crate::storage::records::{JobStep, StepKind};

/// What the caller must do with the step it just asked for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StepDecision {
    /// This step already ran in an earlier attempt. Return the value; the
    /// closure **must not** run — re-running it is the double charge this
    /// whole mechanism exists to prevent.
    Memoized {
        /// Identity of the step whose value this is.
        step_key: String,
        /// The stored bytes, exactly as they were committed.
        result: Vec<u8>,
    },
    /// New ground: run the closure, then commit its result at this position.
    Run(PendingStep),
}

/// A step that has been issued but not yet committed.
///
/// Constructed only by [`StepSequence::begin_run`], so a caller cannot invent a
/// position, and consumed by the commit — which is what keeps the attempt's
/// idea of the sequence and storage's from drifting apart.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingStep {
    seq: i32,
    step_key: String,
    kind: StepKind,
}

impl PendingStep {
    /// Position this step takes in the job's sequence.
    pub fn seq(&self) -> i32 {
        self.seq
    }

    /// Identity of the step.
    pub fn step_key(&self) -> &str {
        &self.step_key
    }

    /// Whether this commits a value or a deadline.
    pub fn kind(&self) -> StepKind {
        self.kind
    }
}

/// The recorded sequence, and where this attempt has got to in it.
///
/// The "fingerprint" of a job's steps is this ordered list of keys — there is
/// no digest column and no extra read. Each step is checked against the
/// snapshot position by position as it is asked for (§3.2), which is what makes
/// a divergence surface *before* the closure runs.
#[derive(Debug)]
pub struct StepSequence {
    job_id: String,
    /// The snapshot, ordered by `seq` and gapless — checked at construction,
    /// because a hole would silently shift every memo after it.
    recorded: Vec<JobStep>,
    /// Keys this attempt has asked for, in order. Kept for the divergence
    /// message, which is only useful if it shows what the running code did.
    issued: Vec<String>,
    /// The same keys as a set: a duplicate explicit key is refused, and a
    /// linear scan of `issued` would make a thousand-step job quadratic.
    issued_keys: HashSet<String>,
    /// Per-name occurrence counters. Explicit keys never touch these — see
    /// [`StepKey`].
    occurrences: HashMap<String, u32>,
    /// How many steps this attempt has resolved: memo hits plus commits.
    position: usize,
    /// The step handed out and not yet committed. At most one at a time.
    pending: Option<PendingStep>,
    /// Rows committed for this job, and the bytes they hold. Seeded from the
    /// snapshot so the caps can be checked without a second read.
    stored_count: usize,
    stored_bytes: usize,
}

impl StepSequence {
    /// Take the snapshot read once at attempt start (§5.1).
    pub fn new(job_id: impl Into<String>, recorded: Vec<JobStep>) -> Result<Self> {
        let job_id = job_id.into();
        for (index, step) in recorded.iter().enumerate() {
            if step.seq != index as i32 {
                // Gapless `seq` is what lets the count be the next free
                // position. A hole means the memo at every later position
                // answers a different step's question.
                return Err(QueueError::Config(format!(
                    "job {job_id} has a hole in its step sequence: position {index} holds seq {}",
                    step.seq
                )));
            }
        }
        let stored_bytes = recorded.iter().map(result_len).sum();
        Ok(Self {
            job_id,
            stored_count: recorded.len(),
            stored_bytes,
            recorded,
            issued: Vec::new(),
            issued_keys: HashSet::new(),
            occurrences: HashMap::new(),
            position: 0,
            pending: None,
        })
    }

    /// Decide what `step.run(name)` — or `step.run(name, key=…)` — must do.
    ///
    /// Advances the position on a memo hit, because nothing further is needed
    /// there. On [`StepDecision::Run`] the position moves only once the commit
    /// lands, so a closure that raises leaves the sequence where it was.
    pub fn begin_run(&mut self, name: &str, key: Option<&str>) -> Result<StepDecision> {
        let step_key = match key {
            Some(key) => StepKey::explicit(name, key)?,
            None => {
                let occurrence = self.occurrences.get(name).copied().unwrap_or(0);
                StepKey::derive(name, occurrence)?
            }
        };
        let decision = self.resolve(&step_key, StepKind::Run)?;
        // Spent only once the key is known to be usable: a refused step must
        // not shift the key of the next one.
        if key.is_none() {
            *self.occurrences.entry(name.to_string()).or_insert(0) += 1;
        }
        Ok(decision)
    }

    /// Acknowledge that `pending` was committed, and move on.
    pub fn commit(&mut self, pending: &PendingStep, encoded_len: usize) -> Result<()> {
        match self.pending.take() {
            Some(outstanding) if &outstanding == pending => {}
            outstanding => {
                self.pending = outstanding;
                return Err(QueueError::Config(format!(
                    "step '{}' of job {} was committed out of turn",
                    abbreviate(pending.step_key()),
                    self.job_id
                )));
            }
        }
        self.position += 1;
        self.stored_count += 1;
        self.stored_bytes += encoded_len;
        Ok(())
    }

    /// Steps recorded past where this attempt stopped (§3.4).
    ///
    /// A **warning**, never a failure: their side effects already happened and
    /// the shortened code has no use for their values. The rows die with the
    /// job. Only meaningful once the task body has returned.
    pub fn orphaned_tail(&self) -> Vec<&str> {
        self.recorded
            .iter()
            .skip(self.position)
            .map(|step| step.step_key.as_str())
            .collect()
    }

    /// The recorded sequence, for a log line that has to show both.
    pub fn recorded_keys(&self) -> Vec<&str> {
        self.recorded
            .iter()
            .map(|step| step.step_key.as_str())
            .collect()
    }

    /// The keys this attempt has asked for, in order.
    pub fn issued_keys(&self) -> Vec<&str> {
        self.issued.iter().map(String::as_str).collect()
    }

    /// How many steps this job has committed, snapshot plus this attempt.
    pub fn committed_steps(&self) -> usize {
        self.stored_count
    }

    /// How many encoded bytes those steps hold.
    pub fn committed_bytes(&self) -> usize {
        self.stored_bytes
    }

    /// Match one step against the snapshot position, or claim new ground.
    fn resolve(&mut self, step_key: &str, kind: StepKind) -> Result<StepDecision> {
        if let Some(outstanding) = &self.pending {
            return Err(QueueError::Config(format!(
                "step '{}' of job {} started while step '{}' is still uncommitted",
                abbreviate(step_key),
                self.job_id,
                abbreviate(outstanding.step_key())
            )));
        }
        if !self.issued_keys.insert(step_key.to_string()) {
            // Two steps sharing a key would memo over each other, and the
            // position check cannot see it — both sequences look identical.
            return Err(QueueError::Config(format!(
                "step key '{}' was used twice in one attempt of job {}; \
                 give each keyed step a key of its own",
                abbreviate(step_key),
                self.job_id
            )));
        }
        self.issued.push(step_key.to_string());

        let position = self.position;
        match self.recorded.get(position) {
            Some(recorded) if recorded.step_key == step_key && recorded.kind == kind => {
                let result = recorded.result.clone().unwrap_or_default();
                self.position += 1;
                Ok(StepDecision::Memoized {
                    step_key: step_key.to_string(),
                    result,
                })
            }
            Some(recorded) => Err(self.divergence(position, recorded, step_key, kind)),
            // Past the snapshot: this attempt got further than the last one.
            None => {
                let seq = i32::try_from(position).map_err(|_| {
                    QueueError::Config(format!(
                        "job {} asked for more steps than a sequence can hold",
                        self.job_id
                    ))
                })?;
                let pending = PendingStep {
                    seq,
                    step_key: step_key.to_string(),
                    kind,
                };
                self.pending = Some(pending.clone());
                Ok(StepDecision::Run(pending))
            }
        }
    }

    fn divergence(
        &self,
        position: usize,
        recorded: &JobStep,
        step_key: &str,
        kind: StepKind,
    ) -> QueueError {
        // Same key, different kind: say so, or the message reads as if nothing
        // changed. `run` replaying onto a recorded `sleep` is exactly this.
        let (expected, found) = if recorded.step_key == step_key {
            (
                format!(
                    "'{}' as a {} step",
                    recorded.step_key,
                    recorded.kind.as_str()
                ),
                format!("'{step_key}' as a {} step", kind.as_str()),
            )
        } else {
            (
                format!("'{}'", abbreviate(&recorded.step_key)),
                format!("'{}'", abbreviate(step_key)),
            )
        };
        QueueError::StepSequenceDiverged(Box::new(StepDivergence {
            job_id: self.job_id.clone(),
            position,
            recorded: render_sequence(&self.recorded_keys(), position),
            running: render_sequence(&self.issued_keys(), position),
            expected,
            found,
        }))
    }
}

fn result_len(step: &JobStep) -> usize {
    step.result.as_ref().map_or(0, Vec::len)
}

/// Render a sequence around the position that failed.
///
/// Bounded on purpose: a job may commit a thousand steps, and an error nobody
/// can read is not louder for being longer. The window keeps the neighbours
/// that make the change recognizable.
fn render_sequence(keys: &[&str], position: usize) -> String {
    const CONTEXT: usize = 5;

    if keys.is_empty() {
        return "(none)".to_string();
    }
    let start = position.saturating_sub(CONTEXT);
    let end = (position + CONTEXT + 1).min(keys.len());
    let mut rendered = String::new();
    if start > 0 {
        rendered.push_str(&format!("…({start} earlier), "));
    }
    rendered.push_str(
        &keys[start..end]
            .iter()
            .map(|key| abbreviate(key))
            .collect::<Vec<_>>()
            .join(", "),
    );
    if end < keys.len() {
        rendered.push_str(&format!(", …({} more)", keys.len() - end));
    }
    rendered
}

#[cfg(test)]
mod tests {
    use super::*;

    fn recorded(seq: i32, step_key: &str, result: &[u8]) -> JobStep {
        JobStep {
            job_id: "job-1".to_string(),
            seq,
            step_key: step_key.to_string(),
            kind: StepKind::Run,
            result: Some(result.to_vec()),
            wake_at: None,
            created_at: 0,
        }
    }

    fn sequence(rows: Vec<JobStep>) -> StepSequence {
        StepSequence::new("job-1", rows).unwrap()
    }

    fn commit(sequence: &mut StepSequence, decision: StepDecision, result: &[u8]) {
        match decision {
            StepDecision::Run(pending) => sequence.commit(&pending, result.len()).unwrap(),
            other => panic!("expected new ground, got {other:?}"),
        }
    }

    #[test]
    fn an_empty_snapshot_runs_every_step() {
        let mut sequence = sequence(vec![]);
        let decision = sequence.begin_run("charge", None).unwrap();
        match &decision {
            StepDecision::Run(pending) => {
                assert_eq!(pending.seq(), 0);
                assert_eq!(pending.step_key(), "charge#0");
                assert_eq!(pending.kind(), StepKind::Run);
            }
            other => panic!("{other:?}"),
        }
        commit(&mut sequence, decision, b"ok");
        assert_eq!(sequence.committed_steps(), 1);
        assert_eq!(sequence.committed_bytes(), 2);
    }

    #[test]
    fn a_recorded_step_comes_back_memoized() {
        let mut sequence = sequence(vec![recorded(0, "charge#0", b"receipt-1")]);
        assert_eq!(
            sequence.begin_run("charge", None).unwrap(),
            StepDecision::Memoized {
                step_key: "charge#0".to_string(),
                result: b"receipt-1".to_vec(),
            }
        );
        // The attempt then gets further than the last one did.
        match sequence.begin_run("notify", None).unwrap() {
            StepDecision::Run(pending) => assert_eq!(pending.seq(), 1),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn a_loop_numbers_each_occurrence() {
        let mut sequence = sequence(vec![]);
        for expected in ["fetch#0", "fetch#1", "fetch#2"] {
            let decision = sequence.begin_run("fetch", None).unwrap();
            match &decision {
                StepDecision::Run(pending) => assert_eq!(pending.step_key(), expected),
                other => panic!("{other:?}"),
            }
            commit(&mut sequence, decision, b"");
        }
    }

    #[test]
    fn a_keyed_step_does_not_spend_an_occurrence() {
        // Adding a keyed call must not shift the key of a later unkeyed one —
        // that would be a divergence caused by an edit that changed nothing
        // about the unkeyed steps.
        let mut sequence = sequence(vec![]);
        let expected = ["fetch:a", "fetch#0", "fetch:b", "fetch#1"];
        let calls: [Option<&str>; 4] = [Some("a"), None, Some("b"), None];
        for (key, expected) in calls.into_iter().zip(expected) {
            let decision = sequence.begin_run("fetch", key).unwrap();
            match &decision {
                StepDecision::Run(pending) => assert_eq!(pending.step_key(), expected),
                other => panic!("{other:?}"),
            }
            commit(&mut sequence, decision, b"");
        }
    }

    #[test]
    fn one_key_may_not_name_two_steps() {
        let mut sequence = sequence(vec![]);
        let decision = sequence.begin_run("fetch", Some("a")).unwrap();
        commit(&mut sequence, decision, b"");
        let err = sequence.begin_run("fetch", Some("a")).unwrap_err();
        assert!(
            matches!(&err, QueueError::Config(message) if message.contains("twice")),
            "{err}"
        );
    }

    #[test]
    fn a_reordered_sequence_names_both_sequences() {
        let mut sequence = sequence(vec![
            recorded(0, "charge#0", b""),
            recorded(1, "notify#0", b""),
            recorded(2, "receipt#0", b""),
        ]);
        sequence.begin_run("charge", None).unwrap();
        sequence.begin_run("notify", None).unwrap();
        let err = sequence.begin_run("audit", None).unwrap_err();

        let QueueError::StepSequenceDiverged(divergence) = &err else {
            panic!("{err}");
        };
        assert_eq!(
            (divergence.job_id.as_str(), divergence.position),
            ("job-1", 2)
        );
        assert_eq!(
            (divergence.expected.as_str(), divergence.found.as_str()),
            ("'receipt#0'", "'audit#0'")
        );

        let message = err.to_string();
        assert!(
            message.contains("charge#0, notify#0, receipt#0"),
            "{message}"
        );
        assert!(message.contains("charge#0, notify#0, audit#0"), "{message}");
        assert!(message.contains("Drain or dead-letter"), "{message}");
        assert_eq!(
            crate::step::classify_step_failure(&err),
            crate::step::StepFailure::Permanent,
            "a divergence must never be retried: the code will not change between attempts"
        );
    }

    #[test]
    fn replaying_a_run_onto_a_sleep_is_a_divergence() {
        let mut sequence = sequence(vec![JobStep {
            kind: StepKind::Sleep,
            result: None,
            wake_at: Some(1),
            ..recorded(0, "nap#0", b"")
        }]);
        let err = sequence.begin_run("nap", None).unwrap_err();
        let message = err.to_string();
        assert!(message.contains("'nap#0' as a sleep step"), "{message}");
        assert!(message.contains("'nap#0' as a run step"), "{message}");
    }

    #[test]
    fn a_long_sequence_is_elided_around_the_divergence() {
        let rows: Vec<JobStep> = (0..40)
            .map(|seq| recorded(seq, &format!("step{seq}#0"), b""))
            .collect();
        let mut sequence = sequence(rows);
        for seq in 0..20 {
            sequence.begin_run(&format!("step{seq}"), None).unwrap();
        }
        let err = sequence.begin_run("wrong", None).unwrap_err().to_string();
        assert!(err.contains("earlier)"), "{err}");
        assert!(err.contains("more)"), "{err}");
        assert!(
            !err.contains("step39#0"),
            "the far tail is not worth printing: {err}"
        );
    }

    #[test]
    fn a_shortened_sequence_is_a_warning_not_a_failure() {
        let mut sequence = sequence(vec![
            recorded(0, "charge#0", b""),
            recorded(1, "receipt#0", b"kept"),
        ]);
        sequence.begin_run("charge", None).unwrap();
        assert_eq!(sequence.orphaned_tail(), vec!["receipt#0"]);
        assert_eq!(sequence.recorded_keys(), vec!["charge#0", "receipt#0"]);
        assert_eq!(sequence.issued_keys(), vec!["charge#0"]);
    }

    #[test]
    fn a_step_may_not_start_while_another_is_uncommitted() {
        let mut sequence = sequence(vec![]);
        sequence.begin_run("charge", None).unwrap();
        let err = sequence.begin_run("notify", None).unwrap_err();
        assert!(
            matches!(&err, QueueError::Config(message) if message.contains("uncommitted")),
            "{err}"
        );
    }

    #[test]
    fn a_commit_must_name_the_step_that_was_issued() {
        let mut sequence = sequence(vec![]);
        let StepDecision::Run(issued) = sequence.begin_run("charge", None).unwrap() else {
            panic!("expected new ground");
        };
        let forged = PendingStep {
            seq: 0,
            step_key: "notify#0".to_string(),
            kind: StepKind::Run,
        };
        assert!(sequence.commit(&forged, 0).is_err());
        // The real one still works: a refused commit must not consume the latch.
        assert!(sequence.commit(&issued, 0).is_ok());
    }

    #[test]
    fn a_hole_in_the_snapshot_is_refused() {
        let err = StepSequence::new("job-1", vec![recorded(1, "charge#0", b"")]).unwrap_err();
        assert!(
            matches!(&err, QueueError::Config(message) if message.contains("hole")),
            "{err}"
        );
    }
}
