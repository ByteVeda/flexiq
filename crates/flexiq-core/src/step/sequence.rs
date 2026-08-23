//! One attempt's walk through a job's step sequence.
//!
//! Pure: it holds the snapshot read at attempt start and decides, for each
//! step the code asks for, whether the value is already known, whether new
//! ground has been reached, or whether the code has changed underneath the
//! recorded sequence. Nothing here touches storage.

use std::collections::{HashMap, HashSet};

use super::key::{abbreviate, StepKey};
use crate::error::{QueueError, Result, StepDivergence};
use crate::storage::records::{JobStep, SleepOutcome, StepKind};

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

/// What the caller must do with the sleep it just asked for.
///
/// Three arms, not two, because a recorded sleep row means different things on
/// either side of its deadline — and because only new ground can be refused by
/// the step-count cap, which is the distinction storage draws too.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SleepDecision {
    /// The deadline has passed. The sleep is a memo hit and the attempt carries
    /// on — which is what makes a job with three sleeps not restart the first
    /// one on the third wake.
    Elapsed {
        /// Identity of the sleep this replays.
        step_key: String,
        /// The deadline it was first given, and slept to.
        wake_at: i64,
    },
    /// New ground: commit a sleep row here and end the attempt.
    Sleep(PendingStep),
    /// A sleep already committed here has not elapsed — the attempt that wrote
    /// it came back early, through a reclaim or an operator requeue. Re-issue
    /// it at the **recorded** position so the stored deadline stands, and end
    /// the attempt.
    Resume(PendingStep),
}

/// Name an unnamed sleep is numbered under, so `sleep("1h")` is `sleep#0`.
///
/// A name is accepted and recommended at every call site: a job whose sequence
/// reads `sleep#0, sleep#1, sleep#2` tells nobody which one diverged.
pub const DEFAULT_SLEEP_NAME: &str = "sleep";

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
/// no digest column and no extra read. Each step is matched against the
/// snapshot as it is asked for (§3.2), which is what makes a divergence surface
/// *before* the closure runs.
///
/// The two identities are matched differently, which is the whole point of
/// having both (§2.3). An unkeyed step is matched **by position**: `fetch#1`
/// means "the second `fetch` of this attempt", so it is only the same step if
/// it is asked for at the same point. An explicit key is matched **by key,
/// wherever it sits** — a key exists precisely so a loop over something
/// unordered can hand its steps back in a different order without every one of
/// them looking like a different question.
#[derive(Debug)]
pub struct StepSequence {
    job_id: String,
    /// The snapshot, ordered by `seq` and gapless — checked at construction,
    /// because a hole would silently shift every memo after it.
    recorded: Vec<JobStep>,
    /// Which recorded steps this attempt has claimed, parallel to `recorded`.
    /// A keyed hit can claim one out of order, so the positional walk skips
    /// what is already spoken for rather than counting blindly.
    claimed: Vec<bool>,
    /// Recorded key to its index, for the keyed lookup.
    by_key: HashMap<String, usize>,
    /// Keys this attempt has asked for, in order. Kept for the divergence
    /// message, which is only useful if it shows what the running code did.
    issued: Vec<String>,
    /// The same keys as a set: a duplicate explicit key is refused, and a
    /// linear scan of `issued` would make a thousand-step job quadratic.
    issued_keys: HashSet<String>,
    /// Per-name occurrence counters. Explicit keys never touch these — see
    /// [`StepKey`].
    occurrences: HashMap<String, u32>,
    /// Where the positional walk has got to in the snapshot.
    cursor: usize,
    /// The step handed out and not yet committed. At most one at a time.
    pending: Option<PendingStep>,
    /// Rows committed for this job, and the bytes they hold. Seeded from the
    /// snapshot so the caps can be checked without a second read, and it is
    /// also the next free `seq` — the sequence is gapless by construction.
    stored_count: usize,
    stored_bytes: usize,
}

/// Where a step this attempt asked for lands in the snapshot.
enum StepMatch {
    /// It replays the recorded row at this index.
    Recorded(usize),
    /// This attempt got further than any before it.
    New(PendingStep),
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
        let by_key = recorded
            .iter()
            .enumerate()
            .map(|(index, step)| (step.step_key.clone(), index))
            .collect();
        Ok(Self {
            job_id,
            stored_count: recorded.len(),
            stored_bytes,
            claimed: vec![false; recorded.len()],
            by_key,
            recorded,
            issued: Vec::new(),
            issued_keys: HashSet::new(),
            occurrences: HashMap::new(),
            cursor: 0,
            pending: None,
        })
    }

    /// Decide what `step.run(name)` — or `step.run(name, key=…)` — must do.
    ///
    /// A memo hit is resolved outright. On [`StepDecision::Run`] nothing counts
    /// as done until the commit lands, so a closure that raises leaves the
    /// sequence exactly where it was.
    pub fn begin_run(&mut self, name: &str, key: Option<&str>) -> Result<StepDecision> {
        let step_key = self.issue(name, key)?;
        Ok(
            match self.landed(&step_key, StepKind::Run, key.is_some())? {
                StepMatch::Recorded(index) => self.memoize(index),
                StepMatch::New(pending) => StepDecision::Run(pending),
            },
        )
    }

    /// Decide what `step.sleep(…)` — named or not, keyed or not — must do.
    ///
    /// `now` is passed in rather than read, because whether a recorded sleep is
    /// finished is the *reader's* derivation from `now >= wake_at` and not a
    /// stored status. That is what leaves no state for a crash to strand: the
    /// row means the right thing without anything having to run at the wake
    /// moment.
    pub fn begin_sleep(
        &mut self,
        name: Option<&str>,
        key: Option<&str>,
        now: i64,
    ) -> Result<SleepDecision> {
        let name = name.unwrap_or(DEFAULT_SLEEP_NAME);
        let step_key = self.issue(name, key)?;
        match self.landed(&step_key, StepKind::Sleep, key.is_some())? {
            StepMatch::New(pending) => Ok(SleepDecision::Sleep(pending)),
            StepMatch::Recorded(index) => {
                let recorded = &self.recorded[index];
                let wake_at = recorded.wake_at.ok_or_else(|| QueueError::StepDiverged {
                    job_id: self.job_id.clone(),
                    seq: recorded.seq,
                    expected: "a sleep step with a deadline".to_string(),
                    found: format!("'{}' with none", abbreviate(&recorded.step_key)),
                })?;
                let seq = recorded.seq;
                let step_key = recorded.step_key.clone();
                self.claimed[index] = true;
                if now >= wake_at {
                    return Ok(SleepDecision::Elapsed { step_key, wake_at });
                }
                // Re-issued at the *recorded* position, so storage recognizes
                // the row and answers with the deadline it already holds. A
                // fresh position would commit a second sleep and start the
                // clock again.
                let pending = PendingStep {
                    seq,
                    step_key,
                    kind: StepKind::Sleep,
                };
                self.pending = Some(pending.clone());
                Ok(SleepDecision::Resume(pending))
            }
        }
    }

    /// Derive this step's identity and spend an occurrence if it needs one.
    ///
    /// The occurrence is spent only once the key is known to be usable: a
    /// refused step must not shift the key of the next one. Explicit keys never
    /// spend one at all, so adding a keyed call cannot move an unkeyed one.
    fn issue(&mut self, name: &str, key: Option<&str>) -> Result<String> {
        let step_key = match key {
            Some(key) => StepKey::explicit(name, key)?,
            None => {
                let occurrence = self.occurrences.get(name).copied().unwrap_or(0);
                StepKey::derive(name, occurrence)?
            }
        };
        self.check_issuable(&step_key)?;
        if key.is_none() {
            *self.occurrences.entry(name.to_string()).or_insert(0) += 1;
        }
        Ok(step_key)
    }

    /// Acknowledge that `pending` was committed, and move on.
    pub fn commit(&mut self, pending: &PendingStep, encoded_len: usize) -> Result<()> {
        self.take_pending(pending)?;
        self.stored_count += 1;
        self.stored_bytes += encoded_len;
        Ok(())
    }

    /// Acknowledge the sleep `pending` issued, with what storage did about it.
    ///
    /// Only a fresh commit adds a row: `AlreadySleeping` means the deadline was
    /// already on disk and the write was a no-op, so counting it would put the
    /// sequence one ahead of storage. A sleep row holds no result, so the byte
    /// total never moves.
    pub fn commit_sleep(&mut self, pending: &PendingStep, outcome: SleepOutcome) -> Result<()> {
        self.take_pending(pending)?;
        if matches!(outcome, SleepOutcome::Slept { .. }) {
            self.stored_count += 1;
        }
        Ok(())
    }

    /// Consume the outstanding step, refusing anything but the one handed out.
    fn take_pending(&mut self, pending: &PendingStep) -> Result<()> {
        match self.pending.take() {
            Some(outstanding) if &outstanding == pending => Ok(()),
            outstanding => {
                self.pending = outstanding;
                Err(QueueError::Config(format!(
                    "step '{}' of job {} was committed out of turn",
                    abbreviate(pending.step_key()),
                    self.job_id
                )))
            }
        }
    }

    /// Recorded steps this attempt never asked for (§3.4).
    ///
    /// A **warning**, never a failure: their side effects already happened and
    /// the shortened code has no use for their values. The rows die with the
    /// job. Only meaningful once the task body has returned.
    pub fn orphaned_tail(&self) -> Vec<&str> {
        self.recorded
            .iter()
            .zip(&self.claimed)
            .filter(|(_, claimed)| !**claimed)
            .map(|(step, _)| step.step_key.as_str())
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

    /// Refuse a step this attempt is in no position to ask for, and record
    /// that it asked.
    fn check_issuable(&mut self, step_key: &str) -> Result<()> {
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
        Ok(())
    }

    /// Where this step lands in the snapshot: on a recorded row, or on new
    /// ground.
    ///
    /// Stops short of deciding what that *means*, because a `run` and a `sleep`
    /// read a recorded row differently — a run row's presence is its
    /// completion, a sleep row's is a deadline. What they share is the match
    /// itself, and the divergence when it fails.
    fn landed(&mut self, step_key: &str, kind: StepKind, keyed: bool) -> Result<StepMatch> {
        match self.recorded_match(step_key, keyed) {
            Some(index) if self.recorded[index].kind == kind => Ok(StepMatch::Recorded(index)),
            // Same key, different kind: `run` replaying onto a recorded `sleep`
            // is a changed sequence like any other.
            Some(index) => Err(self.divergence(index, step_key, kind)),
            None if keyed || self.cursor >= self.recorded.len() => {
                self.new_ground(step_key, kind).map(StepMatch::New)
            }
            // The positional walk reached a step the recorded run does not have
            // here. Nothing later can line up either.
            None => Err(self.divergence(self.cursor, step_key, kind)),
        }
    }

    /// Which recorded step, if any, this one replays.
    ///
    /// A keyed step is looked up by key wherever it sits; an unkeyed one must
    /// be at the cursor, which skips whatever a keyed hit already claimed.
    fn recorded_match(&mut self, step_key: &str, keyed: bool) -> Option<usize> {
        if keyed {
            // Never already claimed: a key issued twice in one attempt is
            // refused above, so at most one lookup can reach any given row.
            return self.by_key.get(step_key).copied();
        }
        while self.cursor < self.recorded.len() && self.claimed[self.cursor] {
            self.cursor += 1;
        }
        let recorded = self.recorded.get(self.cursor)?;
        (recorded.step_key == step_key).then_some(self.cursor)
    }

    /// Hand back a recorded step's value and mark it spoken for.
    fn memoize(&mut self, index: usize) -> StepDecision {
        self.claimed[index] = true;
        StepDecision::Memoized {
            step_key: self.recorded[index].step_key.clone(),
            result: self.recorded[index].result.clone().unwrap_or_default(),
        }
    }

    /// This attempt got further than any before it: the step is new.
    ///
    /// It takes the next free `seq`, which is the number of rows already
    /// stored — not the walk's position, which a keyed hit can leave behind.
    fn new_ground(&mut self, step_key: &str, kind: StepKind) -> Result<PendingStep> {
        let seq = i32::try_from(self.stored_count).map_err(|_| {
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
        Ok(pending)
    }

    fn divergence(&self, position: usize, step_key: &str, kind: StepKind) -> QueueError {
        let recorded = &self.recorded[position];
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
            // Each sequence is windowed around its own index. `position` names
            // a recorded row, and a keyed match finds one wherever it sits —
            // which says nothing about how far this attempt has got. The
            // offending step is always the last one issued.
            running: render_sequence(&self.issued_keys(), self.issued.len().saturating_sub(1)),
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
    let end = (position + CONTEXT + 1).min(keys.len());
    // Clamped both ways: an index past the end of these keys must render a
    // shorter window, never an inverted slice. A helper that only ever runs
    // while an error is being built is the last place to panic.
    let start = position.saturating_sub(CONTEXT).min(end);
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
    fn keyed_steps_replay_in_any_order() {
        // The whole reason `key=` exists: a loop over something unordered may
        // hand its steps back in a different order, and each one must still
        // find its own memo rather than the one that happens to sit there.
        let mut sequence = sequence(vec![
            recorded(0, "fetch:a", b"A"),
            recorded(1, "fetch:b", b"B"),
        ]);
        assert_eq!(
            sequence.begin_run("fetch", Some("b")).unwrap(),
            StepDecision::Memoized {
                step_key: "fetch:b".to_string(),
                result: b"B".to_vec(),
            }
        );
        assert_eq!(
            sequence.begin_run("fetch", Some("a")).unwrap(),
            StepDecision::Memoized {
                step_key: "fetch:a".to_string(),
                result: b"A".to_vec(),
            }
        );
    }

    #[test]
    fn an_unkeyed_step_lines_up_after_reordered_keyed_ones() {
        let mut sequence = sequence(vec![
            recorded(0, "charge#0", b""),
            recorded(1, "fetch:a", b"A"),
            recorded(2, "fetch:b", b"B"),
            recorded(3, "receipt#0", b"R"),
        ]);
        for (name, key) in [
            ("charge", None),
            ("fetch", Some("b")),
            ("fetch", Some("a")),
            ("receipt", None),
        ] {
            let decision = sequence.begin_run(name, key).unwrap();
            assert!(
                matches!(decision, StepDecision::Memoized { .. }),
                "{name} {key:?} should have replayed: {decision:?}"
            );
        }
    }

    #[test]
    fn a_key_the_recorded_run_never_saw_takes_the_next_free_position() {
        // The loop gained an item. The new step appends after every stored row,
        // not at the walk's position — storage demands the next free `seq`.
        let mut sequence = sequence(vec![
            recorded(0, "fetch:a", b"A"),
            recorded(1, "fetch:b", b"B"),
        ]);
        sequence.begin_run("fetch", Some("b")).unwrap();
        match sequence.begin_run("fetch", Some("c")).unwrap() {
            StepDecision::Run(pending) => {
                assert_eq!((pending.seq(), pending.step_key()), (2, "fetch:c"))
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn a_recorded_step_nobody_asked_for_is_reported_not_failed() {
        // The loop lost an item. Its side effects already happened and nothing
        // reads the value, so the row is a warning, not a failed job.
        let mut sequence = sequence(vec![
            recorded(0, "fetch:a", b"A"),
            recorded(1, "fetch:b", b"B"),
        ]);
        sequence.begin_run("fetch", Some("b")).unwrap();
        assert_eq!(sequence.orphaned_tail(), vec!["fetch:a"]);
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

    fn slept(seq: i32, step_key: &str, wake_at: i64) -> JobStep {
        JobStep {
            kind: StepKind::Sleep,
            result: None,
            wake_at: Some(wake_at),
            ..recorded(seq, step_key, b"")
        }
    }

    #[test]
    fn an_unnamed_sleep_is_numbered_by_occurrence() {
        let mut sequence = sequence(vec![]);
        match sequence.begin_sleep(None, None, 0).unwrap() {
            SleepDecision::Sleep(pending) => {
                assert_eq!(pending.step_key(), "sleep#0");
                assert_eq!(pending.kind(), StepKind::Sleep);
                assert_eq!(pending.seq(), 0);
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn a_sleep_is_a_memo_hit_only_once_its_deadline_passes() {
        let rows = vec![slept(0, "cool_off#0", 5_000)];

        let mut early = sequence(rows.clone());
        match early.begin_sleep(Some("cool_off"), None, 4_999).unwrap() {
            SleepDecision::Resume(pending) => {
                assert_eq!(pending.seq(), 0, "re-issued at the recorded position");
                assert_eq!(pending.step_key(), "cool_off#0");
            }
            other => panic!("{other:?}"),
        }

        let mut on_time = sequence(rows);
        assert_eq!(
            on_time.begin_sleep(Some("cool_off"), None, 5_000).unwrap(),
            SleepDecision::Elapsed {
                step_key: "cool_off#0".to_string(),
                wake_at: 5_000,
            },
            "the deadline is reached, not merely passed"
        );
    }

    #[test]
    fn an_elapsed_sleep_lets_the_next_step_line_up() {
        // The sleep is spent from the sequence like any other step, so what
        // follows it matches at the position after it rather than on top of it.
        let mut sequence = sequence(vec![
            slept(0, "cool_off#0", 5_000),
            recorded(1, "charge#0", b"receipt"),
        ]);
        assert!(matches!(
            sequence.begin_sleep(Some("cool_off"), None, 9_000).unwrap(),
            SleepDecision::Elapsed { .. }
        ));
        assert_eq!(
            sequence.begin_run("charge", None).unwrap(),
            StepDecision::Memoized {
                step_key: "charge#0".to_string(),
                result: b"receipt".to_vec(),
            }
        );
        assert!(sequence.orphaned_tail().is_empty());
    }

    #[test]
    fn replaying_a_sleep_onto_a_run_is_a_divergence() {
        // The other direction of the rule `replaying_a_run_onto_a_sleep` pins:
        // a run row carries no deadline, so reading one as a sleep would
        // reschedule the job to nothing.
        let mut sequence = sequence(vec![recorded(0, "cool_off#0", b"done")]);
        let err = sequence.begin_sleep(Some("cool_off"), None, 0).unwrap_err();
        let message = err.to_string();
        assert!(message.contains("'cool_off#0' as a run step"), "{message}");
        assert!(
            message.contains("'cool_off#0' as a sleep step"),
            "{message}"
        );
    }

    #[test]
    fn a_sleep_row_without_a_deadline_is_refused() {
        // Storage never writes one, so this is corruption rather than a
        // divergence in the task's code — but reading it as "no deadline"
        // would reschedule the job to null and lose it.
        let mut sequence = sequence(vec![JobStep {
            kind: StepKind::Sleep,
            result: None,
            wake_at: None,
            ..recorded(0, "cool_off#0", b"")
        }]);
        let err = sequence.begin_sleep(Some("cool_off"), None, 0).unwrap_err();
        assert!(matches!(err, QueueError::StepDiverged { .. }), "{err}");
    }

    #[test]
    fn a_sleep_and_a_run_number_their_names_apart() {
        // `sleep#0` and `charge#0` count on their own names, so a sleep between
        // two runs of the same name does not renumber them.
        let mut sequence = sequence(vec![]);
        let first = sequence.begin_run("charge", None).unwrap();
        commit(&mut sequence, first, b"a");
        let SleepDecision::Sleep(nap) = sequence.begin_sleep(None, None, 0).unwrap() else {
            panic!("expected new ground");
        };
        sequence
            .commit_sleep(&nap, SleepOutcome::Slept { wake_at: 10 })
            .unwrap();
        match sequence.begin_run("charge", None).unwrap() {
            StepDecision::Run(pending) => {
                assert_eq!(pending.step_key(), "charge#1");
                assert_eq!(pending.seq(), 2, "the sleep took a position of its own");
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn a_resumed_sleep_does_not_advance_the_count() {
        // `AlreadySleeping` wrote nothing. Counting it would put the sequence a
        // position ahead of storage, and the next step would be refused.
        let mut sequence = sequence(vec![slept(0, "cool_off#0", 5_000)]);
        let SleepDecision::Resume(pending) =
            sequence.begin_sleep(Some("cool_off"), None, 0).unwrap()
        else {
            panic!("expected a resume");
        };
        sequence
            .commit_sleep(&pending, SleepOutcome::AlreadySleeping { wake_at: 5_000 })
            .unwrap();
        assert_eq!(sequence.committed_steps(), 1);
    }

    #[test]
    fn a_keyed_divergence_far_into_the_snapshot_renders() {
        // The two sequences are indexed differently: a keyed match names a
        // recorded row, which says nothing about how many steps this attempt
        // has issued. Windowing the running sequence around the recorded index
        // once put its start past its end.
        let mut rows: Vec<JobStep> = (0..9)
            .map(|seq| recorded(seq, &format!("step{seq}#0"), b""))
            .collect();
        rows.push(JobStep {
            kind: StepKind::Sleep,
            result: None,
            wake_at: Some(1),
            ..recorded(9, "nap:x", b"")
        });
        let mut sequence = sequence(rows);

        let err = sequence.begin_run("nap", Some("x")).unwrap_err();
        let QueueError::StepSequenceDiverged(divergence) = &err else {
            panic!("{err}");
        };
        assert_eq!(divergence.position, 9);
        assert_eq!(
            divergence.running, "nap:x",
            "one issued step, rendered whole"
        );
        assert!(
            divergence.recorded.contains("nap:x"),
            "{}",
            divergence.recorded
        );
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
