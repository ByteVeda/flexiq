package step

import (
	"errors"
	"fmt"
	"math"
	"strings"
)

// Mirrors crates/flexiq-core/src/step/sequence.rs.

// DefaultSleepName numbers a sleep nobody named, so sleep("1h") is "sleep#0".
//
// A name is accepted and recommended at every call site: a job whose sequence
// reads sleep#0, sleep#1, sleep#2 tells nobody which one diverged.
const DefaultSleepName = "sleep"

// ErrDiverged marks the running code asking for a different step than the one
// recorded at that position.
//
// It is told apart from every other refusal because it is the one the caller
// must not be able to carry on past: the rest are the task's to handle, but a
// memoized result answering a different question than the step asking for it
// is a changed deploy, and an attempt that continues writes into a sequence
// that no longer lines up.
var ErrDiverged = errors.New("the step sequence diverged")

// Pending is a step that has been issued but not yet committed.
//
// Its fields are unexported and it is only ever constructed here, so a caller
// cannot invent a position — which is what keeps the attempt's idea of the
// sequence and storage's from drifting apart.
type Pending struct {
	seq     int32
	stepKey string
	kind    Kind
}

// Seq is the position this step takes in the job's sequence.
func (p Pending) Seq() int32 { return p.seq }

// StepKey is the identity of the step.
func (p Pending) StepKey() string { return p.stepKey }

// Kind says whether it commits a value or a deadline.
func (p Pending) Kind() Kind { return p.kind }

// Decision is what the caller must do with the step it just asked for.
type Decision struct {
	// Memoized is true when this step already ran in an earlier attempt. The
	// body **must not** run — re-running it is the double charge this whole
	// mechanism exists to prevent.
	Memoized bool
	// StepKey identifies the step either way.
	StepKey string
	// Result is the stored bytes, exactly as committed. Only when Memoized.
	Result []byte
	// Pending is the position to commit at. Only when not Memoized.
	Pending Pending
}

// SleepDecision is what the caller must do with the sleep it just asked for.
//
// Three states, not two, because a recorded sleep row means different things
// on either side of its deadline — and because only new ground can be refused
// by the step-count cap, which is the distinction storage draws too.
type SleepDecision struct {
	// Elapsed means the deadline has passed. The sleep is a memo hit and the
	// attempt carries on — which is what makes a job with three sleeps not
	// restart the first one on the third wake.
	Elapsed bool
	// Fresh means new ground: a row is written here and counts against the
	// step cap. False is a resume — a sleep already committed at this position
	// that has not elapsed, because the attempt that wrote it came back early
	// through a reclaim or an operator requeue. A resume is re-issued at the
	// **recorded** position so the stored deadline stands.
	Fresh bool
	// StepKey identifies the sleep.
	StepKey string
	// WakeAt is the deadline it was first given. Only when Elapsed.
	WakeAt int64
	// Pending is the position to commit at. Only when not Elapsed.
	Pending Pending
}

// Sequence is one attempt's walk through a job's recorded steps.
//
// The "fingerprint" of a job's steps is this ordered list of keys — there is no
// digest column and no extra read. Each step is matched against the snapshot as
// it is asked for, which is what makes a divergence surface *before* the body
// runs.
//
// The two identities are matched differently, which is the whole point of
// having both. An unkeyed step is matched **by position**: "fetch#1" means "the
// second fetch of this attempt", so it is only the same step if it is asked for
// at the same point. An explicit key is matched **by key, wherever it sits** —
// a key exists precisely so a loop over something unordered can hand its steps
// back in a different order without every one of them looking like a different
// question.
//
// Not safe for concurrent use. One attempt walks one sequence; the caller
// serializes, and an overlapping step is refused by name rather than raced.
type Sequence struct {
	jobID string
	// recorded is the snapshot, ordered by Seq and gapless — checked at
	// construction, because a hole would silently shift every memo after it.
	recorded []Record
	// claimed is parallel to recorded: which rows this attempt has spoken for.
	// A keyed hit can claim one out of order, so the positional walk skips what
	// is already spoken for rather than counting blindly.
	claimed []bool
	// byKey maps a recorded key to its index, for the keyed lookup.
	byKey map[string]int
	// issued is the keys this attempt has asked for, in order, for the
	// divergence message — which is only useful if it shows what the running
	// code did.
	issued []string
	// issuedKeys is the same keys as a set: a duplicate key is refused, and a
	// linear scan would make a thousand-step job quadratic.
	issuedKeys map[string]struct{}
	// occurrences counts per name. Explicit keys never touch these.
	occurrences map[string]uint32
	// cursor is where the positional walk has got to.
	cursor int
	// pending is the step handed out and not yet committed. At most one.
	pending *Pending
	// storedCount is rows committed for this job, and also the next free Seq —
	// the sequence is gapless by construction. storedBytes is what they hold,
	// seeded from the snapshot so the caps need no second read.
	storedCount int
	storedBytes int
}

// NewSequence takes the snapshot read once at attempt start.
func NewSequence(jobID string, recorded []Record) (*Sequence, error) {
	byKey := make(map[string]int, len(recorded))
	storedBytes := 0
	for index, record := range recorded {
		if record.Seq != int32(index) {
			// Gapless Seq is what lets the count be the next free position. A
			// hole means the memo at every later position answers a different
			// step's question.
			return nil, fmt.Errorf("job %s has a hole in its step sequence: position %d holds seq %d",
				jobID, index, record.Seq)
		}
		byKey[record.StepKey] = index
		storedBytes += len(record.Result)
	}
	return &Sequence{
		jobID:       jobID,
		recorded:    recorded,
		claimed:     make([]bool, len(recorded)),
		byKey:       byKey,
		issuedKeys:  make(map[string]struct{}),
		occurrences: make(map[string]uint32),
		storedCount: len(recorded),
		storedBytes: storedBytes,
	}, nil
}

// BeginRun decides what an unkeyed step.run must do.
//
// A memo hit is resolved outright. Otherwise nothing counts as done until the
// commit lands, so a body that fails leaves the sequence exactly where it was.
func (s *Sequence) BeginRun(name string) (Decision, error) {
	return s.beginRun(name, nil)
}

// BeginRunKeyed decides what a step.run with an explicit key must do.
func (s *Sequence) BeginRunKeyed(name, key string) (Decision, error) {
	return s.beginRun(name, &key)
}

func (s *Sequence) beginRun(name string, key *string) (Decision, error) {
	index, pending, err := s.resolve(name, key, KindRun)
	if err != nil {
		return Decision{}, err
	}
	if index < 0 {
		return Decision{StepKey: pending.stepKey, Pending: pending}, nil
	}
	s.claimed[index] = true
	return Decision{
		Memoized: true,
		StepKey:  s.recorded[index].StepKey,
		Result:   s.recorded[index].Result,
	}, nil
}

// BeginSleep decides what a step.sleep must do.
//
// now is passed in rather than read, because whether a recorded sleep is
// finished is the *reader's* derivation from now >= wake_at and not a stored
// status. That is what leaves no state for a crash to strand: the row means the
// right thing without anything having to run at the wake moment.
func (s *Sequence) BeginSleep(name string, now int64) (SleepDecision, error) {
	if name == "" {
		name = DefaultSleepName
	}
	index, pending, err := s.resolve(name, nil, KindSleep)
	if err != nil {
		return SleepDecision{}, err
	}
	if index < 0 {
		return SleepDecision{Fresh: true, StepKey: pending.stepKey, Pending: pending}, nil
	}

	recorded := s.recorded[index]
	if recorded.WakeAt == nil {
		return SleepDecision{}, fmt.Errorf(
			"%w: on job %s at position %d, expected a sleep step with a deadline, found %q with none",
			ErrDiverged, s.jobID, recorded.Seq, Abbreviate(recorded.StepKey))
	}
	s.claimed[index] = true
	if now >= *recorded.WakeAt {
		return SleepDecision{Elapsed: true, StepKey: recorded.StepKey, WakeAt: *recorded.WakeAt}, nil
	}

	// Re-issued at the *recorded* position, so storage recognizes the row and
	// answers with the deadline it already holds. A fresh position would commit
	// a second sleep and start the clock again.
	resumed := Pending{seq: recorded.Seq, stepKey: recorded.StepKey, kind: KindSleep}
	s.pending = &resumed
	return SleepDecision{StepKey: resumed.stepKey, Pending: resumed}, nil
}

// resolve names this step, checks it may be asked for, and finds where it
// lands: a recorded index, or -1 and the position new ground takes.
//
// Shared by run and sleep, which differ only in what a landing *means* — the
// naming, the guards and the divergence are one path.
func (s *Sequence) resolve(name string, key *string, kind Kind) (int, Pending, error) {
	var (
		stepKey string
		err     error
	)
	if key != nil {
		stepKey, err = Explicit(name, *key)
	} else {
		stepKey, err = Derive(name, s.occurrences[name])
	}
	if err != nil {
		return 0, Pending{}, err
	}
	if err = s.checkIssuable(stepKey); err != nil {
		return 0, Pending{}, err
	}
	index, pending, err := s.landed(stepKey, kind, key != nil)
	if err != nil {
		return 0, Pending{}, err
	}
	// Spent only once the step is known to be usable: a refused one must not
	// shift the key of the next. Explicit keys never spend one at all, so
	// adding a keyed call cannot move an unkeyed one.
	if key == nil {
		s.occurrences[name]++
	}
	return index, pending, nil
}

// Commit acknowledges that pending was written, and moves on.
func (s *Sequence) Commit(pending Pending, encodedLen int) error {
	if err := s.takePending(pending); err != nil {
		return err
	}
	s.storedCount++
	s.storedBytes += encodedLen
	return nil
}

// CommitSleep acknowledges the sleep pending issued, with what storage did
// about it.
//
// Only a fresh row adds one: a deadline already on disk means the write was a
// no-op, and counting it would put the sequence one ahead of storage. A sleep
// row holds no result, so the byte total never moves.
func (s *Sequence) CommitSleep(pending Pending, slept bool) error {
	if err := s.takePending(pending); err != nil {
		return err
	}
	if slept {
		s.storedCount++
	}
	return nil
}

// takePending consumes the outstanding step, refusing anything but the one
// handed out.
func (s *Sequence) takePending(pending Pending) error {
	if s.pending == nil || *s.pending != pending {
		return fmt.Errorf("step %q of job %s was committed out of turn",
			Abbreviate(pending.StepKey()), s.jobID)
	}
	s.pending = nil
	return nil
}

// Abandon drops the outstanding step without counting it.
//
// For a commit that was refused or never answered: the row is not there, the
// sequence stays where it was, and the attempt ends anyway.
func (s *Sequence) Abandon() {
	s.pending = nil
}

// CheckCaps refuses an over-cap commit before the round trip, so the error can
// name the step and the number that failed.
//
// The scheduler checks the same three caps inside its own transaction — this
// one is the good message, that one is the check that holds.
func (s *Sequence) CheckCaps(limits Limits, pending Pending, encodedLen int) error {
	limits = limits.Clamped()
	tooLarge := func(limit string, actual, allowed int) error {
		return fmt.Errorf("step %q exceeds the %s limit: %d > %d",
			Abbreviate(pending.StepKey()), limit, actual, allowed)
	}

	if encodedLen > limits.MaxStepBytes {
		return tooLarge("step bytes", encodedLen, limits.MaxStepBytes)
	}
	if steps := s.storedCount + 1; steps > limits.MaxSteps {
		return tooLarge("step count", steps, limits.MaxSteps)
	}
	if bytes := s.storedBytes + encodedLen; bytes > limits.MaxTotalBytes {
		return tooLarge("total bytes", bytes, limits.MaxTotalBytes)
	}
	return nil
}

// OrphanedTail is the recorded steps this attempt never asked for.
//
// A warning, never a failure: their side effects already happened and the
// shortened code has no use for their values. The rows die with the job. Only
// meaningful once the task body has returned.
func (s *Sequence) OrphanedTail() []string {
	var orphaned []string
	for index, record := range s.recorded {
		if !s.claimed[index] {
			orphaned = append(orphaned, record.StepKey)
		}
	}
	return orphaned
}

// checkIssuable refuses a step this attempt is in no position to ask for, and
// records that it asked.
func (s *Sequence) checkIssuable(stepKey string) error {
	if s.pending != nil {
		return fmt.Errorf("step %q of job %s started while step %q is still uncommitted",
			Abbreviate(stepKey), s.jobID, Abbreviate(s.pending.StepKey()))
	}
	if _, seen := s.issuedKeys[stepKey]; seen {
		// Two steps sharing a key would memo over each other, and the position
		// check cannot see it — both sequences look identical.
		return fmt.Errorf("step key %q was used twice in one attempt of job %s; "+
			"give each keyed step a key of its own", Abbreviate(stepKey), s.jobID)
	}
	s.issuedKeys[stepKey] = struct{}{}
	s.issued = append(s.issued, stepKey)
	return nil
}

// landed says where this step falls in the snapshot: a recorded index, or -1
// and a fresh position.
//
// Stops short of deciding what that *means*, because a run and a sleep read a
// recorded row differently — a run row's presence is its completion, a sleep
// row's is a deadline. What they share is the match itself, and the divergence
// when it fails.
func (s *Sequence) landed(stepKey string, kind Kind, keyed bool) (int, Pending, error) {
	index, found := s.recordedMatch(stepKey, keyed)
	switch {
	case found && s.recorded[index].Kind == kind:
		return index, Pending{}, nil
	case found:
		// Same key, different kind: a run replaying onto a recorded sleep is a
		// changed sequence like any other.
		return 0, Pending{}, s.divergence(index, stepKey, kind)
	case keyed || s.cursor >= len(s.recorded):
		pending, err := s.newGround(stepKey, kind)
		return -1, pending, err
	default:
		// The positional walk reached a step the recorded run does not have
		// here. Nothing later can line up either.
		return 0, Pending{}, s.divergence(s.cursor, stepKey, kind)
	}
}

// recordedMatch says which recorded step, if any, this one replays.
//
// A keyed step is looked up by key wherever it sits; an unkeyed one must be at
// the cursor, which skips whatever a keyed hit already claimed.
func (s *Sequence) recordedMatch(stepKey string, keyed bool) (int, bool) {
	if keyed {
		// Never already claimed: a key issued twice in one attempt is refused
		// above, so at most one lookup can reach any given row.
		index, found := s.byKey[stepKey]
		return index, found
	}
	for s.cursor < len(s.recorded) && s.claimed[s.cursor] {
		s.cursor++
	}
	if s.cursor >= len(s.recorded) || s.recorded[s.cursor].StepKey != stepKey {
		return 0, false
	}
	return s.cursor, true
}

// newGround means this attempt got further than any before it.
//
// The step takes the next free Seq, which is the number of rows already stored
// — not the walk's position, which a keyed hit can leave behind.
func (s *Sequence) newGround(stepKey string, kind Kind) (Pending, error) {
	if s.storedCount > math.MaxInt32 {
		return Pending{}, fmt.Errorf("job %s asked for more steps than a sequence can hold", s.jobID)
	}
	pending := Pending{seq: int32(s.storedCount), stepKey: stepKey, kind: kind}
	s.pending = &pending
	return pending, nil
}

func (s *Sequence) divergence(position int, stepKey string, kind Kind) error {
	recorded := s.recorded[position]
	// Same key, different kind: say so, or the message reads as if nothing
	// changed. A run replaying onto a recorded sleep is exactly this.
	expected := fmt.Sprintf("%q", Abbreviate(recorded.StepKey))
	found := fmt.Sprintf("%q", Abbreviate(stepKey))
	if recorded.StepKey == stepKey {
		expected = fmt.Sprintf("%q as a %s step", recorded.StepKey, recorded.Kind)
		found = fmt.Sprintf("%q as a %s step", stepKey, kind)
	}

	// Each sequence is windowed around its own index. position names a recorded
	// row, and a keyed match finds one wherever it sits — which says nothing
	// about how far this attempt has got. The offending step is always the last
	// one issued.
	return fmt.Errorf("%w: step sequence changed for job %s at position %d\n"+
		"  recorded: %s\n"+
		"  running:  %s\n"+
		"  step %d was %s, now %s\n"+
		"A memoized result would answer a different question than the step asking for it. "+
		"Drain or dead-letter this task's in-flight jobs before deploying a change to its "+
		"step sequence",
		ErrDiverged, s.jobID, position,
		renderSequence(s.recordedKeys(), position),
		renderSequence(s.issued, max(len(s.issued)-1, 0)),
		position, expected, found)
}

func (s *Sequence) recordedKeys() []string {
	keys := make([]string, 0, len(s.recorded))
	for _, record := range s.recorded {
		keys = append(keys, record.StepKey)
	}
	return keys
}

// renderSequence shows a sequence around the position that failed.
//
// Bounded on purpose: a job may commit a thousand steps, and an error nobody
// can read is not louder for being longer. The window keeps the neighbours that
// make the change recognizable.
func renderSequence(keys []string, position int) string {
	const context = 5

	if len(keys) == 0 {
		return "(none)"
	}
	end := min(position+context+1, len(keys))
	// Clamped both ways: an index past the end of these keys must render a
	// shorter window, never an inverted slice.
	start := min(max(position-context, 0), end)

	var rendered strings.Builder
	if start > 0 {
		fmt.Fprintf(&rendered, "…(%d earlier), ", start)
	}
	for index, key := range keys[start:end] {
		if index > 0 {
			rendered.WriteString(", ")
		}
		rendered.WriteString(Abbreviate(key))
	}
	if end < len(keys) {
		fmt.Fprintf(&rendered, ", …(%d more)", len(keys)-end)
	}
	return rendered.String()
}
