package tests

import (
	"strings"
	"testing"

	"github.com/ByteVeda/flexiq/sdks/go/v2/internal/step"
)

func runRow(seq int32, key string, result []byte) step.Record {
	return step.Record{Seq: seq, StepKey: key, Kind: step.KindRun, Result: result, CreatedAt: 1}
}

func sleepRow(seq int32, key string, at int64) step.Record {
	return step.Record{Seq: seq, StepKey: key, Kind: step.KindSleep, WakeAt: &at, CreatedAt: 1}
}

func newSequence(t *testing.T, recorded ...step.Record) *step.Sequence {
	t.Helper()
	sequence, err := step.NewSequence("job-1", recorded)
	if err != nil {
		t.Fatalf("NewSequence: %v", err)
	}
	return sequence
}

// The first attempt records nothing, so every step is new ground and takes the
// next free position.
func TestAnEmptySnapshotRunsEveryStep(t *testing.T) {
	sequence := newSequence(t)

	for position, name := range []string{"charge", "notify"} {
		decision, err := sequence.BeginRun(name)
		if err != nil {
			t.Fatalf("BeginRun(%s): %v", name, err)
		}
		if decision.Memoized {
			t.Fatalf("%s was memoized against an empty snapshot", name)
		}
		if got := decision.Pending.Seq(); got != int32(position) {
			t.Fatalf("%s took seq %d, want %d", name, got, position)
		}
		if want := name + "#0"; decision.Pending.StepKey() != want {
			t.Fatalf("%s keyed %q, want %q", name, decision.Pending.StepKey(), want)
		}
		if err := sequence.Commit(decision.Pending, 0); err != nil {
			t.Fatalf("Commit(%s): %v", name, err)
		}
	}
}

func TestARecordedStepReplaysItsStoredBytes(t *testing.T) {
	sequence := newSequence(t, runRow(0, "charge#0", []byte("receipt")))

	decision, err := sequence.BeginRun("charge")
	if err != nil {
		t.Fatalf("BeginRun: %v", err)
	}
	if !decision.Memoized {
		t.Fatal("a recorded step was not memoized")
	}
	if string(decision.Result) != "receipt" {
		t.Fatalf("replayed %q, want %q", decision.Result, "receipt")
	}
}

// The counter is per name and per attempt, so a second charge is charge#1 and
// a first notify is still notify#0.
func TestOccurrencesCountPerName(t *testing.T) {
	sequence := newSequence(t)

	for _, want := range []string{"charge#0", "notify#0", "charge#1"} {
		name, _, _ := strings.Cut(want, "#")
		decision, err := sequence.BeginRun(name)
		if err != nil {
			t.Fatalf("BeginRun(%s): %v", name, err)
		}
		if decision.Pending.StepKey() != want {
			t.Fatalf("keyed %q, want %q", decision.Pending.StepKey(), want)
		}
		if err := sequence.Commit(decision.Pending, 0); err != nil {
			t.Fatalf("Commit: %v", err)
		}
	}
}

// A refused step must not shift the key of the next: the retry derives
// charge#0 again, not charge#1.
func TestARefusedStepDoesNotSpendItsOccurrence(t *testing.T) {
	sequence := newSequence(t, sleepRow(0, "charge#0", 9))

	// A run landing on a recorded sleep is a divergence, and diverges before
	// the occurrence is spent.
	if _, err := sequence.BeginRun("charge"); err == nil {
		t.Fatal("a run replaying onto a recorded sleep was accepted")
	}

	decision, err := sequence.BeginRun("charge")
	if err == nil {
		t.Fatalf("the retry was accepted, keyed %q", decision.Pending.StepKey())
	}
	if !strings.Contains(err.Error(), "charge#0") {
		t.Fatalf("the retry derived a different key: %v", err)
	}
	if strings.Contains(err.Error(), "charge#1") {
		t.Fatalf("the refused step spent its occurrence: %v", err)
	}
}

// The whole point of an explicit key: a loop over something unordered hands its
// steps back in a different order without every one looking like a new
// question.
func TestAKeyedStepIsFoundWhereverItSits(t *testing.T) {
	sequence := newSequence(t,
		runRow(0, "fetch:a", []byte("A")),
		runRow(1, "fetch:b", []byte("B")),
	)

	for _, key := range []string{"b", "a"} {
		decision, err := sequence.BeginRunKeyed("fetch", key)
		if err != nil {
			t.Fatalf("BeginRunKeyed(%s): %v", key, err)
		}
		if !decision.Memoized {
			t.Fatalf("fetch:%s was not memoized", key)
		}
		if want := strings.ToUpper(key); string(decision.Result) != want {
			t.Fatalf("fetch:%s replayed %q, want %q", key, decision.Result, want)
		}
	}
}

// The one fact most likely to be got wrong: a new step takes the number of rows
// already stored, not the cursor — which an out-of-order keyed hit leaves
// behind.
func TestANewStepTakesTheStoredCountNotTheCursor(t *testing.T) {
	sequence := newSequence(t,
		runRow(0, "fetch:a", []byte("A")),
		runRow(1, "fetch:b", []byte("B")),
	)

	// Claim the *second* row by key. The cursor is still at 0.
	if _, err := sequence.BeginRunKeyed("fetch", "b"); err != nil {
		t.Fatalf("BeginRunKeyed: %v", err)
	}

	decision, err := sequence.BeginRunKeyed("fetch", "c")
	if err != nil {
		t.Fatalf("BeginRunKeyed(c): %v", err)
	}
	if decision.Memoized {
		t.Fatal("an unrecorded key was memoized")
	}
	if got := decision.Pending.Seq(); got != 2 {
		t.Fatalf("new ground took seq %d, want 2 — the stored row count", got)
	}
}

// An unkeyed step is matched by position, and the walk skips rows a keyed hit
// already spoke for.
func TestAnUnkeyedWalkSkipsWhatAKeyedHitClaimed(t *testing.T) {
	sequence := newSequence(t,
		runRow(0, "fetch:a", []byte("A")),
		runRow(1, "notify#0", []byte("N")),
	)

	if _, err := sequence.BeginRunKeyed("fetch", "a"); err != nil {
		t.Fatalf("BeginRunKeyed: %v", err)
	}
	decision, err := sequence.BeginRun("notify")
	if err != nil {
		t.Fatalf("BeginRun: %v", err)
	}
	if !decision.Memoized || string(decision.Result) != "N" {
		t.Fatalf("notify#0 = %+v, want the recorded N", decision)
	}
}

func TestTheSequenceRefusesWhatItCannotAnswer(t *testing.T) {
	t.Run("a hole in the recorded seq", func(t *testing.T) {
		_, err := step.NewSequence("job-1", []step.Record{runRow(1, "charge#0", nil)})
		if err == nil || !strings.Contains(err.Error(), "hole in its step sequence") {
			t.Fatalf("NewSequence error = %v", err)
		}
	})

	t.Run("a key used twice in one attempt", func(t *testing.T) {
		sequence := newSequence(t)
		first, err := sequence.BeginRunKeyed("fetch", "a")
		if err != nil {
			t.Fatalf("BeginRunKeyed: %v", err)
		}
		if err := sequence.Commit(first.Pending, 0); err != nil {
			t.Fatalf("Commit: %v", err)
		}
		if _, err := sequence.BeginRunKeyed("fetch", "a"); err == nil ||
			!strings.Contains(err.Error(), "used twice in one attempt") {
			t.Fatalf("the duplicate key was accepted: %v", err)
		}
	})

	t.Run("a second step while one is uncommitted", func(t *testing.T) {
		sequence := newSequence(t)
		if _, err := sequence.BeginRun("charge"); err != nil {
			t.Fatalf("BeginRun: %v", err)
		}
		if _, err := sequence.BeginRun("notify"); err == nil ||
			!strings.Contains(err.Error(), "is still uncommitted") {
			t.Fatalf("an overlapping step was accepted: %v", err)
		}
	})

	t.Run("a step the recorded run does not have here", func(t *testing.T) {
		sequence := newSequence(t, runRow(0, "charge#0", nil))
		_, err := sequence.BeginRun("refund")
		if err == nil || !strings.Contains(err.Error(), "step sequence changed for job job-1") {
			t.Fatalf("the changed sequence was accepted: %v", err)
		}
		if !strings.Contains(err.Error(), "recorded: charge#0") ||
			!strings.Contains(err.Error(), "running:  refund#0") {
			t.Fatalf("the divergence does not show both sequences:\n%v", err)
		}
	})

	t.Run("a commit out of turn", func(t *testing.T) {
		sequence := newSequence(t)
		decision, err := sequence.BeginRun("charge")
		if err != nil {
			t.Fatalf("BeginRun: %v", err)
		}
		sequence.Abandon()
		if err := sequence.Commit(decision.Pending, 0); err == nil ||
			!strings.Contains(err.Error(), "committed out of turn") {
			t.Fatalf("an abandoned step committed: %v", err)
		}
	})
}

func TestASleepReadsItsRecordedRowAgainstTheClock(t *testing.T) {
	t.Run("new ground", func(t *testing.T) {
		decision, err := newSequence(t).BeginSleep("cooloff", 100)
		if err != nil {
			t.Fatalf("BeginSleep: %v", err)
		}
		if decision.Elapsed || !decision.Fresh {
			t.Fatalf("BeginSleep = %+v, want fresh new ground", decision)
		}
		if decision.Pending.Seq() != 0 || decision.Pending.Kind() != step.KindSleep {
			t.Fatalf("pending = %+v", decision.Pending)
		}
	})

	t.Run("elapsed replays as a memo hit", func(t *testing.T) {
		sequence := newSequence(t, sleepRow(0, "cooloff#0", 100))
		decision, err := sequence.BeginSleep("cooloff", 100)
		if err != nil {
			t.Fatalf("BeginSleep: %v", err)
		}
		if !decision.Elapsed || decision.WakeAt != 100 {
			t.Fatalf("BeginSleep = %+v, want elapsed at 100", decision)
		}
	})

	t.Run("not yet elapsed resumes at the recorded position", func(t *testing.T) {
		sequence := newSequence(t,
			runRow(0, "charge#0", nil),
			sleepRow(1, "cooloff#0", 500),
		)
		if _, err := sequence.BeginRun("charge"); err != nil {
			t.Fatalf("BeginRun: %v", err)
		}
		decision, err := sequence.BeginSleep("cooloff", 100)
		if err != nil {
			t.Fatalf("BeginSleep: %v", err)
		}
		if decision.Elapsed || decision.Fresh {
			t.Fatalf("BeginSleep = %+v, want a resume", decision)
		}
		if decision.Pending.Seq() != 1 {
			t.Fatalf("the resume took seq %d, want the recorded 1", decision.Pending.Seq())
		}
	})

	t.Run("an unnamed sleep is numbered under the default name", func(t *testing.T) {
		decision, err := newSequence(t).BeginSleep("", 0)
		if err != nil {
			t.Fatalf("BeginSleep: %v", err)
		}
		if decision.Pending.StepKey() != "sleep#0" {
			t.Fatalf("keyed %q, want sleep#0", decision.Pending.StepKey())
		}
	})
}

// A resume writes nothing, so counting it would put the sequence one ahead of
// storage and the next step would take an occupied position.
func TestOnlyAFreshSleepAdvancesTheSequence(t *testing.T) {
	sequence := newSequence(t, sleepRow(0, "cooloff#0", 500))
	decision, err := sequence.BeginSleep("cooloff", 100)
	if err != nil {
		t.Fatalf("BeginSleep: %v", err)
	}
	if err = sequence.CommitSleep(decision.Pending, false); err != nil {
		t.Fatalf("CommitSleep: %v", err)
	}

	next, err := sequence.BeginRun("charge")
	if err != nil {
		t.Fatalf("BeginRun: %v", err)
	}
	if next.Pending.Seq() != 1 {
		t.Fatalf("the step after a resumed sleep took seq %d, want 1", next.Pending.Seq())
	}
}

func TestTheCapsRefuseBeforeTheRoundTrip(t *testing.T) {
	limits := step.Limits{MaxStepBytes: 8, MaxTotalBytes: 12, MaxSteps: 2}

	sequence := newSequence(t)
	first, err := sequence.BeginRun("blob")
	if err != nil {
		t.Fatalf("BeginRun: %v", err)
	}
	if err = sequence.CheckCaps(limits, first.Pending, 9); err == nil ||
		!strings.Contains(err.Error(), "exceeds the step bytes limit: 9 > 8") {
		t.Fatalf("the per-step cap did not hold: %v", err)
	}
	if err = sequence.CheckCaps(limits, first.Pending, 8); err != nil {
		t.Fatalf("an in-cap step was refused: %v", err)
	}
	if err = sequence.Commit(first.Pending, 8); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	second, err := sequence.BeginRun("blob")
	if err != nil {
		t.Fatalf("BeginRun: %v", err)
	}
	if err = sequence.CheckCaps(limits, second.Pending, 8); err == nil ||
		!strings.Contains(err.Error(), "exceeds the total bytes limit: 16 > 12") {
		t.Fatalf("the total cap did not hold: %v", err)
	}
	if err = sequence.Commit(second.Pending, 1); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	third, err := sequence.BeginRun("blob")
	if err != nil {
		t.Fatalf("BeginRun: %v", err)
	}
	if err := sequence.CheckCaps(limits, third.Pending, 0); err == nil ||
		!strings.Contains(err.Error(), "exceeds the step count limit: 3 > 2") {
		t.Fatalf("the count cap did not hold: %v", err)
	}
}

// A warning, never a failure: those side effects already happened, and failing
// a job whose code legitimately shortened would be worse than a value nobody
// reads.
func TestStepsTheCodeNoLongerRunsAreReportedNotRefused(t *testing.T) {
	sequence := newSequence(t,
		runRow(0, "charge#0", nil),
		runRow(1, "notify#0", nil),
	)
	if _, err := sequence.BeginRun("charge"); err != nil {
		t.Fatalf("BeginRun: %v", err)
	}
	orphaned := sequence.OrphanedTail()
	if len(orphaned) != 1 || orphaned[0] != "notify#0" {
		t.Fatalf("OrphanedTail = %v, want [notify#0]", orphaned)
	}
}

func TestTheRunKeyIsTheJobsExceptAcrossADeadLetterRetry(t *testing.T) {
	for _, tc := range []struct {
		name     string
		metadata string
		want     string
	}{
		{"no metadata", "", "job-1"},
		{"unrelated metadata", `{"tenant":"acme"}`, "job-1"},
		{"a stamped origin", `{"__origin_job_id":"job-0"}`, "job-0"},
		{"a blank origin", `{"__origin_job_id":""}`, "job-1"},
		{"an origin that is not a string", `{"__origin_job_id":7}`, "job-1"},
		{"metadata that is not an object", `"RETRY_BUDGET_EXHAUSTED"`, "job-1"},
		{"metadata that is not json", "{oops", "job-1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := step.RunKey("job-1", tc.metadata); got != tc.want {
				t.Fatalf("RunKey = %q, want %q", got, tc.want)
			}
		})
	}

	if got := step.IdempotencyKey("job-0", "charge#0"); got != "job-0:charge#0" {
		t.Fatalf("IdempotencyKey = %q", got)
	}
}
