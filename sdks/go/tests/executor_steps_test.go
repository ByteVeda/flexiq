package tests

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	flexiq "github.com/ByteVeda/flexiq/sdks/go/v2"
	"github.com/ByteVeda/flexiq/sdks/go/v2/executor"
	executorv1 "github.com/ByteVeda/flexiq/sdks/go/v2/internal/pb/flexiq/executor/v1"
	"github.com/ByteVeda/flexiq/sdks/go/v2/internal/step"
)

const (
	stepJobID = "job-steps"
	stepTask  = "steps.task"
)

var stepLease = []byte("lease-steps-0001")

// jobStepsFrame is a dispatch's recorded snapshot. It arrives immediately
// before the job frame it belongs to.
func jobStepsFrame(jobID string, snapshot []byte) *executorv1.AttachResponse {
	return &executorv1.AttachResponse{Frame: &executorv1.AttachResponse_JobSteps{
		JobSteps: &executorv1.JobStepsFrame{JobId: jobID, Snapshot: snapshot},
	}}
}

func stepAckFrame(ack *executorv1.StepAckFrame) *executorv1.AttachResponse {
	return &executorv1.AttachResponse{Frame: &executorv1.AttachResponse_StepAck{StepAck: ack}}
}

// okAck is a fresh commit the scheduler wrote.
func okAck(commit *executorv1.StepCommitFrame) *executorv1.AttachResponse {
	return stepAckFrame(&executorv1.StepAckFrame{
		JobId: commit.GetJobId(), Seq: commit.GetSeq(), Ok: true,
		WakeAt: commit.GetWakeAt(),
	})
}

// refusedAck carries a verdict and a message, the way every refusal does.
func refusedAck(commit *executorv1.StepCommitFrame, verdict executorv1.StepFailure, message string) *executorv1.AttachResponse {
	return stepAckFrame(&executorv1.StepAckFrame{
		JobId: commit.GetJobId(), Seq: commit.GetSeq(),
		Ok: false, Error: &message, Failure: verdict,
	})
}

func snapshotOf(t *testing.T, records ...step.Record) []byte {
	t.Helper()
	payload, err := step.EncodeSnapshot(records)
	if err != nil {
		t.Fatalf("EncodeSnapshot: %v", err)
	}
	return payload
}

func mustEncodeResult(t *testing.T, value any) []byte {
	t.Helper()
	encoded, err := flexiq.EncodeResult(value)
	if err != nil {
		t.Fatalf("EncodeResult: %v", err)
	}
	return encoded
}

// stepDispatch scripts a stream that hands over one job with a snapshot and
// then answers its step commits with answer.
//
// answer returning nil sends nothing back, which is how a test about an
// acknowledgement that never comes is written.
func stepDispatch(t *testing.T, snapshot []byte,
	answer func(*executorv1.StepCommitFrame) *executorv1.AttachResponse,
	capabilities ...string,
) func(int, *schedulerStream) error {
	t.Helper()
	return func(_ int, s *schedulerStream) error {
		if err := s.handshake(capabilities...); err != nil {
			return err
		}
		if snapshot != nil {
			if err := s.send(jobStepsFrame(stepJobID, snapshot)); err != nil {
				return err
			}
		}
		job := jobFrame(stepJobID, stepTask, mustEncodeCall(t))
		job.GetJob().Lease = stepLease
		if err := s.send(job); err != nil {
			return err
		}

		for {
			req, ok := s.recvOrEnd()
			if !ok {
				return nil
			}
			commit := req.GetStepCommit()
			if commit == nil {
				continue
			}
			if reply := answer(commit); reply != nil {
				if err := s.send(reply); err != nil {
					return err
				}
			}
		}
	}
}

// alwaysOK answers every commit with a fresh write, which is the ordinary case.
func alwaysOK(commit *executorv1.StepCommitFrame) *executorv1.AttachResponse { return okAck(commit) }

func stepCommits(frames []*executorv1.AttachRequest) []*executorv1.StepCommitFrame {
	var commits []*executorv1.StepCommitFrame
	for _, frame := range frames {
		if commit := frame.GetStepCommit(); commit != nil {
			commits = append(commits, commit)
		}
	}
	return commits
}

// sleptOf finds the slept frame for the one job these tests dispatch. It is a
// settling frame like any other, but settled deliberately does not look for
// one — a slept attempt is neither a success nor a failure.
func sleptOf(frames []*executorv1.AttachRequest) *executorv1.SleptFrame {
	for _, frame := range frames {
		if slept := frame.GetSlept(); slept != nil && slept.GetJobId() == stepJobID {
			return slept
		}
	}
	return nil
}

// runsSteps registers one step-using handler and starts the worker against the
// double.
func runsSteps(t *testing.T, fake *fakeScheduler, handler executor.Handler, opts ...executor.Option) {
	t.Helper()
	w := serveExecutor(t, fake, opts...)
	mustHandle(t, w, stepTask, handler)
	runWorker(t, w)
}

func stepCapabilities() []string {
	return []string{executor.CapLease, executor.CapSideChannel, executor.CapSteps}
}

func TestANewStepRunsItsBodyAndCommitsTheResult(t *testing.T) {
	fake := &fakeScheduler{attach: stepDispatch(t, nil, alwaysOK, stepCapabilities()...)}

	var handedKey string
	runsSteps(t, fake, func(ctx context.Context, job *executor.Job) (any, error) {
		return executor.Step(ctx, job, "charge", func(_ context.Context, key string) (string, error) {
			handedKey = key
			return "receipt-1", nil
		})
	})

	frame := awaitSettled(t, fake, stepJobID)
	if frame.GetSuccess() == nil {
		t.Fatalf("settled with %T, want a success", frame.GetFrame())
	}

	commits := stepCommits(fake.frames())
	if len(commits) != 1 {
		t.Fatalf("sent %d step commits, want 1", len(commits))
	}
	commit := commits[0]
	if commit.GetSeq() != 0 {
		t.Errorf("seq = %d, want 0", commit.GetSeq())
	}
	if commit.GetStepKey() != "charge#0" {
		t.Errorf("step_key = %q, want %q", commit.GetStepKey(), "charge#0")
	}
	if commit.GetKind() != executorv1.StepKind_STEP_KIND_RUN {
		t.Errorf("kind = %v, want RUN", commit.GetKind())
	}
	var committed string
	if err := flexiq.DecodeResult(commit.GetPayload(), &committed); err != nil {
		t.Fatalf("the committed payload does not decode: %v", err)
	}
	if committed != "receipt-1" {
		t.Errorf("committed %q, want %q", committed, "receipt-1")
	}

	// The downstream key is the run and the step, and nothing else: no clock,
	// no payload, no codec. It is what closes the window memoization cannot.
	if want := stepJobID + ":charge#0"; handedKey != want {
		t.Errorf("the body was handed key %q, want %q", handedKey, want)
	}
}

// The whole point: the recorded result comes back and the body never runs.
func TestARecordedStepReplaysWithoutRunningItsBody(t *testing.T) {
	snapshot := snapshotOf(t, step.Record{
		Seq: 0, StepKey: "charge#0", Kind: step.KindRun,
		Result: mustEncodeResult(t, "receipt-1"), CreatedAt: 1,
	})
	fake := &fakeScheduler{attach: stepDispatch(t, snapshot, alwaysOK, stepCapabilities()...)}

	ran := false
	runsSteps(t, fake, func(ctx context.Context, job *executor.Job) (any, error) {
		return executor.Step(ctx, job, "charge", func(context.Context, string) (string, error) {
			ran = true
			return "receipt-2", nil
		})
	})

	frame := awaitSettled(t, fake, stepJobID)
	if ran {
		t.Fatal("the body ran against a recorded step; that is the double charge this exists to prevent")
	}
	if commits := stepCommits(fake.frames()); len(commits) != 0 {
		t.Fatalf("a memo hit sent %d commit(s), want none", len(commits))
	}

	var replayed string
	if err := flexiq.DecodeResult(frame.GetSuccess().GetResult(), &replayed); err != nil {
		t.Fatalf("the result does not decode: %v", err)
	}
	if replayed != "receipt-1" {
		t.Errorf("the task returned %q, want the recorded %q", replayed, "receipt-1")
	}
}

// A byte-identical retransmission after a lost acknowledgement. The scheduler
// says so, and saying so is a success.
func TestAnAlreadyCommittedStepIsASuccess(t *testing.T) {
	fake := &fakeScheduler{attach: stepDispatch(t, nil, func(commit *executorv1.StepCommitFrame) *executorv1.AttachResponse {
		return stepAckFrame(&executorv1.StepAckFrame{
			JobId: commit.GetJobId(), Seq: commit.GetSeq(), Ok: true, Already: true,
		})
	}, stepCapabilities()...)}

	runsSteps(t, fake, func(ctx context.Context, job *executor.Job) (any, error) {
		return executor.Step(ctx, job, "charge", func(context.Context, string) (string, error) {
			return "receipt-1", nil
		})
	})

	if frame := awaitSettled(t, fake, stepJobID); frame.GetSuccess() == nil {
		t.Fatalf("settled with %T, want a success", frame.GetFrame())
	}
}

func TestAStepCommitCarriesTheDispatchLease(t *testing.T) {
	fake := &fakeScheduler{attach: stepDispatch(t, nil, alwaysOK, stepCapabilities()...)}

	runsSteps(t, fake, func(ctx context.Context, job *executor.Job) (any, error) {
		return executor.Step(ctx, job, "charge", func(context.Context, string) (any, error) {
			return nil, nil
		})
	})

	awaitSettled(t, fake, stepJobID)
	commits := stepCommits(fake.frames())
	if len(commits) != 1 {
		t.Fatalf("sent %d step commits, want 1", len(commits))
	}
	// A step commit advances the attempt, so it is checked for a lease like
	// every other frame that does. One without is read as a stale attempt and
	// dropped.
	if got := commits[0].GetLease(); string(got) != string(stepLease) {
		t.Errorf("the commit carried lease %q, want %q", got, stepLease)
	}
}

func TestARefusedStepCarriesItsVerdictIntoTheSettlement(t *testing.T) {
	for _, tc := range []struct {
		name        string
		verdict     executorv1.StepFailure
		sentinel    error
		shouldRetry bool
	}{
		{"retryable", executorv1.StepFailure_STEP_FAILURE_RETRYABLE, executor.ErrStepRetryable, true},
		{"permanent", executorv1.StepFailure_STEP_FAILURE_PERMANENT, executor.ErrStepPermanent, false},
		// An unrecognised verdict reads as retryable: nothing confirmed the
		// write landed, so a replay is the safe reading.
		{"unspecified", executorv1.StepFailure_STEP_FAILURE_UNSPECIFIED, executor.ErrStepRetryable, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeScheduler{attach: stepDispatch(t, nil, func(commit *executorv1.StepCommitFrame) *executorv1.AttachResponse {
				return refusedAck(commit, tc.verdict, "the step store said no")
			}, stepCapabilities()...)}

			var caught error
			runsSteps(t, fake, func(ctx context.Context, job *executor.Job) (any, error) {
				_, err := executor.Step(ctx, job, "charge", func(context.Context, string) (any, error) {
					return nil, nil
				})
				caught = err
				return nil, err
			})

			frame := awaitSettled(t, fake, stepJobID)
			failure := frame.GetFailure()
			if failure == nil {
				t.Fatalf("settled with %T, want a failure", frame.GetFrame())
			}
			if failure.GetShouldRetry() != tc.shouldRetry {
				t.Errorf("should_retry = %v, want %v", failure.GetShouldRetry(), tc.shouldRetry)
			}
			if !errors.Is(caught, tc.sentinel) {
				t.Errorf("the handler caught %v, want it to answer to the %s verdict", caught, tc.name)
			}
			// The verdict crosses as an enum and the message as prose. A client
			// that read the verdict back out of the message would break the
			// first time the message was reworded.
			if !strings.Contains(failure.GetError(), "the step store said no") {
				t.Errorf("the failure does not carry the scheduler's reason: %s", failure.GetError())
			}
		})
	}
}

// Another attempt owns this job now, so this one must stop without writing. A
// frame either way would be this attempt writing over the one that replaced it.
func TestASupersededStepSettlesWithNoFrameAtAll(t *testing.T) {
	fake := &fakeScheduler{attach: stepDispatch(t, nil, func(commit *executorv1.StepCommitFrame) *executorv1.AttachResponse {
		return refusedAck(commit, executorv1.StepFailure_STEP_FAILURE_SUPERSEDED,
			"the execution claim for this job was lost")
	}, stepCapabilities()...)}

	returned := make(chan struct{})
	runsSteps(t, fake, func(ctx context.Context, job *executor.Job) (any, error) {
		defer close(returned)
		_, err := executor.Step(ctx, job, "charge", func(context.Context, string) (any, error) {
			return nil, nil
		})
		if !errors.Is(err, executor.ErrStepSuperseded) {
			return nil, err
		}
		return nil, err
	})

	await(t, fake, "the handler to return", func() bool {
		select {
		case <-returned:
			return true
		default:
			return false
		}
	})
	// Several settle-and-write cycles, the way the no-heartbeat test waits out
	// several intervals: proving an absence needs time for the thing to have
	// appeared.
	time.Sleep(200 * time.Millisecond)

	if frame := settled(fake.frames(), stepJobID); frame != nil {
		t.Fatalf("a superseded attempt sent %T; it must write nothing at all", frame.GetFrame())
	}
	if slept := sleptOf(fake.frames()); slept != nil {
		t.Fatal("a superseded attempt sent a slept frame; it must write nothing at all")
	}
}

// A later refusal must not demote a superseded one.
//
// A body that caught the superseded commit and asked for another step gets an
// ordinary refusal back for the one left uncommitted. Letting that overwrite the
// latch would settle the stale attempt — as a success here, as a failure frame
// if the body returned an error — and either is this attempt writing over the
// one that replaced it.
func TestAnOrdinaryRefusalDoesNotDemoteASupersededOne(t *testing.T) {
	fake := &fakeScheduler{attach: stepDispatch(t, nil, func(commit *executorv1.StepCommitFrame) *executorv1.AttachResponse {
		return refusedAck(commit, executorv1.StepFailure_STEP_FAILURE_SUPERSEDED,
			"the execution claim for this job was lost")
	}, stepCapabilities()...)}

	returned := make(chan struct{})
	var second error
	runsSteps(t, fake, func(ctx context.Context, job *executor.Job) (any, error) {
		defer close(returned)
		_, _ = executor.Step(ctx, job, "charge", func(context.Context, string) (any, error) {
			return nil, nil
		})
		// Caught, ignored, and on to the next one — which the sequence refuses
		// because the first is still uncommitted.
		_, second = executor.Step(ctx, job, "notify", func(context.Context, string) (any, error) {
			return nil, nil
		})
		return "carried on past both", nil
	})

	await(t, fake, "the handler to return", func() bool {
		select {
		case <-returned:
			return true
		default:
			return false
		}
	})
	time.Sleep(200 * time.Millisecond)

	if second == nil {
		t.Fatal("the second step was accepted while the first was still uncommitted")
	}
	if errors.Is(second, executor.ErrStepSuperseded) {
		t.Fatalf("the second refusal was superseded (%v); this test needs an ordinary one to overwrite with", second)
	}
	if frame := settled(fake.frames(), stepJobID); frame != nil {
		t.Fatalf("a superseded attempt sent %T; it must write nothing at all", frame.GetFrame())
	}
	if slept := sleptOf(fake.frames()); slept != nil {
		t.Fatal("a superseded attempt sent a slept frame; it must write nothing at all")
	}
}

// An acknowledgement without a deadline is a broken scheduler, not a deadline
// to invent: the candidate this call proposed is not what the job was
// rescheduled to, and on a replay it is a different time entirely.
func TestASleepAcknowledgedWithoutADeadlineFailsRetryably(t *testing.T) {
	fake := &fakeScheduler{attach: stepDispatch(t, nil, func(commit *executorv1.StepCommitFrame) *executorv1.AttachResponse {
		return stepAckFrame(&executorv1.StepAckFrame{
			JobId: commit.GetJobId(), Seq: commit.GetSeq(), Ok: true,
		})
	}, stepCapabilities()...)}

	var caught error
	runsSteps(t, fake, func(ctx context.Context, job *executor.Job) (any, error) {
		caught = job.Sleep(ctx, "cooloff", time.Hour)
		return nil, caught
	})

	// Either frame ends the attempt, so wait on whichever came and then say
	// which it was — otherwise inventing a deadline reads as a timeout rather
	// than as the wrong frame.
	await(t, fake, "the attempt to end", func() bool {
		return settled(fake.frames(), stepJobID) != nil || sleptOf(fake.frames()) != nil
	})
	if slept := sleptOf(fake.frames()); slept != nil {
		t.Fatalf("wrote a slept frame for a deadline nothing confirmed: %v", slept.GetWakeAt().AsTime())
	}

	frame := settled(fake.frames(), stepJobID)
	// Retryable: the commit may well have landed, and a replay of it comes back
	// as already.
	if failure := frame.GetFailure(); failure == nil || !failure.GetShouldRetry() {
		t.Fatalf("settled with %v, want a retryable failure", frame.GetFrame())
	}
	if !errors.Is(caught, executor.ErrStepRetryable) {
		t.Fatalf("the handler caught %v, want a retryable step error", caught)
	}
	if !strings.Contains(caught.Error(), "without a deadline") {
		t.Errorf("the error does not say what was missing: %v", caught)
	}
}

// An unconfirmed commit is indistinguishable from one that never happened, so
// running out of budget is retryable and the replay re-runs the step under the
// same downstream key.
func TestAnAcknowledgementThatNeverComesFailsRetryably(t *testing.T) {
	fake := &fakeScheduler{attach: stepDispatch(t, nil,
		func(*executorv1.StepCommitFrame) *executorv1.AttachResponse { return nil },
		stepCapabilities()...)}

	var caught error
	runsSteps(t, fake, func(ctx context.Context, job *executor.Job) (any, error) {
		_, err := executor.Step(ctx, job, "charge", func(context.Context, string) (any, error) {
			return nil, nil
		})
		caught = err
		return nil, err
	}, executor.WithStepAckTimeout(80*time.Millisecond))

	frame := awaitSettled(t, fake, stepJobID)
	if failure := frame.GetFailure(); failure == nil || !failure.GetShouldRetry() {
		t.Fatalf("settled with %v, want a retryable failure", frame.GetFrame())
	}
	if !errors.Is(caught, executor.ErrStepRetryable) {
		t.Fatalf("the handler caught %v, want a retryable step error", caught)
	}
	if !strings.Contains(caught.Error(), "did not acknowledge") {
		t.Errorf("the error does not say what went wrong: %v", caught)
	}
}

// The capability that fails rather than degrades. Running un-memoized would be
// a charge step that silently lost its memo.
func TestAStepWithoutTheCapabilityFailsRetryablyAndNamesIt(t *testing.T) {
	fake := &fakeScheduler{attach: stepDispatch(t, nil, alwaysOK,
		executor.CapLease, executor.CapSideChannel)}

	ran := false
	var caught error
	runsSteps(t, fake, func(ctx context.Context, job *executor.Job) (any, error) {
		_, err := executor.Step(ctx, job, "charge", func(context.Context, string) (any, error) {
			ran = true
			return nil, nil
		})
		caught = err
		return nil, err
	})

	frame := awaitSettled(t, fake, stepJobID)
	if ran {
		t.Fatal("the body ran with no step store behind it")
	}
	if failure := frame.GetFailure(); failure == nil || !failure.GetShouldRetry() {
		t.Fatalf("settled with %v, want a retryable failure", frame.GetFrame())
	}
	if !errors.Is(caught, executor.ErrStepUnavailable) || !errors.Is(caught, executor.ErrStepRetryable) {
		t.Fatalf("the handler caught %v, want a retryable unavailable error", caught)
	}
	if !strings.Contains(caught.Error(), "step store") {
		t.Errorf("the error does not name the missing capability: %v", caught)
	}
	if commits := stepCommits(fake.frames()); len(commits) != 0 {
		t.Fatalf("sent %d commit(s) for a capability that was never acknowledged", len(commits))
	}
}

// A snapshot that came back short must never read as "no steps recorded": that
// answer re-runs a charge.
func TestADamagedSnapshotFailsTheStepRatherThanReadingAsEmpty(t *testing.T) {
	whole := snapshotOf(t, step.Record{
		Seq: 0, StepKey: "charge#0", Kind: step.KindRun,
		Result: mustEncodeResult(t, "receipt-1"), CreatedAt: 1,
	})
	fake := &fakeScheduler{attach: stepDispatch(t, whole[:len(whole)-1], alwaysOK, stepCapabilities()...)}

	ran := false
	var caught error
	runsSteps(t, fake, func(ctx context.Context, job *executor.Job) (any, error) {
		_, err := executor.Step(ctx, job, "charge", func(context.Context, string) (any, error) {
			ran = true
			return nil, nil
		})
		caught = err
		return nil, err
	})

	frame := awaitSettled(t, fake, stepJobID)
	if ran {
		t.Fatal("the body ran against a snapshot that did not decode")
	}
	// Retryable, not permanent: a damaged dispatch says nothing about whether
	// the code and the recorded sequence agree, and the next one may arrive
	// whole.
	if failure := frame.GetFailure(); failure == nil || !failure.GetShouldRetry() {
		t.Fatalf("settled with %v, want a retryable failure", frame.GetFrame())
	}
	if !errors.Is(caught, executor.ErrStepRetryable) {
		t.Fatalf("the handler caught %v, want a retryable step error", caught)
	}
	if !strings.Contains(caught.Error(), "truncated") {
		t.Errorf("the error does not say the snapshot was damaged: %v", caught)
	}
}

// The same rule for the one damaged shape Go's json package accepts in silence.
// Read as empty, a null metadata line runs every recorded step body again.
func TestANullSnapshotFailsTheStepRatherThanReadingAsEmpty(t *testing.T) {
	fake := &fakeScheduler{attach: stepDispatch(t, []byte("null\n"), alwaysOK, stepCapabilities()...)}

	ran := false
	var caught error
	runsSteps(t, fake, func(ctx context.Context, job *executor.Job) (any, error) {
		_, err := executor.Step(ctx, job, "charge", func(context.Context, string) (any, error) {
			ran = true
			return nil, nil
		})
		caught = err
		return nil, err
	})

	frame := awaitSettled(t, fake, stepJobID)
	if ran {
		t.Fatal("the body ran against a null snapshot")
	}
	if failure := frame.GetFailure(); failure == nil || !failure.GetShouldRetry() {
		t.Fatalf("settled with %v, want a retryable failure", frame.GetFrame())
	}
	if !errors.Is(caught, executor.ErrStepRetryable) {
		t.Fatalf("the handler caught %v, want a retryable step error", caught)
	}
	if !strings.Contains(caught.Error(), "null metadata line") {
		t.Errorf("the error does not say the snapshot was damaged: %v", caught)
	}
}

func TestASleepCommitsItsDeadlineAndThenEndsTheAttempt(t *testing.T) {
	// The deadline storage settled on, deliberately not the one proposed.
	settledAt := time.Now().Add(90 * time.Minute).UTC().Truncate(time.Second)
	fake := &fakeScheduler{attach: stepDispatch(t, nil, func(commit *executorv1.StepCommitFrame) *executorv1.AttachResponse {
		return stepAckFrame(&executorv1.StepAckFrame{
			JobId: commit.GetJobId(), Seq: commit.GetSeq(), Ok: true,
			WakeAt: timestamppb.New(settledAt),
		})
	}, stepCapabilities()...)}

	runsSteps(t, fake, func(ctx context.Context, job *executor.Job) (any, error) {
		if err := job.Sleep(ctx, "cooloff", time.Hour); err != nil {
			return nil, err
		}
		return "never reached", nil
	})

	await(t, fake, "a slept frame", func() bool { return sleptOf(fake.frames()) != nil })

	commits := stepCommits(fake.frames())
	if len(commits) != 1 {
		t.Fatalf("sent %d step commits, want 1", len(commits))
	}
	commit := commits[0]
	if commit.GetKind() != executorv1.StepKind_STEP_KIND_SLEEP {
		t.Errorf("kind = %v, want SLEEP", commit.GetKind())
	}
	if len(commit.GetPayload()) != 0 {
		t.Errorf("a sleep committed %d payload byte(s); it commits none", len(commit.GetPayload()))
	}
	if commit.GetStepKey() != "cooloff#0" {
		t.Errorf("step_key = %q, want %q", commit.GetStepKey(), "cooloff#0")
	}

	slept := sleptOf(fake.frames())
	// The acknowledgement's deadline, never the candidate: on a replay they are
	// different numbers and the job was rescheduled to the stored one.
	if got := slept.GetWakeAt().AsTime(); !got.Equal(settledAt) {
		t.Errorf("slept at %s, want the deadline the acknowledgement settled on, %s", got, settledAt)
	}
	if settling := settled(fake.frames(), stepJobID); settling != nil {
		t.Fatalf("a slept attempt also sent %T", settling.GetFrame())
	}
}

// What makes a job with three sleeps not restart the first one on the third
// wake.
func TestAnElapsedSleepSendsNothingAndTheAttemptCarriesOn(t *testing.T) {
	past := time.Now().Add(-time.Hour).UnixMilli()
	snapshot := snapshotOf(t, step.Record{
		Seq: 0, StepKey: "cooloff#0", Kind: step.KindSleep, WakeAt: &past, CreatedAt: 1,
	})
	fake := &fakeScheduler{attach: stepDispatch(t, snapshot, alwaysOK, stepCapabilities()...)}

	runsSteps(t, fake, func(ctx context.Context, job *executor.Job) (any, error) {
		if err := job.Sleep(ctx, "cooloff", time.Hour); err != nil {
			return nil, err
		}
		return "carried on", nil
	})

	frame := awaitSettled(t, fake, stepJobID)
	if frame.GetSuccess() == nil {
		t.Fatalf("settled with %T, want the success of an attempt that carried on", frame.GetFrame())
	}
	if commits := stepCommits(fake.frames()); len(commits) != 0 {
		t.Fatalf("an elapsed sleep sent %d commit(s), want none", len(commits))
	}
	if slept := sleptOf(fake.frames()); slept != nil {
		t.Fatal("an elapsed sleep ended the attempt; it is a memo hit")
	}
}

// A resume re-issues the sleep at its recorded position, so storage recognises
// the row and answers with the deadline it already holds. A fresh position
// would commit a second sleep and start the clock again.
func TestASleepThatHasNotElapsedResumesAtItsRecordedPosition(t *testing.T) {
	ahead := time.Now().Add(time.Hour).UnixMilli()
	snapshot := snapshotOf(t,
		step.Record{Seq: 0, StepKey: "charge#0", Kind: step.KindRun, Result: mustEncodeResult(t, "r"), CreatedAt: 1},
		step.Record{Seq: 1, StepKey: "cooloff#0", Kind: step.KindSleep, WakeAt: &ahead, CreatedAt: 2},
	)
	fake := &fakeScheduler{attach: stepDispatch(t, snapshot, func(commit *executorv1.StepCommitFrame) *executorv1.AttachResponse {
		return stepAckFrame(&executorv1.StepAckFrame{
			JobId: commit.GetJobId(), Seq: commit.GetSeq(), Ok: true, Already: true,
			WakeAt: timestamppb.New(time.UnixMilli(ahead)),
		})
	}, stepCapabilities()...)}

	runsSteps(t, fake, func(ctx context.Context, job *executor.Job) (any, error) {
		if _, err := executor.Step(ctx, job, "charge", func(context.Context, string) (string, error) {
			return "unreachable", nil
		}); err != nil {
			return nil, err
		}
		return nil, job.Sleep(ctx, "cooloff", time.Hour)
	})

	await(t, fake, "a slept frame", func() bool { return sleptOf(fake.frames()) != nil })

	commits := stepCommits(fake.frames())
	if len(commits) != 1 {
		t.Fatalf("sent %d step commits, want 1 — the memoized charge commits nothing", len(commits))
	}
	if got := commits[0].GetSeq(); got != 1 {
		t.Errorf("the resume took seq %d, want the recorded 1", got)
	}
	if got := sleptOf(fake.frames()).GetWakeAt().AsTime(); !got.Equal(time.UnixMilli(ahead).UTC()) {
		t.Errorf("slept to %s, want the stored deadline %s", got, time.UnixMilli(ahead).UTC())
	}
}

// A Go task body can ignore an error. The claim is gone either way, so the
// attempt ends in a sleep whether the signal was propagated or not.
func TestASwallowedSleepStillEndsTheAttempt(t *testing.T) {
	fake := &fakeScheduler{attach: stepDispatch(t, nil, alwaysOK, stepCapabilities()...)}

	runsSteps(t, fake, func(ctx context.Context, job *executor.Job) (any, error) {
		_ = job.Sleep(ctx, "cooloff", time.Hour)
		return "carried on past the sleep", nil
	})

	await(t, fake, "a slept frame", func() bool { return sleptOf(fake.frames()) != nil })
	if frame := settled(fake.frames(), stepJobID); frame != nil {
		t.Fatalf("a swallowed sleep settled with %T as well", frame.GetFrame())
	}
}

// A body that catches a refused step and returns a value anyway is taken at its
// word. Only it knows whether the work it was asked to do is done; the cost — a
// completed job whose step sequence has a gap in it — is the caller's, and
// returning the error is the supported way to decline it.
//
// The outcomes that are *not* the body's to decide have their own tests: a
// slept attempt is over, a superseded one writes nothing at all, and a
// divergence fails whatever the body does.
func TestARefusedStepTheBodyReturnedPastIsTakenAtItsWord(t *testing.T) {
	fake := &fakeScheduler{attach: stepDispatch(t, nil, func(commit *executorv1.StepCommitFrame) *executorv1.AttachResponse {
		return refusedAck(commit, executorv1.StepFailure_STEP_FAILURE_PERMANENT, "the step store said no")
	}, stepCapabilities()...)}

	var caught error
	runsSteps(t, fake, func(ctx context.Context, job *executor.Job) (any, error) {
		_, caught = executor.Step(ctx, job, "charge", func(context.Context, string) (any, error) {
			return nil, nil
		})
		return "handled it myself", nil
	})

	frame := awaitSettled(t, fake, stepJobID)
	if frame.GetSuccess() == nil {
		t.Fatalf("settled with %T, want the success the body returned", frame.GetFrame())
	}
	// The error still said what happened. The body simply chose not to
	// propagate it, which is a decision this client does not overrule.
	if !errors.Is(caught, executor.ErrStepPermanent) {
		t.Fatalf("the body caught %v, want a permanent step error", caught)
	}

	var decoded string
	if err := flexiq.DecodeResult(frame.GetSuccess().GetResult(), &decoded); err != nil {
		t.Fatalf("the result does not decode: %v", err)
	}
	if decoded != "handled it myself" {
		t.Errorf("result = %q, want the value the body returned", decoded)
	}
}

// A changed step sequence is caught against the snapshot, before the body runs
// — rather than after it has charged a card.
func TestAChangedSequenceDivergesBeforeTheBodyRuns(t *testing.T) {
	snapshot := snapshotOf(t, step.Record{
		Seq: 0, StepKey: "charge#0", Kind: step.KindRun,
		Result: mustEncodeResult(t, "receipt-1"), CreatedAt: 1,
	})
	fake := &fakeScheduler{attach: stepDispatch(t, snapshot, alwaysOK, stepCapabilities()...)}

	ran := false
	var caught error
	runsSteps(t, fake, func(ctx context.Context, job *executor.Job) (any, error) {
		_, err := executor.Step(ctx, job, "refund", func(context.Context, string) (any, error) {
			ran = true
			return nil, nil
		})
		caught = err
		return nil, err
	})

	frame := awaitSettled(t, fake, stepJobID)
	if ran {
		t.Fatal("the body of a diverged step ran")
	}
	// Permanent: the recorded rows and the deployed code will not line up next
	// attempt either.
	if failure := frame.GetFailure(); failure == nil || failure.GetShouldRetry() {
		t.Fatalf("settled with %v, want a permanent failure", frame.GetFrame())
	}
	if !errors.Is(caught, executor.ErrStepDiverged) ||
		!errors.Is(caught, executor.ErrStepPermanent) ||
		!errors.Is(caught, executor.ErrFatal) {
		t.Fatalf("the handler caught %v, want a permanent divergence", caught)
	}
	if !strings.Contains(caught.Error(), "step sequence changed") {
		t.Errorf("the error does not name the divergence: %v", caught)
	}
}

// The one refusal a task body may not carry on past. Go has no exception tier
// `catch` cannot reach, so the check happens when the handler returns.
func TestADivergenceTheBodyReturnedPastStillFailsTheAttempt(t *testing.T) {
	snapshot := snapshotOf(t, step.Record{
		Seq: 0, StepKey: "charge#0", Kind: step.KindRun,
		Result: mustEncodeResult(t, "receipt-1"), CreatedAt: 1,
	})
	fake := &fakeScheduler{attach: stepDispatch(t, snapshot, alwaysOK, stepCapabilities()...)}

	runsSteps(t, fake, func(ctx context.Context, job *executor.Job) (any, error) {
		_, _ = executor.Step(ctx, job, "refund", func(context.Context, string) (any, error) {
			return nil, nil
		})
		return "carried on past the divergence", nil
	})

	frame := awaitSettled(t, fake, stepJobID)
	failure := frame.GetFailure()
	if failure == nil {
		t.Fatalf("settled with %T, want the divergence", frame.GetFrame())
	}
	// Permanent: the next attempt reads the same rows and runs the same code.
	if failure.GetShouldRetry() {
		t.Error("should_retry = true; a divergence is the same answer every attempt")
	}
	recorded := flexiq.ParseTaskError(failure.GetError())
	if recorded.Type != "StepDivergedError" {
		t.Errorf("errtype = %q, want %q", recorded.Type, "StepDivergedError")
	}
	if !strings.Contains(recorded.Message, "step sequence changed") {
		t.Errorf("the failure does not name the divergence: %s", recorded.Message)
	}
}

// A keyed step is matched by its key wherever it sits, and new ground takes the
// number of rows already stored rather than the walk's position.
func TestAKeyedStepReplaysOutOfOrderAndANewOneTakesTheNextFreePosition(t *testing.T) {
	snapshot := snapshotOf(t,
		step.Record{Seq: 0, StepKey: "fetch:a", Kind: step.KindRun, Result: mustEncodeResult(t, "A"), CreatedAt: 1},
		step.Record{Seq: 1, StepKey: "fetch:b", Kind: step.KindRun, Result: mustEncodeResult(t, "B"), CreatedAt: 2},
	)
	fake := &fakeScheduler{attach: stepDispatch(t, snapshot, alwaysOK, stepCapabilities()...)}

	var replayed []string
	runsSteps(t, fake, func(ctx context.Context, job *executor.Job) (any, error) {
		for _, key := range []string{"b", "a", "c"} {
			value, err := executor.StepKeyed(ctx, job, "fetch", key,
				func(_ context.Context, idem string) (string, error) {
					return "fresh-" + idem, nil
				})
			if err != nil {
				return nil, err
			}
			replayed = append(replayed, value)
		}
		return replayed, nil
	})

	awaitSettled(t, fake, stepJobID)

	want := []string{"B", "A", "fresh-" + stepJobID + ":fetch:c"}
	if len(replayed) != len(want) {
		t.Fatalf("the task saw %v, want %v", replayed, want)
	}
	for i := range want {
		if replayed[i] != want[i] {
			t.Fatalf("the task saw %v, want %v", replayed, want)
		}
	}

	commits := stepCommits(fake.frames())
	if len(commits) != 1 {
		t.Fatalf("sent %d commits, want 1 — two of the three were memo hits", len(commits))
	}
	// Two rows are stored, so the new one is seq 2 — not the cursor, which the
	// out-of-order keyed hits left at 0.
	if got := commits[0].GetSeq(); got != 2 {
		t.Errorf("the new step took seq %d, want 2", got)
	}
}

// The check that holds is the scheduler's, inside its own transaction. This one
// buys an error that names the step and the number that failed.
func TestAnOverCapStepIsRefusedBeforeTheRoundTrip(t *testing.T) {
	fake := &fakeScheduler{attach: stepDispatch(t, nil, alwaysOK, stepCapabilities()...)}

	var caught error
	runsSteps(t, fake, func(ctx context.Context, job *executor.Job) (any, error) {
		_, err := executor.Step(ctx, job, "blob", func(context.Context, string) (string, error) {
			return strings.Repeat("x", 64), nil
		})
		caught = err
		return nil, err
	}, executor.WithStepLimits(executor.StepLimits{MaxStepBytes: 16}))

	frame := awaitSettled(t, fake, stepJobID)
	if failure := frame.GetFailure(); failure == nil || failure.GetShouldRetry() {
		t.Fatalf("settled with %v, want a permanent failure", frame.GetFrame())
	}
	if !errors.Is(caught, executor.ErrStepPermanent) {
		t.Fatalf("the handler caught %v, want a permanent step error", caught)
	}
	if !strings.Contains(caught.Error(), "exceeds the step bytes limit") {
		t.Errorf("the error does not name the cap: %v", caught)
	}
	if commits := stepCommits(fake.frames()); len(commits) != 0 {
		t.Fatalf("an over-cap step sent %d commit(s); it is refused before the round trip", len(commits))
	}
}

// The id the run began under, not the job's own, once an operator has
// resurrected it from the dead-letter queue — so its steps keep minting the
// keys they always have.
func TestTheDownstreamKeyFollowsTheRunAcrossADeadLetterRetry(t *testing.T) {
	fake := &fakeScheduler{attach: func(_ int, s *schedulerStream) error {
		if err := s.handshake(stepCapabilities()...); err != nil {
			return err
		}
		job := jobFrame(stepJobID, stepTask, mustEncodeCall(t))
		job.GetJob().Metadata = metadataBlob("__origin_job_id", "job-origin")
		if err := s.send(job); err != nil {
			return err
		}
		for {
			req, ok := s.recvOrEnd()
			if !ok {
				return nil
			}
			if commit := req.GetStepCommit(); commit != nil {
				if err := s.send(okAck(commit)); err != nil {
					return err
				}
			}
		}
	}}

	var handedKey, runKey string
	runsSteps(t, fake, func(ctx context.Context, job *executor.Job) (any, error) {
		runKey = job.RunKey()
		return executor.Step(ctx, job, "charge", func(_ context.Context, key string) (any, error) {
			handedKey = key
			return nil, nil
		})
	})

	awaitSettled(t, fake, stepJobID)
	if runKey != "job-origin" {
		t.Errorf("RunKey = %q, want the stamped origin %q", runKey, "job-origin")
	}
	if want := "job-origin:charge#0"; handedKey != want {
		t.Errorf("the body was handed key %q, want %q", handedKey, want)
	}
}

// metadataBlob builds the one-key metadata blob a retry_dead stamps.
func metadataBlob(key, value string) *string {
	blob := `{"` + key + `":"` + value + `"}`
	return &blob
}
