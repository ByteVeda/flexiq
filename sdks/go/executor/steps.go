package executor

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	flexiq "github.com/ByteVeda/flexiq/sdks/go/v2"
	executorv1 "github.com/ByteVeda/flexiq/sdks/go/v2/internal/pb/flexiq/executor/v1"
	"github.com/ByteVeda/flexiq/sdks/go/v2/internal/step"
)

// Step runs body once for this job and memoizes what it returned.
//
//	receipt, err := executor.Step(ctx, job, "charge",
//	    func(ctx context.Context, key string) (Receipt, error) {
//	        return gateway.Charge(ctx, order, key)
//	    })
//
// On a later attempt of the same job the body does **not** run: its recorded
// result is decoded and returned. That is the whole point — re-running it is
// the double charge durable steps exist to prevent.
//
// key is the downstream idempotency key for this step, "{run}:{name}#{n}", and
// it is the same string on every attempt. Memoization closes the replay window,
// not the crash window: between "the charge succeeded" and "the step row
// committed" the process can die, and only a key the other service dedupes on
// closes that. Hand this one to any API that takes one.
//
// A package function rather than a method because a Go method cannot have a
// type parameter, and a step whose result decodes into your own type is worth
// more than one that hands back `any`.
//
// Steps are numbered by occurrence within an attempt, so they must be asked for
// in the same order every time. A loop over anything unordered wants
// [StepKeyed].
func Step[T any](ctx context.Context, job *Job, name string,
	body func(ctx context.Context, key string) (T, error),
) (T, error) {
	return runStep(ctx, job, name, nil, body)
}

// StepKeyed runs body once per key rather than once per position.
//
//	total, err := executor.StepKeyed(ctx, job, "refund", order.ID,
//	    func(ctx context.Context, key string) (int64, error) { ... })
//
// A keyed step is matched by its key wherever it sits in the recorded sequence,
// so a loop over a map — or anything else whose order is not guaranteed — can
// hand its steps back in a different order without every one of them looking
// like a different question. It never spends an occurrence, so adding one
// cannot shift the number of a later unkeyed step.
//
// Each key must be used at most once per attempt.
func StepKeyed[T any](ctx context.Context, job *Job, name, key string,
	body func(ctx context.Context, key string) (T, error),
) (T, error) {
	return runStep(ctx, job, name, &key, body)
}

// Sleep ends this attempt and reschedules the job to wake after d.
//
//	if err := job.Sleep(ctx, "cooloff", time.Hour); err != nil {
//	    return nil, err
//	}
//
// It returns [ErrStepSlept] once the deadline is committed, and the task body
// must propagate it: the row is written, the execution claim is released and
// the job is already pending at its deadline. Anything done past this point
// runs unclaimed and will run again on the wake.
//
// On the attempt that wakes, the same call returns nil and execution carries on
// from there — which is what stops a job with three sleeps restarting the first
// one on the third wake.
//
// The clock is read once, here. A replay keeps the deadline the row already
// holds rather than pushing it a full duration further out, so a job that
// crashes into its own sleep does not sleep forever.
func (j *Job) Sleep(ctx context.Context, name string, d time.Duration) error {
	now := time.Now()
	return j.sleepAt(ctx, name, now, now.Add(d))
}

// SleepUntil is [Job.Sleep] against an absolute instant, for a deadline that
// means something outside the job.
func (j *Job) SleepUntil(ctx context.Context, name string, at time.Time) error {
	return j.sleepAt(ctx, name, time.Now(), at)
}

// RunKey is the id this durable run began under.
//
// The job's own id for one that has only ever been retried in place — an
// ordinary retry, a requeue and a sleep wake all keep it. The id it started
// with for one an operator resurrected from the dead-letter queue, which is the
// single boundary where a job's id changes. Every idempotency key this attempt
// mints is built from it.
func (j *Job) RunKey() string {
	if j.steps == nil {
		return j.ID
	}
	return j.steps.runKey
}

// jobSteps is one attempt's durable-step state.
//
// mu guards the sequence and the latch, and is never held across the task
// body or the round trip — so two goroutines stepping the same job get the
// sequence's own "started while step X is still uncommitted" refusal rather
// than a race.
type jobSteps struct {
	session *session
	job     *Job
	jobCtx  context.Context
	runKey  string
	limits  step.Limits

	// fault is a snapshot this dispatch could not use, and permanent says
	// whether it will heal. Kept rather than raised at dispatch: a job that
	// never asks for a step has no use for the snapshot and no reason to fail
	// over it.
	fault     error
	permanent bool

	mu       sync.Mutex
	sequence *step.Sequence
	// latched is the last refusal a step call returned, so a task body that
	// swallowed one cannot settle as a success.
	latched *StepError
	// sleptAt is the deadline the scheduler settled on, once a sleep committed.
	sleptAt *time.Time
}

// newJobSteps opens the step state for a dispatch, over the snapshot that
// arrived immediately before it.
func newJobSteps(jobCtx context.Context, s *session, job *Job, snapshot stepSnapshot) *jobSteps {
	steps := &jobSteps{
		session: s,
		job:     job,
		jobCtx:  jobCtx,
		runKey:  step.RunKey(job.ID, job.Metadata),
		limits:  s.cfg.stepLimits,
		fault:   snapshot.err,
	}
	if snapshot.err != nil {
		return steps
	}

	sequence, err := step.NewSequence(job.ID, snapshot.records)
	if err != nil {
		// A hole in the recorded sequence. Permanent, unlike a snapshot that
		// arrived damaged: the rows on disk will not heal, and every memo after
		// the hole would answer a different step's question.
		steps.fault = err
		steps.permanent = true
		return steps
	}
	steps.sequence = sequence
	return steps
}

// runStep is the one path both step forms take.
func runStep[T any](ctx context.Context, job *Job, name string, key *string,
	body func(ctx context.Context, key string) (T, error),
) (T, error) {
	var zero T

	steps, err := job.stepState()
	if err != nil {
		return zero, err
	}

	decision, err := steps.begin(name, key)
	if err != nil {
		return zero, steps.latch(localStepError(job.ID, "", err))
	}
	if decision.Memoized {
		var replayed T
		if err = flexiq.DecodeResult(decision.Result, &replayed); err != nil {
			// The bytes are on disk and will decode the same way next attempt.
			return zero, steps.latch(permanentStep(job.ID, decision.StepKey,
				"the recorded result does not decode: "+err.Error()))
		}
		return replayed, nil
	}

	pending := decision.Pending
	value, err := body(ctx, step.IdempotencyKey(steps.runKey, pending.StepKey()))
	if err != nil {
		// The sequence keeps its uncommitted step. The attempt is expected to
		// end here, and one that asks for another step instead is refused by
		// name rather than allowed to write past the gap.
		return zero, err
	}

	encoded, err := flexiq.EncodeResult(value)
	if err != nil {
		// The body already ran and its side effects already happened. Retrying
		// would repeat them and fail identically every time, so this is fatal.
		return zero, steps.latch(permanentStep(job.ID, pending.StepKey(),
			"the step returned a value that does not encode: "+err.Error()))
	}
	if err := steps.checkCaps(pending, len(encoded)); err != nil {
		return zero, steps.latch(localStepError(job.ID, pending.StepKey(), err))
	}
	if err := steps.commit(ctx, pending, encoded, nil); err != nil {
		return zero, err
	}
	return value, nil
}

// sleepAt is both sleeps, with the clock read exactly once by the caller.
func (j *Job) sleepAt(ctx context.Context, name string, now, wakeAt time.Time) error {
	steps, err := j.stepState()
	if err != nil {
		return err
	}

	decision, err := steps.beginSleep(name, now)
	if err != nil {
		return steps.latch(localStepError(j.ID, "", err))
	}
	if decision.Elapsed {
		// A memo hit. Nothing crosses the wire and the attempt carries on.
		return nil
	}
	if decision.Fresh {
		// Only new ground can be refused by the step-count cap: a resume writes
		// nothing to count.
		if err := steps.checkCaps(decision.Pending, 0); err != nil {
			return steps.latch(localStepError(j.ID, decision.Pending.StepKey(), err))
		}
	}

	if err := steps.commit(ctx, decision.Pending, nil, &wakeAt); err != nil {
		return err
	}
	return ErrStepSlept
}

// stepState refuses a step this attempt is in no position to take.
func (j *Job) stepState() (*jobSteps, error) {
	steps := j.steps
	if steps == nil || steps.session == nil {
		return nil, unavailableStep(j.ID)
	}
	if !steps.session.stepsAcked() {
		// Refused here rather than left to the scheduler: an executor that did
		// not advertise the capability is told nothing about a job's recorded
		// steps, so running the body would run it un-memoized.
		return nil, steps.latch(unavailableStep(j.ID))
	}
	if steps.fault != nil {
		if steps.permanent {
			return nil, steps.latch(permanentStep(j.ID, "", steps.fault.Error()))
		}
		return nil, steps.latch(snapshotStepError(j.ID, steps.fault))
	}
	return steps, nil
}

func (s *jobSteps) begin(name string, key *string) (step.Decision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if key != nil {
		return s.sequence.BeginRunKeyed(name, *key)
	}
	return s.sequence.BeginRun(name)
}

func (s *jobSteps) beginSleep(name string, now time.Time) (step.SleepDecision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.sequence.BeginSleep(name, now.UnixMilli())
}

func (s *jobSteps) checkCaps(pending step.Pending, encoded int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.sequence.CheckCaps(s.limits, pending, encoded)
}

// commit sends one step commit and blocks until the scheduler answers it.
//
// Blocking is the point: an unconfirmed commit is indistinguishable from one
// that never happened, and carrying on past it re-runs the step on the next
// attempt with the side effect already applied.
//
// wakeAt is the candidate deadline of a sleep, and nil for a run.
func (s *jobSteps) commit(ctx context.Context, pending step.Pending, encoded []byte, wakeAt *time.Time) error {
	ack, err := s.session.commitStep(ctx, s.jobCtx, s.job.ID, pending, encoded, wakeAt)
	if err != nil {
		// Nothing confirmed the write landed, so a replay is safe and the
		// commit stays idempotent if it did land after all.
		return s.latch(retryableStep(s.job.ID, pending.StepKey(), err.Error()))
	}
	if !ack.GetOk() {
		return s.latch(refusalFor(s.job.ID, pending.StepKey(), ack))
	}

	if pending.Kind() == step.KindSleep {
		return s.settleSleep(pending, *wakeAt, ack)
	}
	if err := s.advance(pending, len(encoded)); err != nil {
		return s.latch(localStepError(s.job.ID, pending.StepKey(), err))
	}
	return nil
}

func (s *jobSteps) advance(pending step.Pending, encoded int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.sequence.Commit(pending, encoded)
}

// settleSleep records the deadline storage settled on, which on a replay is not
// the one this call proposed.
func (s *jobSteps) settleSleep(pending step.Pending, candidate time.Time, ack *executorv1.StepAckFrame) error {
	settled := candidate
	if echoed := ack.GetWakeAt(); echoed != nil {
		settled = echoed.AsTime()
	}

	s.mu.Lock()
	// already means the deadline was on disk before this commit and the write
	// was a no-op, so counting it would put the sequence one ahead of storage.
	err := s.sequence.CommitSleep(pending, !ack.GetAlready())
	if err == nil {
		s.sleptAt = &settled
	}
	s.mu.Unlock()

	if err != nil {
		return s.latch(localStepError(s.job.ID, pending.StepKey(), err))
	}
	return nil
}

// latch remembers a refusal so a task body that returns past it cannot settle
// the job as a success, and hands it back for the caller to propagate.
func (s *jobSteps) latch(err *StepError) *StepError {
	s.mu.Lock()
	s.latched = err
	s.mu.Unlock()
	return err
}

// finish closes the attempt out, warning if its code no longer runs steps that
// are recorded for it.
//
// A warning, never a failure: those side effects already happened, and failing
// a job whose code legitimately shortened would be worse than a value nobody
// reads. The rows die with the job.
func (s *jobSteps) finish(log *slog.Logger) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.sequence == nil {
		return
	}
	if orphaned := s.sequence.OrphanedTail(); len(orphaned) > 0 {
		log.Warn("flexiq: this job has recorded steps its code no longer runs",
			"job_id", s.job.ID, "steps", orphaned)
	}
}

// resolution is what the latch has to say about how this attempt must settle.
func (s *jobSteps) resolution() (slept *time.Time, latched *StepError) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.sleptAt, s.latched
}

// sleptFrame ends the attempt without being a failure: the row is committed and
// the job is already scheduled at its deadline.
func sleptFrame(job *Job, wakeAt time.Time, wall *durationpb.Duration) *executorv1.AttachRequest {
	return &executorv1.AttachRequest{
		Frame: &executorv1.AttachRequest_Slept{Slept: &executorv1.SleptFrame{
			JobId:    job.ID,
			TaskName: job.TaskName,
			WakeAt:   timestamppb.New(wakeAt),
			WallTime: wall,
		}},
	}
}
