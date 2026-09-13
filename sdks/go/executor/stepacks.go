package executor

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	executorv1 "github.com/ByteVeda/flexiq/sdks/go/v2/internal/pb/flexiq/executor/v1"
	"github.com/ByteVeda/flexiq/sdks/go/v2/internal/step"
)

// stepSnapshot is what a job_steps frame carried, decoded on arrival.
//
// Decoded here rather than when the first step runs: a snapshot that will not
// parse is a fact about the dispatch, and finding out at the first step call
// would be finding out after the job started. The error is kept and raised
// there anyway, because a job that uses no steps has no reason to fail over it.
type stepSnapshot struct {
	records []step.Record
	err     error
}

// stepAckKey correlates one commit with its answer.
//
// (job id, seq) and not the job id alone: one stream runs many jobs, and one
// job commits many steps.
type stepAckKey struct {
	jobID string
	seq   int32
}

// rememberSnapshot keeps the steps a dispatch carried, for the job frame that
// follows it immediately.
func (s *session) rememberSnapshot(frame *executorv1.JobStepsFrame) {
	jobID := frame.GetJobId()
	records, err := step.DecodeSnapshot(jobID, frame.GetSnapshot())
	if err != nil {
		s.log.Warn("flexiq: a durable-step snapshot did not decode", "job_id", jobID, "error", err)
	}

	s.mu.Lock()
	s.snapshots[jobID] = stepSnapshot{records: records, err: err}
	s.mu.Unlock()
}

// takeSnapshot removes and returns the snapshot for a job.
//
// Absent means an *empty* snapshot, never an unknown one: the scheduler sends
// no frame for a job with no steps.
func (s *session) takeSnapshot(jobID string) stepSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()

	snapshot := s.snapshots[jobID]
	delete(s.snapshots, jobID)
	return snapshot
}

// commitStep frames one step commit and blocks until the scheduler answers it.
//
// The waiter is registered **before** the frame is sent, or a fast scheduler
// could answer before there is anything to answer to.
//
// The wait is bounded three ways and ends on whichever comes first: the
// caller's context, the job's own context — which carries the attempt deadline,
// so waiting never outlives a reap the scheduler has already decided on — and
// the configured step-ack budget. A stream that ends releases every waiter at
// once rather than leaving each to time out alone.
func (s *session) commitStep(
	ctx, jobCtx context.Context,
	jobID string,
	pending step.Pending,
	encoded []byte,
	wakeAt *time.Time,
) (*executorv1.StepAckFrame, error) {
	key := stepAckKey{jobID: jobID, seq: pending.Seq()}
	answers := make(chan *executorv1.StepAckFrame, 1)
	if err := s.awaitAck(key, answers); err != nil {
		return nil, err
	}
	defer s.forgetAck(key)

	frame := &executorv1.StepCommitFrame{
		JobId:   jobID,
		Seq:     pending.Seq(),
		StepKey: pending.StepKey(),
		Kind:    wireStepKind(pending.Kind()),
		Payload: encoded,
	}
	if wakeAt != nil {
		frame.WakeAt = timestamppb.New(*wakeAt)
	}
	// Through send, so the dispatch's lease rides this frame like every other
	// one that advances the attempt.
	s.send(jobID, &executorv1.AttachRequest{
		Frame: &executorv1.AttachRequest_StepCommit{StepCommit: frame},
	})

	budget, cancel := context.WithTimeout(ctx, s.cfg.stepAckTimeout)
	defer cancel()

	select {
	case ack, open := <-answers:
		if !open {
			return nil, errors.New("the connection to the scheduler ended before the commit was acknowledged")
		}
		return ack, nil
	case <-jobCtx.Done():
		return nil, fmt.Errorf("the attempt ended before the commit was acknowledged: %w", jobCtx.Err())
	case <-budget.Done():
		return nil, fmt.Errorf("the scheduler did not acknowledge the commit within %s: %w",
			s.cfg.stepAckTimeout, budget.Err())
	}
}

// awaitAck books a waiter for one commit.
func (s *session) awaitAck(key stepAckKey, answers chan *executorv1.StepAckFrame) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.stepAcks == nil {
		// The reader has gone and abandoned every waiter. Registering now would
		// park until the budget ran out for an answer that cannot arrive.
		return errors.New("the connection to the scheduler ended before the commit was sent")
	}
	if _, outstanding := s.stepAcks[key]; outstanding {
		// One commit per position at a time. The sequence refuses an
		// overlapping step already; this is the same rule at the other end.
		return fmt.Errorf("step %d of job %s already has a commit awaiting an acknowledgement",
			key.seq, key.jobID)
	}
	s.stepAcks[key] = answers
	return nil
}

func (s *session) forgetAck(key stepAckKey) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.stepAcks != nil {
		delete(s.stepAcks, key)
	}
}

// deliverAck hands one acknowledgement to whoever is blocked on it.
func (s *session) deliverAck(frame *executorv1.StepAckFrame) {
	key := stepAckKey{jobID: frame.GetJobId(), seq: frame.GetSeq()}

	s.mu.Lock()
	answers := s.stepAcks[key]
	delete(s.stepAcks, key)
	s.mu.Unlock()

	if answers == nil {
		// A duplicate, or one for a commit that already gave up waiting. The
		// attempt has moved on either way, and the commit stays durable.
		s.log.Debug("flexiq: a step acknowledgement arrived with nothing waiting on it",
			"job_id", key.jobID, "seq", key.seq)
		return
	}
	// Capacity one and one send per waiter, so this cannot block.
	answers <- frame
}

// abandonAcks releases everyone blocked on an acknowledgement, because none is
// coming.
//
// Closing turns each waiter's park into an immediate answer rather than a full
// budget of silence, and nils the map so a commit racing the teardown is
// refused rather than booked against a reader that has gone.
func (s *session) abandonAcks() {
	s.mu.Lock()
	pending := s.stepAcks
	s.stepAcks = nil
	s.mu.Unlock()

	for _, answers := range pending {
		close(answers)
	}
}

// stepsAcked reports whether the scheduler said it will apply step commits.
func (s *session) stepsAcked() bool { return s.stepsOn }

func wireStepKind(kind step.Kind) executorv1.StepKind {
	if kind == step.KindSleep {
		return executorv1.StepKind_STEP_KIND_SLEEP
	}
	return executorv1.StepKind_STEP_KIND_RUN
}
