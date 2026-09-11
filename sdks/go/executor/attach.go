package executor

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	flexiq "github.com/ByteVeda/flexiq/sdks/go/v2"
	executorv1 "github.com/ByteVeda/flexiq/sdks/go/v2/internal/pb/flexiq/executor/v1"
)

// sessionMetadataKey carries the scheduler's own token for this stream, on the
// Attach response's initial metadata.
//
// A -bin key because the value is bytes; grpc-go base64s it on the wire and
// hands the raw bytes back. Heartbeat carries it back verbatim and never the
// executor id, which is a name the executor picked and could be another
// executor's.
const sessionMetadataKey = "flexiq-attach-session-bin"

// settleBuffer is the outbound queue for frames that settle a job.
//
// Unlike the side channel this one never drops. A dispatched job the scheduler
// never hears about again is a slot it believes is busy until the reaper
// notices, and the result of work that actually ran.
const settleBuffer = 64

// flushBudget bounds the wait for queued settling frames to reach the socket
// once the session is ending. Past it the stream is closed anyway: a frame that
// cannot be written is not made writable by waiting longer.
const flushBudget = 5 * time.Second

// heartbeatFloor keeps a short heartbeat interval from becoming a deadline no
// real network answers inside. The deadline is the interval, because a
// capacity report that has not landed by the next tick is already stale — but
// an executor configured to heartbeat every 20ms wants frequent reports, not
// failing ones.
const heartbeatFloor = time.Second

// sessionEnd says why a stream stopped, which is the whole input to what
// happens next.
type sessionEnd int

const (
	// endRotated is a clean stream end: reconnect, immediately, and do not call
	// it a failure.
	endRotated sessionEnd = iota
	// endShutdown is a shutdown frame: stop, and do not reconnect.
	endShutdown
	// endContext is the caller's context ending.
	endContext
	// endError is a transport or protocol failure.
	endError
)

type stream = grpc.BidiStreamingClient[executorv1.AttachRequest, executorv1.AttachResponse]

// session owns one Attach stream from the handshake to its end.
type session struct {
	cfg      *config
	log      *slog.Logger
	client   executorv1.ExecutorServiceClient
	stream   stream
	handlers map[string]Handler
	slots    *slots

	token []byte
	// leaseAcked is what the acknowledgement said, and it does NOT gate the
	// echo. See stampLease.
	leaseAcked bool
	sideOn     bool

	settleCh chan *executorv1.AttachRequest
	side     *sideChannel

	jobsCtx    context.Context
	cancelJobs context.CancelFunc

	// enqueued and written are what "the queue is empty" is measured with.
	// Comparing lengths cannot see the frame the writer has already taken and
	// is still sending, and that frame is the one a shutdown would lose.
	enqueued atomic.Int64
	written  atomic.Int64

	mu      sync.Mutex
	running map[string]*runningJob
	leases  map[string][]byte
	logged  map[string]bool

	inFlight sync.WaitGroup
}

type runningJob struct {
	cancel    context.CancelFunc
	requested atomic.Bool
}

func newSession(cfg *config, log *slog.Logger, client executorv1.ExecutorServiceClient, s stream, handlers map[string]Handler, sl *slots) *session {
	jobsCtx, cancelJobs := context.WithCancel(context.Background())
	return &session{
		cfg:        cfg,
		log:        log,
		client:     client,
		stream:     s,
		handlers:   handlers,
		slots:      sl,
		settleCh:   make(chan *executorv1.AttachRequest, settleBuffer),
		side:       newSideChannel(),
		jobsCtx:    jobsCtx,
		cancelJobs: cancelJobs,
		running:    make(map[string]*runningJob),
		leases:     make(map[string][]byte),
		logged:     make(map[string]bool),
	}
}

// handshake sends hello, reads the session token, and reads the acknowledgement.
//
// The order is the contract's and must not be rearranged. Hello is the first
// thing the scheduler reads on a stream, and a heartbeat that overtakes it is
// read as the handshake and refuses the attach — which is why the heartbeat
// loop does not start until this returns.
func (s *session) handshake(ctx context.Context, cancelStream context.CancelFunc, tasks []string) error {
	hello := &executorv1.AttachRequest{Frame: &executorv1.AttachRequest_Hello{Hello: &executorv1.HelloFrame{
		ExecutorId:      s.cfg.id,
		Sdk:             s.cfg.sdk,
		Version:         s.cfg.version,
		Tasks:           tasks,
		Slots:           s.cfg.wireSlots(),
		ProtocolVersion: ProtocolVersion,
		Capabilities:    []string{CapSideChannel, CapLease},
	}}}
	if err := s.stream.Send(hello); err != nil {
		return attachError(err)
	}

	// A watchdog rather than a deadline on the stream: a deadline would end the
	// whole session at the same moment, and this budget covers the handshake
	// only.
	settled := make(chan struct{})
	defer close(settled)
	go func() {
		select {
		case <-settled:
		case <-ctx.Done():
		case <-time.After(s.cfg.handshakeTimeout):
			cancelStream()
		}
	}()

	header, err := s.stream.Header()
	if err != nil {
		return attachError(err)
	}
	if values := header.Get(sessionMetadataKey); len(values) > 0 {
		s.token = []byte(values[0])
	}

	response, err := s.stream.Recv()
	if err != nil {
		return attachError(err)
	}
	ack := response.GetHelloAck()
	if ack == nil {
		return &AttachError{
			Code:      codes.FailedPrecondition,
			Message:   "the scheduler's first frame was not a hello acknowledgement",
			Permanent: true,
		}
	}

	if ack.GetProtocolVersion() != ProtocolVersion {
		// Both numbers, because the scheduler sent its acknowledgement before
		// refusing for exactly this reason: an operator needs to know which end
		// to upgrade.
		s.log.Error("flexiq: protocol version mismatch",
			"ours", ProtocolVersion, "scheduler", ack.GetProtocolVersion(),
			"scheduler_id", ack.GetSchedulerId())
		return versionMismatch(ack.GetProtocolVersion())
	}

	for _, capability := range ack.GetCapabilities() {
		switch capability {
		case CapSideChannel:
			s.sideOn = true
		case CapLease:
			s.leaseAcked = true
		}
	}

	s.log.Info("flexiq: attached",
		"executor_id", s.cfg.id, "scheduler_id", ack.GetSchedulerId(),
		"slots", s.cfg.slots, "tasks", len(tasks),
		"side_channel", s.sideOn, "lease", s.leaseAcked)
	return nil
}

// read is the session's main loop. It returns why the stream ended.
func (s *session) read(ctx context.Context) (sessionEnd, error) {
	for {
		response, err := s.stream.Recv()
		if err != nil {
			switch {
			case errors.Is(err, io.EOF):
				// The scheduler drained this stream and closed it. A rotation,
				// not a failure.
				return endRotated, nil
			case ctx.Err() != nil:
				// The stream was ended from this side, which is what a drain
				// that finished looks like from in here.
				return endContext, ctx.Err()
			default:
				return endError, attachError(err)
			}
		}

		switch frame := response.GetFrame().(type) {
		case *executorv1.AttachResponse_Job:
			s.dispatch(frame.Job)
		case *executorv1.AttachResponse_Cancel:
			s.requestCancel(frame.Cancel.GetJobId())
		case *executorv1.AttachResponse_Shutdown:
			return endShutdown, nil
		case *executorv1.AttachResponse_HelloAck:
			s.once("hello_ack", "flexiq: a second hello acknowledgement on an attached stream; ignoring it")
		case *executorv1.AttachResponse_JobSteps, *executorv1.AttachResponse_StepAck:
			// This client does not advertise steps, so the scheduler should
			// send none. One arriving anyway is skipped rather than fatal — the
			// stream stays aligned and its jobs keep running.
			s.once("steps", "flexiq: the scheduler sent a durable-step frame to an executor that did not advertise steps; ignoring it")
		case nil:
			// An arm this build does not recognise decodes to no arm at all.
			// That is how a newer scheduler and an older executor stay
			// attached to each other.
			s.once("unknown", "flexiq: the scheduler sent a frame this build does not recognise; ignoring it")
		}
	}
}

// dispatch turns a job frame into a running handler, or into the one settling
// frame that says why it is not.
func (s *session) dispatch(frame *executorv1.JobFrame) {
	job := jobFromFrame(frame, s)

	// The lease is remembered before anything can be sent about this job,
	// refusals included: a frame that should carry one and does not is dropped,
	// and a dropped refusal is a job nobody is running and nobody reported.
	if lease := frame.GetLease(); len(lease) > 0 {
		s.mu.Lock()
		s.leases[job.ID] = lease
		s.mu.Unlock()
	}

	handler, known := s.handlers[job.TaskName]
	if !known {
		// The scheduler matches jobs to executors that advertised the task, so
		// this is a registry that changed under a live stream rather than a
		// routing mistake. Fatal, because the next attempt would find the same
		// registry.
		s.refuse(job, flexiq.EncodeTaskError("TaskNotRegistered", "task not registered: "+job.TaskName, nil), false)
		return
	}

	if !s.slots.acquire() {
		s.refuse(job, flexiq.EncodeTaskError(
			"NoCapacity",
			"executor did not run '"+job.TaskName+"': no free slot",
			nil,
		), true)
		return
	}

	// Registered before the goroutine starts, not inside it. A cancel frame can
	// follow its job frame immediately, and a lookup that misses because the
	// handler had not booked itself in yet is a cancel silently dropped.
	jobCtx, cancel := s.jobContext(job)
	record := &runningJob{cancel: cancel}
	s.mu.Lock()
	s.running[job.ID] = record
	s.mu.Unlock()

	s.inFlight.Add(1)
	go s.run(jobCtx, job, handler, record)
}

func (s *session) refuse(job *Job, errorJSON string, retry bool) {
	s.send(job.ID, failureFrame(job, failure{error: errorJSON, shouldRetry: retry, wall: nil}))
	s.forget(job.ID)
}

// run executes one job and settles it exactly once.
func (s *session) run(jobCtx context.Context, job *Job, handler Handler, record *runningJob) {
	defer s.inFlight.Done()
	defer s.slots.release()

	started := time.Now()
	value, err := invoke(jobCtx, job, handler)
	result := outcome{
		value:     value,
		err:       err,
		wall:      time.Since(started),
		timedOut:  errors.Is(jobCtx.Err(), context.DeadlineExceeded),
		cancelled: record.requested.Load(),
	}
	record.cancel()

	// The telemetry this attempt queued goes out ahead of the frame that
	// settles it. The two queues are separate because one may drop and the
	// other may not, and a log line written during a run that arrives after the
	// run finished is one the scheduler drops for naming a job it is no longer
	// running.
	for _, pending := range s.side.take(job.ID) {
		s.enqueue(pending)
	}

	s.send(job.ID, settle(job, result))
	s.forget(job.ID)
}

func (s *session) jobContext(job *Job) (context.Context, context.CancelFunc) {
	if job.Timeout > 0 {
		// The scheduler's stale-job reap is the backstop and needs no frame,
		// but a handler that never returns holds a slot until it fires. A
		// deadline here is what makes the job actually stop.
		return context.WithTimeout(s.jobsCtx, job.Timeout)
	}
	return context.WithCancel(s.jobsCtx)
}

// invoke calls the handler and turns a panic into an ordinary failure.
//
// A panicking handler must not take the executor down with it: every other job
// on this stream is unrelated to it, and the scheduler is owed a frame about
// this one either way.
func invoke(ctx context.Context, job *Job, handler Handler) (value any, err error) {
	defer func() {
		if recovery := recover(); recovery != nil {
			value = nil
			err = capture(recovery)
		}
	}()
	return handler(ctx, job)
}

// requestCancel delivers a cancel to a running job.
//
// Cancellation is cooperative: this cancels the handler's context and nothing
// more. A handler that ignores it and finishes normally settles normally. A
// cancel naming a job this stream is not running is dropped, which is the
// scheduler's own behaviour in the other direction.
func (s *session) requestCancel(jobID string) {
	s.mu.Lock()
	record := s.running[jobID]
	s.mu.Unlock()

	if record == nil {
		return
	}
	record.requested.Store(true)
	record.cancel()
}

func (s *session) forget(jobID string) {
	s.mu.Lock()
	delete(s.running, jobID)
	delete(s.leases, jobID)
	s.mu.Unlock()
}

// send is the one path a frame about a job takes, which is what makes the lease
// echo structural rather than a thing to remember at each call site.
func (s *session) send(jobID string, req *executorv1.AttachRequest) {
	s.stampLease(jobID, req)
	s.enqueue(req)
}

// enqueue puts an already-stamped frame on the queue that never drops.
func (s *session) enqueue(req *executorv1.AttachRequest) {
	s.enqueued.Add(1)
	select {
	case s.settleCh <- req:
	case <-s.jobsCtx.Done():
		// Nobody is left to write it. Count it as done or the flush that is
		// tearing this session down would wait out its whole budget.
		s.written.Add(1)
	}
}

func (s *session) pushSide(jobID string, progress bool, req *executorv1.AttachRequest) {
	s.stampLease(jobID, req)
	s.side.push(req, jobID, progress)
}

func (s *session) sideChannelOn() bool { return s.sideOn }

// stampLease attaches the dispatch's lease to every frame that settles or
// advances that attempt.
//
// The lease says which dispatch a frame belongs to, so a stalled executor
// cannot write over the attempt that replaced it. It is never inspected, never
// constructed here, and never reused across attempts.
//
// **The acknowledgement does not gate this, and must not.** The scheduler
// decides whether to check our frames for a lease from what `hello` advertised,
// but only advertises the capability back once it holds a lease book — and it
// installs that book when its scheduler role starts, which can be after an
// executor has already attached. An executor that took the acknowledgement
// literally in that window would send no lease, be read as a stale attempt, and
// have every frame about every job dropped.
//
// So the rule is the dispatch's, not the handshake's: a job frame that carried
// a lease gets it echoed. Echoing one the scheduler does not check costs a
// field it ignores; withholding one it does check costs the job's result.
func (s *session) stampLease(jobID string, req *executorv1.AttachRequest) {
	s.mu.Lock()
	lease := s.leases[jobID]
	s.mu.Unlock()
	if len(lease) == 0 {
		return
	}

	switch frame := req.GetFrame().(type) {
	case *executorv1.AttachRequest_Success:
		frame.Success.Lease = lease
	case *executorv1.AttachRequest_Failure:
		frame.Failure.Lease = lease
	case *executorv1.AttachRequest_Cancelled:
		frame.Cancelled.Lease = lease
	case *executorv1.AttachRequest_Slept:
		frame.Slept.Lease = lease
	case *executorv1.AttachRequest_Progress:
		frame.Progress.Lease = lease
	case *executorv1.AttachRequest_TaskLog:
		frame.TaskLog.Lease = lease
	case *executorv1.AttachRequest_StepCommit:
		frame.StepCommit.Lease = lease
	}
	// hello carries none: it belongs to the connection, not to a job.
}

// write is the sole owner of the stream's sending half. grpc-go does not allow
// two goroutines into SendMsg, and a job goroutine must never be the one
// waiting on a socket anyway.
func (s *session) write(ctx context.Context, stop <-chan struct{}) {
	failed := false
	send := func(req *executorv1.AttachRequest) {
		defer s.written.Add(1)
		if failed {
			return
		}
		if err := s.stream.Send(req); err != nil {
			// Keep draining rather than returning: the queue has to empty for
			// the session to finish tearing down, and every frame left in it is
			// about a stream that is already gone.
			failed = true
			s.log.Debug("flexiq: the attach stream stopped accepting frames", "error", err)
		}
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-stop:
			s.drainQueues(send)
			return
		case req := <-s.settleCh:
			send(req)
		case <-s.side.ready():
			if req := s.side.pop(); req != nil {
				s.enqueued.Add(1)
				send(req)
			}
		}
	}
}

// awaitFlush waits for every queued settling frame to have reached the socket.
//
// Bounded: a frame that cannot be written is not made writable by waiting
// longer, and the scheduler's reaper is the backstop for whatever is lost.
func (s *session) awaitFlush() {
	deadline := time.After(flushBudget)
	for {
		if s.written.Load() >= s.enqueued.Load() {
			return
		}
		select {
		case <-deadline:
			s.log.Warn("flexiq: gave up flushing results",
				"queued", s.enqueued.Load()-s.written.Load())
			return
		case <-time.After(time.Millisecond):
		}
	}
}

// drainQueues empties both queues once, settling frames first. They are the
// ones that cannot be regenerated.
func (s *session) drainQueues(send func(*executorv1.AttachRequest)) {
	for {
		select {
		case req := <-s.settleCh:
			send(req)
		default:
			for {
				req := s.side.pop()
				if req == nil {
					return
				}
				s.enqueued.Add(1)
				send(req)
			}
		}
	}
}

// heartbeat reports free capacity off the dispatch stream.
//
// Off it on purpose: a heartbeat sharing the stream it reports on cannot tell a
// busy stream from a dead peer. It carries the scheduler's own session token,
// never this executor's id.
func (s *session) heartbeat(ctx context.Context) {
	if len(s.token) == 0 {
		// Nothing identifies the stream, so a heartbeat could only name an id,
		// which is the one thing it must not do.
		s.log.Warn("flexiq: the attach response carried no session token; heartbeats are off for this stream")
		return
	}

	ticker := time.NewTicker(s.cfg.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if s.slots.isDraining() {
				// The zero-capacity heartbeat that begin-drain already sent is
				// the last word this stream has to say.
				return
			}
			if !s.beat(ctx) {
				return
			}
		}
	}
}

func (s *session) beat(ctx context.Context) bool {
	callCtx, cancel := context.WithTimeout(ctx, s.heartbeatDeadline())
	defer cancel()

	_, err := s.client.Heartbeat(callCtx, &executorv1.HeartbeatRequest{
		Session:   s.token,
		FreeSlots: s.slots.available(),
	})
	if err == nil {
		return true
	}
	if status.Code(err) == codes.NotFound {
		// The scheduler has no stream under this token any more. The reader
		// will find that out too; saying so once is enough.
		s.log.Debug("flexiq: the scheduler no longer knows this session; heartbeats stop")
		return false
	}
	if ctx.Err() == nil {
		s.log.Warn("flexiq: heartbeat failed", "error", err)
	}
	return ctx.Err() == nil
}

// beginDrain stops this stream taking new work and tells the scheduler so.
func (s *session) beginDrain(ctx context.Context) {
	if s.slots.isDraining() {
		return
	}
	s.slots.drain()
	if len(s.token) == 0 {
		return
	}

	// Deadlined, because this runs on the teardown path *before* the stream
	// context is cancelled. A peer that holds the connection open and never
	// answers would otherwise park the teardown here, and Run could neither
	// close the stream nor reconnect.
	callCtx, cancel := context.WithTimeout(ctx, s.heartbeatDeadline())
	defer cancel()
	_, _ = s.client.Heartbeat(callCtx, &executorv1.HeartbeatRequest{Session: s.token, FreeSlots: 0})
}

// heartbeatDeadline bounds one heartbeat call. Unary RPCs inherit only the
// context they are given, and the contexts these are given outlive them.
func (s *session) heartbeatDeadline() time.Duration {
	return max(s.cfg.heartbeatInterval, heartbeatFloor)
}

// waitInFlight waits for the jobs this stream is already running, up to budget.
//
// Past it the connection closes anyway and what is still running is left to the
// scheduler's reaper. A handler that ignores its context must not be able to
// hang the process.
func (s *session) waitInFlight(budget time.Duration) {
	done := make(chan struct{})
	go func() {
		s.inFlight.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(budget):
		s.log.Warn("flexiq: gave up waiting for jobs still running",
			"in_flight", s.slots.inFlight(), "budget", budget)
	}

	if dropped := s.side.droppedCount(); dropped > 0 {
		// Worth one line at the end rather than one per drop: the number says
		// the side channel was saturated, and the individual frames it names
		// carried no result.
		s.log.Warn("flexiq: dropped progress and log frames on a saturated side channel", "dropped", dropped)
	}
}

// once logs a message one time per stream. A scheduler that keeps sending a
// frame this build cannot read would otherwise fill a log with one sentence.
func (s *session) once(key, message string) {
	s.mu.Lock()
	seen := s.logged[key]
	s.logged[key] = true
	s.mu.Unlock()

	if !seen {
		s.log.Warn(message)
	}
}
