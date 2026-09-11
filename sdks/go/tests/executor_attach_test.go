package tests

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ByteVeda/flexiq/sdks/go/v2/executor"
	executorv1 "github.com/ByteVeda/flexiq/sdks/go/v2/internal/pb/flexiq/executor/v1"
)

func mustHandle(t *testing.T, w *executor.Worker, task string, handler executor.Handler) {
	t.Helper()
	if err := w.Handle(task, handler); err != nil {
		t.Fatalf("Handle(%q): %v", task, err)
	}
}

func noopHandler(context.Context, *executor.Job) (any, error) { return nil, nil }

func firstHello(frames []*executorv1.AttachRequest) *executorv1.HelloFrame {
	for _, frame := range frames {
		if hello := frame.GetHello(); hello != nil {
			return hello
		}
	}
	return nil
}

func TestHelloIsTheFirstFrameAndAdvertisesEveryRegisteredTask(t *testing.T) {
	fake := &fakeScheduler{
		attach: func(t *testing.T, _ int, s *schedulerStream) error {
			s.handshake(t, executor.CapLease, executor.CapSideChannel)
			drain(s)
			return nil
		},
	}
	w := serveExecutor(t, fake, executor.WithSlots(4))
	mustHandle(t, w, "billing.refund", noopHandler)
	mustHandle(t, w, "billing.charge", noopHandler)
	runWorker(t, w)

	await(t, "the handshake", func() bool { return firstHello(fake.frames()) != nil })

	frames := fake.frames()
	if got := frames[0].GetHello(); got == nil {
		t.Fatalf("the first frame was %T, want hello: nothing may precede it on a stream", frames[0].GetFrame())
	}

	hello := firstHello(frames)
	if hello.GetExecutorId() != testExecutorID {
		t.Errorf("executor_id = %q, want %q", hello.GetExecutorId(), testExecutorID)
	}
	if hello.GetSlots() != 4 {
		t.Errorf("slots = %d, want 4", hello.GetSlots())
	}
	if hello.GetProtocolVersion() != 1 {
		t.Errorf("protocol_version = %d, want 1", hello.GetProtocolVersion())
	}
	if hello.GetSdk() != "go" {
		t.Errorf("sdk = %q, want %q", hello.GetSdk(), "go")
	}

	// Sorted, so two runs of the same worker advertise the same list and a
	// registry fingerprint derived from it does not move.
	want := []string{"billing.charge", "billing.refund"}
	if got := hello.GetTasks(); len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("tasks = %v, want %v", got, want)
	}
}

func TestTheExecutorNeverAdvertisesTheStepsCapability(t *testing.T) {
	fake := &fakeScheduler{
		attach: func(t *testing.T, _ int, s *schedulerStream) error {
			s.handshake(t, executor.CapLease, executor.CapSideChannel, executor.CapSteps)
			drain(s)
			return nil
		},
	}
	w := serveExecutor(t, fake)
	mustHandle(t, w, "t", noopHandler)
	runWorker(t, w)

	await(t, "the handshake", func() bool { return firstHello(fake.frames()) != nil })

	// A scheduler willing to do steps changes nothing: the rule is that a
	// client sends no frame for a behaviour it did not advertise, and this one
	// implements none.
	for _, capability := range firstHello(fake.frames()).GetCapabilities() {
		if capability == executor.CapSteps {
			t.Fatal("hello advertised steps, which this client does not implement")
		}
	}
}

func TestTheSessionTokenIsReadFromMetadataAndEchoedOnEveryHeartbeat(t *testing.T) {
	fake := &fakeScheduler{
		attach: func(t *testing.T, _ int, s *schedulerStream) error {
			s.handshake(t, executor.CapLease)
			drain(s)
			return nil
		},
	}
	w := serveExecutor(t, fake)
	mustHandle(t, w, "t", noopHandler)
	runWorker(t, w)

	await(t, "a heartbeat", func() bool { return len(fake.beats()) > 0 })

	beat := fake.beats()[0]
	if string(beat.GetSession()) != testSessionToken {
		t.Errorf("heartbeat session = %q, want the token from the response metadata %q",
			beat.GetSession(), testSessionToken)
	}
	// An id is a name the executor picked and could be another executor's, so
	// the request has nowhere to put one. Proving the session round-trips is
	// what proves the client did not reach for the id instead.
	if beat.GetFreeSlots() != 2 {
		t.Errorf("free_slots = %d, want 2", beat.GetFreeSlots())
	}
}

func TestAnAttachWithoutASessionTokenSendsNoHeartbeats(t *testing.T) {
	fake := &fakeScheduler{
		withoutSessionToken: true,
		attach: func(t *testing.T, _ int, s *schedulerStream) error {
			s.handshake(t, executor.CapLease)
			drain(s)
			return nil
		},
	}
	w := serveExecutor(t, fake)
	mustHandle(t, w, "t", noopHandler)
	runWorker(t, w)

	await(t, "the handshake", func() bool { return firstHello(fake.frames()) != nil })
	time.Sleep(100 * time.Millisecond) // several heartbeat intervals

	if beats := fake.beats(); len(beats) != 0 {
		t.Fatalf("sent %d heartbeat(s) with no session token; the only other thing to name is the executor id, which must never be sent", len(beats))
	}
}

func TestAProtocolVersionMismatchIsPermanentAndReconnectsNoFurther(t *testing.T) {
	fake := &fakeScheduler{
		attach: func(t *testing.T, _ int, s *schedulerStream) error {
			s.recv(t)
			// A real scheduler acknowledges first and refuses second, so both
			// ends can log both numbers. The client must read the ack.
			s.send(t, ack(99))
			return status.Error(codes.FailedPrecondition, "protocol version mismatch: we speak 99, peer speaks 1")
		},
	}
	w := serveExecutor(t, fake)
	mustHandle(t, w, "t", noopHandler)
	_, errs := runWorker(t, w)

	err := awaitError(t, errs)
	var refusal *executor.AttachError
	if !errors.As(err, &refusal) {
		t.Fatalf("Run returned %v (%T), want an *executor.AttachError", err, err)
	}
	if refusal.Code != codes.FailedPrecondition || !refusal.Permanent {
		t.Fatalf("refusal = %+v, want a permanent FAILED_PRECONDITION", refusal)
	}
	if count := fake.attachCount(); count != 1 {
		t.Fatalf("attached %d times; a version mismatch is the same answer every time", count)
	}
}

func TestADuplicateExecutorIDIsPermanent(t *testing.T) {
	fake := &fakeScheduler{
		attach: func(t *testing.T, _ int, s *schedulerStream) error {
			s.handshake(t, executor.CapLease)
			return status.Error(codes.AlreadyExists,
				"executor go-executor-test is already attached; wait for the previous stream to end")
		},
	}
	w := serveExecutor(t, fake)
	mustHandle(t, w, "t", noopHandler)
	_, errs := runWorker(t, w)

	err := awaitError(t, errs)
	var refusal *executor.AttachError
	if !errors.As(err, &refusal) || refusal.Code != codes.AlreadyExists || !refusal.Permanent {
		t.Fatalf("Run returned %v, want a permanent ALREADY_EXISTS", err)
	}
	if count := fake.attachCount(); count != 1 {
		t.Fatalf("attached %d times; another stream holding the id is the same answer every time", count)
	}
}

func TestATransportFailureReconnects(t *testing.T) {
	fake := &fakeScheduler{
		attach: func(t *testing.T, attempt int, s *schedulerStream) error {
			if attempt == 1 {
				s.recv(t)
				return status.Error(codes.Unavailable, "the scheduler is starting")
			}
			s.handshake(t, executor.CapLease)
			drain(s)
			return nil
		},
	}
	w := serveExecutor(t, fake)
	mustHandle(t, w, "t", noopHandler)
	runWorker(t, w)

	await(t, "a second attach", func() bool { return fake.attachCount() >= 2 })
}

func TestACleanStreamEndIsARotationAndReconnects(t *testing.T) {
	fake := &fakeScheduler{
		attach: func(t *testing.T, attempt int, s *schedulerStream) error {
			s.handshake(t, executor.CapLease)
			if attempt == 1 {
				// Exactly what a rotation is: the scheduler drains the stream
				// and closes it, with no frame to say so.
				return nil
			}
			drain(s)
			return nil
		},
	}
	w := serveExecutor(t, fake)
	mustHandle(t, w, "t", noopHandler)
	_, errs := runWorker(t, w)

	await(t, "a reconnect after the rotation", func() bool { return fake.attachCount() >= 2 })

	select {
	case err := <-errs:
		t.Fatalf("Run returned %v; a rotation is not a failure and not a reason to stop", err)
	default:
	}
}

func TestAShutdownFrameStopsRunWithoutReconnecting(t *testing.T) {
	fake := &fakeScheduler{
		attach: func(t *testing.T, _ int, s *schedulerStream) error {
			s.handshake(t, executor.CapLease)
			s.send(t, &executorv1.AttachResponse{
				Frame: &executorv1.AttachResponse_Shutdown{Shutdown: &executorv1.ShutdownFrame{}},
			})
			drain(s)
			return nil
		},
	}
	w := serveExecutor(t, fake)
	mustHandle(t, w, "t", noopHandler)
	_, errs := runWorker(t, w)

	if err := awaitErrorOrNil(t, errs); err != nil {
		t.Fatalf("Run returned %v, want nil: shutdown is an ordinary stop", err)
	}
	if count := fake.attachCount(); count != 1 {
		t.Fatalf("attached %d times; shutdown means stop, not reconnect", count)
	}
}

func TestAnUnknownFrameArmIsSkippedAndTheStreamStaysAligned(t *testing.T) {
	fake := &fakeScheduler{
		attach: func(t *testing.T, _ int, s *schedulerStream) error {
			s.handshake(t, executor.CapLease)
			// No arm set at all, which is what a frame type this build does not
			// know decodes to. A newer scheduler and an older executor stay
			// attached exactly because this is skipped rather than fatal.
			s.send(t, &executorv1.AttachResponse{})
			// A step frame, which this executor never asked for.
			s.send(t, &executorv1.AttachResponse{Frame: &executorv1.AttachResponse_JobSteps{
				JobSteps: &executorv1.JobStepsFrame{JobId: "job-1", Snapshot: []byte("[]\n")},
			}})
			s.send(t, jobFrame("job-1", "t", mustEncodeCall(t)))
			drain(s)
			return nil
		},
	}
	w := serveExecutor(t, fake)
	mustHandle(t, w, "t", func(context.Context, *executor.Job) (any, error) { return "done", nil })
	runWorker(t, w)

	await(t, "the job after the frames the executor could not read", func() bool {
		return settled(fake.frames(), "job-1") != nil
	})
}

func TestCancellingRunDrainsAndReturnsTheContextError(t *testing.T) {
	started := make(chan struct{})
	released := make(chan struct{})
	fake := &fakeScheduler{
		attach: func(t *testing.T, _ int, s *schedulerStream) error {
			s.handshake(t, executor.CapLease)
			s.send(t, jobFrame("job-1", "slow", mustEncodeCall(t)))
			drain(s)
			return nil
		},
	}
	w := serveExecutor(t, fake)
	mustHandle(t, w, "slow", func(context.Context, *executor.Job) (any, error) {
		close(started)
		<-released
		return "finished anyway", nil
	})
	cancel, errs := runWorker(t, w)

	<-started
	cancel()
	// The drain keeps the stream open for the job already running, so its
	// result still reaches the scheduler.
	close(released)

	err := awaitErrorOrNil(t, errs)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run returned %v, want context.Canceled", err)
	}
	if frame := settled(fake.frames(), "job-1"); frame == nil || frame.GetSuccess() == nil {
		t.Fatalf("the job running through the drain settled as %v, want a success", frame)
	}
}

func awaitError(t *testing.T, errs <-chan error) error {
	t.Helper()
	select {
	case err := <-errs:
		if err == nil {
			t.Fatal("Run returned nil, want an error")
		}
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return")
		return nil
	}
}

func awaitErrorOrNil(t *testing.T, errs <-chan error) error {
	t.Helper()
	select {
	case err := <-errs:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return")
		return nil
	}
}
