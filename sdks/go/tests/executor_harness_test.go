package tests

import (
	"context"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"

	"github.com/ByteVeda/flexiq/sdks/go/v2/executor"
	executorv1 "github.com/ByteVeda/flexiq/sdks/go/v2/internal/pb/flexiq/executor/v1"
)

// The executor harness: a scriptable ExecutorService double on an in-process
// connection.
//
// A double rather than a real flexiq-server because what these tests are about
// is this client's half of the handshake and the frames it writes. Half of them
// need the scheduler to do something a real one never would — refuse a version,
// send a frame arm that does not exist, end a stream mid-job — and a real
// server cannot be asked for any of that.

const (
	// testSessionToken stands in for the 16 bytes a real scheduler mints. Only
	// its round trip matters; nothing here decodes it, and nothing should.
	testSessionToken = "\x00\x01\x02\x03session-bytes"
	testExecutorID   = "go-executor-test"
)

// schedulerStream is one attached stream, from the double's side.
type schedulerStream struct {
	grpc.BidiStreamingServer[executorv1.AttachRequest, executorv1.AttachResponse]
	fake *fakeScheduler
}

// recv reads the next frame from the executor and records it.
func (s *schedulerStream) recv(t *testing.T) *executorv1.AttachRequest {
	t.Helper()
	req, err := s.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	s.fake.record(req)
	return req
}

// recvOrEnd reads the next frame, reporting a stream the executor closed.
func (s *schedulerStream) recvOrEnd() (*executorv1.AttachRequest, bool) {
	req, err := s.Recv()
	if err != nil {
		return nil, false
	}
	s.fake.record(req)
	return req, true
}

func (s *schedulerStream) send(t *testing.T, frame *executorv1.AttachResponse) {
	t.Helper()
	if err := s.Send(frame); err != nil {
		t.Fatalf("Send: %v", err)
	}
}

// handshake reads hello and answers it, which is what every test that is not
// about the handshake wants.
func (s *schedulerStream) handshake(t *testing.T, capabilities ...string) {
	t.Helper()
	if hello := s.recv(t).GetHello(); hello == nil {
		t.Fatalf("the executor's first frame was %T, want hello", s.fake.lastFrame())
	}
	s.send(t, ack(1, capabilities...))
}

func ack(version uint32, capabilities ...string) *executorv1.AttachResponse {
	return &executorv1.AttachResponse{Frame: &executorv1.AttachResponse_HelloAck{
		HelloAck: &executorv1.HelloAckFrame{
			SchedulerId:     "scheduler-test",
			ProtocolVersion: version,
			Capabilities:    capabilities,
		},
	}}
}

func jobFrame(id, task string, payload []byte) *executorv1.AttachResponse {
	return &executorv1.AttachResponse{Frame: &executorv1.AttachResponse_Job{
		Job: &executorv1.JobFrame{Id: id, TaskName: task, Payload: payload, Queue: "default"},
	}}
}

// fakeScheduler answers whatever a test scripts. Attach is given the attempt
// number so a test about reconnecting can behave differently the second time.
type fakeScheduler struct {
	executorv1.UnimplementedExecutorServiceServer

	attach    func(t *testing.T, attempt int, s *schedulerStream) error
	heartbeat func(context.Context, *executorv1.HeartbeatRequest) (*executorv1.HeartbeatResponse, error)
	// withoutSessionToken suppresses the metadata a real scheduler always sends.
	withoutSessionToken bool

	t *testing.T

	mu         sync.Mutex
	attempts   int
	received   []*executorv1.AttachRequest
	heartbeats []*executorv1.HeartbeatRequest
	attached   chan struct{}
}

func (f *fakeScheduler) record(req *executorv1.AttachRequest) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.received = append(f.received, req)
}

func (f *fakeScheduler) lastFrame() any {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.received) == 0 {
		return nil
	}
	return f.received[len(f.received)-1].GetFrame()
}

// frames returns a copy, so an assertion cannot race the stream still running.
func (f *fakeScheduler) frames() []*executorv1.AttachRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*executorv1.AttachRequest(nil), f.received...)
}

func (f *fakeScheduler) attachCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.attempts
}

func (f *fakeScheduler) beats() []*executorv1.HeartbeatRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*executorv1.HeartbeatRequest(nil), f.heartbeats...)
}

func (f *fakeScheduler) Attach(raw grpc.BidiStreamingServer[executorv1.AttachRequest, executorv1.AttachResponse]) error {
	f.mu.Lock()
	f.attempts++
	attempt := f.attempts
	f.mu.Unlock()

	if !f.withoutSessionToken {
		// A real scheduler sends this as initial metadata at RPC entry, before
		// it has read hello. Sending it here mirrors that ordering, which is
		// what the client's Header() call depends on.
		if err := raw.SendHeader(metadata.Pairs("flexiq-attach-session-bin", testSessionToken)); err != nil {
			return err
		}
	}

	if f.attached != nil {
		select {
		case f.attached <- struct{}{}:
		default:
		}
	}

	stream := &schedulerStream{BidiStreamingServer: raw, fake: f}
	if f.attach == nil {
		stream.handshake(f.t)
		drain(stream)
		return nil
	}
	return f.attach(f.t, attempt, stream)
}

// drain reads until the executor closes its half, so a scripted stream can end
// by simply returning.
func drain(s *schedulerStream) {
	for {
		if _, ok := s.recvOrEnd(); !ok {
			return
		}
	}
}

func (f *fakeScheduler) Heartbeat(ctx context.Context, req *executorv1.HeartbeatRequest) (*executorv1.HeartbeatResponse, error) {
	f.mu.Lock()
	f.heartbeats = append(f.heartbeats, req)
	f.mu.Unlock()

	if f.heartbeat == nil {
		return &executorv1.HeartbeatResponse{}, nil
	}
	return f.heartbeat(ctx, req)
}

// worker builds a worker wired to the double, with the timings a test wants:
// short enough that a case about reconnecting finishes, long enough that a
// loaded machine does not fail one about anything else.
func serveExecutor(t *testing.T, fake *fakeScheduler, opts ...executor.Option) *executor.Worker {
	t.Helper()
	fake.t = t

	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	executorv1.RegisterExecutorServiceServer(server, fake)
	go func() {
		_ = server.Serve(listener)
	}()

	dialer := func(ctx context.Context, _ string) (net.Conn, error) {
		return listener.DialContext(ctx)
	}
	base := []executor.Option{
		executor.WithToken(testToken),
		executor.WithInsecureTransport(),
		executor.WithID(testExecutorID),
		executor.WithSlots(2),
		executor.WithHandshakeTimeout(2 * time.Second),
		executor.WithHeartbeatInterval(20 * time.Millisecond),
		executor.WithShutdownDrain(2 * time.Second),
		executor.WithReconnectBackoff(time.Millisecond, 5*time.Millisecond),
		executor.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
		executor.WithGRPCDialOptions(grpc.WithContextDialer(dialer)),
	}
	w, err := executor.New("passthrough:///bufnet", append(base, opts...)...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	t.Cleanup(func() {
		_ = w.Close()
		server.Stop()
		_ = listener.Close()
	})
	return w
}

// runWorker starts the worker and hands back the channel its error arrives on, plus
// the cancel that stops it. The test is responsible for reaching one or the
// other; the cleanup only makes sure nothing is left running.
func runWorker(t *testing.T, w *executor.Worker) (context.CancelFunc, <-chan error) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	errs := make(chan error, 1)
	go func() {
		errs <- w.Run(ctx)
		// Closed as well as sent on, so a test that already read the error and
		// the cleanup that reads it again are both satisfied.
		close(errs)
	}()

	t.Cleanup(func() {
		cancel()
		select {
		case <-errs:
		case <-time.After(5 * time.Second):
			t.Error("Run did not return after its context was cancelled")
		}
	})
	return cancel, errs
}

// await fails the test rather than hanging when something never happens.
func await(t *testing.T, what string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// settled finds the one settling frame for a job, which is the thing almost
// every test here asserts on.
func settled(frames []*executorv1.AttachRequest, jobID string) *executorv1.AttachRequest {
	for _, frame := range frames {
		switch arm := frame.GetFrame().(type) {
		case *executorv1.AttachRequest_Success:
			if arm.Success.GetJobId() == jobID {
				return frame
			}
		case *executorv1.AttachRequest_Failure:
			if arm.Failure.GetJobId() == jobID {
				return frame
			}
		case *executorv1.AttachRequest_Cancelled:
			if arm.Cancelled.GetJobId() == jobID {
				return frame
			}
		}
	}
	return nil
}
