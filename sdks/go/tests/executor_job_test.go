package tests

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	flexiq "github.com/ByteVeda/flexiq/sdks/go/v2"
	"github.com/ByteVeda/flexiq/sdks/go/v2/executor"
	executorv1 "github.com/ByteVeda/flexiq/sdks/go/v2/internal/pb/flexiq/executor/v1"
)

type charge struct {
	OrderID     string `cbor:"order_id"`
	AmountCents int64  `cbor:"amount_cents"`
}

func mustEncodeCall(t *testing.T, args ...any) []byte {
	t.Helper()
	payload, err := flexiq.EncodeCall(args, nil)
	if err != nil {
		t.Fatalf("EncodeCall: %v", err)
	}
	return payload
}

// dispatches scripts a stream that hands over one job and then waits.
func dispatches(job *executorv1.AttachResponse, capabilities ...string) func(*testing.T, int, *schedulerStream) error {
	return func(t *testing.T, _ int, s *schedulerStream) error {
		s.handshake(t, capabilities...)
		s.send(t, job)
		drain(s)
		return nil
	}
}

func awaitSettled(t *testing.T, fake *fakeScheduler, jobID string) *executorv1.AttachRequest {
	t.Helper()
	await(t, "a settling frame for "+jobID, func() bool { return settled(fake.frames(), jobID) != nil })
	return settled(fake.frames(), jobID)
}

func TestASuccessfulJobSettlesWithItsResultEncoded(t *testing.T) {
	fake := &fakeScheduler{attach: dispatches(
		jobFrame("job-1", "billing.charge", mustEncodeCall(t, charge{OrderID: "ord-1", AmountCents: 1000})),
		executor.CapLease, executor.CapSideChannel,
	)}
	w := serveExecutor(t, fake)
	mustHandle(t, w, "billing.charge", func(_ context.Context, job *executor.Job) (any, error) {
		var received charge
		if err := job.Bind(&received); err != nil {
			return nil, err
		}
		if received.OrderID != "ord-1" || received.AmountCents != 1000 {
			return nil, errors.New("the payload did not decode into the handler's own type")
		}
		return map[string]any{"receipt": received.OrderID}, nil
	})
	runWorker(t, w)

	success := awaitSettled(t, fake, "job-1").GetSuccess()
	if success == nil {
		t.Fatalf("the job settled as something other than a success")
	}
	if success.GetTaskName() != "billing.charge" {
		t.Errorf("task_name = %q, want %q", success.GetTaskName(), "billing.charge")
	}

	var decoded map[string]any
	if err := flexiq.DecodeResult(success.GetResult(), &decoded); err != nil {
		t.Fatalf("the result is not a readable envelope: %v", err)
	}
	if decoded["receipt"] != "ord-1" {
		t.Errorf("result = %v, want the receipt the handler returned", decoded)
	}
	if success.GetWallTime() == nil {
		t.Error("wall_time is unset")
	}
}

func TestReturningNothingIsAnAbsentResultNotAnEmptyOne(t *testing.T) {
	fake := &fakeScheduler{attach: dispatches(jobFrame("job-1", "t", mustEncodeCall(t)), executor.CapLease)}
	w := serveExecutor(t, fake)
	mustHandle(t, w, "t", func(context.Context, *executor.Job) (any, error) { return nil, nil })
	runWorker(t, w)

	success := awaitSettled(t, fake, "job-1").GetSuccess()
	if success == nil {
		t.Fatal("the job did not settle as a success")
	}
	// Absent and present-and-empty are different answers, and the frame has a
	// way to say each. Collapsing them would report "returned nothing" as
	// "returned an empty value" to every reader downstream.
	if success.Result != nil {
		t.Fatalf("result = %v, want it absent", success.Result)
	}
}

func TestAFailedJobRetriesAndAFatalOneDoesNot(t *testing.T) {
	for _, tc := range []struct {
		name        string
		err         error
		shouldRetry bool
	}{
		{"an ordinary error retries", errors.New("the card issuer timed out"), true},
		{"a fatal error does not", executor.Fatal(errors.New("no such customer")), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeScheduler{attach: dispatches(jobFrame("job-1", "t", mustEncodeCall(t)), executor.CapLease)}
			w := serveExecutor(t, fake)
			mustHandle(t, w, "t", func(context.Context, *executor.Job) (any, error) { return nil, tc.err })
			runWorker(t, w)

			failure := awaitSettled(t, fake, "job-1").GetFailure()
			if failure == nil {
				t.Fatal("the job did not settle as a failure")
			}
			if failure.GetShouldRetry() != tc.shouldRetry {
				t.Errorf("should_retry = %v, want %v", failure.GetShouldRetry(), tc.shouldRetry)
			}
			if failure.GetTimedOut() {
				t.Error("timed_out is set on a failure that did not time out")
			}

			parsed := flexiq.ParseTaskError(failure.GetError())
			if !parsed.Structured {
				t.Fatalf("the error is not the canonical JSON: %q", failure.GetError())
			}
			if !strings.Contains(parsed.Message, tc.err.Error()) {
				t.Errorf("message = %q, want the handler's own text", parsed.Message)
			}
			if parsed.Traceback == nil {
				t.Error("traceback is nil; the key is required and its empty form is []")
			}
		})
	}
}

func TestAPanicSettlesTheJobRatherThanEndingTheStream(t *testing.T) {
	fake := &fakeScheduler{attach: func(t *testing.T, _ int, s *schedulerStream) error {
		s.handshake(t, executor.CapLease)
		s.send(t, jobFrame("job-1", "t", mustEncodeCall(t)))
		s.send(t, jobFrame("job-2", "t", mustEncodeCall(t)))
		drain(s)
		return nil
	}}
	w := serveExecutor(t, fake)
	mustHandle(t, w, "t", func(_ context.Context, job *executor.Job) (any, error) {
		if job.ID == "job-1" {
			panic("the handler exploded")
		}
		return "fine", nil
	})
	runWorker(t, w)

	failure := awaitSettled(t, fake, "job-1").GetFailure()
	if failure == nil {
		t.Fatal("the panicking job did not settle as a failure")
	}
	parsed := flexiq.ParseTaskError(failure.GetError())
	if parsed.Type != "panic" {
		t.Errorf("errtype = %q, want %q", parsed.Type, "panic")
	}
	if !strings.Contains(parsed.Message, "the handler exploded") {
		t.Errorf("message = %q, want the panic's own value", parsed.Message)
	}
	if len(parsed.Traceback) == 0 {
		t.Error("traceback is empty; a panic is the one failure Go can offer frames for")
	}

	// The other job on the same stream is unrelated to the one that panicked.
	if awaitSettled(t, fake, "job-2").GetSuccess() == nil {
		t.Error("a panic in one handler took down another job on the same stream")
	}
}

func TestAJobOverItsTimeoutSettlesAsTimedOut(t *testing.T) {
	job := jobFrame("job-1", "slow", mustEncodeCall(t))
	job.GetJob().Timeout = durationpb.New(50 * time.Millisecond)

	fake := &fakeScheduler{attach: dispatches(job, executor.CapLease)}
	w := serveExecutor(t, fake)
	mustHandle(t, w, "slow", func(ctx context.Context, _ *executor.Job) (any, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	})
	runWorker(t, w)

	failure := awaitSettled(t, fake, "job-1").GetFailure()
	if failure == nil {
		t.Fatal("the job did not settle as a failure")
	}
	if !failure.GetTimedOut() {
		t.Error("timed_out is not set")
	}
	if !failure.GetShouldRetry() {
		t.Error("should_retry is not set; a timeout is retryable")
	}
	// The reaper's own wording, so a timeout the executor reported and one the
	// scheduler synthesized read identically in the same list.
	if message := flexiq.ParseTaskError(failure.GetError()).Message; message != "job timed out after 50ms" {
		t.Errorf("message = %q, want %q", message, "job timed out after 50ms")
	}
}

func TestACancelFrameCancelsTheHandlerAndSettlesCancelled(t *testing.T) {
	fake := &fakeScheduler{attach: func(t *testing.T, _ int, s *schedulerStream) error {
		s.handshake(t, executor.CapLease)
		s.send(t, jobFrame("job-1", "slow", mustEncodeCall(t)))
		s.send(t, &executorv1.AttachResponse{Frame: &executorv1.AttachResponse_Cancel{
			Cancel: &executorv1.CancelFrame{JobId: "job-1"},
		}})
		drain(s)
		return nil
	}}
	w := serveExecutor(t, fake)
	mustHandle(t, w, "slow", func(ctx context.Context, _ *executor.Job) (any, error) {
		<-ctx.Done()
		return nil, executor.ErrCancelled
	})
	runWorker(t, w)

	frame := awaitSettled(t, fake, "job-1")
	if frame.GetCancelled() == nil {
		t.Fatalf("the job settled as %T, want a cancelled frame", frame.GetFrame())
	}
}

func TestAHandlerThatIgnoresItsCancelStillSettlesNormally(t *testing.T) {
	fake := &fakeScheduler{attach: func(t *testing.T, _ int, s *schedulerStream) error {
		s.handshake(t, executor.CapLease)
		s.send(t, jobFrame("job-1", "stubborn", mustEncodeCall(t)))
		s.send(t, &executorv1.AttachResponse{Frame: &executorv1.AttachResponse_Cancel{
			Cancel: &executorv1.CancelFrame{JobId: "job-1"},
		}})
		drain(s)
		return nil
	}}
	w := serveExecutor(t, fake)
	mustHandle(t, w, "stubborn", func(ctx context.Context, _ *executor.Job) (any, error) {
		<-ctx.Done()
		// Cancellation is cooperative. Nothing stops a goroutine that returns
		// a result anyway, and the frame must describe what actually happened.
		return "finished regardless", nil
	})
	runWorker(t, w)

	if awaitSettled(t, fake, "job-1").GetSuccess() == nil {
		t.Fatal("a handler that ignored its cancel and succeeded did not settle as a success")
	}
}

func TestAJobWithNoFreeSlotIsRefusedRetryablyRatherThanDropped(t *testing.T) {
	release := make(chan struct{})
	fake := &fakeScheduler{attach: func(t *testing.T, _ int, s *schedulerStream) error {
		s.handshake(t, executor.CapLease)
		s.send(t, jobFrame("job-1", "slow", mustEncodeCall(t)))
		s.send(t, jobFrame("job-2", "slow", mustEncodeCall(t)))
		drain(s)
		return nil
	}}
	w := serveExecutor(t, fake, executor.WithSlots(1))
	mustHandle(t, w, "slow", func(context.Context, *executor.Job) (any, error) {
		<-release
		return "done", nil
	})
	runWorker(t, w)

	// The scheduler reserves a slot before it writes a job frame, so it is
	// designed never to oversend. Designed never to is not cannot, and a job
	// answered with nothing is a job nobody ever hears about again.
	failure := awaitSettled(t, fake, "job-2").GetFailure()
	if failure == nil {
		t.Fatal("the job that arrived with no free slot was not answered")
	}
	if !failure.GetShouldRetry() {
		t.Error("should_retry is not set; the executor was busy, not broken")
	}
	if message := flexiq.ParseTaskError(failure.GetError()).Message; !strings.Contains(message, "no free slot") {
		t.Errorf("message = %q, want it to name the reason", message)
	}

	close(release)
	if awaitSettled(t, fake, "job-1").GetSuccess() == nil {
		t.Error("the job that did hold the slot did not settle as a success")
	}
}

func TestAJobNamingAnUnregisteredTaskIsRefusedFatally(t *testing.T) {
	fake := &fakeScheduler{attach: dispatches(jobFrame("job-1", "nobody.implements", mustEncodeCall(t)), executor.CapLease)}
	w := serveExecutor(t, fake)
	mustHandle(t, w, "t", noopHandler)
	runWorker(t, w)

	failure := awaitSettled(t, fake, "job-1").GetFailure()
	if failure == nil {
		t.Fatal("the job was not answered")
	}
	if failure.GetShouldRetry() {
		t.Error("should_retry is set; the next attempt would find the same registry")
	}
}

func TestALeaseIsEchoedOnEveryFrameAboutTheAttempt(t *testing.T) {
	lease := []byte("lease-bytes-0001")
	job := jobFrame("job-1", "t", mustEncodeCall(t))
	job.GetJob().Lease = lease

	fake := &fakeScheduler{attach: dispatches(job, executor.CapLease, executor.CapSideChannel)}
	w := serveExecutor(t, fake)
	mustHandle(t, w, "t", func(_ context.Context, j *executor.Job) (any, error) {
		j.Progress(50)
		if err := j.Log(executor.LevelInfo, "halfway", map[string]any{"step": 1}); err != nil {
			return nil, err
		}
		return "done", nil
	})
	runWorker(t, w)

	awaitSettled(t, fake, "job-1")
	await(t, "the side-channel frames", func() bool {
		var progress, logs int
		for _, frame := range fake.frames() {
			if frame.GetProgress() != nil {
				progress++
			}
			if frame.GetTaskLog() != nil {
				logs++
			}
		}
		return progress > 0 && logs > 0
	})

	// A frame that should carry a lease and does not is dropped, and what it
	// was reporting is a job that ran without its result being recorded.
	for _, frame := range fake.frames() {
		var got []byte
		switch arm := frame.GetFrame().(type) {
		case *executorv1.AttachRequest_Success:
			got = arm.Success.GetLease()
		case *executorv1.AttachRequest_Progress:
			got = arm.Progress.GetLease()
		case *executorv1.AttachRequest_TaskLog:
			got = arm.TaskLog.GetLease()
		case *executorv1.AttachRequest_Hello:
			// Hello belongs to the connection, not to a job, and carries none.
			continue
		default:
			continue
		}
		if string(got) != string(lease) {
			t.Errorf("%T carried lease %q, want %q", frame.GetFrame(), got, lease)
		}
	}
}

func TestALeaseIsEchoedEvenWhenTheAcknowledgementWithheldTheCapability(t *testing.T) {
	lease := []byte("lease-bytes-0001")
	job := jobFrame("job-1", "t", mustEncodeCall(t))
	job.GetJob().Lease = lease

	// The acknowledgement carries no lease capability, and the dispatch carries
	// a lease anyway. That window is real: the scheduler decides whether to
	// check an executor's frames for a lease from what hello advertised, but
	// only advertises the capability back once it holds a lease book — which it
	// installs when its scheduler role starts, after an executor may already
	// have attached. Taking the acknowledgement literally here would have every
	// frame about every job read as a stale attempt and dropped.
	fake := &fakeScheduler{attach: dispatches(job, executor.CapSideChannel)}
	w := serveExecutor(t, fake)
	mustHandle(t, w, "t", func(context.Context, *executor.Job) (any, error) { return "done", nil })
	runWorker(t, w)

	success := awaitSettled(t, fake, "job-1").GetSuccess()
	if success == nil {
		t.Fatal("the job did not settle as a success")
	}
	if string(success.GetLease()) != string(lease) {
		t.Fatalf("lease = %q, want the dispatch's own %q", success.GetLease(), lease)
	}
}

func TestNoLeaseIsSentWhenTheDispatchCarriedNone(t *testing.T) {
	fake := &fakeScheduler{attach: dispatches(jobFrame("job-1", "t", mustEncodeCall(t)),
		executor.CapLease, executor.CapSideChannel)}
	w := serveExecutor(t, fake)
	mustHandle(t, w, "t", func(context.Context, *executor.Job) (any, error) { return "done", nil })
	runWorker(t, w)

	success := awaitSettled(t, fake, "job-1").GetSuccess()
	if success == nil {
		t.Fatal("the job did not settle as a success")
	}
	// A lease is the scheduler's own value handed back. There is nothing to
	// hand back, and constructing one is the thing a client must never do.
	if success.Lease != nil {
		t.Fatalf("lease = %q, want none: the dispatch carried none", success.Lease)
	}
}

func TestProgressIsClampedToTheRangeTheFrameDeclares(t *testing.T) {
	fake := &fakeScheduler{attach: dispatches(jobFrame("job-1", "t", mustEncodeCall(t)),
		executor.CapLease, executor.CapSideChannel)}
	w := serveExecutor(t, fake)
	mustHandle(t, w, "t", func(_ context.Context, job *executor.Job) (any, error) {
		// The scheduler stores whatever arrives; nothing on its side checks the
		// range the frame documents. A value outside it would be read back
		// wrong rather than refused.
		job.Progress(250)
		return nil, nil
	})
	runWorker(t, w)

	awaitSettled(t, fake, "job-1")
	await(t, "a progress frame", func() bool {
		for _, frame := range fake.frames() {
			if frame.GetProgress() != nil {
				return true
			}
		}
		return false
	})

	for _, frame := range fake.frames() {
		if progress := frame.GetProgress(); progress != nil && progress.GetProgress() != 100 {
			t.Fatalf("progress = %d, want it clamped to 100", progress.GetProgress())
		}
	}
}

func TestTheSideChannelIsSilentWithoutItsCapability(t *testing.T) {
	fake := &fakeScheduler{attach: dispatches(jobFrame("job-1", "t", mustEncodeCall(t)), executor.CapLease)}
	w := serveExecutor(t, fake)
	mustHandle(t, w, "t", func(_ context.Context, job *executor.Job) (any, error) {
		job.Progress(50)
		if err := job.Log(executor.LevelInfo, "a line", nil); err != nil {
			return nil, err
		}
		if err := job.Publish(map[string]any{"partial": true}); err != nil {
			return nil, err
		}
		return "done", nil
	})
	runWorker(t, w)

	awaitSettled(t, fake, "job-1")
	for _, frame := range fake.frames() {
		if frame.GetProgress() != nil || frame.GetTaskLog() != nil {
			t.Fatalf("sent %T without the side-channel capability; the calls degrade to no-ops", frame.GetFrame())
		}
	}
}

func TestAPublishedPartialIsALogAtResultLevel(t *testing.T) {
	fake := &fakeScheduler{attach: dispatches(jobFrame("job-1", "t", mustEncodeCall(t)),
		executor.CapLease, executor.CapSideChannel)}
	w := serveExecutor(t, fake)
	mustHandle(t, w, "t", func(_ context.Context, job *executor.Job) (any, error) {
		return nil, job.Publish(map[string]any{"rows": 42})
	})
	runWorker(t, w)

	awaitSettled(t, fake, "job-1")
	await(t, "the published partial", func() bool {
		for _, frame := range fake.frames() {
			if frame.GetTaskLog() != nil {
				return true
			}
		}
		return false
	})

	for _, frame := range fake.frames() {
		log := frame.GetTaskLog()
		if log == nil {
			continue
		}
		if log.GetLevel() != executor.LevelResult {
			t.Errorf("level = %q, want %q", log.GetLevel(), executor.LevelResult)
		}
		if log.GetMessage() != "" {
			t.Errorf("message = %q, want it empty: a partial's value lives in extra", log.GetMessage())
		}
		var extra map[string]any
		if err := json.Unmarshal(log.GetExtra(), &extra); err != nil {
			t.Fatalf("extra is not JSON: %v", err)
		}
		if extra["rows"] != float64(42) {
			t.Errorf("extra = %v, want the published value itself, unwrapped", extra)
		}
	}
}

func TestBindRefusesAKeywordArgumentRatherThanDroppingIt(t *testing.T) {
	payload, err := flexiq.EncodeCall(nil, map[string]any{"amount_cents": 1000})
	if err != nil {
		t.Fatalf("EncodeCall: %v", err)
	}

	fake := &fakeScheduler{attach: dispatches(jobFrame("job-1", "t", payload), executor.CapLease)}
	w := serveExecutor(t, fake)
	mustHandle(t, w, "t", func(_ context.Context, job *executor.Job) (any, error) {
		var received charge
		bindErr := job.Bind(&received)
		if bindErr == nil {
			return nil, errors.New("Bind accepted a keyword argument it has nowhere to put")
		}
		// Call still reads them, for a handler that wants to decide for itself.
		call, callErr := job.Call()
		if callErr != nil {
			return nil, callErr
		}
		if len(call.Kwargs) != 1 {
			return nil, errors.New("Call did not return the keyword arguments")
		}
		return bindErr.Error(), nil
	})
	runWorker(t, w)

	success := awaitSettled(t, fake, "job-1").GetSuccess()
	if success == nil {
		t.Fatal("the job did not settle as a success")
	}
	var message string
	if err := flexiq.DecodeResult(success.GetResult(), &message); err != nil {
		t.Fatalf("DecodeResult: %v", err)
	}
	if !strings.Contains(message, "keyword argument") {
		t.Errorf("Bind's error was %q, want it to name what it refused", message)
	}
}
