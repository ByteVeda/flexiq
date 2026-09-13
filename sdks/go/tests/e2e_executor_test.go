//go:build integration

package tests

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	flexiq "github.com/ByteVeda/flexiq/sdks/go/v2"
	"github.com/ByteVeda/flexiq/sdks/go/v2/executor"
)

// The executor door against a real flexiq-server.
//
// The bufconn suite pins this client's half of the protocol against a double
// that can be told to misbehave. This one pins the half a double cannot: that a
// real scheduler accepts the handshake, dispatches to the tasks the handshake
// advertised, and records what comes back where the producer door can read it.

// attachWorker starts a worker against the live server and stops it when the
// test ends.
//
// It does not wait for the handshake, because it does not have to: a job
// enqueued before its executor attaches simply stays pending until one does,
// which is the same thing that happens in production every time a deployment
// restarts.
func attachWorker(t *testing.T, id string, register func(*executor.Worker)) {
	t.Helper()

	_, token, err := live.mint(id, "execute")
	if err != nil {
		t.Fatalf("mint an execute-scoped token: %v", err)
	}

	w, err := executor.New(live.addr,
		executor.WithToken(token),
		executor.WithInsecureTransport(),
		executor.WithID(id),
		executor.WithSlots(2),
		executor.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	register(w)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Error("the worker did not stop after its context was cancelled")
		}
		_ = w.Close()
	})
}

// awaitJob polls the producer door until the job reaches a terminal state.
//
// There is no completion notification on this door — no watch and no server
// stream — so polling is the supported route, not a shortcut.
func awaitJob(t *testing.T, jobID string) flexiq.Job {
	t.Helper()

	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		job, err := producer.GetJob(context.Background(), jobID, flexiq.GetJobOptions{IncludeResult: true})
		if err == nil && job.Status.IsTerminal() {
			return job
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("job %s never reached a terminal state:\n%s", jobID, live.logTail())
	return flexiq.Job{}
}

func TestAGoWorkerRunsAJobTheProducerDoorEnqueued(t *testing.T) {
	type receipt struct {
		OrderID string `cbor:"order_id"`
		Cents   int64  `cbor:"cents"`
	}

	attachWorker(t, "go-e2e-happy", func(w *executor.Worker) {
		if err := w.Handle("go.e2e.charge", func(_ context.Context, job *executor.Job) (any, error) {
			var charged charge
			if err := job.Bind(&charged); err != nil {
				return nil, executor.Fatal(err)
			}
			job.Progress(50)
			if err := job.Log(executor.LevelInfo, "charging", map[string]any{"order": charged.OrderID}); err != nil {
				return nil, err
			}
			return receipt{OrderID: charged.OrderID, Cents: charged.AmountCents}, nil
		}); err != nil {
			t.Fatalf("Handle: %v", err)
		}
	})

	result, err := producer.Enqueue(context.Background(), flexiq.EnqueueRequest{
		Task:    "go.e2e.charge",
		Args:    []any{charge{OrderID: "ord-e2e-1", AmountCents: 2500}},
		Options: flexiq.EnqueueOptions{Queue: executorQueue},
	})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	job := awaitJob(t, result.Job.ID)
	if job.Status != flexiq.StatusComplete {
		t.Fatalf("job status = %s, want complete. Error: %s", job.Status, job.Error)
	}

	var decoded receipt
	if err := job.DecodeResult(&decoded); err != nil {
		t.Fatalf("the recorded result is not a readable envelope: %v", err)
	}
	if decoded.OrderID != "ord-e2e-1" || decoded.Cents != 2500 {
		t.Fatalf("result = %+v, want the receipt the handler returned", decoded)
	}
}

func TestAFatalFailureFromAGoWorkerIsRecordedAsStructured(t *testing.T) {
	attachWorker(t, "go-e2e-failure", func(w *executor.Worker) {
		if err := w.Handle("go.e2e.fail", func(context.Context, *executor.Job) (any, error) {
			return nil, executor.Fatal(errors.New("no such customer"))
		}); err != nil {
			t.Fatalf("Handle: %v", err)
		}
	})

	result, err := producer.Enqueue(context.Background(), flexiq.EnqueueRequest{
		Task:    "go.e2e.fail",
		Options: flexiq.EnqueueOptions{Queue: executorQueue},
	})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	job := awaitJob(t, result.Job.ID)
	if job.Status == flexiq.StatusComplete {
		t.Fatal("a handler that returned a fatal error was recorded as a success")
	}

	// should_retry is the executor's decision, and the scheduler never inspects
	// an error to second-guess it. A fatal failure therefore does not retry.
	failure, recorded := job.TaskError()
	if !recorded {
		t.Fatal("the job recorded no error at all")
	}
	if !failure.Structured {
		t.Fatalf("the recorded error is not the canonical JSON: %q", job.Error)
	}
	if !strings.Contains(failure.Message, "no such customer") {
		t.Errorf("message = %q, want the handler's own text", failure.Message)
	}
	if failure.Traceback == nil {
		t.Error("traceback is nil; the key is required and its empty form is []")
	}
}

func TestAProduceScopedTokenCannotOpenAnExecutorStream(t *testing.T) {
	_, token, err := live.mint("go-e2e-wrong-scope", "produce")
	if err != nil {
		t.Fatalf("mint a produce-scoped token: %v", err)
	}

	w, err := executor.New(live.addr,
		executor.WithToken(token),
		executor.WithInsecureTransport(),
		executor.WithID("go-e2e-wrong-scope"),
		executor.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })

	if err := w.Handle("go.e2e.never", func(context.Context, *executor.Job) (any, error) {
		return nil, nil
	}); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// A scope refusal is the same answer every time, so Run must return it
	// rather than reconnect into it forever.
	runErr := w.Run(ctx)
	var refusal *executor.AttachError
	if !errors.As(runErr, &refusal) {
		t.Fatalf("Run returned %v (%T), want an *executor.AttachError", runErr, runErr)
	}
	if !refusal.Permanent {
		t.Fatalf("refusal = %+v, want it permanent", refusal)
	}
}

// Durable steps against a real scheduler.
//
// The bufconn suite drives the frames; only a live server can prove the two
// halves of the snapshot agree. The scheduler encodes it in Rust and this
// client decodes it in Go, and a step whose memo did not survive that round
// trip is a step that runs its body twice.

func TestADurableStepSurvivesTheAttemptThatWroteIt(t *testing.T) {
	type receipt struct {
		Key   string `cbor:"key"`
		Cents int64  `cbor:"cents"`
	}

	var charges, attempts atomic.Int32

	attachWorker(t, "go-e2e-steps", func(w *executor.Worker) {
		if err := w.Handle("go.e2e.step", func(ctx context.Context, job *executor.Job) (any, error) {
			attempts.Add(1)

			written, err := executor.Step(ctx, job, "charge",
				func(_ context.Context, key string) (receipt, error) {
					charges.Add(1)
					return receipt{Key: key, Cents: 2500}, nil
				})
			if err != nil {
				return nil, err
			}

			// Ends this attempt. The job is rescheduled, and the attempt that
			// wakes replays the charge above from its recorded row instead of
			// running it again.
			if err := job.Sleep(ctx, "settlement", 2*time.Second); err != nil {
				return nil, err
			}
			return written, nil
		}); err != nil {
			t.Fatalf("Handle: %v", err)
		}
	})

	result, err := producer.Enqueue(context.Background(), flexiq.EnqueueRequest{
		Task:    "go.e2e.step",
		Options: flexiq.EnqueueOptions{Queue: executorQueue},
	})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	job := awaitJob(t, result.Job.ID)
	if job.Status != flexiq.StatusComplete {
		t.Fatalf("job status = %s, want complete. Error: %s\n%s", job.Status, job.Error, live.logTail())
	}

	if ran := attempts.Load(); ran != 2 {
		t.Fatalf("the task body ran %d time(s), want 2: one that slept and one that woke", ran)
	}
	if charged := charges.Load(); charged != 1 {
		t.Fatalf("the step body ran %d time(s), want 1; that is the double charge this exists to prevent", charged)
	}

	var decoded receipt
	if err := job.DecodeResult(&decoded); err != nil {
		t.Fatalf("the recorded result is not a readable envelope: %v", err)
	}
	// The memo came back through the scheduler's own snapshot encoder, bytes
	// intact, and the key it was minted under is the run's.
	if want := result.Job.ID + ":charge#0"; decoded.Key != want {
		t.Fatalf("the replayed receipt carried key %q, want %q", decoded.Key, want)
	}
	if decoded.Cents != 2500 {
		t.Errorf("cents = %d, want 2500", decoded.Cents)
	}
}

// A sleep that has already elapsed is a memo hit, so the second wake does not
// start the first sleep over. Without that the job would sleep forever and this
// test would never see it finish.
func TestAJobWithTwoSleepsDoesNotRestartTheFirstOnTheSecondWake(t *testing.T) {
	var attempts atomic.Int32

	attachWorker(t, "go-e2e-sleeps", func(w *executor.Worker) {
		if err := w.Handle("go.e2e.sleeps", func(ctx context.Context, job *executor.Job) (any, error) {
			attempts.Add(1)
			if err := job.Sleep(ctx, "first", time.Second); err != nil {
				return nil, err
			}
			if err := job.Sleep(ctx, "second", time.Second); err != nil {
				return nil, err
			}
			return attempts.Load(), nil
		}); err != nil {
			t.Fatalf("Handle: %v", err)
		}
	})

	result, err := producer.Enqueue(context.Background(), flexiq.EnqueueRequest{
		Task:    "go.e2e.sleeps",
		Options: flexiq.EnqueueOptions{Queue: executorQueue},
	})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	job := awaitJob(t, result.Job.ID)
	if job.Status != flexiq.StatusComplete {
		t.Fatalf("job status = %s, want complete. Error: %s\n%s", job.Status, job.Error, live.logTail())
	}

	var ran int64
	if err := job.DecodeResult(&ran); err != nil {
		t.Fatalf("the recorded result is not a readable envelope: %v", err)
	}
	if ran != 3 {
		t.Fatalf("the task body ran %d time(s), want 3: the first attempt and one wake per sleep", ran)
	}
}
