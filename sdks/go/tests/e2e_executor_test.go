//go:build integration

package tests

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
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
