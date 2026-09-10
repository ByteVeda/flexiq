//go:build integration

package tests

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"google.golang.org/grpc/codes"

	flexiq "github.com/ByteVeda/flexiq/sdks/go/v2"
)

// The server every test here shares, and a client holding a produce-scoped
// token for it. One server for the suite: starting one costs a second, and
// nothing below writes state another test reads — each uses a queue of its own.
var (
	live     *server
	producer *flexiq.Client
)

func TestMain(m *testing.M) {
	// Separate from TestMain because os.Exit runs no deferred function, and
	// every one of them here is a process or a directory to clean up.
	os.Exit(run(m))
}

func run(m *testing.M) int {
	binary, err := serverBinary()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}

	dir, err := os.MkdirTemp("", "flexiq-go-e2e-")
	if err != nil {
		fmt.Fprintln(os.Stderr, "temp directory:", err)
		return 1
	}
	defer func() { _ = os.RemoveAll(dir) }()

	live, err = start(binary, dir+"/flexiq.db")
	if err != nil {
		fmt.Fprintln(os.Stderr, "start flexiq-server:", err)
		return 1
	}
	defer live.stop()

	_, token, err := live.mint("go-e2e-producer", "produce")
	if err != nil {
		fmt.Fprintln(os.Stderr, "mint a producer token:", err)
		return 1
	}
	producer, err = live.dial(token)
	if err != nil {
		fmt.Fprintln(os.Stderr, "dial", live.addr+":", err)
		return 1
	}
	defer func() { _ = producer.Close() }()

	code := m.Run()
	if code != 0 {
		// The assertion says what failed; this says what the other half was
		// doing while it did.
		fmt.Fprintln(os.Stderr, live.logTail())
	}
	return code
}

// testContext bounds a call so a hung server fails the test that is about it
// rather than the package timeout.
func testContext(t *testing.T) context.Context {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// order is a struct rather than a map because the payload assertion below is
// byte-for-byte, and Go map iteration order is unspecified.
type order struct {
	OrderID     string `cbor:"order_id"`
	AmountCents int64  `cbor:"amount_cents"`
}

// TestEnqueuedPayloadSurvivesTheServer is the round trip the double cannot
// stand in for: the envelope this client encodes is stored and handed back
// unchanged, which is the whole basis for a worker in another runtime being
// able to read it.
func TestEnqueuedPayloadSurvivesTheServer(t *testing.T) {
	ctx := testContext(t)

	args := []any{order{OrderID: "ord-0001", AmountCents: 1000}}
	kwargs := map[string]any{"currency": "EUR"}
	sent, err := flexiq.EncodeCall(args, kwargs)
	if err != nil {
		t.Fatalf("EncodeCall: %v", err)
	}

	result, err := producer.Enqueue(ctx, flexiq.EnqueueRequest{
		Task:    "orders.process",
		Args:    args,
		Kwargs:  kwargs,
		Options: flexiq.EnqueueOptions{Queue: "e2e-roundtrip"},
	})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if result.Job.ID == "" {
		t.Fatal("the server enqueued a job with no id")
	}
	if result.Job.TaskName != "orders.process" || result.Job.Queue != "e2e-roundtrip" {
		t.Errorf("enqueued %s on %q, want orders.process on e2e-roundtrip",
			result.Job.TaskName, result.Job.Queue)
	}
	if result.Job.Namespace != e2eNamespace {
		t.Errorf("job landed in namespace %q, want %q", result.Job.Namespace, e2eNamespace)
	}
	if result.Job.Status != flexiq.StatusPending {
		t.Errorf("a fresh job is %s, want PENDING", result.Job.Status)
	}

	// A read leaves the payload out unless it is asked for. The double asserts
	// this client sends the flag; only a server proves the flag is honoured.
	lean, err := producer.GetJob(ctx, result.Job.ID, flexiq.GetJobOptions{})
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if lean.Payload != nil {
		t.Errorf("a read that did not ask for the payload got %d bytes of one", len(lean.Payload))
	}

	full, err := producer.GetJob(ctx, result.Job.ID, flexiq.GetJobOptions{IncludePayload: true})
	if err != nil {
		t.Fatalf("GetJob(IncludePayload): %v", err)
	}
	if !bytes.Equal(full.Payload, sent) {
		t.Fatalf("the stored payload is not the one sent\n got: %x\nwant: %x", full.Payload, sent)
	}

	call, err := full.DecodePayload()
	if err != nil {
		t.Fatalf("DecodePayload: %v", err)
	}
	assertEqual(t, "args", call.Args, []any{
		map[string]any{"order_id": "ord-0001", "amount_cents": int64(1000)},
	})
	assertEqual(t, "kwargs", call.Kwargs, kwargs)
}

// TestUniqueKeyDedupesAgainstTheActiveJob pins the one response field that
// describes what the call did rather than the state it left: without it the
// two calls are indistinguishable, and a producer that set a unique key learns
// nothing.
func TestUniqueKeyDedupesAgainstTheActiveJob(t *testing.T) {
	ctx := testContext(t)

	request := flexiq.EnqueueRequest{
		Task: "orders.process",
		Args: []any{order{OrderID: "ord-unique", AmountCents: 250}},
		Options: flexiq.EnqueueOptions{
			Queue:     "e2e-unique",
			UniqueKey: "charge:ord-unique",
		},
	}

	first, err := producer.Enqueue(ctx, request)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if first.Deduplicated {
		t.Error("the first enqueue of a unique key reported a duplicate")
	}

	second, err := producer.Enqueue(ctx, request)
	if err != nil {
		t.Fatalf("Enqueue again: %v", err)
	}
	if !second.Deduplicated {
		t.Error("the second enqueue of an active unique key was not reported as deduplicated")
	}
	if second.Job.ID != first.Job.ID {
		t.Errorf("deduplicated onto job %s, want the original %s", second.Job.ID, first.Job.ID)
	}
}

// TestListingsCarryNoPayload pins the rule a client cannot see and depends on:
// a page of a hundred jobs is not a page of a hundred payloads.
func TestListingsCarryNoPayload(t *testing.T) {
	ctx := testContext(t)

	for i := range 3 {
		_, err := producer.Enqueue(ctx, flexiq.EnqueueRequest{
			Task:    "orders.process",
			Args:    []any{order{OrderID: fmt.Sprintf("ord-list-%d", i), AmountCents: int64(i)}},
			Options: flexiq.EnqueueOptions{Queue: "e2e-listing"},
		})
		if err != nil {
			t.Fatalf("Enqueue %d: %v", i, err)
		}
	}

	page, err := producer.ListJobs(ctx, flexiq.ListJobsQuery{Queue: "e2e-listing"})
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	if len(page.Jobs) != 3 {
		t.Fatalf("listed %d jobs, want 3", len(page.Jobs))
	}
	for _, job := range page.Jobs {
		if job.Payload != nil {
			t.Errorf("job %s came back in a listing carrying %d bytes of payload", job.ID, len(job.Payload))
		}
		if job.Queue != "e2e-listing" {
			t.Errorf("the queue filter returned a job on %q", job.Queue)
		}
	}
}

// TestCancelReportsTheStateItLeaves covers the response shape that makes
// calling it twice safe.
func TestCancelReportsTheStateItLeaves(t *testing.T) {
	ctx := testContext(t)

	result, err := producer.Enqueue(ctx, flexiq.EnqueueRequest{
		Task:    "orders.process",
		Args:    []any{order{OrderID: "ord-cancel", AmountCents: 1}},
		Options: flexiq.EnqueueOptions{Queue: "e2e-cancel"},
	})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	cancelled, err := producer.CancelJob(ctx, result.Job.ID)
	if err != nil {
		t.Fatalf("CancelJob: %v", err)
	}
	if cancelled.Status != flexiq.StatusCancelled {
		t.Errorf("cancelling a pending job left it %s, want CANCELLED", cancelled.Status)
	}

	again, err := producer.CancelJob(ctx, result.Job.ID)
	if err != nil {
		t.Fatalf("CancelJob again: %v", err)
	}
	if again.Status != flexiq.StatusCancelled {
		t.Errorf("cancelling a cancelled job left it %s, want CANCELLED", again.Status)
	}
}

// TestQueueStatsCountsTheNamespacesOwnJobs proves the counts are per queue,
// which is what makes the number readable at all on a shared namespace.
func TestQueueStatsCountsTheNamespacesOwnJobs(t *testing.T) {
	ctx := testContext(t)

	const queue = "e2e-stats"
	for i := range 2 {
		if _, err := producer.Enqueue(ctx, flexiq.EnqueueRequest{
			Task:    "orders.process",
			Args:    []any{order{OrderID: fmt.Sprintf("ord-stats-%d", i), AmountCents: 1}},
			Options: flexiq.EnqueueOptions{Queue: queue},
		}); err != nil {
			t.Fatalf("Enqueue %d: %v", i, err)
		}
	}

	stats, err := producer.QueueStats(ctx, queue)
	if err != nil {
		t.Fatalf("QueueStats: %v", err)
	}
	if stats.Pending != 2 {
		t.Errorf("%s holds %d pending jobs, want 2", queue, stats.Pending)
	}
	if stats.Running != 0 || stats.Completed != 0 || stats.Failed != 0 {
		t.Errorf("nothing drains %s, yet it reports %+v", queue, stats)
	}
}

// TestMissingJobIsJobNotFound is the first of the three errors the double can
// only assert this client *reads*. A reason is a promise about what the server
// sends, and this is where that promise is kept or broken.
func TestMissingJobIsJobNotFound(t *testing.T) {
	ctx := testContext(t)

	// Well-formed and nobody's: an id the server has to look up rather than
	// refuse as malformed.
	_, err := producer.GetJob(ctx, "0192f3c4-0000-7000-8000-000000000000", flexiq.GetJobOptions{})
	if err == nil {
		t.Fatal("reading a job that does not exist succeeded")
	}
	if !errors.Is(err, flexiq.ReasonJobNotFound) {
		t.Fatalf("GetJob on a missing id: %v, want %s", err, flexiq.ReasonJobNotFound)
	}

	wireErr, ok := flexiq.AsError(err)
	if !ok {
		t.Fatalf("%v did not arrive as a *flexiq.Error", err)
	}
	if wireErr.Code != codes.NotFound {
		t.Errorf("code is %s, want NotFound", wireErr.Code)
	}
}

// TestRevokedTokenIsUnauthenticated is the claim `token revoke` makes and only
// a running server can test: it takes effect on the next call, with no restart.
func TestRevokedTokenIsUnauthenticated(t *testing.T) {
	ctx := testContext(t)

	id, token, err := live.mint("go-e2e-revoked", "produce")
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	client, err := live.dial(token)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = client.Close() }()

	if _, statsErr := client.QueueStats(ctx, "e2e-revoked"); statsErr != nil {
		t.Fatalf("the token must work before it is revoked: %v", statsErr)
	}

	if revokeErr := live.revoke(id); revokeErr != nil {
		t.Fatalf("revoke: %v", revokeErr)
	}

	// Same client, same open connection: revocation is checked per call, so
	// nothing about the transport has to change for it to bite.
	_, err = client.QueueStats(ctx, "e2e-revoked")
	if err == nil {
		t.Fatal("a revoked token was still accepted")
	}
	if !errors.Is(err, flexiq.ReasonUnauthenticated) {
		t.Fatalf("after revoke: %v, want %s", err, flexiq.ReasonUnauthenticated)
	}

	wireErr, ok := flexiq.AsError(err)
	if !ok {
		t.Fatalf("%v did not arrive as a *flexiq.Error", err)
	}
	if wireErr.Code != codes.Unauthenticated {
		t.Errorf("code is %s, want Unauthenticated", wireErr.Code)
	}
}

// TestExecuteScopeCannotReachTheProducerDoor pins the other half of the
// credential model: which door a token opens is its scope, not its existence.
func TestExecuteScopeCannotReachTheProducerDoor(t *testing.T) {
	ctx := testContext(t)

	_, token, err := live.mint("go-e2e-executor", "execute")
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	client, err := live.dial(token)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = client.Close() }()

	_, err = client.Enqueue(ctx, flexiq.EnqueueRequest{
		Task:    "orders.process",
		Args:    []any{order{OrderID: "ord-scope", AmountCents: 1}},
		Options: flexiq.EnqueueOptions{Queue: "e2e-scope"},
	})
	if err == nil {
		t.Fatal("an execute-scoped token enqueued a job")
	}
	if !errors.Is(err, flexiq.ReasonScopeDenied) {
		t.Fatalf("Enqueue with the wrong scope: %v, want %s", err, flexiq.ReasonScopeDenied)
	}

	wireErr, ok := flexiq.AsError(err)
	if !ok {
		t.Fatalf("%v did not arrive as a *flexiq.Error", err)
	}
	if wireErr.Code != codes.PermissionDenied {
		t.Errorf("code is %s, want PermissionDenied", wireErr.Code)
	}
	// The accessor exists so a caller can log which scope was missing; it is
	// only useful if the server actually populates the key it reads.
	scope, ok := wireErr.Scope()
	if !ok || scope != "produce" {
		t.Errorf("the refusal names scope %q (present: %v), want produce", scope, ok)
	}
}
