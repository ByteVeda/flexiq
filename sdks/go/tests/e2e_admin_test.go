//go:build integration

package tests

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"testing"

	flexiq "github.com/ByteVeda/flexiq/sdks/go/v2"
	"github.com/ByteVeda/flexiq/sdks/go/v2/admin"
)

// operator mints the credential an infrastructure tool would hold — admin,
// inspect and tokens — and an admin client for it, closed with the test.
func operator(t *testing.T) (id string, client *admin.Client) {
	t.Helper()

	id, token, err := live.mint("go-e2e-operator-"+t.Name(), "tokens", "admin", "inspect")
	if err != nil {
		t.Fatalf("mint an operator token: %v", err)
	}
	client, err = admin.New(live.addr, flexiq.WithToken(token), flexiq.WithInsecureTransport())
	if err != nil {
		t.Fatalf("admin.New: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return id, client
}

// TestAdminQueueOverrideRoundTrip drives the calls a queue resource makes:
// set, read back, pause, resume, clear — and a value the server refuses.
func TestAdminQueueOverrideRoundTrip(t *testing.T) {
	ctx := testContext(t)
	_, client := operator(t)
	const queue = "e2e-admin-queue"

	limit := int32(2)
	if _, err := client.SetQueueOverride(ctx, queue, admin.QueueOverride{RateLimit: "10/s", MaxConcurrent: &limit}); err != nil {
		t.Fatalf("SetQueueOverride: %v", err)
	}
	override, found, err := client.GetQueueOverride(ctx, queue)
	if err != nil || !found {
		t.Fatalf("GetQueueOverride = found %v, err %v", found, err)
	}
	if override.RateLimit != "10/s" || override.MaxConcurrent == nil || *override.MaxConcurrent != 2 {
		t.Errorf("override read back as %+v", override)
	}

	if _, err = client.PauseQueue(ctx, queue); err != nil {
		t.Fatalf("PauseQueue: %v", err)
	}
	read, found, err := client.GetQueue(ctx, queue)
	if err != nil || !found || !read.Paused {
		t.Errorf("after a pause GetQueue = %+v, found %v, err %v", read, found, err)
	}
	resumed, err := client.ResumeQueue(ctx, queue)
	if err != nil || resumed.Paused {
		t.Errorf("ResumeQueue = %+v, %v", resumed, err)
	}

	if err = client.ClearQueueOverride(ctx, queue); err != nil {
		t.Fatalf("ClearQueueOverride: %v", err)
	}
	if _, found, err = client.GetQueueOverride(ctx, queue); err != nil || found {
		t.Errorf("after a clear GetQueueOverride = found %v, err %v", found, err)
	}

	_, err = client.SetQueueOverride(ctx, queue, admin.QueueOverride{RateLimit: "fast"})
	if !errors.Is(err, flexiq.ReasonInvalidRequest) {
		t.Errorf("a malformed rate limit: want ReasonInvalidRequest, got %v", err)
	}
}

// TestAdminPeriodicTaskRoundTrip: the payload this client encodes is the
// payload the server hands back, which is what drift detection compares.
func TestAdminPeriodicTaskRoundTrip(t *testing.T) {
	ctx := testContext(t)
	_, client := operator(t)
	const name = "e2e-admin-nightly"

	args := []any{order{OrderID: "ord-0002", AmountCents: 250}}
	put, err := client.PutPeriodicTask(ctx, admin.PeriodicTaskSpec{
		Name:        name,
		Task:        "orders.reconcile",
		Cron:        "0 0 0 1 1 *",
		Queue:       "e2e-admin-periodic",
		Timezone:    "Europe/Berlin",
		Args:        args,
		StartPaused: true,
	})
	if err != nil {
		t.Fatalf("PutPeriodicTask: %v", err)
	}
	if put.Enabled || put.Timezone != "Europe/Berlin" || put.NextRun.IsZero() || put.Payload != nil {
		t.Errorf("put answered %+v", put)
	}

	read, err := client.GetPeriodicTask(ctx, name, admin.GetPeriodicTaskOptions{IncludePayload: true})
	if err != nil {
		t.Fatalf("GetPeriodicTask: %v", err)
	}
	sent, err := flexiq.EncodeCall(args, nil)
	if err != nil {
		t.Fatalf("EncodeCall: %v", err)
	}
	if !bytes.Equal(read.Payload, sent) {
		t.Errorf("payload came back as %x, sent %x", read.Payload, sent)
	}

	resumed, err := client.ResumePeriodicTask(ctx, name)
	if err != nil || !resumed.Enabled {
		t.Errorf("ResumePeriodicTask = %+v, %v", resumed, err)
	}
	// A replace never changes whether a task is paused.
	replaced, err := client.PutPeriodicTask(ctx, admin.PeriodicTaskSpec{
		Name: name, Task: "orders.reconcile", Cron: "0 0 0 1 1 *", Queue: "e2e-admin-periodic", StartPaused: true,
	})
	if err != nil || !replaced.Enabled {
		t.Errorf("a replace with StartPaused = %+v, %v; want it still enabled", replaced, err)
	}

	listed, err := client.ListPeriodicTasks(ctx, admin.ListPeriodicTasksQuery{Queue: "e2e-admin-periodic"})
	if err != nil || len(listed) != 1 || listed[0].Name != name {
		t.Errorf("ListPeriodicTasks = %+v, %v", listed, err)
	}

	if err = client.DeletePeriodicTask(ctx, name); err != nil {
		t.Fatalf("DeletePeriodicTask: %v", err)
	}
	_, err = client.GetPeriodicTask(ctx, name, admin.GetPeriodicTaskOptions{})
	if !errors.Is(err, flexiq.ReasonPeriodicTaskNotFound) {
		t.Errorf("a deleted task: want ReasonPeriodicTaskNotFound, got %v", err)
	}
}

// TestAdminQuotaRoundTrip sets only row ceilings: the namespace is the suite's
// own, and a depth or rate limit would reach every other test's enqueues.
func TestAdminQuotaRoundTrip(t *testing.T) {
	ctx := testContext(t)
	_, client := operator(t)
	// A failure midway must not leave the ceilings behind for the next test.
	t.Cleanup(func() { _ = client.ClearNamespaceQuota(context.Background()) })

	archived, dead := int64(100000), int64(50000)
	stored, err := client.SetNamespaceQuota(ctx, admin.NamespaceQuota{
		MaxArchivedRows: &archived,
		MaxDeadRows:     &dead,
		OnExcess:        admin.OverflowReject,
	})
	if err != nil {
		t.Fatalf("SetNamespaceQuota: %v", err)
	}
	if stored.MaxPending != nil || stored.MaxArchivedRows == nil || *stored.MaxArchivedRows != archived {
		t.Errorf("stored quota is %+v", stored)
	}

	read, err := client.GetNamespaceQuota(ctx)
	if err != nil || read.MaxDeadRows == nil || *read.MaxDeadRows != dead {
		t.Errorf("GetNamespaceQuota = %+v, %v", read, err)
	}

	if err = client.ClearNamespaceQuota(ctx); err != nil {
		t.Fatalf("ClearNamespaceQuota: %v", err)
	}
	if read, err = client.GetNamespaceQuota(ctx); err != nil || !read.IsZero() {
		t.Errorf("after a clear GetNamespaceQuota = %+v, %v", read, err)
	}
}

// TestAdminTokenLifecycle mints a token, uses it, revokes it, and watches it
// stop working — the whole life a token resource manages.
func TestAdminTokenLifecycle(t *testing.T) {
	ctx := testContext(t)
	operatorID, client := operator(t)

	// The operator's own token lives the default 90 days, so a default-lived
	// mint made after it would outlive it.
	_, err := client.CreateToken(ctx, admin.CreateTokenRequest{Name: "too-long", Scopes: []string{"inspect"}})
	if !errors.Is(err, flexiq.ReasonInvalidRequest) {
		t.Fatalf("an outliving mint: want ReasonInvalidRequest, got %v", err)
	}

	minted, err := client.CreateToken(ctx, admin.CreateTokenRequest{
		Name: "e2e-reader", Scopes: []string{"inspect"}, ExpireDays: 30,
	})
	if err != nil {
		t.Fatalf("CreateToken: %v", err)
	}
	if minted.Secret == "" || minted.Token.Status != admin.TokenStatusActive ||
		minted.Token.CreatedBy != "token:"+operatorID || minted.Token.Namespace != e2eNamespace {
		t.Errorf("minted %+v", minted.Token)
	}

	reader, err := admin.New(live.addr, flexiq.WithToken(minted.Secret), flexiq.WithInsecureTransport())
	if err != nil {
		t.Fatalf("admin.New: %v", err)
	}
	defer func() { _ = reader.Close() }()
	if _, err = reader.ListQueues(ctx); err != nil {
		t.Fatalf("the minted token cannot read: %v", err)
	}

	got, err := client.GetToken(ctx, minted.Token.ID)
	if err != nil || got.Name != "e2e-reader" || !slices.Equal(got.Scopes, []string{"inspect"}) {
		t.Errorf("GetToken = %+v, %v", got, err)
	}
	all, err := client.ListTokens(ctx)
	if err != nil || !slices.ContainsFunc(all, func(tok admin.Token) bool { return tok.ID == minted.Token.ID }) {
		t.Errorf("ListTokens did not list the minted token: %v", err)
	}

	revoked, err := client.RevokeToken(ctx, minted.Token.ID)
	if err != nil || revoked.Status != admin.TokenStatusRevoked || revoked.RevokedAt.IsZero() {
		t.Fatalf("RevokeToken = %+v, %v", revoked, err)
	}
	if _, err = reader.ListQueues(ctx); !errors.Is(err, flexiq.ReasonUnauthenticated) {
		t.Errorf("a revoked token: want ReasonUnauthenticated, got %v", err)
	}

	if _, err = client.GetToken(ctx, "no-such-token"); !errors.Is(err, flexiq.ReasonTokenNotFound) {
		t.Errorf("an unknown id: want ReasonTokenNotFound, got %v", err)
	}
}
