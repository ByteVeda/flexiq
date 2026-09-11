//go:build integration

package tests

import (
	"errors"
	"testing"

	"google.golang.org/grpc/codes"

	flexiq "github.com/ByteVeda/flexiq/sdks/go/v2"
)

// linearGraph is two steps, the second waiting on the first, on a queue of the
// caller's choosing.
func linearGraph(queue string) flexiq.WorkflowGraph {
	return flexiq.WorkflowGraph{
		Nodes: []flexiq.WorkflowNode{
			{
				Name:    "charge",
				Task:    "billing.charge",
				Args:    []any{order{OrderID: "ord-0007", AmountCents: 700}},
				Options: flexiq.WorkflowNodeOptions{Queue: queue},
			},
			{
				Name:    "ship",
				Task:    "fulfilment.ship",
				Options: flexiq.WorkflowNodeOptions{Queue: queue, MaxRetries: 2},
			},
		},
		Edges: []flexiq.WorkflowEdge{{From: "charge", To: "ship"}},
	}
}

// TestAStaticGraphBecomesJobsTheSchedulerChains is the claim the double cannot
// make: what this client compiles is a graph the server executes. The proof is
// on the jobs — one per node, and the successor waiting on its predecessor
// through the ordinary dependency mechanism rather than through a tracker.
func TestAStaticGraphBecomesJobsTheSchedulerChains(t *testing.T) {
	ctx := testContext(t)

	submitted, err := producer.SubmitWorkflow(ctx, flexiq.SubmitWorkflowRequest{
		Name:   "go-e2e-linear",
		Graph:  linearGraph("wf-linear"),
		Params: `{"origin":"go-e2e"}`,
	})
	if err != nil {
		t.Fatalf("SubmitWorkflow: %v", err)
	}
	if submitted.RunID == "" {
		t.Fatal("submission carried no run id")
	}

	run, err := producer.GetWorkflowRun(ctx, submitted.RunID)
	if err != nil {
		t.Fatalf("GetWorkflowRun: %v", err)
	}
	if run.ID != submitted.RunID || run.DefinitionID == "" {
		t.Errorf("run is %q against definition %q", run.ID, run.DefinitionID)
	}
	if !run.State.IsKnown() || run.State.IsTerminal() {
		t.Errorf("a freshly submitted run is %s", run.State)
	}
	if len(run.Nodes) != 2 {
		t.Fatalf("run carries %d nodes, want 2", len(run.Nodes))
	}

	charge, ok := run.Node("charge")
	if !ok {
		t.Fatal("run has no node named charge")
	}
	ship, ok := run.Node("ship")
	if !ok {
		t.Fatal("run has no node named ship")
	}
	if charge.JobID == "" || ship.JobID == "" {
		t.Fatalf("a static graph left a node unenqueued: charge %q, ship %q", charge.JobID, ship.JobID)
	}

	chargeJob, err := producer.GetJob(ctx, charge.JobID, flexiq.GetJobOptions{IncludePayload: true})
	if err != nil {
		t.Fatalf("GetJob(charge): %v", err)
	}
	if chargeJob.TaskName != "billing.charge" || chargeJob.Queue != "wf-linear" {
		t.Errorf("charge runs %q on %q", chargeJob.TaskName, chargeJob.Queue)
	}
	if chargeJob.HasDeps {
		t.Error("the first node waits on a dependency")
	}
	// The node's arguments travel as the same envelope an enqueue writes.
	call, err := chargeJob.DecodePayload()
	if err != nil {
		t.Fatalf("decode the charge payload: %v", err)
	}
	if len(call.Args) != 1 {
		t.Fatalf("charge carries %d arguments, want 1", len(call.Args))
	}

	shipJob, err := producer.GetJob(ctx, ship.JobID, flexiq.GetJobOptions{})
	if err != nil {
		t.Fatalf("GetJob(ship): %v", err)
	}
	if shipJob.TaskName != "fulfilment.ship" {
		t.Errorf("ship runs %q", shipJob.TaskName)
	}
	if !shipJob.HasDeps {
		t.Error("the successor does not wait on its predecessor")
	}
	if shipJob.MaxRetries != 2 {
		t.Errorf("ship allows %d retries, want the node's 2", shipJob.MaxRetries)
	}
}

// TestADynamicConstructIsRefusedNamingItsNode is the refusal this client sends
// rather than pre-empts. The server is the one that decides which constructs it
// can advance, and the whole value of the round trip is the two metadata keys
// that come back with the no.
func TestADynamicConstructIsRefusedNamingItsNode(t *testing.T) {
	ctx := testContext(t)

	graph := linearGraph("wf-gated")
	graph.Nodes[1].Options.Gate = &flexiq.Gate{Message: "sign off before shipping"}

	_, err := producer.SubmitWorkflow(ctx, flexiq.SubmitWorkflowRequest{
		Name:  "go-e2e-gated",
		Graph: graph,
	})
	if !errors.Is(err, flexiq.ReasonWorkflowConstructUnsupported) {
		t.Fatalf("SubmitWorkflow answered %v, want a refused construct", err)
	}

	wireErr, ok := flexiq.AsError(err)
	if !ok {
		t.Fatalf("error is %T, want *flexiq.Error", err)
	}
	if wireErr.Code != codes.FailedPrecondition {
		t.Errorf("code is %s, want FailedPrecondition", wireErr.Code)
	}
	construct, ok := wireErr.WorkflowConstruct()
	if !ok {
		t.Fatal("the refusal named no node and no field")
	}
	if construct.Node != "ship" || construct.Field != "gate" {
		t.Errorf("refusal names node %q field %q, want ship and gate", construct.Node, construct.Field)
	}
}

// TestResubmittingANameStartsASecondRun pins the two halves of a submission
// having no unique key: the same graph under the same name is a second run and
// not a deduplicated one, and a *different* graph under that name is refused
// rather than silently reused — a run's definition has to describe the graph
// that produced its jobs.
func TestResubmittingANameStartsASecondRun(t *testing.T) {
	ctx := testContext(t)

	request := flexiq.SubmitWorkflowRequest{
		Name:  "go-e2e-resubmitted",
		Graph: linearGraph("wf-resubmit"),
	}
	first, err := producer.SubmitWorkflow(ctx, request)
	if err != nil {
		t.Fatalf("SubmitWorkflow: %v", err)
	}
	second, err := producer.SubmitWorkflow(ctx, request)
	if err != nil {
		t.Fatalf("SubmitWorkflow again: %v", err)
	}
	if first.RunID == second.RunID {
		t.Error("a retried submission returned the first run; there is no unique key here")
	}

	changed := request
	changed.Graph.Nodes[1].Task = "fulfilment.hold"
	_, err = producer.SubmitWorkflow(ctx, changed)
	if !errors.Is(err, flexiq.ReasonInvalidRequest) {
		t.Fatalf("a different graph under the same name answered %v", err)
	}
}

// TestAnUnknownWorkflowRunIsNotFound covers the read side's one refusal, which
// is also what a run in another namespace answers.
func TestAnUnknownWorkflowRunIsNotFound(t *testing.T) {
	ctx := testContext(t)

	_, err := producer.GetWorkflowRun(ctx, "00000000-0000-7000-8000-000000000000")
	wireErr, ok := flexiq.AsError(err)
	if !ok {
		t.Fatalf("error is %T, want *flexiq.Error", err)
	}
	if wireErr.Code != codes.NotFound {
		t.Errorf("code is %s, want NotFound", wireErr.Code)
	}
}
