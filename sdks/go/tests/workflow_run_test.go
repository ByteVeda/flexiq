package tests

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	flexiq "github.com/ByteVeda/flexiq/sdks/go/v2"
	pb "github.com/ByteVeda/flexiq/sdks/go/v2/internal/pb/flexiq/v1"
)

// TestGetWorkflowRunFoldsNodesIntoTheRun covers the read model: two wire
// messages arrive and one value comes back, with an absent timestamp staying
// distinguishable from the epoch.
func TestGetWorkflowRunFoldsNodesIntoTheRun(t *testing.T) {
	started := time.Now().Add(-time.Minute).UTC().Truncate(time.Millisecond)
	jobID := "job-9"
	nodeError := "billing declined"

	client := serve(t, &fakeProducer{
		getWorkflowRun: func(_ context.Context, req *pb.GetWorkflowRunRequest) (*pb.GetWorkflowRunResponse, error) {
			return &pb.GetWorkflowRunResponse{
				Run: &pb.WorkflowRun{
					Id:           req.GetRunId(),
					DefinitionId: "def-1",
					State:        pb.WorkflowState_WORKFLOW_STATE_RUNNING,
					StartedAt:    timestamppb.New(started),
				},
				Nodes: []*pb.WorkflowNode{
					{
						Name:   "charge",
						Status: pb.WorkflowNodeStatus_WORKFLOW_NODE_STATUS_FAILED,
						JobId:  &jobID,
						Error:  &nodeError,
					},
					{Name: "ship", Status: pb.WorkflowNodeStatus_WORKFLOW_NODE_STATUS_PENDING},
				},
			}, nil
		},
	})

	run, err := client.GetWorkflowRun(context.Background(), "run-7")
	if err != nil {
		t.Fatalf("GetWorkflowRun: %v", err)
	}

	if run.ID != "run-7" || run.DefinitionID != "def-1" {
		t.Errorf("run is %q against definition %q", run.ID, run.DefinitionID)
	}
	if run.State != flexiq.WorkflowStateRunning {
		t.Errorf("state is %s", run.State)
	}
	if !run.StartedAt.Equal(started) {
		t.Errorf("started at %s, want %s", run.StartedAt, started)
	}
	if !run.CompletedAt.IsZero() {
		t.Errorf("an unset completion arrived as %s, want the zero time", run.CompletedAt)
	}
	if run.Error != "" || run.ParentRunID != "" || run.ParentNodeName != "" {
		t.Errorf("unset optional strings arrived as %q, %q, %q", run.Error, run.ParentRunID, run.ParentNodeName)
	}

	if len(run.Nodes) != 2 {
		t.Fatalf("run carries %d nodes, want 2", len(run.Nodes))
	}
	charge, ok := run.Node("charge")
	if !ok {
		t.Fatal("run has no node named charge")
	}
	if charge.Status != flexiq.WorkflowNodeStatusFailed || charge.JobID != jobID || charge.Error != nodeError {
		t.Errorf("charge is %s on job %q: %q", charge.Status, charge.JobID, charge.Error)
	}
	ship, ok := run.Node("ship")
	if !ok {
		t.Fatal("run has no node named ship")
	}
	if ship.JobID != "" {
		t.Errorf("a node with no job carries job id %q", ship.JobID)
	}
	if _, ok := run.Node("nothing"); ok {
		t.Error("Node found a node the run does not have")
	}
}

// TestUnknownWorkflowEnumsSurviveAsNumbers is the rule that keeps a client
// working against a newer server: a value with no name here is carried through
// rather than rejected, and is never read as finished.
func TestUnknownWorkflowEnumsSurviveAsNumbers(t *testing.T) {
	client := serve(t, &fakeProducer{
		getWorkflowRun: func(context.Context, *pb.GetWorkflowRunRequest) (*pb.GetWorkflowRunResponse, error) {
			return &pb.GetWorkflowRunResponse{
				Run:   &pb.WorkflowRun{Id: "run-7", State: 99},
				Nodes: []*pb.WorkflowNode{{Name: "a", Status: 98}},
			}, nil
		},
	})

	run, err := client.GetWorkflowRun(context.Background(), "run-7")
	if err != nil {
		t.Fatalf("GetWorkflowRun: %v", err)
	}

	if run.State != 99 {
		t.Errorf("state is %d, want the number carried through", run.State)
	}
	if run.State.IsKnown() || run.State.IsTerminal() {
		t.Error("a state this build has no name for read as known or terminal")
	}
	if run.State.String() != "WorkflowState(99)" {
		t.Errorf("state prints as %q", run.State)
	}

	node := run.Nodes[0]
	if node.Status != 98 || node.Status.IsKnown() || node.Status.IsTerminal() {
		t.Errorf("node status %d read as known or terminal", node.Status)
	}
}

// TestWorkflowStateTerminality pins which states a poller may stop on. A
// compensation in flight is still work, and 2 is the node status 2.0.0 removed.
func TestWorkflowStateTerminality(t *testing.T) {
	terminal := []flexiq.WorkflowState{
		flexiq.WorkflowStateCompleted,
		flexiq.WorkflowStateCompletedWithFailures,
		flexiq.WorkflowStateFailed,
		flexiq.WorkflowStateCancelled,
		flexiq.WorkflowStateCompensated,
		flexiq.WorkflowStateCompensationFailed,
	}
	for _, state := range terminal {
		if !state.IsTerminal() || !state.IsKnown() {
			t.Errorf("%s is not terminal", state)
		}
	}

	notTerminal := []flexiq.WorkflowState{
		flexiq.WorkflowStatePending,
		flexiq.WorkflowStateRunning,
		flexiq.WorkflowStatePaused,
		flexiq.WorkflowStateCompensating,
	}
	for _, state := range notTerminal {
		if state.IsTerminal() {
			t.Errorf("%s read as terminal", state)
		}
	}

	if flexiq.WorkflowStateUnspecified.IsKnown() {
		t.Error("the unspecified state read as known")
	}
	// 2 was WORKFLOW_NODE_STATUS_READY until 2.0.0. It is a hole, not a value.
	if flexiq.WorkflowNodeStatus(2).IsKnown() {
		t.Error("the removed READY status read as known")
	}
	if !flexiq.WorkflowNodeStatusCacheHit.IsTerminal() {
		t.Error("a cache hit read as unfinished")
	}
	if flexiq.WorkflowNodeStatusWaitingApproval.IsTerminal() {
		t.Error("a node waiting on a gate read as terminal")
	}
}
