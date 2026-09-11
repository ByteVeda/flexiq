package flexiq

import (
	"context"
	"time"

	pb "github.com/ByteVeda/flexiq/sdks/go/v2/internal/pb/flexiq/v1"
)

// WorkflowRun is a run as a reader sees it.
//
// A time the server did not set is the zero [time.Time]; test with IsZero,
// never against a sentinel.
type WorkflowRun struct {
	// ID is the run, and is what was submitted under.
	ID string
	// DefinitionID names the graph this run walks. Two runs of one unchanged
	// graph share it.
	DefinitionID string
	// State is the run's lifecycle state.
	State WorkflowState
	// Error is why the run failed, empty otherwise.
	Error string
	// ParentRunID and ParentNodeName are set when this run is a sub-workflow:
	// the run that started it, and the node in that run awaiting it.
	ParentRunID    string
	ParentNodeName string
	CreatedAt      time.Time
	StartedAt      time.Time
	CompletedAt    time.Time

	// Nodes is every node the run has, in no particular order.
	//
	// The wire carries it beside the run rather than inside it, mirroring how
	// the two are stored; this model folds them together because a run without
	// its nodes answers almost nothing a caller asked.
	Nodes []WorkflowRunNode
}

// Node finds one node of the run by name. The second return is false when the
// run has no node by that name.
func (r WorkflowRun) Node(name string) (WorkflowRunNode, bool) {
	for _, node := range r.Nodes {
		if node.Name == name {
			return node, true
		}
	}
	return WorkflowRunNode{}, false
}

// WorkflowRunNode is one node's state within a run.
type WorkflowRunNode struct {
	// Name is the node's name in the submitted graph.
	Name string
	// Status is the node's state.
	Status WorkflowNodeStatus
	// JobID is the job carrying this node's work. Empty while the node has no
	// job yet — a deferred node before a tracker creates one, or a node whose
	// result came from a cache hit.
	JobID string
	// Error is why the node failed, empty otherwise.
	Error string
	// StartedAt and CompletedAt are zero until the node reaches them.
	StartedAt   time.Time
	CompletedAt time.Time
}

// GetWorkflowRun reads a run and every node it has.
//
// There is no completion notification on this door — no watch, no server
// stream. Poll this, and stop when [WorkflowState.IsTerminal] says to.
//
// A run in another namespace answers NOT_FOUND, indistinguishable from a run
// that never existed.
func (c *Client) GetWorkflowRun(ctx context.Context, runID string) (WorkflowRun, error) {
	resp, err := c.producer.GetWorkflowRun(ctx, &pb.GetWorkflowRunRequest{RunId: runID})
	if err != nil {
		return WorkflowRun{}, fromRPC(err)
	}
	return workflowRunFromProto(resp.GetRun(), resp.GetNodes()), nil
}

// workflowRunFromProto maps the two wire messages onto the one read model.
//
// An enum value this build has no name for is carried through as its number
// rather than rejected, and a field it has no name for is dropped — the same
// trade [jobFromProto] makes, and for the same reason.
func workflowRunFromProto(msg *pb.WorkflowRun, nodes []*pb.WorkflowNode) WorkflowRun {
	run := WorkflowRun{}
	if msg != nil {
		run = WorkflowRun{
			ID:             msg.GetId(),
			DefinitionID:   msg.GetDefinitionId(),
			State:          WorkflowState(msg.GetState()),
			Error:          msg.GetError(),
			ParentRunID:    msg.GetParentRunId(),
			ParentNodeName: msg.GetParentNodeName(),
			CreatedAt:      asTime(msg.GetCreatedAt()),
			StartedAt:      asTime(msg.GetStartedAt()),
			CompletedAt:    asTime(msg.GetCompletedAt()),
		}
	}

	run.Nodes = make([]WorkflowRunNode, 0, len(nodes))
	for _, node := range nodes {
		run.Nodes = append(run.Nodes, WorkflowRunNode{
			Name:        node.GetName(),
			Status:      WorkflowNodeStatus(node.GetStatus()),
			JobID:       node.GetJobId(),
			Error:       node.GetError(),
			StartedAt:   asTime(node.GetStartedAt()),
			CompletedAt: asTime(node.GetCompletedAt()),
		})
	}
	return run
}
