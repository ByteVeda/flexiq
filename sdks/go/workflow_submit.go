package flexiq

import (
	"context"
	"fmt"

	pb "github.com/ByteVeda/flexiq/sdks/go/v2/internal/pb/flexiq/v1"
)

// SubmitWorkflowRequest is one workflow to submit.
type SubmitWorkflowRequest struct {
	// Name is the definition to submit under. Every submission is version 1 of
	// it: resubmitting a name whose version 1 holds a different graph is
	// refused rather than silently reused, because a run's definition must
	// describe the graph that produced its jobs.
	Name string
	// Graph is the workflow's shape.
	Graph WorkflowGraph
	// Params is caller data recorded on the run, opaque JSON text and
	// byte-preserved — Job.Metadata's convention, not a call a task receives.
	Params string
}

// SubmitWorkflowResult is what a submission produced.
type SubmitWorkflowResult struct {
	// RunID is what [Client.GetWorkflowRun] polls on.
	RunID string
}

// SubmitWorkflow submits a graph and returns the run it created.
//
// # Static graphs only
//
// The server pre-enqueues one job per node, chained by the graph's edges, and
// the ordinary scheduler advances it — no tracker process is involved, and any
// worker with workflow tracking enabled advances the run, not only whoever
// submitted it.
//
// That is also the limit. A node setting [WorkflowNodeOptions.Gate],
// [WorkflowNodeOptions.Cache], [WorkflowNodeOptions.FanOut],
// [WorkflowNodeOptions.FanIn] or [WorkflowNodeOptions.SubWorkflow] is refused,
// FAILED_PRECONDITION with reason [ReasonWorkflowConstructUnsupported], before
// anything is written — nothing outside a live SDK process can advance a run
// that uses one. Read the offending node and field with
// [Error.WorkflowConstruct]. The fields are here because the wire carries them
// and a refusal names the field it refused; submitting one is a round trip
// that changes nothing.
//
// # It is not idempotent
//
// There is no unique-key equivalent for a workflow, so a call retried after a
// dropped connection submits a second run. A caller that must not submit twice
// records the run id it got before retrying, or submits under a name it can
// check for first.
func (c *Client) SubmitWorkflow(ctx context.Context, req SubmitWorkflowRequest) (SubmitWorkflowResult, error) {
	msg, err := req.toProto()
	if err != nil {
		return SubmitWorkflowResult{}, err
	}

	resp, err := c.producer.SubmitWorkflow(ctx, msg)
	if err != nil {
		return SubmitWorkflowResult{}, fromRPC(err)
	}
	return SubmitWorkflowResult{RunID: resp.GetRunId()}, nil
}

func (r SubmitWorkflowRequest) toProto() (*pb.SubmitWorkflowRequest, error) {
	if r.Name == "" {
		return nil, fmt.Errorf("flexiq: submit workflow: name is empty")
	}
	if err := r.Graph.validate(); err != nil {
		return nil, err
	}

	graph, err := r.Graph.toProto()
	if err != nil {
		return nil, err
	}
	msg := &pb.SubmitWorkflowRequest{Name: r.Name, Graph: graph}
	if r.Params != "" {
		msg.ParamsJson = &r.Params
	}
	return msg, nil
}
