package flexiq

import (
	"fmt"
	"strconv"
	"time"

	pb "github.com/ByteVeda/flexiq/sdks/go/v2/internal/pb/flexiq/v1"
)

// WorkflowGraph is a workflow's shape: the steps it runs and the order they
// run in.
//
// The wire carries the shape and the per-step configuration as two lists that
// must agree, because the server compiles them separately. One [WorkflowNode]
// produces both, so there is no second list here to keep in step.
type WorkflowGraph struct {
	// Nodes are the steps. Every name is non-empty and distinct.
	Nodes []WorkflowNode
	// Edges order them: From must finish before To becomes eligible. A graph
	// with no edges is every node at once.
	Edges []WorkflowEdge
}

// WorkflowEdge orders one node after another.
type WorkflowEdge struct {
	// From must reach a terminal state before To becomes eligible.
	From string
	// To is the node that waits.
	To string
}

// WorkflowNode is one step: the task it runs, the arguments it runs with, and
// how it relates to the steps around it.
//
// The body is Args and Kwargs, encoded into the cross-SDK envelope before the
// call goes out — the same encoding [EnqueueRequest] uses, because a workflow
// step is a job. Raw replaces both for a caller that already has an encoded
// envelope.
type WorkflowNode struct {
	// Name identifies the node within its graph, and is what an edge and a
	// [WorkflowRunNode] name.
	Name string
	// Task is the registered task to run. It is not validated by the server:
	// enqueuing a name nobody implements succeeds and the job dead-letters.
	Task string
	// Args are the positional arguments.
	Args []any
	// Kwargs are the keyword arguments.
	Kwargs map[string]any
	// Raw is a pre-encoded payload envelope. When set, Args and Kwargs are
	// ignored and these bytes reach storage untouched.
	Raw []byte
	// Options are the node's knobs. The zero value is a step on the run's
	// defaults.
	Options WorkflowNodeOptions
}

// WorkflowNodeOptions are the per-step knobs, one field per field of the wire's
// node configuration.
//
// Every zero value means "the run's default", the rule [EnqueueOptions]
// already states. The five pointer fields are the constructs this door does
// not execute — see [Client.SubmitWorkflow].
type WorkflowNodeOptions struct {
	// Queue is empty for the default queue.
	Queue string
	// Priority: higher runs first.
	Priority int32
	// MaxRetries is attempts after the first.
	MaxRetries int32
	// Timeout is how long one attempt may run.
	Timeout time.Duration
	// Condition filters the node against its predecessors' outcome. The zero
	// value is no filter.
	Condition EdgeCondition
	// Compensate is the task to run if a saga rolls this node back. A task
	// name, never JSON.
	Compensate string

	// Gate pauses the node until an external decision arrives.
	Gate *Gate
	// Cache memoizes the node's result across submissions.
	Cache *Cache
	// FanOut expands the node into one job per item at runtime.
	FanOut *FanOut
	// FanIn collects a fan-out back into one node.
	FanIn *FanIn
	// SubWorkflow runs a child workflow as part of this node.
	SubWorkflow *SubWorkflow
}

// Gate pauses a node until an external decision arrives.
type Gate struct {
	// Timeout is how long to wait for a decision. Zero waits indefinitely.
	Timeout time.Duration
	// OnTimeout is what an elapsed timeout decides.
	OnTimeout OnTimeout
	// Message is shown to whoever decides.
	Message string
}

// Cache memoizes a node's result and skips re-running it on a later, otherwise
// identical, submission.
type Cache struct {
	// TTL is how long the memo stays good. Zero caches indefinitely.
	TTL time.Duration
}

// FanOut expands a node into one job per item at runtime.
type FanOut struct {
	// ItemsFrom is the node whose result supplies the items. Empty infers the
	// single predecessor structurally.
	ItemsFrom string
}

// FanIn collects a fan-out's expanded results back into one node.
type FanIn struct {
	// From is the fan-out node this collects.
	From string
}

// SubWorkflow is a child workflow submitted and tracked as part of a node.
type SubWorkflow struct {
	// Name is the child definition's name.
	Name string
	// Version is the child definition's version.
	Version int32
	// Graph is the child's shape, the same type as its parent's.
	Graph WorkflowGraph
	// DeferredNodes are nodes in Graph whose job is not pre-enqueued at
	// submission — a tracker creates it once the node's condition resolves.
	DeferredNodes []string
}

// EdgeCondition filters a node against its predecessors' outcome.
//
// The values are the wire's. A predicate written as code is not representable
// here, because it is code rather than data.
type EdgeCondition int32

const (
	// EdgeConditionUnspecified is no filter, identical in behaviour to
	// [EdgeConditionAlways].
	EdgeConditionUnspecified EdgeCondition = 0
	// EdgeConditionOnSuccess runs the node only if every predecessor
	// succeeded.
	EdgeConditionOnSuccess EdgeCondition = 1
	// EdgeConditionOnFailure runs the node only if a predecessor failed.
	EdgeConditionOnFailure EdgeCondition = 2
	// EdgeConditionAlways runs the node regardless of outcome. Identical to
	// [EdgeConditionUnspecified]; it exists so a caller can say so explicitly
	// rather than by omission.
	EdgeConditionAlways EdgeCondition = 3
)

func (c EdgeCondition) String() string {
	switch c {
	case EdgeConditionUnspecified:
		return nameUnspecified
	case EdgeConditionOnSuccess:
		return "ON_SUCCESS"
	case EdgeConditionOnFailure:
		return "ON_FAILURE"
	case EdgeConditionAlways:
		return "ALWAYS"
	default:
		return "EdgeCondition(" + strconv.FormatInt(int64(c), 10) + ")"
	}
}

// OnTimeout is what a [Gate] decides when its timeout elapses with no decision.
type OnTimeout int32

const (
	// OnTimeoutUnspecified leaves the decision to the server's default.
	OnTimeoutUnspecified OnTimeout = 0
	// OnTimeoutApprove lets the node run.
	OnTimeoutApprove OnTimeout = 1
	// OnTimeoutReject fails the node.
	OnTimeoutReject OnTimeout = 2
)

func (t OnTimeout) String() string {
	switch t {
	case OnTimeoutUnspecified:
		return nameUnspecified
	case OnTimeoutApprove:
		return "APPROVE"
	case OnTimeoutReject:
		return "REJECT"
	default:
		return "OnTimeout(" + strconv.FormatInt(int64(t), 10) + ")"
	}
}

// validate refuses a graph the server refuses on every path, so the message
// names the node rather than arriving as a compile failure from inside the
// server's own graph loader.
//
// It stops there. A cycle is left to the server, which topologically sorts the
// graph anyway and answers INVALID_ARGUMENT; carrying a second topological
// sort here would be a second implementation of the same rule.
func (g WorkflowGraph) validate() error {
	if len(g.Nodes) == 0 {
		return fmt.Errorf("flexiq: submit workflow: graph has no nodes")
	}

	declared := make(map[string]struct{}, len(g.Nodes))
	for i, node := range g.Nodes {
		switch {
		case node.Name == "":
			return fmt.Errorf("flexiq: submit workflow: node %d has no name", i)
		case node.Task == "":
			return fmt.Errorf("flexiq: submit workflow: node %q has no task", node.Name)
		}
		if _, duplicate := declared[node.Name]; duplicate {
			return fmt.Errorf("flexiq: submit workflow: node %q is declared twice", node.Name)
		}
		declared[node.Name] = struct{}{}
	}

	// An edge naming a node the graph does not declare fails server-side as
	// that node "missing from step_metadata", which describes the server's
	// compilation rather than the caller's mistake.
	for _, edge := range g.Edges {
		if _, ok := declared[edge.From]; !ok {
			return fmt.Errorf("flexiq: submit workflow: edge %q -> %q starts at a node the graph does not declare", edge.From, edge.To)
		}
		if _, ok := declared[edge.To]; !ok {
			return fmt.Errorf("flexiq: submit workflow: edge %q -> %q ends at a node the graph does not declare", edge.From, edge.To)
		}
	}
	return nil
}

// toProto splits one node list into the two the wire carries: the bare shape
// the server's DAG needs, and the configuration keyed by the same names.
//
// A nested graph under a [SubWorkflow] goes through here too and is
// deliberately not validated: the server refuses the node for setting
// sub_workflow at all, before it compiles anything, so a local check would
// refuse it for a reason the server never reaches.
func (g WorkflowGraph) toProto() (*pb.WorkflowGraph, error) {
	msg := &pb.WorkflowGraph{
		Nodes:       make([]*pb.WorkflowGraphNode, 0, len(g.Nodes)),
		Edges:       make([]*pb.WorkflowGraphEdge, 0, len(g.Edges)),
		NodeConfigs: make([]*pb.WorkflowNodeConfig, 0, len(g.Nodes)),
	}
	for _, node := range g.Nodes {
		config, err := node.toProto()
		if err != nil {
			return nil, err
		}
		msg.Nodes = append(msg.Nodes, &pb.WorkflowGraphNode{Name: node.Name})
		msg.NodeConfigs = append(msg.NodeConfigs, config)
	}
	for _, edge := range g.Edges {
		msg.Edges = append(msg.Edges, &pb.WorkflowGraphEdge{From: edge.From, To: edge.To})
	}
	return msg, nil
}

func (n WorkflowNode) toProto() (*pb.WorkflowNodeConfig, error) {
	body, err := n.body()
	if err != nil {
		return nil, err
	}

	config := &pb.WorkflowNodeConfig{
		Name:     n.Name,
		TaskName: n.Task,
		Body:     body,
	}
	if err := n.Options.apply(config); err != nil {
		return nil, fmt.Errorf("flexiq: submit workflow: node %q: %w", n.Name, err)
	}
	return config, nil
}

// body picks the node's payload arm, [EnqueueRequest.body]'s rule: there is
// always one, because an absent body is not an empty one and the server
// refuses a node that sets neither.
func (n WorkflowNode) body() (*pb.WorkflowNodeConfig_Raw, error) {
	if n.Raw != nil {
		return &pb.WorkflowNodeConfig_Raw{Raw: n.Raw}, nil
	}
	encoded, err := EncodeCall(n.Args, n.Kwargs)
	if err != nil {
		return nil, fmt.Errorf("flexiq: submit workflow: node %q: %w", n.Name, err)
	}
	return &pb.WorkflowNodeConfig_Raw{Raw: encoded}, nil
}

// apply writes the options onto the node configuration. Each optional scalar is
// left unset at its zero value rather than sent as one, so the run's default
// stays the run's to pick.
func (o WorkflowNodeOptions) apply(config *pb.WorkflowNodeConfig) error {
	if o.Queue != "" {
		config.Queue = &o.Queue
	}
	if o.Priority != 0 {
		config.Priority = &o.Priority
	}
	if o.MaxRetries != 0 {
		config.MaxRetries = &o.MaxRetries
	}
	if o.Compensate != "" {
		config.Compensate = &o.Compensate
	}
	config.Timeout = durationOf(o.Timeout)
	config.Condition = pb.EdgeCondition(o.Condition)

	config.Gate = o.Gate.toProto()
	config.Cache = o.Cache.toProto()
	config.FanOut = o.FanOut.toProto()
	config.FanIn = o.FanIn.toProto()

	sub, err := o.SubWorkflow.toProto()
	if err != nil {
		return err
	}
	config.SubWorkflow = sub
	return nil
}

func (g *Gate) toProto() *pb.GateConfig {
	if g == nil {
		return nil
	}
	msg := &pb.GateConfig{
		Timeout:   durationOf(g.Timeout),
		OnTimeout: pb.OnTimeout(g.OnTimeout),
	}
	if g.Message != "" {
		msg.Message = &g.Message
	}
	return msg
}

func (c *Cache) toProto() *pb.CacheConfig {
	if c == nil {
		return nil
	}
	return &pb.CacheConfig{Ttl: durationOf(c.TTL)}
}

func (f *FanOut) toProto() *pb.FanOutConfig {
	if f == nil {
		return nil
	}
	msg := &pb.FanOutConfig{}
	if f.ItemsFrom != "" {
		msg.ItemsFrom = &f.ItemsFrom
	}
	return msg
}

func (f *FanIn) toProto() *pb.FanInConfig {
	if f == nil {
		return nil
	}
	return &pb.FanInConfig{From: f.From}
}

func (s *SubWorkflow) toProto() (*pb.SubWorkflowSpec, error) {
	if s == nil {
		return nil, nil
	}
	graph, err := s.Graph.toProto()
	if err != nil {
		return nil, err
	}
	return &pb.SubWorkflowSpec{
		Name:              s.Name,
		Version:           s.Version,
		Graph:             graph,
		DeferredNodeNames: s.DeferredNodes,
	}, nil
}
