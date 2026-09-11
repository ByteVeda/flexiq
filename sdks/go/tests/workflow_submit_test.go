package tests

import (
	"context"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	flexiq "github.com/ByteVeda/flexiq/sdks/go/v2"
	pb "github.com/ByteVeda/flexiq/sdks/go/v2/internal/pb/flexiq/v1"
)

// captureGraph makes the double record the request and answer with a run id.
func captureGraph(t *testing.T, into **pb.SubmitWorkflowRequest) *flexiq.Client {
	t.Helper()

	return serve(t, &fakeProducer{
		submitWorkflow: func(_ context.Context, req *pb.SubmitWorkflowRequest) (*pb.SubmitWorkflowResponse, error) {
			*into = req
			return &pb.SubmitWorkflowResponse{RunId: "run-7"}, nil
		},
	})
}

// nodeConfig finds one node's configuration in a submitted graph.
func nodeConfig(t *testing.T, graph *pb.WorkflowGraph, name string) *pb.WorkflowNodeConfig {
	t.Helper()

	for _, config := range graph.GetNodeConfigs() {
		if config.GetName() == name {
			return config
		}
	}
	t.Fatalf("graph carries no config for node %q", name)
	return nil
}

// TestSubmitWorkflowSplitsOneNodeListIntoTwo is the mapping the Go type exists
// for: the wire carries a graph's shape and its configuration as two lists
// that must agree node for node, and a caller holds one list.
func TestSubmitWorkflowSplitsOneNodeListIntoTwo(t *testing.T) {
	var got *pb.SubmitWorkflowRequest
	client := captureGraph(t, &got)

	result, err := client.SubmitWorkflow(context.Background(), flexiq.SubmitWorkflowRequest{
		Name: "checkout",
		Graph: flexiq.WorkflowGraph{
			Nodes: []flexiq.WorkflowNode{
				{Name: "charge", Task: "billing.charge"},
				{Name: "ship", Task: "fulfilment.ship"},
			},
			Edges: []flexiq.WorkflowEdge{{From: "charge", To: "ship"}},
		},
		Params: `{"order":"ord-1"}`,
	})
	if err != nil {
		t.Fatalf("SubmitWorkflow: %v", err)
	}
	if result.RunID != "run-7" {
		t.Errorf("run id is %q, want run-7", result.RunID)
	}

	if got.GetName() != "checkout" {
		t.Errorf("name is %q", got.GetName())
	}
	if got.GetParamsJson() != `{"order":"ord-1"}` {
		t.Errorf("params are %q", got.GetParamsJson())
	}

	graph := got.GetGraph()
	if len(graph.GetNodes()) != 2 || len(graph.GetNodeConfigs()) != 2 {
		t.Fatalf("graph carries %d nodes and %d configs, want 2 and 2",
			len(graph.GetNodes()), len(graph.GetNodeConfigs()))
	}
	if graph.GetNodes()[0].GetName() != "charge" || graph.GetNodes()[1].GetName() != "ship" {
		t.Errorf("node names are %q and %q", graph.GetNodes()[0].GetName(), graph.GetNodes()[1].GetName())
	}
	if nodeConfig(t, graph, "charge").GetTaskName() != "billing.charge" {
		t.Errorf("charge runs %q", nodeConfig(t, graph, "charge").GetTaskName())
	}

	if len(graph.GetEdges()) != 1 {
		t.Fatalf("graph carries %d edges, want 1", len(graph.GetEdges()))
	}
	if edge := graph.GetEdges()[0]; edge.GetFrom() != "charge" || edge.GetTo() != "ship" {
		t.Errorf("edge is %q -> %q", edge.GetFrom(), edge.GetTo())
	}
}

// TestWorkflowNodeBodyIsTheSameEnvelopeEnqueueSends pins that a workflow step
// is a job: its arguments are the cross-SDK envelope, byte for byte the vector
// Enqueue is held to, not a second encoding for a second call.
func TestWorkflowNodeBodyIsTheSameEnvelopeEnqueueSends(t *testing.T) {
	var got *pb.SubmitWorkflowRequest
	client := captureGraph(t, &got)

	raw := mustHex(t, "028280a1616bf5")
	if _, err := client.SubmitWorkflow(context.Background(), flexiq.SubmitWorkflowRequest{
		Name: "w",
		Graph: flexiq.WorkflowGraph{Nodes: []flexiq.WorkflowNode{
			{Name: "encoded", Task: "t", Args: []any{1, "a"}},
			{Name: "preencoded", Task: "t", Args: []any{"ignored"}, Raw: raw},
			{Name: "empty", Task: "t"},
		}},
	}); err != nil {
		t.Fatalf("SubmitWorkflow: %v", err)
	}

	graph := got.GetGraph()
	// The vector from BINDING_CONTRACT.md: f(1, "a") with no kwargs.
	if want := "028282016161a0"; hex.EncodeToString(nodeConfig(t, graph, "encoded").GetRaw()) != want {
		t.Errorf("encoded body is %s, want %s",
			hex.EncodeToString(nodeConfig(t, graph, "encoded").GetRaw()), want)
	}
	if got := nodeConfig(t, graph, "preencoded").GetRaw(); hex.EncodeToString(got) != hex.EncodeToString(raw) {
		t.Errorf("pre-encoded body is %s, want the raw bytes %s", hex.EncodeToString(got), hex.EncodeToString(raw))
	}
	// A node with no arguments still sets the arm — the server refuses a node
	// that sets none, and an absent body is not an empty one.
	if _, ok := nodeConfig(t, graph, "empty").GetBody().(*pb.WorkflowNodeConfig_Raw); !ok {
		t.Errorf("a node with no arguments left its body arm unset")
	}
}

// TestWorkflowNodeOptionsMapOntoTheWire walks the per-step knobs, including the
// four carrying explicit presence: a zero has to arrive unset, or the node
// takes a value the caller never chose instead of the run's default.
func TestWorkflowNodeOptionsMapOntoTheWire(t *testing.T) {
	var got *pb.SubmitWorkflowRequest
	client := captureGraph(t, &got)

	if _, err := client.SubmitWorkflow(context.Background(), flexiq.SubmitWorkflowRequest{
		Name: "w",
		Graph: flexiq.WorkflowGraph{Nodes: []flexiq.WorkflowNode{
			{Name: "set", Task: "t", Options: flexiq.WorkflowNodeOptions{
				Queue:      "payments",
				Priority:   5,
				MaxRetries: 3,
				Timeout:    90 * time.Second,
				Condition:  flexiq.EdgeConditionOnFailure,
				Compensate: "billing.refund",
			}},
			{Name: "default", Task: "t"},
		}},
	}); err != nil {
		t.Fatalf("SubmitWorkflow: %v", err)
	}

	set := nodeConfig(t, got.GetGraph(), "set")
	if set.GetQueue() != "payments" {
		t.Errorf("queue is %q", set.GetQueue())
	}
	if set.GetPriority() != 5 || set.GetMaxRetries() != 3 {
		t.Errorf("priority is %d and max retries %d", set.GetPriority(), set.GetMaxRetries())
	}
	if set.GetTimeout().AsDuration() != 90*time.Second {
		t.Errorf("timeout is %s", set.GetTimeout().AsDuration())
	}
	if set.GetCondition() != pb.EdgeCondition_EDGE_CONDITION_ON_FAILURE {
		t.Errorf("condition is %s", set.GetCondition())
	}
	if set.GetCompensate() != "billing.refund" {
		t.Errorf("compensate is %q", set.GetCompensate())
	}

	unset := nodeConfig(t, got.GetGraph(), "default")
	switch {
	case unset.Queue != nil:
		t.Error("an unset queue arrived as a value")
	case unset.Priority != nil:
		t.Error("an unset priority arrived as a value")
	case unset.MaxRetries != nil:
		t.Error("unset max retries arrived as a value")
	case unset.Compensate != nil:
		t.Error("an unset compensation task arrived as a value")
	case unset.GetTimeout() != nil:
		t.Error("an unset timeout arrived as a value")
	}
}

// TestDynamicConstructsReachTheWire is the decision this client makes about the
// five constructs the door refuses: it sends them. Refusing them here would put
// the server's rule in two places, and the client's copy is the one that goes
// stale the day the door learns to advance a gate.
func TestDynamicConstructsReachTheWire(t *testing.T) {
	var got *pb.SubmitWorkflowRequest
	client := captureGraph(t, &got)

	if _, err := client.SubmitWorkflow(context.Background(), flexiq.SubmitWorkflowRequest{
		Name: "w",
		Graph: flexiq.WorkflowGraph{Nodes: []flexiq.WorkflowNode{
			{Name: "approve", Task: "t", Options: flexiq.WorkflowNodeOptions{
				Gate: &flexiq.Gate{
					Timeout:   time.Hour,
					OnTimeout: flexiq.OnTimeoutReject,
					Message:   "sign off on the refund",
				},
			}},
			{Name: "memoized", Task: "t", Options: flexiq.WorkflowNodeOptions{
				Cache: &flexiq.Cache{TTL: 24 * time.Hour},
			}},
			{Name: "spread", Task: "t", Options: flexiq.WorkflowNodeOptions{
				FanOut: &flexiq.FanOut{ItemsFrom: "approve"},
			}},
			{Name: "collect", Task: "t", Options: flexiq.WorkflowNodeOptions{
				FanIn: &flexiq.FanIn{From: "spread"},
			}},
			{Name: "child", Task: "t", Options: flexiq.WorkflowNodeOptions{
				SubWorkflow: &flexiq.SubWorkflow{
					Name:    "refund",
					Version: 2,
					Graph: flexiq.WorkflowGraph{Nodes: []flexiq.WorkflowNode{
						{Name: "inner", Task: "billing.refund"},
					}},
					DeferredNodes: []string{"inner"},
				},
			}},
		}},
	}); err != nil {
		t.Fatalf("SubmitWorkflow: %v", err)
	}

	graph := got.GetGraph()
	gate := nodeConfig(t, graph, "approve").GetGate()
	if gate.GetTimeout().AsDuration() != time.Hour ||
		gate.GetOnTimeout() != pb.OnTimeout_ON_TIMEOUT_REJECT ||
		gate.GetMessage() != "sign off on the refund" {
		t.Errorf("gate is %v", gate)
	}
	if ttl := nodeConfig(t, graph, "memoized").GetCache().GetTtl(); ttl.AsDuration() != 24*time.Hour {
		t.Errorf("cache ttl is %s", ttl.AsDuration())
	}
	if from := nodeConfig(t, graph, "spread").GetFanOut().GetItemsFrom(); from != "approve" {
		t.Errorf("fan-out reads items from %q", from)
	}
	if from := nodeConfig(t, graph, "collect").GetFanIn().GetFrom(); from != "spread" {
		t.Errorf("fan-in collects %q", from)
	}

	// The nested graph is a WorkflowGraph like its parent, and travels as data:
	// the server refuses the node for setting sub_workflow before it compiles
	// anything, so nothing here inspects it.
	sub := nodeConfig(t, graph, "child").GetSubWorkflow()
	if sub.GetName() != "refund" || sub.GetVersion() != 2 {
		t.Errorf("sub-workflow is %q version %d", sub.GetName(), sub.GetVersion())
	}
	if len(sub.GetGraph().GetNodeConfigs()) != 1 ||
		sub.GetGraph().GetNodeConfigs()[0].GetTaskName() != "billing.refund" {
		t.Errorf("nested graph is %v", sub.GetGraph())
	}
	if len(sub.GetDeferredNodeNames()) != 1 || sub.GetDeferredNodeNames()[0] != "inner" {
		t.Errorf("deferred nodes are %v", sub.GetDeferredNodeNames())
	}
}

// TestSubmitWorkflowRefusesWhatNoServerAccepts covers the checks that happen
// before the RPC. Each is something the server refuses on every path, named
// here against the caller's own graph rather than against the server's
// compilation of it — and each proves no request went out at all.
func TestSubmitWorkflowRefusesWhatNoServerAccepts(t *testing.T) {
	twoNodes := []flexiq.WorkflowNode{{Name: "a", Task: "t"}, {Name: "b", Task: "t"}}

	cases := []struct {
		name string
		req  flexiq.SubmitWorkflowRequest
	}{
		{"no name", flexiq.SubmitWorkflowRequest{
			Graph: flexiq.WorkflowGraph{Nodes: twoNodes},
		}},
		{"no nodes", flexiq.SubmitWorkflowRequest{Name: "w"}},
		{"a node with no name", flexiq.SubmitWorkflowRequest{
			Name:  "w",
			Graph: flexiq.WorkflowGraph{Nodes: []flexiq.WorkflowNode{{Task: "t"}}},
		}},
		{"a node with no task", flexiq.SubmitWorkflowRequest{
			Name:  "w",
			Graph: flexiq.WorkflowGraph{Nodes: []flexiq.WorkflowNode{{Name: "a"}}},
		}},
		{"a node declared twice", flexiq.SubmitWorkflowRequest{
			Name: "w",
			Graph: flexiq.WorkflowGraph{Nodes: []flexiq.WorkflowNode{
				{Name: "a", Task: "t"}, {Name: "a", Task: "u"},
			}},
		}},
		{"an edge from nowhere", flexiq.SubmitWorkflowRequest{
			Name: "w",
			Graph: flexiq.WorkflowGraph{
				Nodes: twoNodes,
				Edges: []flexiq.WorkflowEdge{{From: "ghost", To: "b"}},
			},
		}},
		{"an edge to nowhere", flexiq.SubmitWorkflowRequest{
			Name: "w",
			Graph: flexiq.WorkflowGraph{
				Nodes: twoNodes,
				Edges: []flexiq.WorkflowEdge{{From: "a", To: "ghost"}},
			},
		}},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			fake := &fakeProducer{}
			client := serve(t, fake)

			if _, err := client.SubmitWorkflow(context.Background(), testCase.req); err == nil {
				t.Fatal("SubmitWorkflow accepted it")
			}
			if fake.calls != 0 {
				t.Errorf("%d requests went out, want 0", fake.calls)
			}
		})
	}
}

// TestWorkflowConstructUnsupportedNamesNodeAndField is the refusal a caller has
// to act on: a graph can set several, the server refuses on the first, and a
// bare reason leaves the caller reading a message written for a human to find
// out which node to fix.
func TestWorkflowConstructUnsupportedNamesNodeAndField(t *testing.T) {
	refusal := status.New(codes.FailedPrecondition,
		"node 'approve' sets 'gate', which SubmitWorkflow does not execute yet")
	withDetails, err := refusal.WithDetails(&errdetails.ErrorInfo{
		Domain:   flexiq.ErrorDomain,
		Reason:   string(flexiq.ReasonWorkflowConstructUnsupported),
		Metadata: map[string]string{"node": "approve", "field": "gate"},
	})
	if err != nil {
		t.Fatalf("attach details: %v", err)
	}

	client := serve(t, &fakeProducer{
		submitWorkflow: func(context.Context, *pb.SubmitWorkflowRequest) (*pb.SubmitWorkflowResponse, error) {
			return nil, withDetails.Err()
		},
	})

	_, err = client.SubmitWorkflow(context.Background(), flexiq.SubmitWorkflowRequest{
		Name: "w",
		Graph: flexiq.WorkflowGraph{Nodes: []flexiq.WorkflowNode{
			{Name: "approve", Task: "t", Options: flexiq.WorkflowNodeOptions{Gate: &flexiq.Gate{}}},
		}},
	})
	if !errors.Is(err, flexiq.ReasonWorkflowConstructUnsupported) {
		t.Fatalf("errors.Is did not match the reason: %v", err)
	}

	wireErr, ok := flexiq.AsError(err)
	if !ok {
		t.Fatalf("error is %T, want *flexiq.Error", err)
	}
	construct, ok := wireErr.WorkflowConstruct()
	if !ok {
		t.Fatal("WorkflowConstruct reported nothing")
	}
	if construct.Node != "approve" || construct.Field != "gate" {
		t.Errorf("refusal names node %q field %q", construct.Node, construct.Field)
	}
}

// TestWorkflowConstructIsAbsentWithoutBothKeys keeps the accessor honest: a
// half-populated refusal is a server bug, and answering it with an empty node
// name would have a caller print "node \"\" sets gate".
func TestWorkflowConstructIsAbsentWithoutBothKeys(t *testing.T) {
	client := serve(t, &fakeProducer{
		submitWorkflow: func(context.Context, *pb.SubmitWorkflowRequest) (*pb.SubmitWorkflowResponse, error) {
			st, err := status.New(codes.FailedPrecondition, "refused").WithDetails(&errdetails.ErrorInfo{
				Domain:   flexiq.ErrorDomain,
				Reason:   string(flexiq.ReasonWorkflowConstructUnsupported),
				Metadata: map[string]string{"node": "approve"},
			})
			if err != nil {
				return nil, err
			}
			return nil, st.Err()
		},
	})

	_, err := client.SubmitWorkflow(context.Background(), flexiq.SubmitWorkflowRequest{
		Name:  "w",
		Graph: flexiq.WorkflowGraph{Nodes: []flexiq.WorkflowNode{{Name: "approve", Task: "t"}}},
	})
	wireErr, ok := flexiq.AsError(err)
	if !ok {
		t.Fatalf("error is %T, want *flexiq.Error", err)
	}
	if _, ok := wireErr.WorkflowConstruct(); ok {
		t.Error("WorkflowConstruct answered on a refusal carrying only one key")
	}
}
