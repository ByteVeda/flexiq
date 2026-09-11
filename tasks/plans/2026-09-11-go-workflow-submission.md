# Workflow submission over the producer door, in Go (#907)

## The gap

The Go client ships six of the eight `flexiq.v1` RPCs (#829). `SubmitWorkflow`
and `GetWorkflowRun` are the two it left, both MAY-level in
`contracts/REMOTE_SDK_CONTRACT.md`. `sdks/go/README.md:79` says so in as many
words.

They were left because of the surface, not the calls: `workflow.proto` is 300
lines of graph configuration, and mapping it onto readable Go types is the work.
The two RPC methods are a dozen lines each.

## What the server actually accepts

`crates/flexiq-server/src/grpc/producer/workflows.rs` refuses, before anything
is written, any node setting `gate`, `cache`, `fan_out`, `fan_in` or
`sub_workflow` — `FAILED_PRECONDITION`, reason
`WORKFLOW_CONSTRUCT_UNSUPPORTED`, metadata `node` and `field`. A graph that
clears the check is compiled to a bare DAG plus a name-keyed `StepMetadata` map
and handed to `flexiq_workflows::lifecycle::submit_workflow`, which pre-enqueues
one `Job` per node with a `depends_on` chain. Nothing tracks it at runtime; the
core scheduler is what advances it.

So the door executes **static graphs only**, and that is not a gap to be closed
later by this client — it is the door's shape.

## The decision: the Go type mirrors `WorkflowNodeConfig`, all five constructs included

`WorkflowNode` carries every field the wire message does — name, task, body,
the four static knobs (`queue`, `priority`, `max_retries`, `timeout`),
`condition`, `compensate`, **and** `gate`, `cache`, `fan_out`, `fan_in` and
`sub_workflow`.

- One shape for the contract. A Go type that silently dropped five fields would
  be a second, smaller contract a reader has to discover by compiling — and the
  proto already documents each of the five, including the refusal, in the place
  a caller reads.
- The refusal is the server's rule and stays there. Writing it into the client
  as well gives one rule two homes, and the client's copy is the one that goes
  stale the day the door learns to advance a gate — at which point a client
  that refused locally is a client that has to ship before the feature works.
- The refusal is worth arriving usefully, which is the issue's actual ask:
  `Error.WorkflowConstruct()` reads `node` and `field` off the metadata, so a
  caller names both rather than printing a bare reason. `SubWorkflow` carrying
  a nested `WorkflowGraph` means one graph can refuse for a node inside a node,
  and naming which is the whole point.

The Go names are `Gate`, `Cache`, `FanOut`, `FanIn` and `SubWorkflow`, each a
pointer on `WorkflowNodeOptions` so unset is distinguishable from zeroed, which
is exactly what `optional` means on the wire.

## The decision: one `WorkflowRun`, nodes folded in

`GetWorkflowRunResponse` is a run and a repeated node. The read model folds them
into one `WorkflowRun` carrying `Nodes`, because `run.Nodes[0].Status` is what a
caller wants to write and `detail.Run` / `detail.Nodes` is two names for one
answer. `JobPage` is the precedent for a response-shaped struct; this is the
case where it buys nothing.

## Steps

- [x] 1. The graph types and `SubmitWorkflow`
- [x] 2. The run read model and `GetWorkflowRun`
- [x] 3. `WORKFLOW_CONSTRUCT_UNSUPPORTED` names its node and its field
- [x] 4. Tests: the double, and a real server
- [x] 5. The README and the package doc

### 1. The graph types and `SubmitWorkflow` — a `workflow_*` file family

The package is flat and one file per concern, so a surface this size takes a
prefixed family rather than a longer file: `workflow_graph.go` is the shape,
`workflow_submit.go` the call, `workflow_run.go` what comes back and
`workflow_status.go` the enums those carry. The tests mirror the names.

```go
type WorkflowGraph struct {
    Nodes []WorkflowNode
    Edges []WorkflowEdge
}

type WorkflowNode struct {
    Name    string
    Task    string
    Args    []any
    Kwargs  map[string]any
    Raw     []byte
    Options WorkflowNodeOptions
}

type WorkflowNodeOptions struct {
    Queue      string
    Priority   int32
    MaxRetries int32
    Timeout    time.Duration
    Condition  EdgeCondition
    Compensate string

    // The five this door refuses today. Nil is unset.
    Gate        *Gate
    Cache       *Cache
    FanOut      *FanOut
    FanIn       *FanIn
    SubWorkflow *SubWorkflow
}

type WorkflowEdge struct{ From, To string }

type EdgeCondition int32   // proto's numbers; Unspecified reads as Always
type OnTimeout int32       // what a gate does when its timeout elapses

type Gate struct { Timeout time.Duration; OnTimeout OnTimeout; Message string }
type Cache struct { TTL time.Duration }
type FanOut struct { ItemsFrom string }
type FanIn struct { From string }
type SubWorkflow struct {
    Name          string
    Version       int32
    Graph         WorkflowGraph
    DeferredNodes []string
}

type SubmitWorkflowRequest struct {
    Name   string
    Graph  WorkflowGraph
    Params string   // opaque JSON text, Job.Metadata's convention
}

type SubmitWorkflowResult struct{ RunID string }

func (c *Client) SubmitWorkflow(ctx, SubmitWorkflowRequest) (SubmitWorkflowResult, error)
```

One Go `WorkflowNode` produces both a `pb.WorkflowGraphNode` and its
`pb.WorkflowNodeConfig`: the proto splits shape from configuration because the
server compiles the two separately, and a caller has no reason to hold two lists
that must agree. Body arm picking is `EnqueueRequest.body`'s, verbatim in
behaviour — `Raw` wins, otherwise `EncodeCall(Args, Kwargs)`, and the arm is
always set because the server refuses a node with none.

Zero values mean "the server's default", the rule `EnqueueOptions` already
states. It costs nothing here: the server's defaults for priority and
max-retries are both zero (`DEFAULT_PRIORITY`, `DEFAULT_MAX_RETRIES` in
`workflows.rs`), so an explicit zero and an omission are the same submission.

A result struct rather than a bare run-id string: nothing else on this client
returns a scalar, and the RPC's response is additive like every other.

**Local validation**, refusing only what the server refuses on every path, each
message naming the node:

- an empty graph — no nodes
- an empty node name, and a duplicate one (the server keys two maps by it)
- an empty task name (`EnqueueRequest.toProto` refuses the same thing)
- an edge naming a node the graph does not declare — it has no `StepMetadata`
  entry, so `submit_workflow` refuses it as "missing from step_metadata"

Cycles are left to the server: detecting one means carrying a topological sort
in the client, and `topological_order` already answers `INVALID_ARGUMENT`.

A `SubWorkflow`'s nested graph is **not** validated — it travels as data. The
server refuses the node for setting `sub_workflow` at all, before it compiles
anything, so a local check on the nested graph would answer a question the
server never reaches and would name the wrong reason for the refusal.

### 2. The run read model — `sdks/go/workflow_run.go`, `workflow_status.go`

Beside `job.go`, which it reads like.

```go
type WorkflowState int32        // + IsTerminal, IsKnown, String
type WorkflowNodeStatus int32   // + IsTerminal, IsKnown, String
type WorkflowRun struct { ID, DefinitionID string; State WorkflowState; ...; Nodes []WorkflowRunNode }
type WorkflowRunNode struct { Name string; Status WorkflowNodeStatus; JobID string; ... }

func (c *Client) GetWorkflowRun(ctx, runID string) (WorkflowRun, error)
```

`IsTerminal` is `WorkflowState::is_terminal` in `crates/flexiq-workflows/src/state.rs`
— `COMPLETED`, `COMPLETED_WITH_FAILURES`, `FAILED`, `CANCELLED`, `COMPENSATED`,
`COMPENSATION_FAILED`; `COMPENSATING` and `PAUSED` are not. An unrecognised
value is never terminal, `JobStatus.IsTerminal`'s rule and the proto's explicit
instruction.

`WORKFLOW_NODE_STATUS_READY` is reserved, so the node-status numbers have a hole
at 2. The Go constants are written out one by one, never derived.

Absent timestamps become the zero `time.Time` through the existing `asTime`, and
the optional strings (`error`, `parent_run_id`, `parent_node_name`, `job_id`)
become `""` — an absent one and an empty one say the same thing for all four.

### 3. The refusal — `sdks/go/errors.go`

```go
type WorkflowConstructInfo struct{ Node, Field string }
func (e *Error) WorkflowConstruct() (WorkflowConstructInfo, bool)
```

`QueueFull()`'s shape exactly: gated on the reason, false unless both keys are
present. `ReasonWorkflowConstructUnsupported` is already declared.

### 4. Tests

`sdks/go/tests/harness_test.go` — `submitWorkflow` and `getWorkflowRun` on the
double.

`sdks/go/tests/workflow_submit_test.go` and `workflow_run_test.go`, against the
double:

- a graph becomes the nodes, edges and node configs the server compiles: one
  config per node, the body arm set, options carried, `condition` mapped
- `Args`/`Kwargs` encode to the same envelope `Enqueue` puts on the wire, and
  `Raw` reaches the wire untouched
- every local refusal, and that no RPC went out (`fake.calls`)
- all five dynamic constructs reach the wire set — including a nested
  `SubWorkflow` graph — because the client refuses none of them
- `WORKFLOW_CONSTRUCT_UNSUPPORTED` reads back as node + field
- the run read model: enum carry-through for a status this build has no name
  for, unset timestamps as zero, `IsTerminal`/`IsKnown`/`String`

`sdks/go/tests/e2e_workflow_test.go`, behind the existing `integration` tag:
submit a two-node linear graph against a real `flexiq-server`, then
`GetWorkflowRun` shows the run with both nodes and a job id on each. It is the
claim the double cannot make — that what this client compiles is a graph the
server executes.

### 5. Documentation

`sdks/go/README.md` — two rows in the surface table, and the line at :79
replaced by a Workflows section: static graphs only and why, no `unique_key`
equivalent so a retry after a dropped connection submits twice, and no version
field — resubmitting a name with a different graph is refused.

`sdks/go/doc.go` — a paragraph, since it is the package's front page.

## Verification

`make check` from `sdks/go` (build, vet, lint, race), then `make server` and
`make e2e` for the integration test. No proto change, so `buf generate` stays
clean and `sdks/go/internal/pb` is untouched — `workflow.pb.go` is already
generated and committed.
