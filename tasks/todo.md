# Workflow submission over the producer door, in Go (#907)

Plan: `tasks/plans/2026-09-11-go-workflow-submission.md`

The Go client ships six of the eight `flexiq.v1` RPCs. `SubmitWorkflow` and
`GetWorkflowRun` are the two it left — not for the calls, but for the 300 lines
of `workflow.proto` behind them. The door executes **static graphs only**, but
the Go type mirrors `WorkflowNodeConfig` whole — gate, cache, fan-out, fan-in
and sub-workflow included — so the contract has one shape, and the refusal
arrives naming its node and its field rather than as a bare reason.

- [x] 1. The graph types and `SubmitWorkflow`
- [x] 2. The run read model and `GetWorkflowRun`
- [x] 3. `WORKFLOW_CONSTRUCT_UNSUPPORTED` names its node and its field
- [x] 4. Tests: the double, and a real server
- [x] 5. The README and the package doc

## Review

`sdks/go` now speaks all eight `flexiq.v1` RPCs. The submission surface is a
`workflow_*` family rather than one file: `workflow_graph.go` is the shape a
caller builds, `workflow_submit.go` the call, `workflow_run.go` what comes back,
`workflow_status.go` the enums those carry, and the tests mirror the names.

One Go `WorkflowNode` compiles into both wire lists — the bare graph node and
its `WorkflowNodeConfig` — so the two can never disagree, and it carries all
five dynamic constructs. Refusing them locally would put the server's rule in
two places and leave the client's copy to go stale the day the door learns to
advance a gate; instead `Error.WorkflowConstruct()` reads the `node` and `field`
the refusal carries.

What is checked before the RPC is only what no server accepts: an empty graph, a
nameless or duplicated node, a node with no task, an edge naming a node the
graph never declared. Cycles are the server's to find, and a nested
sub-workflow graph travels as data.

Verified with `make check` (build, vet, lint at zero issues, race suite) and
`make server && make e2e` — the four new end-to-end tests submit a real graph,
confirm the successor job waits on its predecessor through the ordinary
dependency chain, watch a real server refuse a gate by name, and prove a
resubmission is a second run rather than a deduplicated one.
