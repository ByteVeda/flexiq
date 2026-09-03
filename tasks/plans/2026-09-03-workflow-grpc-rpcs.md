# #771 — SubmitWorkflow and GetWorkflowRun

Part of #710. The half of #714 §7.6 deferred: workflow submission and reads
over `flexiq.v1`.

Governed by `tasks/specs/2026-09-01-flexiq-v1-proto-design.md` §7.6, D25, D26,
D27. §11's row for #771 lists the six ways this fails; each is answered below.

## The shape

Two problems, both closed in the spec already:

1. **The wire never carries `dag_data` bytes.** `WorkflowGraph` is typed —
   nodes, edges, and a `WorkflowNodeConfig` per node with every `StepMetadata`
   field, including `gate`/`cache`/`fan_out`/`fan_in`/`sub_workflow` as typed
   submessages (D25). The full message set is in §7.6 — this plan does not
   repeat it, only the Rust that produces and consumes it.
2. **`SubmitWorkflow` executes only statically-sequenceable graphs** (D26). A
   node setting `gate`/`cache`/`fan_out`/`fan_in`/`sub_workflow` is refused,
   `FAILED_PRECONDITION`, naming the node — because nothing today advances a
   dynamic construct outside a live SDK tracker process, and a silently-stuck
   `Pending` run is the failure §5.4 already refuses for a namespace a token
   cannot pick.

A static graph gets exactly what `PyQueue::submit_workflow`'s static path
produces today (`crates/flexiq-python/src/py_queue/workflow_ops/lifecycle.rs:40-239`):
one `Job` row per node, pre-enqueued with a `depends_on` chain from the DAG
edges. That logic is already pyo3-free below its `#[pymethods]` signature — it
only touches `flexiq_core::storage::StorageBackend` and
`flexiq_workflows::{WorkflowStorage, topological_order}` — so it moves into
`flexiq-workflows` verbatim and both `flexiq-python` and `flexiq-server` call
the one copy. `mark_workflow_node_result` (the completion side,
`workflow_ops/nodes.rs:31-152`) does **not** move: it is called by a worker's
own tracker reacting to that worker's own job completion, resolves
`run_id`/`node_name` from the job's storage metadata rather than anything the
submitter held in memory, and does not care whether `PyQueue::submit_workflow`
or `SubmitWorkflow` created the job. Nothing here touches it.

Namespace scoping needs no new plumbing. `WorkflowStorageBackend` is
constructed once per namespace at process start
(`crates/flexiq-server/src/config/backend.rs:23-27`, already shared with the
dashboard via `dashboard/state.rs:18`) — unlike `flexiq_core::Storage`, its
trait methods take no `namespace` parameter at all, because the value is baked
into the handle at construction (mirrors `WorkflowSqliteStorage::new(pool,
namespace)`). `Producer` gains a second field holding that same handle.

## The lift: `flexiq-workflows`

New file `crates/flexiq-workflows/src/lifecycle.rs`, `pub mod lifecycle;` in
`lib.rs`. Ported from `lifecycle.rs:40-239` with the `#[pymethods]`/`PyResult`
layer stripped and the `PyQueue` fields it read turned into explicit
parameters — `deferred_node_names`/`cache_hit_nodes` stay, because Python's
existing dynamic-workflow callers (fan-out expansion, sub-workflow submission)
still need them; `SubmitWorkflow` always passes both empty, since D26 refuses
before this function is ever called on a dynamic graph:

```rust
pub struct SubmitStaticWorkflowRequest {
    pub name: String,
    pub version: i32,
    pub dag_bytes: Vec<u8>,
    pub step_metadata: HashMap<String, StepMetadata>,
    pub node_payloads: HashMap<String, Vec<u8>>,
    pub queue_default: String,
    pub params_json: Option<String>,
    pub deferred_node_names: HashSet<String>,
    pub cache_hit_nodes: HashMap<String, String>,
    pub parent_run_id: Option<String>,
    pub parent_node_name: Option<String>,
    pub default_timeout_ms: i64,
    pub default_priority: i32,
    pub result_ttl_ms: Option<i64>,
    pub namespace: Option<String>,
}

pub struct WorkflowRunHandle {
    pub run_id: String,
    pub definition_id: String,
}

pub fn submit_workflow(
    storage: &StorageBackend,
    wf_storage: &WorkflowStorageBackend,
    request: SubmitStaticWorkflowRequest,
) -> flexiq_core::error::Result<WorkflowRunHandle>
```

One behavioural change from the ported body: `timeout_ms = meta.timeout_ms
.unwrap_or(self.default_timeout * 1000)` becomes `meta.timeout_ms
.unwrap_or(request.default_timeout_ms)` — the `* 1000` lived in `PyQueue`
because its field was seconds; the shared function takes milliseconds
directly (matching `producer/convert.rs::DEFAULT_TIMEOUT_MS`), and
`PyQueue`'s thin wrapper does the `* 1000` at the call site instead.

`parse_step_metadata` and `build_metadata_json`
(`crates/flexiq-python/src/py_queue/workflow_ops/mod.rs:116-132`) move into
the same file — both are already pyo3-free except `parse_step_metadata`'s
`PyValueError`, which becomes a `flexiq_workflows::WorkflowError`. Before
deleting the originals: `rg -n 'build_metadata_json|parse_step_metadata'
crates/flexiq-python/src` — if a caller outside `lifecycle.rs` uses either
(fan-out/sub-workflow paths are the candidates), re-export both from
`flexiq_workflows::lifecycle` at the `workflow_ops` call sites instead of
inlining a second copy.

`crates/flexiq-python/src/py_queue/workflow_ops/lifecycle.rs`'s
`submit_workflow` `#[pymethods]` becomes:

```rust
pub fn submit_workflow(&self, /* unchanged signature */) -> PyResult<PyWorkflowHandle> {
    let wf_storage = workflow_storage(self)?;
    let step_metadata = parse_step_metadata(step_metadata_json)
        .map_err(|e| PyValueError::new_err(e.to_string()))?;
    let handle = flexiq_workflows::lifecycle::submit_workflow(
        &self.storage,
        &wf_storage,
        flexiq_workflows::lifecycle::SubmitStaticWorkflowRequest {
            name: name.to_string(),
            version,
            dag_bytes,
            step_metadata,
            node_payloads,
            queue_default: queue_default.to_string(),
            params_json,
            deferred_node_names: deferred_node_names.unwrap_or_default().into_iter().collect(),
            cache_hit_nodes: cache_hit_nodes.unwrap_or_default(),
            parent_run_id,
            parent_node_name,
            default_timeout_ms: self.default_timeout * 1000,
            default_priority: self.default_priority,
            result_ttl_ms: self.result_ttl_ms,
            namespace: self.namespace.clone(),
        },
    )
    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    Ok(PyWorkflowHandle { run_id: handle.run_id, name: name.to_string(), definition_id: handle.definition_id })
}
```

The existing tests in `lifecycle.rs` (`submit_workflow_links_linear_dag_via_depends_on`
and siblings, lines 361-668) stay where they are and keep passing unmodified —
they exercise the wrapper, which now delegates. New tests for the ported logic
itself move to `crates/flexiq-workflows/src/lifecycle.rs`'s own `#[cfg(test)]`
module: port the same five scenarios (linear DAG via `depends_on`, deferred
nodes skip job creation, cache-hit nodes terminal without a job, reused
`(name, version)` returns the same definition id, missing `step_metadata` is
refused) against `WorkflowSqliteStorage::in_memory` directly, no `PyQueue`
needed.

## The handler: `flexiq-server`

**`crates/flexiq-server/src/config/backend.rs`** — no change; `Backend` already
carries `workflows: WorkflowStorageBackend` (line 27).

**`crates/flexiq-server/src/grpc/producer/mod.rs`** — `Producer` gains a
second field:

```rust
pub struct Producer {
    storage: StorageBackend,
    workflows: WorkflowStorageBackend,
}

impl Producer {
    pub fn new(storage: StorageBackend, workflows: WorkflowStorageBackend) -> Self {
        Self { storage, workflows }
    }
}
```

`Scoped<'_>` gains a `workflows: &'a WorkflowStorageBackend` field and a
`pub(crate) fn workflows(&self) -> &WorkflowStorageBackend` accessor, set in
`Producer::scope`. Add `pub mod workflows;` beside `enqueue`/`reads`/`cancel`,
and two new arms in the `ProducerService for Producer` impl:

```rust
async fn submit_workflow(&self, request: Request<pb::SubmitWorkflowRequest>) -> Result<Response<pb::SubmitWorkflowResponse>, Status> {
    let (scoped, message) = self.scope(request)?;
    workflows::submit_workflow(&scoped, message).await
}
async fn get_workflow_run(&self, request: Request<pb::GetWorkflowRunRequest>) -> Result<Response<pb::GetWorkflowRunResponse>, Status> {
    let (scoped, message) = self.scope(request)?;
    workflows::get_workflow_run(&scoped, message).await
}
```

**`crates/flexiq-server/src/grpc/producer/workflows.rs`** (new):

```rust
pub async fn submit_workflow(
    scoped: &Scoped<'_>,
    request: pb::SubmitWorkflowRequest,
) -> Result<Response<pb::SubmitWorkflowResponse>, Status> {
    let graph = request.graph.ok_or_else(|| WireError::invalid_request("graph is required"))?;
    refuse_dynamic_constructs(&graph)?;                       // D26
    let (dag_bytes, step_metadata, node_payloads) = compile_graph(&graph)?;
    let params_json = decode_params(request.params)?;         // oneof params_raw/params_structured -> Option<String>, reusing producer::structured
    let handle = flexiq_workflows::lifecycle::submit_workflow(
        scoped.storage(), scoped.workflows(),
        flexiq_workflows::lifecycle::SubmitStaticWorkflowRequest {
            name: request.name, version: 1, dag_bytes, step_metadata, node_payloads,
            queue_default: "default".to_string(), params_json,
            deferred_node_names: Default::default(), cache_hit_nodes: Default::default(),
            parent_run_id: None, parent_node_name: None,
            default_timeout_ms: convert::DEFAULT_TIMEOUT_MS, default_priority: 0,
            result_ttl_ms: None, namespace: Some(scoped.namespace().to_string()),
        },
    )
    .map_err(|e| WireError::from_queue_error(&e))?;
    Ok(Response::new(pb::SubmitWorkflowResponse { run_id: handle.run_id }))
}

pub async fn get_workflow_run(
    scoped: &Scoped<'_>,
    request: pb::GetWorkflowRunRequest,
) -> Result<Response<pb::GetWorkflowRunResponse>, Status> {
    let run = scoped.workflows().get_workflow_run(&request.run_id)
        .map_err(|e| WireError::from_queue_error(&e))?
        .ok_or_else(|| Status::not_found(format!("workflow run '{}' not found", request.run_id)))?;
    let nodes = scoped.workflows().get_workflow_nodes(&request.run_id)
        .map_err(|e| WireError::from_queue_error(&e))?;
    Ok(Response::new(pb::GetWorkflowRunResponse {
        run: Some(convert::workflow_run_to_wire(run)),
        nodes: nodes.into_iter().map(convert::workflow_node_to_wire).collect(),
    }))
}
```

`compile_graph` builds a `dagron_core::SerializableGraph`-shaped value
directly — `nodes: graph.nodes.iter().map(|n| SerializableNode { name: n.name.clone(), payload: None })`, `edges` the same with `weight: 1.0`, `label: None`
— and `serde_json::to_vec` it for `dag_bytes` (no `dagron_core::DAG` builder
needed; `flexiq_workflows::topological_order` only ever reads this JSON back).
Node bodies compile through the existing `producer/structured.rs` encoder —
the same one `enqueue.rs` uses for `EnqueueRequest.body` — so a `raw` node
payload is stored verbatim and a `structured` one is encoded through
`flexiq_core::wire::encode_call`, never a third encoding.

`refuse_dynamic_constructs` walks `graph.node_configs`; the first node with
`gate.is_some() || cache.is_some() || fan_out.is_some() || fan_in.is_some() ||
sub_workflow.is_some()` fails the whole call:

```rust
fn refuse_dynamic_constructs(graph: &pb::WorkflowGraph) -> Result<(), Status> {
    for node in &graph.node_configs {
        let field = if node.gate.is_some() { "gate" }
            else if node.cache.is_some() { "cache" }
            else if node.fan_out.is_some() { "fan_out" }
            else if node.fan_in.is_some() { "fan_in" }
            else if node.sub_workflow.is_some() { "sub_workflow" }
            else { continue };
        return Err(WireError::workflow_construct_unsupported(&node.name, field).into());
    }
    Ok(())
}
```

**`crates/flexiq-server/src/grpc/status/reason.rs`** — one new constant:
`pub const WORKFLOW_CONSTRUCT_UNSUPPORTED: &str = "WORKFLOW_CONSTRUCT_UNSUPPORTED";`

**`crates/flexiq-server/src/grpc/status/mod.rs`** — one new constructor beside
`invalid_request`/`no_such_method`:

```rust
/// A node uses a construct `SubmitWorkflow` cannot yet execute (D26).
pub fn workflow_construct_unsupported(node: &str, field: &str) -> Self {
    let mut wire = Self {
        code: Code::FailedPrecondition,
        reason: reason::WORKFLOW_CONSTRUCT_UNSUPPORTED,
        message: format!("node '{node}' sets '{field}', which SubmitWorkflow does not execute yet"),
        metadata: HashMap::new(),
        retry_after: None,
    };
    wire.metadata.insert("node".to_string(), node.to_string());
    wire.metadata.insert("field".to_string(), field.to_string());
    wire
}
```

**`crates/flexiq-server/src/grpc/producer/convert.rs`** — widen
`DEFAULT_TIMEOUT_MS` from `const` to `pub(crate) const` (`workflows.rs` needs
it too, and it should stay the one definition rather than a second literal
`300_000`). Add, beside `status_to_wire`, the same exhaustive shape for
`WorkflowState` and `WorkflowNodeStatus` (D27) — **one direction only**:
neither wire enum is ever read back off a request in this issue (unlike
`JobStatus`, which `ListJobs` filters by, `status_from_wire` has no analogue
here), so only `_to_wire` exists, not a matching `_from_wire` with no caller:

```rust
pub fn workflow_state_to_wire(state: WorkflowState) -> pb::WorkflowState { /* 10-arm exhaustive match, offset by one, mirrors status_to_wire */ }
pub fn workflow_node_status_to_wire(status: WorkflowNodeStatus) -> pb::WorkflowNodeStatus { /* 11-arm exhaustive match */ }
pub fn workflow_run_to_wire(run: WorkflowRun) -> pb::WorkflowRun { /* field-for-field, timestamps via timestamp() */ }
pub fn workflow_node_to_wire(node: WorkflowNode) -> pb::WorkflowNode { /* field-for-field */ }
```

with a test over all 10 `WorkflowState` variants and all 11
`WorkflowNodeStatus` variants asserting `wire as i32 == variant as i32 + 1`
for each — the half of `the_status_enums_are_offset_by_exactly_one` that
applies without a `_from_wire` side.

**`crates/flexiq-server/src/grpc/listener.rs`** — `serve` takes a fourth
parameter `workflows: WorkflowStorageBackend`, passed to
`Producer::new(storage.clone(), workflows.clone())`.
**`crates/flexiq-server/src/grpc/mod.rs`** — `serve` wrapper takes and forwards
the same parameter.
**`crates/flexiq-server/src/runtime/mod.rs:216-221`** — the call site becomes
`crate::grpc::serve(grpc, backend.storage.clone(), backend.workflows.clone(), door, shutdown.clone())`.

## The facade

D2/D15 apply unchanged: `flexiq-server/src/grpc/facade/routes.rs` gains

```rust
pub enum Rpc { /* existing six */, SubmitWorkflow, GetWorkflowRun }
pub enum Binding { /* existing seven */, SubmitWorkflow, GetWorkflowRun }
```

`Binding::SubmitWorkflow` → `POST /v1/workflows`, `Verb::Post`; `Binding::GetWorkflowRun`
→ `GET /v1/workflows/{run_id}`, `Verb::Get` — `GetWorkflowRun`'s
`NO_SIDE_EFFECTS` level is already pinned in §6's table, so
`a_get_serves_exactly_the_no_side_effects_rpcs` fails immediately if either
verb is swapped. Both added to `ROUTES`.

`crates/flexiq-server/src/grpc/facade/json/request.rs` gains a `SubmitWorkflow`
struct mirroring `Enqueue`'s shape (`into_message() -> Result<pb::SubmitWorkflowRequest, String>`,
same nested-oneof pattern `Enqueue`'s `StructuredArgs` conversion already uses
for `body`, applied recursively to every `WorkflowNodeConfig.body` in the
graph). `json/response.rs` gains `submit_workflow`/`get_workflow_run`
following `enqueue`/`get_job`'s pattern. The four drift tests at the bottom of
`routes.rs` (`every_producer_rpc_has_a_route`,
`every_route_names_an_rpc_the_package_declares`,
`a_get_serves_exactly_the_no_side_effects_rpcs`,
`no_two_bindings_answer_the_same_method_on_the_same_pattern`) need no changes
— they walk `ROUTES` and the descriptor, so they simply start checking the two
new entries.

## Build order

Each commit compiles and tests on its own (pre-commit stashes unstaged tracked
files, so a commit needing a later one's hunks fails the hook,
[[feedback_precommit_drops_untracked]]).

1. **contract.** `contracts/proto/flexiq/v1/workflow.proto` — every message in
   §7.6's amendment. `scripts/proto-check.sh --fix` regenerates
   `contracts/descriptor.binpb`. `buf format`/`lint` clean.
2. **`flexiq-workflows` — the lift.** `src/lifecycle.rs`: `submit_workflow`,
   `SubmitStaticWorkflowRequest`, `WorkflowRunHandle`, `parse_step_metadata`,
   `build_metadata_json`, ported tests. `pub mod lifecycle;` in `lib.rs`.
3. **`flexiq-python` — delegate.** `workflow_ops/lifecycle.rs`'s
   `submit_workflow` calls the shared function; `workflow_ops/mod.rs` drops
   its own `parse_step_metadata`/`build_metadata_json` (or re-exports, per the
   grep above). Existing Python-facing tests unchanged and green —
   `cargo test -p flexiq-python --features workflows -j2`.
4. **`flexiq-server` — `convert.rs`.** `workflow_state_to_wire`/
   `workflow_node_status_to_wire`/`workflow_run_to_wire`/`workflow_node_to_wire`,
   round-trip test. `WireError::workflow_construct_unsupported` +
   `reason::WORKFLOW_CONSTRUCT_UNSUPPORTED`.
5. **`flexiq-server` — the handler.** `Producer` gains `workflows`; `grpc/producer/workflows.rs`
   (`submit_workflow`, `get_workflow_run`, `compile_graph`,
   `refuse_dynamic_constructs`); wired into the `ProducerService` trait impl;
   `listener.rs`/`grpc/mod.rs`/`runtime/mod.rs` thread the new parameter.
6. **`flexiq-server` — the facade.** `routes.rs` (`Rpc`/`Binding` variants,
   `ROUTES`), `json/request.rs` (`SubmitWorkflow`), `json/response.rs`
   (`submit_workflow`, `get_workflow_run`).
7. **tests.** `crates/flexiq-server/tests/` gains an end-to-end test:
   `SubmitWorkflow` a two-node linear graph over the gRPC client, run an
   in-process `flexiq-python` worker with `trackWorkflows()` against the same
   SQLite file, poll `GetWorkflowRun` to `Completed`. A second test submits a
   graph with `gate` set on one node and asserts `FAILED_PRECONDITION` +
   `reason=WORKFLOW_CONSTRUCT_UNSUPPORTED` + `metadata.node` naming it.

## Acceptance

- No `bytes dag_data` field exists anywhere in `contracts/proto/flexiq/v1/`.
- A static graph submitted over gRPC is tracked and its steps run by an
  unmodified `flexiq-python` worker against the same database — the
  end-to-end test in step 7.
- A graph using `gate`/`cache`/`fan_out`/`fan_in`/`sub_workflow` is refused at
  submit time, naming the node, before anything is written to storage.
- `buf breaking contracts/proto --against '.git#ref=origin/master,subdir=contracts/proto'`
  passes — every change is additive (D3, D4).
- `WorkflowState` and `WorkflowNodeStatus` conversions are exhaustive, no
  wildcard arm, pinned by a test that fails to compile when a variant is
  added.
- `GetWorkflowRun` returns the run and every node's status, mirroring
  `dashboard/routes/workflows.rs`'s `detail()` handler.
- The four facade drift tests in `routes.rs` pass with the two new RPCs
  included, with no hand-written allowlist touched.

## Not in this issue

Executing `gate`/`cache`/`fan_out`/`fan_in`/`sub_workflow` over gRPC — the
message shapes exist (D25) so this is a later, additive release once a
tracking substrate reachable from `flexiq-server` exists. `SubmitWorkflow`
idempotency (no `unique_key` equivalent) — already a stated gap in §10 point
10, unchanged by this issue. Cancelling a run, approving a gate, listing runs
over gRPC — operator actions, stay behind the dashboard's `Admin` gate (D13).
Node and Java SDK changes — neither is touched; `mark_workflow_node_result`
and its napi/Java equivalents keep working exactly as they do today.
