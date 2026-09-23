# `flexiq.admin.v1` (#836) — design

Issue: #836. Absorbs the "admin, separately" and "read-only" bullets of #839
for this package only; per-queue / per-task resource patterns stay #839's.

## The problem

`flexiq.v1` submits and reads; `flexiq.executor.v1` claims and reports. Every
operator action — pause a queue, work the dead-letter queue, look at workers,
manage periodic tasks, change a rate limit — is dashboard-only. The dashboard's
HTTP API is not a contract, and `fq` (#832) tabled eight operations it cannot
offer (`docs/content/docs/server/operate/cli.mdx`, "What the wire cannot do
yet"). Seven land here; drain is filed separately (D9).

## What reading the code found

The issue's constraint — **every RPC namespace-scoped** — is false of the core
for five of the surfaces it names. Today these are one global keyspace:

| Surface | Where | Today |
| --- | --- | --- |
| Queue pause | `queue_state (queue_name)`, Redis `queues:paused` | a pause in tenant A pauses same-named queue in every tenant — the scheduler's `active_queues()` reads it unscoped |
| Worker registry | `workers (worker_id)`, Redis `worker:<id>` | `list_workers()` returns every tenant's workers |
| DLQ purge | `purge_dead`, `purge_dead_by_task` | purge deletes every tenant's dead letters |
| Task/queue overrides | settings KV `overrides:task:<n>` / `overrides:queue:<n>` | shared by every tenant using the same task name |
| Queue enumeration | dashboard `list_queues` unions stats (scoped) with pauses + overrides (not) | leaks other tenants' queue names |

Also: periodic tasks have **no dashboard surface at all** and no "trigger now"
primitive; operator drain has **no mechanism** (workers only self-report
`Draining`); there is no windowed per-queue rate source.

The user chose to namespace the core first, then build the door on top.

## Decisions

### D1 — Namespace identity follows the periodic rule

For every surface made namespaced here, `namespace: Option<&str>` on an
**identity** (`pause_queue`, `resume_queue`, `list_paused_queues`,
`list_workers`, override keys) names **one** namespace, `None` = the default
namespace, never a wildcard — the `(namespace, name)` rule from #918, and the
rule `dequeue` already follows. A scheduler in namespace N sees only N's
pauses. The DLQ purges are the exception, deliberately: `list_dead` /
`retry_dead` / `delete_dead` already treat `None` as *unscoped*, so
`purge_dead(older_than, namespace)` and `purge_dead_by_task(task, namespace)`
follow **their** siblings — `None` purges every namespace (the dashboard with
no namespace and the TUI keep today's behaviour). `purge_dead_with_ttl` is
retention, cluster-wide, unchanged.

### D2 — Queue pause: `m0020`, table rebuild, legacy key kept for default

`m0020_queue_state_namespace`: copy of `m0018`'s shape — scratch table with
nullable `namespace`, `insert_select`, drop, rename, `raw_ddl` expression
unique index `((namespace IS NULL), COALESCE(namespace, ''), queue_name)`.
Copied rows land in the default namespace. Diesel cannot target an expression
index, so the upsert becomes UPDATE-else-INSERT in a new
`diesel_common/queue_state.rs` macro, folding the hand-duplicated SQLite /
Postgres pair (the last one left after #933).

Redis: default namespace keeps `queues:paused` (so an upgrade does not resume
a default-namespace pause); namespace N uses `queues:paused:<segment>` with the
length-prefixed `namespace_segment` from #773.

**Breaking, documented:** a pause set by a *namespaced* shell before the
upgrade was global; after it, it belongs to the default namespace. Changelog
says re-pause after upgrading.

### D3 — Workers: `m0021`, nullable column

`workers.namespace` via plain `add_column` (worker_id stays the PK).
`WorkerRegistration::namespace()` builder setter (the struct is
`#[non_exhaustive]` + builder for exactly this); `WorkerInfo.namespace`.
`list_workers(namespace)` filters (Redis: one global `workers:all` set, filter
in the loop — reap stays global). `list_live_worker_ids`, `reap_dead_workers`,
heartbeat and status writes stay global: they are keyed by a globally unique
id, and a dead worker is dead in every namespace.

Four registration sites pass the handle's namespace: `worker/runner.rs`,
`flexiq-python` `run_worker`, `flexiq-node` `spawn_worker_lifecycle` (new
param), `flexiq-java` `register_live_worker` (new param). Eight
`list_workers()` consumers pass theirs. **Behaviour change, flagged:** the
dashboard readiness probe (`dashboard/probes.rs`) now asks about its own
namespace's workers. A worker from before the upgrade has `NULL` = default
until it re-registers on restart.

### D4 — DLQ: namespaced purges + `get_dead`

`purge_dead(older_than_ms, namespace)`, `purge_dead_by_task(task, namespace)`
per D1. New `Storage::get_dead(dead_id, namespace) -> Option<DeadJob>` (three
backends + both forwarding sites + contract test) — "inspect an entry" has no
primitive today. Callers updated: dashboard purge route, TUI, three shells'
admin bindings (each passes its handle namespace — so a namespaced SDK purge
stops deleting other tenants', which is a fix).

### D5 — Overrides: namespaced keys, default keeps the legacy layout

Layout (written once in `BINDING_CONTRACT.md`, the first time it is
normative):

- default namespace: `overrides:task:<name>`, `overrides:queue:<name>` —
  unchanged, so existing deployments need no migration;
- namespace N: `overrides:ns:<len>:<N>:task:<name>` /
  `…:queue:<name>`.

The `ns:` scope segment means the two layouts never share a prefix, so a
default listing (`overrides:task:` → suffix is the name) cannot misparse a
namespaced key, whatever a task name contains. The key builder lives once in
`flexiq-core` (`overrides::key(scope, namespace, name)` + `prefix(scope,
namespace)`); Python, Node and Java re-implement the three-line format in
their dashboard stores and worker-start apply paths, pinned by a shared vector
test per SDK. Java's worker does not apply overrides at all today — existing
parity gap, noted in the follow-ups, not fixed here.

**Effect timing is the dashboard's:** overrides are read at worker start by
the Python and Node shells. The RPC documents that a change reaches the next
worker start, and no running worker.

### D6 — Throughput: windowed counts from `completed_at`

New `Storage::queue_throughput(since_ms, namespace) -> HashMap<String,
QueueThroughput { completed, failed, cancelled }>` — terminal jobs (live +
archived tables) with `completed_at >= since_ms`, grouped by queue, the same
query shape as `stats_all_queues`. Redis: same scan `stats_all_queues` does for
a namespaced read (the door is always namespaced). The wire returns **counts
over a window it echoes**, never a computed rate: a rate implies a smoothing
choice the server should not make. `fq` divides.

### D7 — Periodic: declare, never overwrite operator state

- `PutPeriodicTask` → `Storage::declare_periodic` (#919): on an existing row it
  replaces definition columns, never `enabled`/`last_run`, and moves
  `next_run` only when cron or timezone changed; a new row is inserted with
  `enabled = !start_paused`. `next_run` from `periodic::next_cron_time_tz`.
  The body is the `raw`/`structured` oneof `Enqueue` uses — the stored `args`
  column holds the same envelope; `kwargs` stays `NULL` (the Python shell
  already folds kwargs into `args`; writing both would double-encode).
- A schedule also declared in code is **redeclared at that worker's next
  start**; documented on the RPC.
- `TriggerPeriodicTask`: hoist `check_periodic`'s inline `NewJob` into
  `scheduler::periodic_job(task, now, unique_key)`, used by both paths. A
  manual trigger passes `unique_key = None` (the automatic key is
  `periodic:<name>:<now>`; reusing it would silently no-op a trigger landing
  in the same millisecond as a firing). It does not touch `next_run`/`last_run`.
- Pause/resume → `set_periodic_enabled`; delete → `delete_periodic`.

### D8 — Scopes: `inspect` and `admin`, by idempotency level

Two new `Scope` bits. Within `flexiq.admin.v1` the requirement is a function of
the method's descriptor: `NO_SIDE_EFFECTS` → `inspect`, anything else →
`admin`. The gate reads the embedded descriptor once at startup into a
path → scope map, so adding an RPC needs no gate edit and cannot be
misclassified. Not a hierarchy, like `produce`/`execute`: an operator token is
minted with both. Facade: `/v1/admin/` is matched **before** `/v1/`, GET →
`inspect`, POST → `admin` (the GET-iff-NO_SIDE_EFFECTS drift test makes the
two classifications agree). `produce` reaches nothing under `/v1/admin/`.

`tokens/cli.rs` `ScopeArg`, dashboard `SCOPE_HELP` gain both.

### D9 — Drain is not in v1

No `DrainWorker` RPC until a worker can be told to drain. Filed as a follow-up
(signal on the worker row, read on heartbeat, honoured by core + every shell).
The number range is left free in `AdminService` ordering only — proto3 RPCs
carry no numbers.

### D10 — JSON facade covers the package

Admin gets `google.api.http` bindings, facade routes and OpenAPI coverage
(`flexiq-openapi` walks a list of packages, not one constant). Verbs stay
GET/POST (D15): deletes and state changes are AIP-136 custom methods, and the
`{param}:verb` suffix dispatch the facade already does for `:cancel`
generalises to a per-resource verb table.

### D11 — Contract text that must change

Three places say an admin surface must never be on this door
(`producer_service.proto` service comment, `tokens.mdx` "Neither scope is an
admin surface", `grpc/mod.rs` module doc), plus `REMOTE_SDK_CONTRACT.md`'s
"exactly two scopes" and "Neither door reaches the admin surface". The
principle survives — *an operator surface and a producer surface must not
share a credential* — and is now enforced by scope rather than by absence.

## The proto (review this before any handler is written)

`contracts/proto/flexiq/admin/v1/admin_service.proto`, `package
flexiq.admin.v1`. Imports `flexiq/v1/job.proto` (for `Job`, `StructuredArgs`);
`flexiq.v1` never imports admin. Cap 4 MiB, like the producer door.
Error reasons added to the closed list: `DEAD_LETTER_NOT_FOUND`,
`PERIODIC_TASK_NOT_FOUND` (both `NOT_FOUND`; cross-namespace is
indistinguishable from absent). Validation failures (bad cron, bad timezone,
bad rate string, window out of range) are `INVALID_REQUEST`.

```proto
service AdminService {
  // Queues -------------------------------------------------------------
  rpc ListQueues(ListQueuesRequest) returns (ListQueuesResponse)            // GET  /v1/admin/queues                         NO_SIDE_EFFECTS
  rpc PauseQueue(PauseQueueRequest) returns (PauseQueueResponse)            // POST /v1/admin/queues/{queue}:pause           IDEMPOTENT
  rpc ResumeQueue(ResumeQueueRequest) returns (ResumeQueueResponse)         // POST /v1/admin/queues/{queue}:resume          IDEMPOTENT
  rpc GetThroughput(GetThroughputRequest) returns (GetThroughputResponse)   // GET  /v1/admin/throughput                     NO_SIDE_EFFECTS

  // Dead letters -------------------------------------------------------
  rpc ListDeadLetters(...)   // GET  /v1/admin/deadLetters                      NO_SIDE_EFFECTS
  rpc GetDeadLetter(...)     // GET  /v1/admin/deadLetters/{dead_letter_id}     NO_SIDE_EFFECTS
  rpc ReplayDeadLetter(...)  // POST /v1/admin/deadLetters/{dead_letter_id}:replay   (not idempotent: makes a job)
  rpc DeleteDeadLetter(...)  // POST /v1/admin/deadLetters/{dead_letter_id}:delete   IDEMPOTENT
  rpc PurgeDeadLetters(...)  // POST /v1/admin/deadLetters:purge  body "*"         (not idempotent: count differs)

  // Workers ------------------------------------------------------------
  rpc ListWorkers(...)       // GET  /v1/admin/workers                          NO_SIDE_EFFECTS

  // Periodic tasks -----------------------------------------------------
  rpc ListPeriodicTasks(...)   // GET  /v1/admin/periodicTasks                  NO_SIDE_EFFECTS
  rpc GetPeriodicTask(...)     // GET  /v1/admin/periodicTasks/{name}           NO_SIDE_EFFECTS
  rpc PutPeriodicTask(...)     // POST /v1/admin/periodicTasks  body "*"        IDEMPOTENT
  rpc DeletePeriodicTask(...)  // POST /v1/admin/periodicTasks/{name}:delete    IDEMPOTENT
  rpc PausePeriodicTask(...)   // POST /v1/admin/periodicTasks/{name}:pause     IDEMPOTENT
  rpc ResumePeriodicTask(...)  // POST /v1/admin/periodicTasks/{name}:resume    IDEMPOTENT
  rpc TriggerPeriodicTask(...) // POST /v1/admin/periodicTasks/{name}:trigger   (not idempotent)

  // Overrides ----------------------------------------------------------
  rpc ListOverrides(...)       // GET  /v1/admin/overrides                      NO_SIDE_EFFECTS
  rpc SetTaskOverride(...)     // POST /v1/admin/tasks/{task_name}/override  body "*"   IDEMPOTENT
  rpc ClearTaskOverride(...)   // POST /v1/admin/tasks/{task_name}/override:clear      IDEMPOTENT
  rpc SetQueueOverride(...)    // POST /v1/admin/queues/{queue}/override  body "*"     IDEMPOTENT
  rpc ClearQueueOverride(...)  // POST /v1/admin/queues/{queue}/override:clear         IDEMPOTENT
}
```

Messages (field numbers as they will ship):

```proto
message Queue {
  string name = 1;
  bool paused = 2;
  optional google.protobuf.Timestamp paused_at = 3;
  // Same six counters as flexiq.v1.QueueStatsResponse.
  int64 pending = 4; int64 running = 5; int64 completed = 6;
  int64 failed = 7;  int64 dead = 8;    int64 cancelled = 9;
}
message ListQueuesRequest {}
// Every queue the namespace has a job, a pause or an override for, by name.
// Not paginated: bounded by queue count, like QueueStats.
message ListQueuesResponse { repeated Queue queues = 1; }
message PauseQueueRequest  { string queue = 1; }
message PauseQueueResponse { Queue queue = 1; }   // resulting state
message ResumeQueueRequest  { string queue = 1; }
message ResumeQueueResponse { Queue queue = 1; }

message GetThroughputRequest {
  // Unset = 5 minutes. Refused above 24 hours or at or below zero.
  google.protobuf.Duration window = 1;
}
message QueueThroughput {
  string queue = 1; int64 completed = 2; int64 failed = 3; int64 cancelled = 4;
}
message GetThroughputResponse {
  google.protobuf.Duration window = 1;           // as applied
  google.protobuf.Timestamp since = 2;           // window start, server clock
  repeated QueueThroughput queues = 3;           // only queues with a count
}

message DeadLetter {
  string id = 1; string original_job_id = 2; string queue = 3;
  string task_name = 4;
  optional string error = 5;                     // canonical TaskError JSON when structured
  int32 retry_count = 6;
  google.protobuf.Timestamp failed_at = 7;
  optional string metadata = 8;
  int32 priority = 9; int32 max_retries = 10;
  bytes payload = 16;                            // GetDeadLetter with include_payload only
}
message ListDeadLettersRequest  { int32 page_size = 1; string page_token = 2; }   // newest first, keyset
message ListDeadLettersResponse { repeated DeadLetter dead_letters = 1; string next_page_token = 2; }
message GetDeadLetterRequest    { string dead_letter_id = 1; bool include_payload = 2; }
message GetDeadLetterResponse   { DeadLetter dead_letter = 1; }
message ReplayDeadLetterRequest  { string dead_letter_id = 1; }
message ReplayDeadLetterResponse { flexiq.v1.Job job = 1; }   // the new job; entry is gone
message DeleteDeadLetterRequest  { string dead_letter_id = 1; }
message DeleteDeadLetterResponse { bool deleted = 1; }        // false = already absent
message PurgeDeadLettersRequest {
  // Only entries that failed before this instant. Unset = every entry.
  optional google.protobuf.Timestamp failed_before = 1;
  // Only this task's entries. Unset = every task. Combinable with the above.
  optional string task_name = 2;
}
message PurgeDeadLettersResponse { int64 purged = 1; }

enum WorkerStatus { WORKER_STATUS_UNSPECIFIED = 0; WORKER_STATUS_ACTIVE = 1; WORKER_STATUS_DRAINING = 2; }
message Worker {
  string worker_id = 1; repeated string queues = 2; WorkerStatus status = 3;
  google.protobuf.Timestamp last_heartbeat = 4;
  int32 threads = 5;
  optional google.protobuf.Timestamp started_at = 6;
  optional string hostname = 7; optional int64 pid = 8;
  optional string pool_type = 9; optional string sdk = 10; optional string sdk_version = 11;
}
message ListWorkersRequest  {}
message ListWorkersResponse { repeated Worker workers = 1; }   // live registry rows, not paginated

message PeriodicTask {
  string name = 1; string task_name = 2; string cron = 3;
  optional string timezone = 4;                  // IANA; unset = UTC
  string queue = 5; bool enabled = 6;
  optional google.protobuf.Timestamp last_run = 7;
  google.protobuf.Timestamp next_run = 8;
  bytes payload = 16;                            // GetPeriodicTask with include_payload only
}
message ListPeriodicTasksRequest  {}
message ListPeriodicTasksResponse { repeated PeriodicTask periodic_tasks = 1; }
message GetPeriodicTaskRequest  { string name = 1; bool include_payload = 2; }
message GetPeriodicTaskResponse { PeriodicTask periodic_task = 1; }
message PutPeriodicTaskRequest {
  string name = 1; string task_name = 2; string cron = 3;
  optional string timezone = 4;
  string queue = 5;                              // empty = "default"
  oneof body { bytes raw = 6; flexiq.v1.StructuredArgs structured = 7; }   // unset = no arguments
  bool start_paused = 8;                         // honoured only when the row is new
}
message PutPeriodicTaskResponse { PeriodicTask periodic_task = 1; }
message DeletePeriodicTaskRequest  { string name = 1; }
message DeletePeriodicTaskResponse { bool deleted = 1; }
message PausePeriodicTaskRequest  { string name = 1; }
message PausePeriodicTaskResponse { PeriodicTask periodic_task = 1; }
message ResumePeriodicTaskRequest  { string name = 1; }
message ResumePeriodicTaskResponse { PeriodicTask periodic_task = 1; }
message TriggerPeriodicTaskRequest  { string name = 1; }
message TriggerPeriodicTaskResponse { flexiq.v1.Job job = 1; }

// Every field optional: unset = no override, the task's declared value wins.
message TaskOverride {
  string task_name = 1;
  optional string rate_limit = 2;                // "<count>/<s|m|h|d>", count >= 1
  optional int32 max_concurrent = 3;             // >= 0
  optional int32 max_retries = 4;                // >= 0
  optional google.protobuf.Duration retry_backoff = 5;   // >= 0
  optional google.protobuf.Duration timeout = 6;         // >= 1 s
  optional int32 priority = 7;
  optional bool paused = 8;
}
message QueueOverride {
  string queue = 1;
  optional string rate_limit = 2;
  optional int32 max_concurrent = 3;
  // No `paused`: PauseQueue is the one way to pause a queue.
}
message ListOverridesRequest  {}
message ListOverridesResponse { repeated TaskOverride tasks = 1; repeated QueueOverride queues = 2; }
// Replaces the whole override document (not a merge): the response is the
// stored state, and setting every field unset is a Clear.
message SetTaskOverrideRequest    { TaskOverride override = 1; }   // task_name from the path
message SetTaskOverrideResponse   { TaskOverride override = 1; }
message ClearTaskOverrideRequest  { string task_name = 1; }
message ClearTaskOverrideResponse {}
message SetQueueOverrideRequest   { QueueOverride override = 1; }
message SetQueueOverrideResponse  { QueueOverride override = 1; }
message ClearQueueOverrideRequest  { string queue = 1; }
message ClearQueueOverrideResponse {}
```

Open points for review:

1. `timeout`/`retry_backoff` become `Duration` on the wire (D20) while the KV
   stores seconds (int / float). Conversion is lossless one way; a sub-second
   `timeout` is refused rather than rounded.
2. `QueueOverride.paused` is dropped on the wire. The dashboard's queue
   override can still carry `paused` and calls `pause_queue` — the admin door
   has one way to pause.
3. `ListQueues` is unpaginated. Bounded by queue count; add a token later
   additively if needed.
4. `SetTaskOverride` replaces, it does not merge. Merge-with-null is the
   dashboard's HTTP shape and has no proto3 equivalent short of a FieldMask.

## Clients

- **`fq`**: `fq dlq list|show|replay|delete|purge`, `fq pause <queue>`,
  `fq resume <queue>`, `fq queues --list` (ListQueues, adds a paused column),
  `fq throughput [--window 5m]` (prints per-minute rates it computes),
  `fq workers`, `fq periodic list|show|put|delete|pause|resume|trigger`,
  `fq overrides list|set-task|clear-task|set-queue|clear-queue`.
  `crates/flexiq-cli/src/pb.rs` gains `pub mod admin`. `fq queues <name>` is
  unchanged. `grpc_cli.rs` render-parity test extended to the admin messages.
- **Go**: `sdks/go/buf.gen.yaml` gains the admin path; stubs committed. No Go
  client wrapper.

## Follow-ups to file (after review, with the user's go-ahead)

- Operator-initiated worker drain (D9).
- Java worker never applies task/queue overrides.
- Python shell registers periodics with hardcoded `enabled: true` (already
  noted in memory, not filed).

## Non-goals

Per-queue / per-task token patterns (#839). Circuit-breaker admin. Settings KV
over the wire (the issue forbids a generic KV door). Webhook / token admin.
Changing when overrides take effect.
