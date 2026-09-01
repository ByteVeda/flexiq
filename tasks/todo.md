# #714 — the `flexiq.v1` producer service

## Problem
#713 bound a gRPC listener that serves `grpc.health.v1` and reflection and
nothing else. The producer door is still not on the network: the only way to
submit work is to be a process with the Rust core compiled in. This issue puts
`ProducerService` on that listener.

Reviewed against §11 of `tasks/specs/2026-09-01-flexiq-v1-proto-design.md`,
which fails #714 if a `namespace` field appears in a request, a `dag_data`
bytes field ships, `EnqueueBatch` claims atomicity or drops `deduplicated` or
reports a rolled-back item as enqueued, `payload`/`result` lack explicit
presence, listings carry payloads, the cursor is a tuple, or a service is not
named `*Service`.

## Scope calls
- **Workflows are deferred.** §7.6 sanctions it explicitly: if the structured
  `WorkflowGraph` is not settled, `SubmitWorkflow`/`GetWorkflowRun` ship in a
  later additive release. What must never happen is a `bytes dag_data`
  placeholder — a field number spent on dagron's internal JSON is spent
  forever. A follow-up issue carries them.
- **Field 3 of `EnqueueRequest` is left unspent for #715.** The `body` oneof
  ships with `raw = 2` only; `structured = 3` is a new oneof arm, which §2.1
  rule 4 makes additive. #714 does not freeze a message shape #715 owns.
- **The producer door refuses a non-loopback bind until #716.** #716 owns the
  interceptor; until it lands there is no credential at all, and an enqueue
  door is not a health endpoint. This mirrors `config/listen.rs:74-82`, which
  refuses a non-loopback attach bind with no token. Cost, paid in this PR: the
  chart's `grpc.enabled` binds loopback and cross-pod gRPC is unavailable until
  #716 replaces the refusal with a token check.
- **`deduplicated` is surfaced from core, not guessed.** `enqueue_unique`
  knows which branch it took and throws it away at the return. A server-side
  `created_at < now` heuristic is wrong inside a 1 ms window, and this is a
  permanent wire field.
- **Codegen runs off `contracts/descriptor.binpb`, not `protoc`.**
  `tonic_prost_build::compile_fds` takes a `FileDescriptorSet` directly, so the
  build has no protoc dependency and the generated Rust is provably the same
  artifact buf lints, gates and the binary already serves for reflection.

## Plan

### 1. Core: report the dedupe (prereq, its own commit)
- [ ] `Storage::enqueue_unique_reporting(NewJob) -> Result<(Job, bool)>` and
      `enqueue_unique_batch_reporting(Vec<NewJob>) -> Result<Vec<(Job, bool)>>`
      in `traits.rs`.
- [ ] `diesel_common/jobs.rs`: the existing bodies become the `_reporting`
      ones (`Job::from(row)` → `true`, `job.clone()` → `false`); the old names
      become one-line wrappers. One implementation per backend, no caller
      breaks.
- [ ] Same in `redis_backend/jobs/enqueue.rs`.
- [ ] `delegate!` in `storage/mod.rs`.
- [ ] Parity test in `tests/rust/storage_tests.rs` — runs on SQLite, Postgres
      and Redis in CI.

### 2. Protos
- [ ] `contracts/proto/buf.yaml`: `deps: [buf.build/googleapis/googleapis]`,
      committed `buf.lock`. Needed for `google.rpc.Status` on the batch item
      arm (§7.4).
- [ ] `scripts/proto-guard.sh` copies `buf.lock` beside each fixture — a
      `buf.yaml` with a dep and no lock file does not build.
- [ ] `job.proto` grows `Job`, `EnqueueOptions`, `Debounce`. Numbers 1–15 on
      `Job` are the read model's core (§1.4); the optional tail starts at 16.
      `payload`/`result` are `optional bytes` — absent and empty are different
      answers (D19).
- [ ] `producer_service.proto`: `ProducerService` with `Enqueue`,
      `EnqueueBatch`, `GetJob`, `ListJobs`, `CancelJob`, `QueueStats`.
      `NO_SIDE_EFFECTS` on the three reads and nowhere else; `IDEMPOTENT` on
      `CancelJob` (D15, §6).
- [ ] Regenerate `contracts/descriptor.binpb` via `scripts/proto-check.sh --fix`.

### 3. Codegen
- [ ] `crates/flexiq-server/build.rs`: when `CARGO_FEATURE_GRPC` is set, decode
      `contracts/descriptor.binpb` and `tonic_prost_build::configure()
      .extern_path(".google.rpc", "::tonic_types::pb").compile_fds(fds)`.
      `rerun-if-changed` on the descriptor.
- [ ] Optional deps under the `grpc` feature: `prost`, `prost-types`,
      `tonic-prost`, `tonic-types`; build-deps `tonic-prost-build`, `prost`,
      `prost-types`.

### 4. The service
- [ ] `grpc/pb.rs` — the generated module.
- [ ] `grpc/blocking.rs` — one `spawn_blocking` helper mapping
      `QueueError` → `tonic::Status`. `health.rs` rolls its own inline; a
      service with six RPCs does not.
- [ ] `grpc/status/{mod,reason}.rs` — §4.2's table as an **exhaustive** match
      with no wildcard, the closed `reason` list as a Rust module (§1.2), and
      D9 sanitisation by provenance: `Storage`, `Pool`, `Redis` **and `Other`**
      log their cause and send a fixed string.
- [ ] `grpc/producer/convert.rs` — the Unix-ms ↔ `Timestamp`/`Duration`
      boundary lives here and nowhere else (D20); exhaustive `JobStatus`
      conversion, offset by one.
- [ ] `grpc/producer/cursor.rs` — the page token is an opaque base64 string,
      never the `(created_at, id)` tuple (§7.5).
- [ ] `grpc/producer/{enqueue,reads,cancel}.rs` + `mod.rs` holding the single
      `impl ProducerService`.
- [ ] `Enqueue` dispatches on options, not on RPCs (D14): `debounce` →
      `enqueue_debounced`, else `unique_key` → `enqueue_unique_reporting`,
      else `enqueue`.
- [ ] `EnqueueBatch`: Diesel is one transaction, so an item failure fails the
      **RPC** with the failing item's reason plus `metadata{index}`; Redis can
      partially apply, so it answers `OK` with per-item results. No atomicity
      promised (D17).
- [ ] Every `Storage` call passes `Some(namespace)` from `GrpcConfig` and never
      `None` (D11, §5.2). One accessor, so #716 swaps it for a `Principal`.
- [ ] Register on the listener with `PRODUCER_MAX_MESSAGE_BYTES`.

### 5. The refusal
- [ ] `config/grpc.rs`: a non-loopback TCP bind refuses, naming #716. Unix
      sockets keep the filesystem as the boundary, as attach does.
- [ ] Chart binds loopback while `grpc.enabled`; `values.yaml`, the chart
      README and the deployment guide say why and name the follow-up.

### 6. Tests and CI
- [ ] Inline: status totality, agreement with `classify_step_failure` over the
      arms it names (D8, §4.4), `JobStatus` round trip both directions, the
      timestamp boundary, cursor round trip.
- [ ] `tests/grpc_producer.rs`: enqueue → get → list → cancel → stats; the same
      `unique_key` twice returns one id twice with `deduplicated` false then
      true; a job in another namespace reads `NOT_FOUND`; `ListJobs` carries no
      payload; `GetJob` omits payload/result unless asked; batch per-item
      results.
- [ ] `ci.yml`: `contracts/**` joins the `rust` path filter — Rust consumes the
      protos now.

## Review

Six RPCs on the listener #713 bound, plus the core change that makes one of
their response fields answerable. Six things worth writing down.

**Codegen needs no `protoc`, and that turned out to be the better design rather
than a workaround.** `tonic_prost_build::compile_fds` takes a
`FileDescriptorSet` directly, so `build.rs` reads `contracts/descriptor.binpb`
— the artifact buf already builds, lints, gates for breaking changes and the
binary already embeds for reflection. The generated Rust therefore cannot
describe a different contract from the one CI checks or the one a `grpcurl`
client discovers. It also means editing a `.proto` without running
`scripts/proto-check.sh --fix` changes nothing in Rust, which is the same
staleness the proto job already fails on.

**`google.rpc.Status` cost a buf module dependency, and the guard did not
survive it.** `buf.yaml` gains `deps: [buf.build/googleapis/googleapis]` and a
committed `buf.lock`. `scripts/proto-guard.sh` stages the *production*
`buf.yaml` beside each fixture on purpose — that is what makes the fixtures test
our configuration rather than buf — and buf refuses to build a module whose
declared deps have no lock file beside them, so all eight cases went red until
the lock was staged too.

**`deduplicated` could not be computed above the backend.** `enqueue_unique`
generates the job id inside the insert, so a caller has no candidate to compare
the answer against, and a `created_at < now` heuristic is wrong whenever a
concurrent producer wins the slot inside the same millisecond — on a field that
is permanent. The backends now expose `enqueue_unique_reporting`, the old names
are one-line wrappers over it, and there is still one implementation per
backend. A parity test in the contract suite pins it on all three.

**Two amendments to the design doc, both forced by the code.** §4.1 gained
`INVALID_REQUEST`: §4.2 enumerates `QueueError`, a malformed request has none,
and D7 still requires every error to carry a reason. §7.4 gained the rule that
`index` is present only when the failure is *attributable* —
`Storage::enqueue_unique_batch` returns one error for the whole batch and
nothing in `DependencyNotFound` or `QueueFull` names a position, so a
rolled-back batch honestly cannot name an item, while the request-shape
refusals (checked per item before anything is written) and the
partially-applying path both can.

**The chart cannot offer the role at all right now, and loopback was not a way
out.** The plan said the chart would bind loopback; kubelet dials the **pod IP**
for both the `grpc` and the `tcpSocket` probe, so a loopback listener never
passes readiness. `grpc.enabled` therefore fails at `helm template` time with
that reason, rather than rendering a Deployment that CrashLoopBackOffs. The cost
is stated rather than hidden: `ci-chart.yml` loses its gRPC-only probe
assertions, because the combination they render can no longer be rendered. The
probe wiring is still in `deployment.yaml` and the assertions come back with the
credential.

**`diesel` is re-exported from `flexiq-core`.** `QueueError::Storage` wraps a
`diesel::result::Error`, so telling a constraint violation from an unreachable
database — which the error mapping has to do — was impossible for a consumer
that does not depend on Diesel itself. Re-exporting means it matches against the
version the core was built with rather than one it picked.

### Verified

- `cargo test -p flexiq-core` and the SQLite contract suite, including the new
  dedupe-reporting parity test. Postgres and Redis run it in CI.
- `cargo test -p flexiq-server --features grpc`: 15 suites green, 8 of them the
  new producer integration tests.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`.
- `cargo check -p flexiq-server` with no features and with
  `postgres,redis,grpc`, both warning-free.
- `scripts/proto-check.sh` and `scripts/proto-guard.sh` (8/8).
- `helm template` on the valid combinations, and the two gRPC refusals.

### Not in this change

- `SubmitWorkflow` / `GetWorkflowRun`. §7.6 sanctions deferring them, and a
  `bytes dag_data` placeholder would spend a field number on dagron's internal
  JSON forever. They need a `WorkflowGraph` message designed first.
- The `structured` arm of `EnqueueRequest.body`. Field 3 is left unspent; a new
  oneof arm is additive.
- Authentication. The door refuses a non-loopback bind until it has one.
