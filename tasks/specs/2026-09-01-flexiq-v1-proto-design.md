# The `flexiq.v1` wire contract, and what it must not promise

**Date:** 2026-09-01
**Status:** Approved — the contract the #710 sub-issues are reviewed against
**Issue:** [#711](https://github.com/ByteVeda/flexiq/issues/711), part of
[#710](https://github.com/ByteVeda/flexiq/issues/710)
**Governs:** #712 (buf CI) · #713 (server role) · #714 (producer service) · #715
(payload) · #716 (shared secret) · #717 (scoped tokens) · #718 (JSON facade) ·
#719 (lease token) · #720 (executor transport) · #721 (docs)

## Why this document exists

A wire contract is decided once. Field numbers are permanent, and the first
release is the one every later release has to stay compatible with — so a
service written before the rules are written *becomes* the rules, inferred from
whatever happened to ship.

FlexiQ has never had a network contract. Everything it has today is either an
in-process contract (`BINDING_CONTRACT.md`, versioned by `CONTRACT_VERSION` and a
storage floor, upgraded in lockstep) or a same-machine one (the attach frame
protocol, negotiated by capabilities). Neither one is a thing a third party
encodes against and keeps working for years. This is, and the habits of the
other two do not transfer.

This document decides the seven things #711 names, and one more the evidence
forced: what the namespace *is* on the wire, given that inside `Storage` it means
three different things.

## The evidence

Facts established by reading the code. Most of what follows is downstream of
these.

**E1. There is no `QueueError` → status mapping to inherit.** The dashboard maps
exactly one variant — `SettingConflict` → 409 — and drops every other variant
into a 500 with the body `{"error":"Internal server error"}`
(`crates/flexiq-server/src/dashboard/error.rs:86-96`, `:78`). All three shells
collapse the whole enum into one generic exception carrying the `Display` string:
`PyRuntimeError` in Python (e.g.
`crates/flexiq-python/src/py_queue/mod.rs:265`), `napi::Error(GenericFailure, …)`
in Node (`crates/flexiq-node/src/error.rs:7-9`), `FlexiQException` in Java
(`crates/flexiq-java/src/error.rs:55-59`). The wire error model is new work, and
it is the first surface on which a client can branch on something other than a
message.

**E2. One classifier exists, and it is step-scoped.**
`classify_step_failure` (`crates/flexiq-core/src/step/failure.rs:41-73`) sorts a
`QueueError` into `Retryable | Permanent | Superseded`, including a split on
Diesel's `DatabaseErrorKind` — `UniqueViolation`, `ForeignKeyViolation`,
`NotNullViolation`, `CheckViolation` are permanent, every other database error is
retryable. The ordinary job path has no equivalent: `should_retry` is a bool the
*executor* sets, because "only it can see the exception; the core never inspects
one" (`crates/flexiq-core/src/worker/protocol.rs:352-354`). So the wire's retry
semantics are new — and they must not contradict the one classifier that exists.

**E3. `namespace: None` means three different things.** Verified per
implementation, not per doc comment:

| Call shape | `None` means | Evidence |
|---|---|---|
| `dequeue*` | *only* the rows whose `namespace` is SQL NULL | `crates/flexiq-core/src/storage/sqlite/jobs.rs:84-87`, `postgres/jobs.rs:111,136-137`, doc at `traits.rs:78-79` |
| id-addressed (`get_job`, `cancel_job`, `request_cancel`, `mark_cancelled`, …) | **any** namespace — no scoping at all | `job_in_namespace` at `diesel_common/jobs.rs:1225-1229`, used at `:1432,:1445,:1189,:1213`; doc at `traits.rs:159-160` |
| aggregate (`list_jobs`, `list_jobs_after`, `stats`, `stats_by_queue`) | every namespace, no filter clause emitted | `diesel_common/jobs.rs:1459-1469`, doc at `traits.rs:238` |

The middle row is the dangerous one: a service that forwards a caller's "no
namespace" straight into `get_job` reads every tenant's jobs. §5 exists because
of this row.

**E4. Cross-namespace dependency edges are already refused, and refused
silently.** `validate_dependency` rejects a `depends_on` id whose namespace
differs, with the same `DependencyNotFound` a missing id produces, so a scoped
caller learns nothing about ids outside its own namespace
(`crates/flexiq-core/src/storage/diesel_common/jobs.rs:172-226`, called from
`enqueue` at `:277`; Redis mirrors it at
`redis_backend/jobs/enqueue.rs:181-226`; the rule is stated at `traits.rs:22-28`).
The wire inherits the property, and §5 makes it structural.

**E5. `QueueError::QueueFull`'s `Display` string is a load-bearing wire.** Each
SDK parses `pending` and `cap` back off the end of the message to rebuild its own
typed rejection; the wording is pinned by a test
(`crates/flexiq-core/src/error.rs:47-56`, `:199-218`). That is an FFI expedient —
a binding's fast path may carry only a string. A network contract has structured
error details available and must not inherit the expedient.

**E6. The frame protocol evolves by capability, not by version.**
`PROTOCOL_VERSION` is hard-checked and never downgraded, and optional behaviour
is announced in `hello`/`hello_ack` `capabilities[]` instead of bumping it
(`crates/flexiq-core/src/worker/protocol.rs:36-64`). An unknown frame type is
skipped rather than fatal, which is why every frame must declare its blob under a
field literally named `payload_len`
(`protocol.rs:18-22`, `:1098-1122`). Protobuf has its own answer to both problems
— unknown fields and unknown `oneof` arms — and merging the two mechanisms would
give the executor door two ways to say the same thing.

**E7. An executor never names its own authority.** `StepCommit` carries no owner
by design: an owner an executor fills in is an owner it can forge, so the
scheduler resolves `(owner, attempt)` from the dispatch it recorded
(`protocol.rs:370-376`, `BINDING_CONTRACT.md` "A step frame never carries an
owner, a namespace or a cap"). Everything the executor package sends inherits
this, including #719's lease token — which is minted by the scheduler and opaque
to the holder.

**E8. The contract floor is a startup condition, not a request condition.**
`CONTRACT_VERSION` is 1, the floor lives at `contract:min_sdk`, and
`ensure_contract_supported` runs once per open
(`crates/flexiq-core/src/contract.rs:23,32,82-91`). In `flexiq-server` it runs
inside `backend::open()` (`crates/flexiq-server/src/config/backend.rs:57`) and
fails the process before any listener binds. `ContractTooOld` therefore reaches a
client only when the floor is raised under a running server.

**E9. The server already has the shape a fourth role slots into, including a
namespace.** Three roles, each with its own env var and listener, an
at-least-one-role check (`crates/flexiq-server/src/config/mod.rs:78-84`), a DSN
rule (`:87-92`), and a single process-wide `FLEXIQ_NAMESPACE`
(`config/mod.rs:32-33,:68`) threaded into the scheduler and every dashboard route
(`runtime/mod.rs:40,67,123`; `dashboard/routes/jobs.rs:28`). There is no
per-request namespace anywhere in the server today. Also settled there: a
non-loopback attach bind with no token refuses to start
(`config/listen.rs:74-82`), and TLS env vars are rejected outright as unhonoured
because the token is a bearer credential and transport security belongs to a
sidecar or mesh (`listen.rs:23-25,61-70`).

**E10. `structured` has a documented ceiling.** `contracts/wire-vectors.json`
pins nine `encode` cases and three `decode_only` ones; two of the three carry
`round_trip_only` precisely because JSON cannot hold the value — 9007199254740993
and a byte string. Anything JSON-shaped, including protobuf's dynamic value
types, refuses those rather than rounding them (#715).

**E11. `enqueue_batch` is not atomic on every backend.** Diesel runs the whole
batch — dependency validation and every chunked multi-row INSERT — inside one
`write_transaction`
(`crates/flexiq-core/src/storage/diesel_common/jobs.rs:287,309`). Redis issues
the same writes as one `redis::pipe()`, which is a pipeline and not a
transaction: the commands travel in one round trip, nothing rolls back, and a
partial batch is observable (`redis_backend/jobs/enqueue.rs:291,327`). The unique
variant does not even pipeline — it loops the single-job path, and says why
(`enqueue.rs:331-340`). A wire promise of all-or-nothing would be a promise one
backend cannot keep.

**E12. `JobStatus` already has two string spellings.** `as_str()` is lowercase
and used in listings (`crates/flexiq-core/src/job.rs:40-49`); `wire_name()` is
capitalised, is what serde emits, and is what the Redis Lua scripts compare
against — pinned by `wire_name_matches_serde_output`
(`job.rs:58-67`, `:379-395`). A third spelling on the wire is unavoidable; what
is avoidable is letting it be a *string*.

## Decisions

| # | Decision | Rationale |
|---|---|---|
| D1 | **Two packages.** `flexiq.v1` holds the shared types and `ProducerService`; `flexiq.executor.v1` holds `ExecutorService` and imports the shared types. Never the reverse import. | The audiences, credentials and exposure differ, and the split turns three rules into package properties: the JSON facade serves `flexiq.v1` and nothing else (D2), a token scope is "may call this package" (#717), and a fix forced on the executor frames by `BINDING_CONTRACT.md` cannot touch the producer wire. Temporal pays the other way: it repeats "We do not expose worker API to HTTP" once per worker RPC. |
| D2 | **The JSON facade transcodes `flexiq.v1`. There is no per-RPC allowlist.** | A per-RPC list is a thing someone forgets to add to. #718's drift test then checks one package for coverage rather than checking a list against itself. |
| D3 | **`v1` is permanent.** Fields are deprecated in place, never renumbered; on removal both the number **and** the name are reserved. New RPCs, messages, fields and enum values are additive and never bump the package. | Temporal has not left `v1` in nine years. The cost of a `v2` is running both for a deprecation window; the cost of a deprecated field is a line of comment. |
| D4 | **Field *names* are frozen, not just numbers**, and `buf breaking` runs at `WIRE_JSON` because of it. | The facade (#718) publishes JSON field names to clients that have no `.proto`. A rename is invisible to binary protobuf and fatal to them. |
| D5 | **The proto never restates `BINDING_CONTRACT.md`; it references it.** Where a value's meaning is defined there — the payload envelope, step rules, capability semantics, the contract floor — the proto carries the value and not a second definition of it. | Two definitions of one fact drift, and the epic already forbids "two implementations of one envelope" (#715). |
| D6 | **A `CONTRACT_VERSION` bump never changes a wire message, and a wire change never bumps `CONTRACT_VERSION`.** | They version different things on different clocks. A network client cannot be upgraded in lockstep with the storage; that is the whole reason the contract is on a socket. |
| D7 | **Every error carries a `google.rpc.Code` *and* a `google.rpc.ErrorInfo` with `domain = "flexiq.byteveda.org"` and a `reason` from a closed list**, with typed metadata where a client needs a number (§4). The reason, not the code and not the message, is the stable identifier. | E1: nothing today lets a client branch except on prose. E5: the one place a number is already parsed out of prose is the bug this replaces. |
| D8 | **The code/reason table agrees with `classify_step_failure` on every arm that function names explicitly, and a test pins the agreement.** Its `_ => Retryable` fallthrough is excluded: the two answer different questions (§4.4). | E2. Two classifiers that disagree about whether a failure is permanent is worse than one — but "this attempt may run again" and "this client may resend" are not the same sentence, and pinning them together would make `Timeout` a retryable RPC. |
| D9 | **Storage errors are sanitised.** A Diesel, r2d2 or Redis `Display` is logged server-side and never sent; the client gets `UNAVAILABLE` + `STORAGE_UNAVAILABLE`. | Those strings carry SQL, schema and connection detail. The dashboard already made this call for its 500s, in those words — "the cause is for the operator's log, never the response — it can carry a DSN or a query fragment" (`dashboard/error.rs:72-78`). |
| D10 | **The namespace comes from the credential. No request message carries one, in either package.** If multi-namespace credentials ever arrive, it becomes the metadata header `flexiq-namespace`, validated against the token's allowed set — never a body field. | A tonic interceptor sees metadata, not a decoded message. #716's guarantee is that a *new RPC is checked without anyone touching auth code*; a namespace in the body is checked per-RPC and loses that guarantee on the first RPC someone forgets. |
| D11 | **The NULL namespace is not addressable over the wire, and a listener serves exactly one namespace — the process's own.** A gRPC listener requires a non-empty `FLEXIQ_NAMESPACE` and refuses to start without one; #717 refuses to mint a token for any other namespace. | E3: `None` on an id-addressed read means "any namespace". A wire that can express `None` is one interceptor bug away from cross-tenant reads. E9: one process runs one scheduler on one namespace, so a token scoped elsewhere would accept writes nothing ever dequeues (§5.4). Refusing to start mirrors `listen.rs:74-82`, which the operator already meets. |
| D12 | **Cross-namespace anything is indistinguishable from nonexistent** — `NOT_FOUND` for a read, `DEPENDENCY_NOT_FOUND` for an edge. No existence oracle. | E4 already does this inside storage; the wire must not add a way to tell the cases apart (e.g. by latency-free `PERMISSION_DENIED`). |
| D13 | **Absent by design, and absent as a rule:** the `Storage` admin surface, the settings KV, dead-letter operations, locks, topics, steps, mesh, and every method that exists because a backend needed it (`dequeue*`, `claim_execution`, `reap_*`, `try_acquire_token`). The proto maps what the SDKs expose, not what the database supports. | #710 states the tax: mirroring `Storage` inherits ~110 methods at three-backend parity and makes every future storage method a wire change. |
| D14 | **Uniqueness, debouncing and batching are options and shapes, not RPCs.** One `Enqueue`, one `EnqueueBatch`; `enqueue_unique` and `enqueue_debounced` are storage entry points, not producer verbs. | Same rule as D13 from the other side: the SDKs expose one `enqueue` with options (`app.py:730-806`), and the wire should not expose the dispatch inside it. |
| D15 | **`NO_SIDE_EFFECTS` on `GetJob`, `ListJobs`, `QueueStats`, `GetWorkflowRun` — and nowhere else. The facade serves `GET` iff `NO_SIDE_EFFECTS`.** `Enqueue` stays `IDEMPOTENCY_UNKNOWN` even though a `unique_key` makes a given call idempotent. | The level is a property of a method, not of one request. A conditional level is a level a proxy will get wrong. |
| D16 | **Responses describe resulting state, not what the call did** — with one named exception, `EnqueueResponse.deduplicated`, because a producer provably needs it. | It is what makes `CancelJob` genuinely retryable rather than merely retryable-in-effect. |
| D17 | **No batch atomicity is promised.** `EnqueueBatch` returns a per-item result, and a partial success is a normal outcome. | E11. |
| D18 | **`JobStatus` is an enum on the wire, mapped to the Rust discriminant by an exhaustive conversion, and never a string.** An unrecognised status means "not terminal" to a reader. | E12: two spellings already exist and a third string would be a third thing to keep in step. An enum's unknown value is at least a value a reader can be told what to do with. |
| D19 | **Explicit presence for everything whose absence differs from its zero** — `result`, `progress`, `expires_at`, `result_ttl_ms`, `started_at`, `completed_at`. | The frame protocol already draws this distinction and says so: "`result_len: null` means the task returned nothing; `0` means it returned an empty value. They are distinct." |
| D20 | **`google.protobuf.Timestamp` and `Duration` on the wire; the Unix-ms boundary is exactly the service impl.** The dashboard API keeps milliseconds — it is a different contract and does not move. | #714's reason: Hatchet's string duration expressions. Ours: `Job` timestamps are `i64` ms everywhere inside (`job.rs:71-135`), so the conversion must live in one module or it will live in twenty. |
| D21 | **`metadata` stays an opaque JSON string, byte-preserved.** Not `google.protobuf.Struct`. | It is `Option<String>` in the core; a Struct round-trip reorders keys and loses exactly the values `wire-vectors.json` documents as unrepresentable (E10). |
| D22 | **The producer door caps messages at 4 MiB; the executor door caps them at `MAX_PAYLOAD_BYTES` + 4 MiB of envelope headroom (68 MiB).** A payload limit and a message limit are different numbers and both are declared, in one place per package. | 4 MiB is gRPC's default and #718's body cap, so the two producer doors agree about "too large". The executor door carries payloads the local frame protocol already allows (64 MiB); tonic's `max_decoding_message_size` measures the *serialized message*, so a 64 MiB `bytes` field plus its tag, length prefix and sibling fields exceeds a 64 MiB limit and the gRPC transport would reject work the TCP transport accepts. |
| D23 | **Generated code lives in `flexiq-server` behind the `grpc` feature. A `flexiq-proto` crate is not published until the protos are stable.** | Publishing generated types creates a *second* permanent compatibility surface — a Rust API under `cargo-semver-checks` — on top of the wire one, and #704 already showed what that wall costs. |
| D24 | **The executor package mirrors the frames additively and keeps `capabilities[]` as strings.** An unknown frame arm is ignored, not fatal. The registry fingerprint is never a wire field. | E6: turning capabilities into an enum would make adding one a proto change, undoing the mechanism. #703 replaced the fingerprint with derivation from `tasks[]`; a wire field would be a second copy of one fact, free to disagree. |

---

## §1 Package, layout and names

### 1.1 Layout

```text
contracts/
  wire-vectors.json                      # exists
  descriptor.binpb                       # committed, #712 — reflection without a checkout
  proto/
    buf.yaml  buf.gen.yaml               # #712
    flexiq/v1/
      job.proto                          # Job, JobStatus, EnqueueOptions, Debounce, StructuredArgs
      producer_service.proto             # ProducerService + its request/response messages
      workflow.proto                     # WorkflowGraph, WorkflowRun, submit/read messages
    flexiq/executor/v1/
      executor_service.proto             # ExecutorService, ExecutorFrame, SchedulerFrame
```

`contracts/proto` is the buf module root. `PACKAGE_DIRECTORY_MATCH` (in `MINIMAL`,
so it applies at any category) forces the directory to equal the package, which
is why `flexiq/executor/v1/` and not `flexiq/v1/executor/`.

`flexiq.executor.v1` imports `flexiq/v1/job.proto`. The reverse import is
forbidden: the producer package must remain compilable and reviewable without the
executor one, because it is the package a third party generates from.

### 1.2 What `buf lint` at `STANDARD` forces

#712 pins the lint category, and the category renames two things the sibling
issues already sketched. Deciding it here means the rename happens once rather
than in review.

**The category is spelled `STANDARD`.** It was `DEFAULT` until buf v1.40.0 and
the old name still resolves, so a reviewer working from older knowledge will read
`STANDARD` as a typo. It is not one, and `buf.yaml` should say `STANDARD`.

- **`SERVICE_SUFFIX`** — a service name must end in `Service`. `service Producer`
  (#714) and `service Executor` (#720) become **`ProducerService`** and
  **`ExecutorService`**.
- **`RPC_REQUEST_RESPONSE_UNIQUE`** — no response type shared by two RPCs and no
  `google.protobuf.Empty`. Every RPC gets its own request and response message
  even when the response is empty today; an empty message is the only shape that
  can grow a field later.
- **`RPC_REQUEST_STANDARD_NAME` / `RPC_RESPONSE_STANDARD_NAME`** — `EnqueueRequest`
  / `EnqueueResponse`. #714's sketch already complies.
- **`ENUM_VALUE_PREFIX` and `ENUM_ZERO_VALUE_SUFFIX`** — every value is prefixed
  with the enum name and the zero value ends in `_UNSPECIFIED`:
  `JOB_STATUS_UNSPECIFIED`, `JOB_STATUS_PENDING`, …
- **`PACKAGE_VERSION_SUFFIX`** — both packages already end in `v1`.

**A trap this rules out.** `ENUM_VALUE_PREFIX` is also the reason the error
`reason` list in §4 is *not* an enum: an enum value would have to be named
`ERROR_REASON_QUEUE_FULL` while `ErrorInfo.reason` must read `QUEUE_FULL`, and
the two spellings would be maintained by hand. The list lives in one Rust module
and in §4, with a test asserting every `QueueError` variant maps into it.

### 1.3 The two packages we do not own

`grpc.health.v1` and the reflection service (#713) are third-party packages
served alongside ours. They are exempt from D2 — the JSON facade does not
transcode them — and from §2 entirely: we do not version, extend or reserve
anything in a package we did not define. If a health check needs to say something
FlexiQ-specific, it says it in `flexiq.v1`, not in a field bolted onto
`grpc.health.v1`.

### 1.4 Field-number discipline

Numbers 1–15 encode in one byte. On the messages that appear in every response —
`Job`, `EnqueueOptions` — they are spent on the fields that are always present,
and 16+ on the optional tail. A message that is nearly full at 15 does not get a
16th hot field; it gets a nested message.

Nothing else about numbering is decided here. #714 assigns numbers under D3 and
D4, and once a number ships it is spent forever whether or not the field turned
out to be a good idea.

---

## §2 The versioning rule

### 2.1 The rule

1. **Never renumber a field.** Not for tidiness, not during review of an
   unreleased RPC once that RPC has been generated into anything.
2. **Deprecate in place.** `[deprecated = true]` plus a comment saying what to
   read instead. A deprecated field keeps being populated for as long as any
   supported client reads it.
3. **On removal, reserve both.** `reserved 7;` *and* `reserved "old_field";` —
   the number so nothing reuses the slot, the name because D4 makes JSON names
   part of the contract.
4. **Additive changes never bump anything.** New RPC, new message, new field, new
   enum value, new `oneof` arm inside an existing `oneof`.
5. **Meaning and units are frozen with the number.** A field that meant
   milliseconds means milliseconds forever. Needing different semantics means
   needing a different field.
6. **Readers tolerate what they do not know** — unknown fields, unknown enum
   values, unknown `oneof` arms. This mirrors the frame protocol's
   "an unknown frame type is skipped, not fatal" (E6) and is stated in the proto
   comments, because a client reads the `.proto` and not this document.

### 2.2 The escape hatch, and when it closes

Until the release that first ships the gRPC role, the protos carry a file-level
comment marking them unstable, and #712's skip label is the only sanctioned way
to break them. After that release the label is for reverting a mistake made in
the same release train, never for changing a shipped shape.

### 2.3 What would force a `v2`

A change that is (a) not additive, (b) not expressible as a new RPC, message or
service, and (c) actually required. Every candidate visible from here fails at
least one:

| Candidate | Why it is not a `v2` |
|---|---|
| Multi-namespace credentials | A metadata header (D10). Additive. |
| A protobuf payload codec (`0x04`) | A new arm in the body `oneof` (#715). Additive — and reserved, not claimed. |
| Completion notification / a watch stream | A new RPC. §10 says v1 does not promise one. |
| The lease token (#719) | Lands *before* the executor package's first release, so it is never a retrofit. |
| Workflow graph evolution | New fields on `WorkflowGraph`. Additive. |
| A storage change that strands a field | The field is deprecated in place. D6 forbids the alternative. |

So: **no planned change forces a `v2`, and the burden of proof is on the change,
not on `v1`.** If one arrives anyway, `flexiq.v2` is a new package served
alongside `v1` for at least one major, never an edit to `v1` files.

---

## §3 Two contracts: the proto and `BINDING_CONTRACT.md`

### 3.1 They are not the same kind of promise

| | `BINDING_CONTRACT.md` | `flexiq.v1` / `flexiq.executor.v1` |
|---|---|---|
| Between | a language shell and `flexiq-core`, in one process | a client and a server, over a socket |
| Versioned by | `CONTRACT_VERSION` + the `contract:min_sdk` floor | field numbers, permanently |
| Upgrade shape | both sides ship together; the floor gates the rest | independent, indefinitely |
| Breaking is | a level bump plus an operator raising the floor | not available |

### 3.2 Which wins

- **On payload bytes, step semantics, capability meaning, status semantics and
  the contract floor — `BINDING_CONTRACT.md` wins.** The proto carries those
  values and does not define them. If a proto comment and the binding contract
  disagree about what `0x02` is, the proto comment is the bug.
- **On request and response shape, error codes and reasons, where the namespace
  comes from, idempotency levels and message limits — the proto wins**, and the
  binding contract says nothing about them. It has no opinion on any of it,
  because none of it exists in-process.

### 3.3 Where each may change without the other

- A new capability, a new frame, a new `Storage` method, a `CONTRACT_VERSION`
  bump: **no proto change**, unless a `flexiq.v1` message names the thing that
  changed. D6 is the hard form of this — a level bump must not move a wire field,
  because the client on the other end was not part of the deploy.
- A new RPC, a new option field, a new error reason: **no `CONTRACT_VERSION`
  bump**. The storage did not change.
- The one seam where they touch is the executor package, which transports frames
  the binding contract defines. When they disagree there, the binding contract
  wins and the proto is corrected — additively, per §2.1. D1 exists so that this
  correction cannot reach the producer package.

---

## §4 The error model

### 4.1 Shape

Every non-`OK` response carries a `google.rpc.Status` whose `details` include
exactly one `google.rpc.ErrorInfo`:

```text
domain   = "flexiq.byteveda.org"
reason   = one of the closed list below            # the stable identifier
metadata = map<string, string>, per reason         # never parsed out of prose
```

**`ErrorInfo.metadata` is `map<string, string>`, so every numeric value needs a
stated encoding.** Values are **base-10 ASCII, no grouping, no unit suffix, `-`
for negative**. The width and signedness are per key, and they are the Rust
field's, not a uniform `int64` — a byte count that is `u64` in
`crates/flexiq-core/src/error.rs` must not be narrowed to fit one parser:

| Key | Reason | Type and unit |
|---|---|---|
| `queue` | `QUEUE_FULL` | queue name, verbatim |
| `pending`, `cap` | `QUEUE_FULL` | `int64`, jobs (`error.rs:62,64`) |
| `speaks`, `required` | `CONTRACT_TOO_OLD` | `uint32`, contract level (`error.rs:93,95`) |
| `limit` | `STEP_LIMIT_EXCEEDED` | one of `step bytes`, `total bytes`, `step count` |
| `actual`, `allowed` | `STEP_LIMIT_EXCEEDED` | `uint64`, in `limit`'s unit (`error.rs:138,140`) |
| `index` | any, from `EnqueueBatch` | `int32`, 0-based position in the request (§7.4) |

`actual` is unsigned because its producers cast a `usize` byte count, and a
signed reading would turn a value above `i64::MAX` into a negative one. Nothing
realistic reaches that, which is exactly why it would be found late.

`index` is the one cross-cutting key: it accompanies whatever reason the failing
item raised rather than getting a reason of its own, because a client that gets
`QUEUE_FULL` on a batch needs both facts — what went wrong and which item. Every
other key is sent only with the reason its row names.

A value that will not parse is a server bug, and a client treats it as absent
rather than failing the response — the code and the reason already carry the
decision.

`RESOURCE_EXHAUSTED` additionally carries `google.rpc.RetryInfo`. The human
message is for logs and may be reworded in any release; `reason` may not.

**Amended during #714: one reason has no `QueueError` behind it.**
`INVALID_REQUEST` covers a request the service refuses before any storage call —
no `body` arm set, an unreadable `page_token`, a `Debounce` with no window, a
`debounce` block inside an `EnqueueBatch`. §4.2 enumerates `QueueError`, and
none of these is one; D7 nevertheless requires every error to carry a reason, so
the list needs a member for the case where the request itself is the fault. It
is `INVALID_ARGUMENT`, never retryable, and it is the only reason on the closed
list that §4.2's table does not produce.

This is what replaces E5. `QUEUE_FULL` arrives with
`metadata{queue, pending, cap}` as separate values, and the `Display` string
stays exactly where it is — an FFI expedient for a boundary that can only carry a
string, not something the network inherits.

### 4.2 The mapping

Reachability: **P** = producer door, **X** = executor door, **—** = not reachable
from either (listed so a future RPC does not invent a second answer).

| `QueueError` | Code | `reason` | Retry? | Where |
|---|---|---|---|---|
| `Storage(Diesel, neither `DatabaseError` nor `NotFound`)`, `Pool`, `Redis` | `UNAVAILABLE` | `STORAGE_UNAVAILABLE` | yes, backoff | P, X |
| `Storage(DatabaseError(any other kind))` | `UNAVAILABLE` | `STORAGE_UNAVAILABLE` | yes, backoff | P, X |
| `Storage(diesel::result::Error::NotFound)` | `INTERNAL` | `INTERNAL` | no | P, X |
| `Storage(DatabaseError(Unique/FK/NotNull/Check))` | `INTERNAL` | `STORAGE_CONSTRAINT` | no | P, X |
| `Json`, `Serialization` — caused by client bytes | `INVALID_ARGUMENT` | `MALFORMED_PAYLOAD` | no | P, X |
| `Json`, `Serialization` — produced server-side | `INTERNAL` | `INTERNAL` | no | P, X |
| `JobNotFound` | `NOT_FOUND` | `JOB_NOT_FOUND` | no | P |
| `DependencyNotFound` | `FAILED_PRECONDITION` | `DEPENDENCY_NOT_FOUND` | no | P |
| `QueueFull` | `RESOURCE_EXHAUSTED` | `QUEUE_FULL` + `{queue,pending,cap}` | yes, after `RetryInfo` | P |
| `RateLimitExceeded` | `RESOURCE_EXHAUSTED` | `RATE_LIMITED` | yes, after `RetryInfo` | P |
| `ContractTooOld` | `FAILED_PRECONDITION` | `CONTRACT_TOO_OLD` + `{speaks,required}` | no | P, X |
| `TaskNotRegistered` | `FAILED_PRECONDITION` | `TASK_NOT_REGISTERED` | no | X |
| `Timeout` | `DEADLINE_EXCEEDED` | `JOB_TIMEOUT` | no | X |
| `ClaimLost` | `FAILED_PRECONDITION` | `CLAIM_LOST` | no — see below | X |
| `StepDiverged`, `StepSequenceDiverged` | `FAILED_PRECONDITION` | `STEP_DIVERGED` | no | X |
| `StepLimitExceeded` | `INVALID_ARGUMENT` | `STEP_LIMIT_EXCEEDED` + `{limit,actual,allowed}` | no | X |
| `StepRefused` | `FAILED_PRECONDITION` | `STEP_REFUSED` | no | X |
| `Worker`, `Scheduler` | `INTERNAL` | `INTERNAL` | no | P, X |
| `Config` | `INTERNAL` | `SERVER_MISCONFIGURED` | no | P, X |
| `LockNotAcquired` | `ABORTED` | `LOCK_HELD` | yes | — (D13) |
| `SettingConflict` | `ABORTED` | `SETTING_CONFLICT` | yes | — (D13) |
| `Other` | `UNKNOWN` | `UNKNOWN` | no | P, X |
| *(auth, #716/#717)* | `UNAUTHENTICATED` | `UNAUTHENTICATED` | after refresh | P, X |
| *(auth, #717 scopes)* | `PERMISSION_DENIED` | `SCOPE_DENIED` | no | P, X |

`StepLimitExceeded` is `INVALID_ARGUMENT` rather than `RESOURCE_EXHAUSTED` on
purpose: the commit cannot succeed at any later time or under any server state,
which is exactly what `INVALID_ARGUMENT` means and exactly what
`RESOURCE_EXHAUSTED` would deny.

`ContractTooOld` and `QUEUE_FULL` land on different codes, as #711 requires: one
is a deployment that must be upgraded, the other is a queue that will drain.

**`Storage` wraps *every* Diesel error, including `NotFound`, which is why it has
a row of its own and why the generic row excludes it by name** — an
implementation that matched the rows in order would otherwise settle `NotFound`
on `UNAVAILABLE` and never reach its own row. A row that is genuinely absent is normalised before it
becomes an error — `get_job` returns `Option` via `.optional()`
(`diesel_common/jobs.rs:1422-1447`) and the id-addressed paths answer `false` —
so a raw `NotFound` reaching the boundary is a query that forgot `.optional()`,
not a missing row. Retrying it will fail identically, so it is `INTERNAL` and
never `UNAVAILABLE`. A service that mapped it to `NOT_FOUND` would be worse
still: it would answer "no such job" to a caller whose job exists.

**`ClaimLost` is `FAILED_PRECONDITION`, not `ABORTED`, and the reason is §4.3.**
`ABORTED` is the code gRPC defines for a concurrency conflict the caller should
retry at a higher level, and §4.3 puts it in the retry-with-backoff class. A
claim loss is a concurrency conflict where retrying is the single worst thing a
client can do: the job is proceeding under another owner, and a resent frame is
the double-execution the `(owner, attempt)` fence exists to prevent (E7). A code
whose retry class depends on reading `reason` is a code that a generic client, or
any middlebox, gets wrong. `ABORTED` is left to the two variants that really do
mean "read again and retry" — both of which are absent from the v1 surface
(D13).

### 4.3 What a client retries

**Two questions, in order.** *Is this code retryable at all*, and *is this method
safe to send twice*. A client that asks only the first will replay writes, and a
retry policy configured per-code — which is the only granularity gRPC's own
retry config has — must therefore be attached per-method, not service-wide.

**Question one: the code.** Derivable from the code alone — there is deliberately
no `retryable` field, because a client that has to read one has an error model
that does not work for a client written before the field existed:

- **Retryable:** `UNAVAILABLE`, `ABORTED`, and `RESOURCE_EXHAUSTED` only after
  the delay in `RetryInfo`.
- **Retryable once, after refreshing the credential:** `UNAUTHENTICATED`.
- **Never:** `INVALID_ARGUMENT`, `NOT_FOUND`, `FAILED_PRECONDITION`,
  `PERMISSION_DENIED`, `UNIMPLEMENTED`, `INTERNAL`, `UNKNOWN`.
- **`DEADLINE_EXCEEDED` and `CANCELLED` say nothing about the server** — the
  request may have been applied in full. They are retryable only under question
  two.

**Question two: the method.** A retry is safe when the method is
`NO_SIDE_EFFECTS` or `IDEMPOTENT` (§6) — so `GetJob`, `ListJobs`, `QueueStats`,
`GetWorkflowRun` and `CancelJob` may be retried automatically on any code from
the first list, `DEADLINE_EXCEEDED` and `CANCELLED` included.

`Enqueue`, `EnqueueBatch` and `SubmitWorkflow` are `IDEMPOTENCY_UNKNOWN` and
**must not be retried automatically**. `UNAVAILABLE` does not promise the write
did not land: a commit followed by a dropped connection produces it. The single
exception is an `Enqueue` carrying a `unique_key`, which is what the key is for
and why §10 tells clients that intend to retry to always set one.
`EnqueueBatch` has no equivalent unless every item carries a key, and
`SubmitWorkflow` has none at all.

**The `unique_key` window is the job's active life, not forever.**
`enqueue_unique` "returns the existing **active** job when a duplicate is found"
(`traits.rs:34-35`) — a terminal job releases the key, which is the behaviour a
recurring task needs and not a defect. So a retry converges only while the
original is still pending or running; the same request replayed after the job
completed or dead-lettered enqueues a second one, correctly by that rule and
surprisingly to a client that read "idempotent". A client's total retry deadline
therefore has to be shorter than the job's own life, and a producer that needs
convergence beyond it needs a durable key of its own — an application-level
record, not a queue primitive. Stated here so the reference page (#721) can say
it rather than implying `unique_key` is an idempotency key without an expiry.

### 4.4 The two invariants a test enforces

1. **Agreement with `classify_step_failure` (D8), on the arms it names.** For
   every variant that function matches *explicitly*: `Permanent` ⇒ the code is in
   the never-retry set; `Retryable` ⇒ it is in the retry set; `Superseded` ⇒
   `CLAIM_LOST`, which is in the never-retry set.

   **The wildcard is excluded on purpose, and the reason is that the two
   functions answer different questions.** `classify_step_failure` ends in
   `_ => StepFailure::Retryable` (`step/failure.rs:71`) — a fail-safe default at
   the step-ack boundary, where "retryable" means *this job attempt may run
   again*. The wire's retry class means *this client may resend this request*.
   They coincide on every arm `classify_step_failure` names, and they must not be
   conflated elsewhere: `Timeout`, `Worker`, `Scheduler` and `Other` all fall
   through that wildcard, and none of them is a request a client should send
   again. The test asserts agreement over the named arms and asserts nothing
   about the fallthrough.
2. **Totality.** The `QueueError` → `(Code, reason)` function matches
   exhaustively, with no wildcard arm. A new variant fails the build rather than
   arriving on the wire as `UNKNOWN`. The one nested match that keeps a wildcard
   is `DatabaseErrorKind`, which is non-exhaustive upstream — the same reason
   `classify_step_failure` keeps one there (`step/failure.rs:63-68`) — and its
   default is `UNAVAILABLE`, matching that function's `Retryable`.

### 4.5 Sanitisation (D9)

`Storage`, `Pool` and `Redis` messages are logged with their cause and replaced
on the wire by a fixed string.

**`Other` is sanitised too, and that is not obvious from the variant name.**
Sanitisation is by *provenance*, not by variant: `RedisBackend::conn` stringifies
a `redis::RedisError` into `QueueError::Other`
(`redis_backend/mod.rs:86`), and the Redis job and step paths do the same for
malformed replies. A boundary that switched on the variant alone would forward
exactly the connection detail D9 exists to withhold, on the one backend whose
errors most often name a host. So `Other` carries a fixed message and its cause
goes to the log.

The upstream fix — `RedisBackend::conn` raising `QueueError::Redis`, which the
`#[from]` already supports — is a small core change worth making separately. The
wire rule must not depend on it having happened.

Every other variant forwards its `Display`, which is already written for users.

---

## §5 Namespace

### 5.1 It is a property of the credential

No request message in either package has a namespace field. Under #716's shared
secret the `Principal`'s namespace is the server's configured one; under #717 it
is carried by the token. Nothing a client sends can change it.

The reason is mechanical, not stylistic: **a tonic interceptor sees
`MetadataMap`, not a decoded request message.** #716's acceptance criterion is
that "a newly added RPC is checked without anyone touching auth code". A
namespace in the request body is checked inside each handler, so the first
handler that forgets is a cross-tenant read. Metadata keeps the check in the one
place that already wraps everything — the same property
`auth/middleware.rs::gate_request` gives the dashboard router.

If multi-namespace credentials ever land, the namespace arrives as
`flexiq-namespace` metadata and the authenticator validates it against the
token's allowed set. Still one place. Still not the body.

### 5.2 The NULL namespace is not addressable (D11)

E3 is the reason. `None` means "only the NULL rows" to a dequeue, "every
namespace" to an id-addressed read, and "no filter" to a listing. A wire that can
express `None` puts a value with three meanings one interceptor bug away from
`get_job`.

So the gRPC role requires a non-empty namespace, and refuses to start without
one — the same shape as the existing refusal to bind a non-loopback attach port
with no token (`config/listen.rs:74-82`). The service passes
`Some(principal.namespace)` to every `Storage` call and never `None`.

The cost is real and belongs in #721: **jobs written with the default (NULL)
namespace are invisible over gRPC.** A deployment that wants the gRPC door sets
`FLEXIQ_NAMESPACE` and configures its producers with the same value. That is one
line of configuration, paid once, in exchange for making cross-tenant reads
structurally impossible rather than conditionally absent.

### 5.3 Everything scoped, nothing leaked (D12)

- A read for a job in another namespace returns `NOT_FOUND` — identical to a job
  that does not exist. Not `PERMISSION_DENIED`, which would confirm the id.
- A `depends_on` id in another namespace returns `DEPENDENCY_NOT_FOUND` —
  identical to a missing id. This already holds inside storage (E4); the wire
  adds that a client cannot even *name* another namespace, so the edge is
  impossible twice.
- Listings and stats are always filtered, never the unfiltered `None` form.

### 5.4 One listener, one namespace — and why a token cannot pick another

E9: a `flexiq-server` process holds one `FLEXIQ_NAMESPACE` and runs one
`Scheduler` bound to it (`runtime/mod.rs:40,67,123`). The dequeue loop asks
storage for that namespace and no other.

So a `produce` token minted for a *different* namespace would be a working
enqueue path into a queue this process never polls. The write succeeds — §5.2
allows any non-empty namespace at the storage layer — and the job sits `Pending`
forever with nothing on the wire to say why. It is the worst failure shape
available: a success response and no work.

**#717 therefore validates a token's namespace against the server's own at mint
time and refuses a mismatch.** Serving more than one namespace from one process
is a scheduler change, not a token change, and it is not in this epic. Until it
is, "which namespace does this token carry" has exactly one legal answer per
deployment, and #717's namespace field is there to make the *later* change
additive — not to offer a choice today.

---

## §6 Idempotency levels, and the HTTP method rule

```text
NO_SIDE_EFFECTS      GetJob · ListJobs · QueueStats · GetWorkflowRun
IDEMPOTENT           CancelJob
(unset / UNKNOWN)    Enqueue · EnqueueBatch · SubmitWorkflow
```

- **`NO_SIDE_EFFECTS` is what makes a `GET` legal**, and #718 serves `GET` for
  exactly these four and `POST` for everything else. The rule is stated as an
  iff so that adding an RPC does not require a judgement call.
- **`CancelJob` is `IDEMPOTENT` because its response describes state** (D16).
  `Storage::cancel_job` returns `false` for a job that is no longer pending
  (`traits.rs:156-160`), so a bool would tell a retrying client "I did not cancel it"
  about a job it cancelled a moment ago. The response carries the job's resulting
  status instead, and a second call answers the same thing as the first.
- **`Enqueue` is not `IDEMPOTENT`, even with a `unique_key`.** The level is a
  property of the method; a level that is true only for some requests is a level
  a proxy will apply to the others.
- **No caching semantics.** `NO_SIDE_EFFECTS` is not a cacheability claim, and
  the facade emits no `ETag` or `Cache-Control` beyond `no-store`. Job state
  changes continuously; a cached `GetJob` is a wrong answer with a fast response
  time.

---

## §7 The producer surface: the shapes that are decisions

Field numbers below are pinned only where the shape *is* the decision. #714
assigns the rest under §1.4, D3 and D4.

### 7.1 The body (#715)

```proto
message EnqueueRequest {
  string task_name = 1;
  oneof body {
    bytes          raw        = 2;   // 0x02 || CBOR([args, kwargs]), verbatim
    StructuredArgs structured = 3;   // encoded server-side into the same envelope
  }
  EnqueueOptions options = 4;
}
```

- `raw` reaches storage untouched. It is what an SDK sends.
- `structured` is encoded through **the same encoder the shells use** — one
  implementation of the envelope, per D5 — and refuses the values
  `wire-vectors.json` marks unrepresentable rather than rounding them (E10).
- The `oneof` is the extension point that keeps `0x04` reserved rather than
  claimed: a future codec is a third arm, not a `v2`.
- An absent `body` is not an empty body. `raw = ""` is a zero-length payload;
  no arm set is `INVALID_ARGUMENT`.

### 7.2 `EnqueueOptions` mirrors `NewJob`

`NewJob` has fifteen fields (`crates/flexiq-core/src/job.rs:291-322`). The proto
mirrors the producer-settable ones and invents no second vocabulary:

```proto
message EnqueueOptions {
  string   queue          = 1;
  int32    priority       = 2;
  int32    max_retries    = 3;
  google.protobuf.Timestamp scheduled_at = 4;
  google.protobuf.Duration  timeout      = 5;
  optional string unique_key    = 6;
  optional string metadata      = 7;   // opaque JSON text (D21)
  optional string notes         = 8;
  repeated string depends_on    = 9;
  optional google.protobuf.Timestamp expires_at    = 10;
  optional google.protobuf.Duration  result_ttl    = 11;
  optional Debounce                  debounce      = 12;
}

message Debounce {                     // records.rs:426-457 + NewJob.debounce_key
  string   key             = 1;
  google.protobuf.Duration window   = 2;
  google.protobuf.Duration max_wait = 3;
  bool     replace_payload = 4;
  optional int64 max_pending = 5;
}
```

Not present, deliberately: `namespace` (§5) and any `idempotent`/`auto:` flag.
The `auto:` key is `sha256` over the serialized payload and belongs to the
shells; a server-side second implementation would be a second thing that has to
agree byte-for-byte with three others. A client that wants it computes it and
sends it as `unique_key`. Adding it later is additive.

`Debounce` is a nested message rather than five top-level fields because the
three-of-three validation the shells already do (`py_queue/mod.rs:343-375`)
becomes "the message is present or it is not".

Two defaults the field comments must state, because proto3 cannot and a client
cannot guess:

- **An empty `queue` means `"default"`**, substituted server-side. It is the name
  the shells already default to (`sdks/python/flexiq/app.py:941,953`), and a
  queue literally named `""` is not addressable anyway.
- **Higher `priority` runs first** (`crates/flexiq-core/src/job.rs:83`). The
  direction is half the meaning of the field, and D3 freezes meaning with the
  number.

### 7.3 `Job`, and what a response carries

`Job` mirrors the read model (`job.rs:71-135`) with D19 presence, D20 timestamps
and one addition of its own:

- **`payload` and `result` are not sent unless asked for.** `GetJob` takes
  `bool include_payload` / `bool include_result`; `ListJobs` never carries
  either. This is the wire form of the blob-free listings #432 already
  established in storage (`NarrowJobRow`), and without it a page of 100 jobs is
  a page of 100 payloads.
- **`optional bytes payload` and `optional bytes result`** — both need explicit
  presence, and for two different reasons. A result that is absent and one that
  is empty are different answers, per D19 and the frame protocol's own
  `result_len` rule. A payload is absent when the caller did not ask for it and
  empty when the job carries no body — and §7.1 permits `raw = ""`, so a plain
  proto3 `bytes` would decode "not requested" and "genuinely empty" identically.
- `status` is `JobStatus`, the enum (D18).
- `namespace` is output-only and always the caller's. It is present so a client
  logging a job record has it, not so a client can select one.

```proto
enum JobStatus {                       // 1:1 with job.rs:8-68, pinned by a test
  JOB_STATUS_UNSPECIFIED = 0;
  JOB_STATUS_PENDING     = 1;
  JOB_STATUS_RUNNING     = 2;
  JOB_STATUS_COMPLETE    = 3;
  JOB_STATUS_FAILED      = 4;
  JOB_STATUS_DEAD        = 5;
  JOB_STATUS_CANCELLED   = 6;
}
```

The values are **not** the Rust discriminants (`Pending = 0`): zero is reserved
for `_UNSPECIFIED` by lint and by the "unknown value" convention, so the two are
offset by one and converted through an exhaustive match with no wildcard. A test
pins both directions and fails when a variant is added. The two existing strings
(`as_str()`, `wire_name()`) stay exactly where they are and the wire uses
neither (E12).

### 7.4 `EnqueueResponse` and the batch

```proto
message EnqueueResponse {
  Job  job          = 1;
  bool deduplicated = 2;   // the named exception to D16
}
```

`deduplicated` is true when a `unique_key` matched an existing active job and
`enqueue_unique` returned it (`traits.rs:34-36`). #714's acceptance —
"the same `unique_key` sent twice returns one job id twice" — holds, and the
producer can still tell the two calls apart, which is the whole reason a producer
sets a `unique_key`.

`EnqueueBatch` returns one result per input item, in input order:

```proto
message EnqueueBatchItemResult {
  oneof outcome {
    EnqueueResponse enqueued = 1;   // Job + deduplicated, exactly as Enqueue answers
    google.rpc.Status error  = 2;
  }
}
```

The item arm is `EnqueueResponse`, not a bare `Job`, because a batch item can
dedupe on its `unique_key` for the same reason a single enqueue can, and a
producer needs that answer just as much per item as it does per call (§7.4's
named exception to D16). Reusing the response message also means a client's
single-enqueue handling works unchanged on a batch item.

**No atomicity is promised** (D17, E11), and the failure shape follows the
backend rather than papering over it:

- **A backend whose batch is one transaction** — Diesel — fails the *RPC*. One
  item's failure rolls back every insert, so returning earlier items as
  `enqueued` would report jobs that do not exist. The top-level `Status` keeps
  the failing item's own reason, and adds `metadata{index}` — the 0-based
  position in the request, per §4.1's table — whenever the failing item can be
  named, so a client learns what went wrong and which item in one answer.

  **Amended during #714: `index` is present only when the failure is
  attributable**, which is why the sentence above says "whenever" rather than
  "always". `Storage::enqueue_unique_batch` takes the whole batch and
  returns one error for it; nothing in `DependencyNotFound` or `QueueFull` names
  a position, so an all-or-nothing storage failure genuinely cannot be pinned to
  an item, and inventing a number would be worse than omitting one. So `index`
  accompanies every failure the *service* attributes — the request-shape
  refusals of §4.1's amendment, which are checked per item before anything is
  written — and every per-item failure on the partially-applying path. It is
  absent on a rolled-back batch, where the honest answer is that the whole batch
  failed. Restoring it there is a storage change, not a wire one.
- **A backend that can partially apply** — Redis, an unrolled-back pipeline —
  answers `OK` with per-item results, and an `error` arm means that item alone
  did not land.

A client that treats an `enqueued` arm as durable is correct under both, which is
the property the split exists to give it.

### 7.5 Reads and pagination

`ListJobs` mirrors `list_jobs_after` (`traits.rs:224-232`): filters on status,
queue and task name, and a keyset cursor.

**The cursor is an opaque string, never the `(created_at, id)` tuple.** Redis has
no seekable index and applies the keyset in memory over the same candidate set
(`traits.rs:217-223`), so the cursor's contents must stay free to change per
backend and per release. A tuple on the wire freezes both. Clients pass back
`next_page_token` and interpret nothing.

`QueueStats` returns the six counters of `QueueStats`
(`crates/flexiq-core/src/storage/mod.rs:214-229`) — `pending`, `running`,
`completed`, `failed`, `dead`, `cancelled` — and nothing derived from them.

### 7.6 Workflows

`WorkflowDefinition.dag_data` is JSON produced by `dagron_core::SerializableGraph`
(`crates/flexiq-workflows/src/definition.rs:49-58`, built at
`crates/flexiq-python/src/py_workflow/mod.rs:119-127`). It is an internal detail
of a dependency.

**`SubmitWorkflow` takes a structured `WorkflowGraph` message and never raw
`dag_data` bytes.** The service compiles it into the internal form — the same
relationship `structured` args have with the CBOR envelope (§7.1). Accepting the
bytes would export a dependency's serialization format as a permanent contract by
accident, which is precisely the failure #714 names. Node bodies inside the graph
reuse the `raw`/`structured` `oneof`.

If the graph message is not settled when #714 is otherwise ready, **`SubmitWorkflow`
and `GetWorkflowRun` ship in a later, additive release.** What must not happen is
a `bytes dag_data = N` placeholder: a field number spent on an internal format is
spent forever.

---

## §8 The executor surface (`flexiq.executor.v1`)

Stage two, and constrained mostly by things already decided elsewhere.

- **The frames are the frames.** `hello`, `hello_ack`, `job`, `job_steps`,
  `cancel`, `shutdown`, `step_ack`, `progress`, `task_log`, `step_commit`,
  `success`, `failure`, `cancelled`, `slept` — the set in
  `BINDING_CONTRACT.md`, with the same ordering rules. `ExecutorFrame` and
  `SchedulerFrame` are each one message with a `oneof`, so an unrecognised arm is
  ignored rather than fatal — the protobuf form of "an unknown frame type is
  skipped, not fatal" (E6, D24).
- **`capabilities[]` stays `repeated string`** (D24). An enum would make adding a
  capability a proto change, which is the exact cost the capability mechanism
  exists to avoid.
- **The registry fingerprint is never a wire field.** `hello` carries `tasks[]`
  and the scheduler derives the fingerprint, as #703 established. A wire
  fingerprint would be a second copy of one fact, free to disagree.
- **Nothing the executor sends names its own authority** (E7): no owner, no
  attempt, no namespace, no cap. The scheduler resolves all of them from the
  dispatch it recorded.
- **The lease token (#719) is present from this package's first release**, opaque
  `bytes`, minted by the scheduler, required on **every executor→scheduler frame
  that names a dispatched job**: `success`, `failure`, `cancelled`, `slept`,
  `progress`, `task_log` and `step_commit`. `cancelled` and `slept` are attempt
  outcomes — one ends the attempt, the other reschedules the job — so a stale
  executor's copy writes over a live attempt exactly as a stale `success` would.
  The rule is "does this frame settle or advance an attempt", not a list, so a
  frame added later inherits it. `hello` and `heartbeat` are the connection's
  own, and carry no token. The token is not a replacement for the
  `(owner, attempt)` fence; it is the input the scheduler resolves against its
  dispatch record. It is here from the start because a token is a few bytes in a
  protocol and nearly impossible to retrofit.
- **`Heartbeat` is a unary RPC, not a frame on the dispatch stream** (#720).
- **`max_decoding_message_size` is `MAX_PAYLOAD_BYTES` + 4 MiB = 68 MiB** (D22).
  Setting it *to* `MAX_PAYLOAD_BYTES` would still reject a maximum payload:
  tonic's limit measures the serialized message, and a 64 MiB `bytes` field
  carries a tag, a length prefix and its sibling fields on top. The headroom is
  the difference between a payload limit and a message limit, and it is what
  keeps the two `Transport` implementations interchangeable. The acceptance test
  sends a `MAX_PAYLOAD_BYTES` payload over both transports, not one under it.
- **No HTTP binding, ever** (D2). Not "not yet".

---

## §9 Types, presence and limits

| Concern | Decision |
|---|---|
| Ids | `string`. UUIDv7 today (`job.rs:331`), but the wire promises "opaque string", not a UUID, and never `bytes`. |
| Times | `google.protobuf.Timestamp`; durations `google.protobuf.Duration` (D20). Storage is Unix ms, so sub-millisecond precision is truncated on the way in — stated in the field comment, not discovered. |
| Presence | `optional` for every field whose absence differs from its zero (D19). Not wrapper types. |
| Enums | `_UNSPECIFIED = 0`; readers tolerate unknown values; an unknown `JobStatus` means "not terminal", never "failed". |
| `metadata` | opaque JSON `string`, byte-preserved (D21). |
| Payload / result | `bytes`, opaque, never re-encoded (D5, and #710's "not a fourth payload codec"). |
| Message size | 4 MiB producer; executor **message** 68 MiB for a 64 MiB **payload** (D22) — the two are different numbers and the headroom is deliberate. Each declared once. |
| Transport security | Not terminated here. Same posture as attach: the token is a bearer credential and TLS belongs to a sidecar or mesh (`config/listen.rs:23-25,61-70`). #721 carries the warning until #717 closes. |

---

## §10 What the contract must not promise

Every line here is a thing a reasonable client would otherwise assume. They
belong in the `.proto` comments as well as in #721.

1. **No ordering.** Priority and `scheduled_at` influence dispatch; nothing
   promises that two jobs enqueued in order run in order.
2. **No exactly-once execution.** `unique_key` dedupes an *enqueue*. Execution is
   at-least-once and always was.
3. **No batch atomicity** (D17, E11).
4. **No task-name validation at enqueue.** The server holds no task registry — a
   `hello` frame is where a registry becomes visible, and the producer door never
   sees one. Enqueuing a task nobody implements succeeds and the job eventually
   dead-letters. The executor door's registry divergence warning is a diagnostic
   for attached executors, not a validation the producer inherits.
5. **No completion notification.** No watch, no server-stream of job state in v1.
   The facade cannot transcode a stream (#718), and a completion watch is a real
   feature that deserves its own design rather than a field. Poll `GetJob` or use
   a webhook subscription.
6. **No payload interpretation.** `raw` is opaque; `structured` refuses what it
   cannot represent (E10) and says which values those are in the reference, not a
   footnote.
7. **No admin surface on this credential, ever** (D13). Pausing queues, settings,
   DLQ purges, webhook secrets and circuit-breaker internals stay behind the
   dashboard's `Admin` gate, where a session and RBAC already cover them.
8. **No cross-namespace anything, and no way to name another namespace** (§5).
   One listener serves one namespace, and a credential does not widen that
   (§5.4) — a job accepted into a namespace this process does not schedule would
   be a success response and no work.
9. **No permanent job ids.** Retention archives and then deletes; `get_job` reads
   live rows and then archived ones (`diesel_common/jobs.rs:1422-1447`) and then
   answers `NOT_FOUND`. A `NOT_FOUND` does not mean the job never existed.
10. **No safe blind retry of a write.** `DEADLINE_EXCEEDED`, `CANCELLED` and
    `UNAVAILABLE` on an `Enqueue` without a `unique_key` may all have landed —
    a commit followed by a dropped connection produces the last one — and no
    field on the wire can tell. Clients that retry set a `unique_key`;
    `EnqueueBatch` needs one per item and `SubmitWorkflow` has no equivalent, so
    neither is automatically retryable at all (§4.3). This is the single most
    important sentence in the reference page.
    **And a `unique_key` is not an idempotency key without an expiry** — it
    dedupes against the *active* job only, so a retry that arrives after the
    original finished enqueues a second one (§4.3).
11. **Not a replacement for embedded mode.** Local, no daemon, straight to
    SQLite stays the default. Server mode is additive, and #721 must answer this
    above the fold.
12. **Not a migration off attach.** The frame protocol keeps its property that a
    JSON header and raw bytes let anyone write an executor with a standard
    library alone. gRPC earns a place beside it.

---

## §11 Review checklist for the sub-issues

A sub-issue fails review if it contradicts a row here without amending this
document first.

| Issue | Reviewed against | Fails if |
|---|---|---|
| **#712** buf CI | D3, D4, §1.1, §1.2 | Lint runs below `STANDARD`; breaking runs below `WIRE_JSON`; a removed field reserves only its number; the descriptor is generated but not committed; buf's version floats. |
| **#713** server role | D11, D22, D23, E9 | The gRPC role starts without a namespace; it reimplements listener parsing instead of using `config/listen.rs`; it is not gated by a cargo feature; it skips the "at least one role" and graceful-shutdown paths. |
| **#714** producer service | D13, D14, D15, D16, D17, D19, D20, D21, §7 | A `namespace` field appears in a request; a `dag_data` bytes field ships; `EnqueueBatch` claims atomicity, drops `deduplicated`, or reports a rolled-back item as enqueued; `payload`/`result` lack explicit presence; listings carry payloads; the cursor is a tuple; services are not `*Service`. |
| **#715** payload | D5, §7.1, E10 | A second envelope encoder appears; `structured` rounds a `round_trip_only` vector instead of refusing it; `raw` is re-encoded on the way through. |
| **#716** shared secret | D10, §5.1 | The namespace is read from a request body; the check is per-RPC rather than one interceptor; `Principal` lacks a namespace and scope from the first commit; a non-loopback bind with no token starts. |
| **#717** scoped tokens | D10, D11, D13, §5.4 | The namespace is taken from the request; a token is mintable for a namespace the process does not serve; a `produce` token can open an executor stream; an unconfigured token store serves anything; the untrusted-network warning survives the PR. |
| **#718** JSON facade | D2, D15, D22, §4.1 | A route exists for an `flexiq.executor.v1` RPC; a `GET` serves an RPC that is not `NO_SIDE_EFFECTS`; the body cap and the gRPC cap disagree; errors are rendered in a shape other than §4.1; the drift test checks a hand-written list instead of the package. |
| **#719** lease token | E7, §8 | The token is minted or chosen by the executor; any frame that settles or advances an attempt — `cancelled` and `slept` included — goes without one; a reclaim or reap does not move the epoch; a stale completion is swallowed rather than raised; it is negotiated by a version bump instead of a capability. |
| **#720** executor transport | D1, D22, D24, §8 | It becomes a second dispatcher rather than a fourth `Transport`; capabilities become an enum; a fingerprint rides the wire; heartbeats ride the dispatch stream; an unknown frame arm is fatal; the message limit is set to the payload limit rather than above it; the executor package gains an HTTP binding. |
| **#721** docs | §4.3, §5.2, §10 | The page reads as though embedded mode is deprecated; the NULL-namespace cost is unstated; `unique_key` is presented as an idempotency key without saying it expires with the job; the `raw`/`structured` precision loss is a footnote; the untrusted-network warning is missing while #717 is open. |

---

## §12 Out of scope

- **Topic pub/sub on the wire.** Publishing is producer-shaped, but a subscriber
  needs lease, ack, nack and cursor operations — an executor-shaped stream with
  its own lifecycle. Shipping publish alone would advertise a door that does not
  open. It is an additive `flexiq.v1` service when someone asks for it, and
  additive costs nothing.
- **A protobuf payload codec.** `0x04+` stays reserved (#710). Claiming it means a
  fourth cross-SDK codec at permanent three-way parity.
- **Multi-namespace credentials.** #717 binds one namespace per token; the
  metadata header in D10 is the sanctioned growth path and is not built now.
- **Generated client SDKs.** Publishing them adds a second permanent
  compatibility surface (D23). The `.proto` files and the committed descriptor
  are what is published in v1.
- **mTLS termination.** Same answer as attach (§9).
