# FlexiQ — remote client contract

What a program that is **not** a FlexiQ SDK must do to speak to a running
`flexiq-server` over the network, and what it may skip and still be called a
FlexiQ client. It is the contract for a client that holds no database
credential, links no native binding, and has only a gRPC library and this
directory.

Everything here is normative. **MUST**, **MUST NOT**, **SHOULD** and **MAY**
carry their RFC 2119 meanings; a claim without one of them is background. Where
a rule can be checked, the check is named — and the bar for the half the
`.proto` calls opaque bytes is [`wire-vectors.json`](wire-vectors.json), in this
directory, next to this file.

## Two contracts, and this is the other one

`crates/flexiq-core/BINDING_CONTRACT.md` is the **FFI** contract: what a
language shell implements against `flexiq-core` in the same process. It ships in
lockstep with the core and its compatibility obligations are internal. This
document is the **wire** contract, and the obligations are the opposite shape.

| | `BINDING_CONTRACT.md` | This document |
|---|---|---|
| **Between** | a language shell and `flexiq-core`, in one process | a client and a server, over a socket |
| **Versioned by** | `CONTRACT_VERSION` and the `contract:min_sdk` floor | field numbers, permanently |
| **Upgrade shape** | both sides ship together; the floor gates the rest | independent, indefinitely |
| **Breaking is** | a level bump plus an operator raising the floor | not available |
| **Enforced by** | `cargo tree`, the shell's own test suite | `buf breaking` at `WIRE_JSON`, `contracts/proto-guard` |

**Where they disagree, one of them is authoritative and it is not always the
same one.**

- On **payload bytes, durable-step semantics, capability meaning, job-status
  semantics and the contract floor**, `BINDING_CONTRACT.md` wins. This document
  and the `.proto` carry those values; they do not define them.
- On **request and response shape, error codes and reasons, where the namespace
  comes from, idempotency levels and message limits**, this document wins, and
  `BINDING_CONTRACT.md` says nothing about them, because none of it exists
  in-process.

A third document, `tasks/specs/2026-09-01-flexiq-v1-proto-design.md`, records
why each of these is what it is. It is a decision record, not a contract: read
it for the reasoning, never for the rule.

## Conformance

### The bar

**A client is conformant when it agrees with
[`wire-vectors.json`](wire-vectors.json).** Every FlexiQ SDK asserts that file
in its own suite for exactly this reason: an encoder that drifts fails its own
build instead of quietly producing payloads its peers cannot read.

The file is `$schema_version: 1` and carries twelve vectors in two arrays.

| Array | Cases | The obligation |
|---|---|---|
| `encode` | 9 | A client **MUST** decode every one. It **MUST** produce the exact `hex` for every case its own call API can express. |
| `decode_only` | 3 | Three cases this file cannot state as an `args` value. A client **MUST** decode each. |

The exemptions are stated, not implied:

- A runtime with **no keyword arguments** is exempt from producing the cases
  with a non-empty `kwargs`, and from those only. It **MUST** still decode them.
- A case marked `round_trip_only` pins no value, because JSON cannot hold one —
  `int-beyond-double-precision` is `2^53 + 1` and `byte-string` is a CBOR byte
  string. A client **MUST** re-encode what it decoded to the same `hex`.
- The `float` case pins the value and not the bytes: an encoder may legitimately
  choose a narrower width.

**A hex string is never edited to make a test pass.** A diff to one is a
wire-format change, and it breaks every job already enqueued.

### Getting the vectors without a checkout

`wire-vectors.json` and this document are **not** packaged into any SDK
distribution — not the Python wheel, not the npm tarball, not the jar. They are
published as a release asset, and that is the supported route:

```bash
VERSION=2.0.0
base="https://github.com/ByteVeda/flexiq/releases/download/server-v${VERSION}"
curl -sSLO "${base}/flexiq-proto-${VERSION}.tar.gz"      # proto/ + wire-vectors.json + this file
curl -sSLO "${base}/flexiq-descriptor-${VERSION}.binpb"  # the compiled descriptor
```

The tarball carries `proto/` with its own `buf.yaml` and `buf.lock`, so `buf`
resolves the one external dependency — `buf.build/googleapis/googleapis`, for
`google.rpc.Status` — without a client declaring it. A build pipeline needs
neither a checkout nor a running server.

Two other routes exist and each answers less. **Server reflection** describes
the RPCs and nothing about the payload inside them, and it is served off the
same committed descriptor, so it cannot describe a different contract from the
one the server implements. **The repository** has the same tree at
`contracts/`; read it at a tag, never at `master`.

### The three claims a conformance run makes

A client that passes has established exactly this, and **SHOULD NOT** be
described as having established more:

1. Its encoder and decoder agree with every other FlexiQ runtime about bytes.
2. Its `auto:` idempotency keys will match theirs, because that key is a hash
   over these bytes.
3. Nothing about its RPC behaviour. Retry discipline, error branching and
   capability honesty are not vector-testable, and they are the rest of this
   document.

## The surface

Two packages, two doors, two credentials. `flexiq.executor.v1` **may** import
`flexiq.v1`; the reverse import is forbidden, so a client generated for the
producer door stays compilable on its own.

### A producer client — `flexiq.v1.ProducerService`

| RPC | Level | Idempotency | Notes |
|---|---|---|---|
| `Enqueue` | **MUST** | none | The door. |
| `GetJob` | **MUST** | `NO_SIDE_EFFECTS` | There is no completion notification anywhere in `v1`; a client that cannot read a job back cannot observe an outcome at all. |
| `CancelJob` | **SHOULD** | `IDEMPOTENT` | Cheap, and without it there is no way to stop work already submitted. |
| `EnqueueBatch` | MAY | none | An optimisation over N `Enqueue` calls, and a harder one: no atomicity, a result per item, one `unique_key` per item. |
| `ListJobs` | MAY | `NO_SIDE_EFFECTS` | Needs cursor handling. |
| `QueueStats` | MAY | `NO_SIDE_EFFECTS` | |
| `SubmitWorkflow` | MAY | none | Static graphs only — see below. |
| `GetWorkflowRun` | MAY | `NO_SIDE_EFFECTS` | |

Whatever subset it implements, a producer client:

- **MUST** send `authorization: Bearer <token>` on every call. There is no
  anonymous path except the two health methods.
- **MUST** branch on `ErrorInfo.reason`, never on the status message.
- **MUST** treat an unknown field, an unknown enum value and an unrecognised
  `oneof` arm as "not this build", never as an error. **A `JobStatus` it does
  not recognise is not terminal** — `JobStatus` reserves zero for
  `JOB_STATUS_UNSPECIFIED`, so its values sit one off the Rust discriminants,
  and only the values a client knows to be terminal are.
- **MUST NOT** retry a write blind. `UNAVAILABLE`, `DEADLINE_EXCEEDED` and
  `CANCELLED` on an `Enqueue` may all mean the write landed and the connection
  dropped after it, and no field on the wire distinguishes them. A client that
  retries **MUST** set `unique_key` and reuse the same value.
- **MUST NOT** expect to name a namespace. There is no field for one.
- **MUST** raise its gRPC library's message-size limit if it sends payloads over
  4 MiB — `PRODUCER_MAX_MESSAGE_BYTES` is 4 MiB in each direction, which is
  gRPC's own default and the same cap the JSON facade applies to a request body.

`unique_key` **is not an idempotency key.** It dedupes against the *active* job
only, so once the original completes or dead-letters the key is released and the
same request enqueues a second job. A client's total retry deadline **MUST** be
shorter than the job's own life.

`EnqueueBatch` promises no atomicity, and it fails in two shapes a client
**MUST** handle separately. Where the batch could not partially apply, **the RPC
itself fails** and the error carries the failing item's `index` — returning the
earlier items as enqueued would report jobs that do not exist. Where it could,
the RPC succeeds and a client **MUST** read `EnqueueBatchResponse.results` per
item: an `enqueued` arm is `EnqueueResponse`, an `error` arm is a
`google.rpc.Status` meaning that item alone did not land. Under both shapes an
`enqueued` arm is durable.

`GetJob` does **not** return `Job.payload` by default: a payload is the largest
thing a job carries and most readers do not want it. A client that needs it
**MUST** set `GetJobRequest.include_payload`.

`ListJobs` pages newest-first through an opaque `page_token`. A client **MUST**
treat it as opaque and **MUST NOT** construct one: it is server-issued, and a
token that does not decode is `INVALID_ARGUMENT` with reason `INVALID_REQUEST`.

`SubmitWorkflow` accepts a static graph. A node setting `gate`, `cache`,
`fan_out`, `fan_in` or `sub_workflow` is refused `FAILED_PRECONDITION` with
reason `WORKFLOW_CONSTRUCT_UNSUPPORTED`, carrying `node` and `field`. The
message can carry all five; nothing outside a live SDK process can advance a run
that uses them.

### An executor client — `flexiq.executor.v1.ExecutorService`

| RPC | Level | Notes |
|---|---|---|
| `Attach` | **MUST** | One bidirectional stream for the lifetime of a connection. |
| `Heartbeat` | **SHOULD** | Unary, deliberately off the dispatch stream: a heartbeat sharing the stream it reports on cannot tell a busy stream from a dead peer. It reports `free_slots` and feeds an operator's view; the stream, not the heartbeat, is what liveness is. |

The handshake, in order. A client **MUST NOT** reorder it:

1. Send `HelloFrame` **first** — `executor_id`, `sdk`, `version`, the `tasks`
   it has handlers for, `slots`, `protocol_version` and the `capabilities` it
   implements. It carries no credential; the bearer token already authorised the
   call.
2. Wait for `HelloAckFrame`. No `job` frame will precede it, and a `Heartbeat`
   that overtakes it is read *as* the handshake and refuses the attach.
3. Read the session token from the response metadata, key
   `flexiq-attach-session-bin`. `Heartbeat` **MUST** carry that value back
   verbatim and **MUST NOT** carry `executor_id` instead — an id is a name the
   executor picked, and could therefore be another executor's.

One live stream per `executor_id`; a second attach under an attached id is
refused.

Frames a client **MUST** implement: `hello`, and exactly one settling frame per
dispatched job — `success`, `failure` or `cancelled`. `should_retry` on a
`failure` is the **executor's** decision; only it can see the exception, and the
scheduler never inspects one.

Every other frame is gated on a capability, and **a client MUST NOT send a frame
for a behaviour it did not advertise**:

| Capability | Unlocks | Degradation |
|---|---|---|
| `side_channel` | `progress`, `task_log` | Silent — the calls become no-ops. |
| `steps` | `step_commit`, and `job_steps`/`step_ack` inbound | **Fails rather than degrades.** A durable step that silently did not commit is a step that will re-run a charge. |
| `lease` | A `lease` on every dispatch, checked on every frame about one | Silent. |

Where `lease` is in force, a client **MUST** echo it on every frame that settles
or advances that attempt — `success`, `failure`, `cancelled`, `slept`,
`progress`, `task_log`, `step_commit` — and **MUST NOT** inspect one, construct
one, or reuse one across attempts. `hello` and `Heartbeat` carry none; they
belong to the connection, not to a job. **A frame that should carry a lease and
does not is dropped**, and what it was reporting is a job that ran without its
result being recorded.

An executor client:

- **MUST** treat an unrecognised `oneof` arm as skippable, in both directions.
  That is how a newer scheduler and an older executor stay attached.
- **MUST** raise its gRPC message-size limit to at least
  `EXECUTOR_MAX_MESSAGE_BYTES`, 68 MiB — the 64 MiB the worker frame protocol
  allows a payload, plus 4 MiB of envelope headroom. A client that leaves the
  4 MiB library default in place **attaches cleanly and fails on its first
  large job**.
- **MUST** treat a clean stream end as "reconnect" and a `shutdown` frame as
  "stop". Streams are bounded on purpose —
  `FLEXIQ_GRPC_EXECUTOR_STREAM_MAX_AGE`, 1800 seconds by default — because a
  gRPC stream cannot be load-balanced once started. The scheduler stops matching
  new work to a stream and drains it before ending one, so a rotation never
  costs a job in flight. **A rotation is not an error**, and a client that logs
  one as a failure will page someone every half hour.

### What no remote client may do

- **An executor cannot enqueue.** Not a runtime refusal — there is no
  enqueue-shaped RPC in `flexiq.executor.v1` at all, and `flexiq.v1` is a
  separate package behind a separate scope. A task that fans out to a second
  stage goes back through the producer door as an ordinary client, holding a
  second, `produce`-scoped credential of its own.
- **Nothing a client sends names a namespace, an owner, an attempt or a
  resource cap.** Anything a client could name is something a client could
  forge; the scheduler applies every one of those from the dispatch it recorded.
- **`flexiq.executor.v1` has no HTTP binding, ever.** The JSON facade serves
  `flexiq.v1` and nothing else, so this door needs a real gRPC library.
- **Neither door reaches the admin surface.** See
  [The delta from an embedded SDK](#the-delta-from-an-embedded-sdk).

### If there is no gRPC library either

The eight `flexiq.v1` RPCs are also served as ordinary HTTP with JSON bodies, on
the same listener and the same credential. `GET` is served for exactly the four
`NO_SIDE_EFFECTS` RPCs and `POST` for everything else, so the method is never a
judgement call.

Bodies and query strings are proto3 JSON: lowerCamelCase field names with the
`.proto`'s `snake_case` names also accepted, and **a name that is neither is
refused rather than ignored** — a silently dropped typo enqueues a job the
caller did not describe. Timestamps are RFC 3339, durations are seconds with an
`s` (`"30s"`, `"1.500s"`), `bytes` fields are base64, and **64-bit integers are
strings**, since a JSON number is a double.

The HTTP status is a pure function of the `google.rpc.Code`, with no per-case
exception. One consequence looks wrong and is not: a request body over the 4 MiB
cap answers **400** with `"status": "OUT_OF_RANGE"`, not 413. A mapping with one
exception in it is two mappings.

## Serialization

`Job.payload` and `Job.result` are `bytes`, and the `.proto` calls them opaque.
This is what is inside them. It is the same envelope the in-process SDKs write,
defined normatively in `crates/flexiq-core/BINDING_CONTRACT.md`; there is one
encoder for it in the tree, and the gRPC door calls that one.

**A payload is one tag byte, then the body that codec produced.**

| Tag | Body | Cross-SDK | Rule |
|---|---|---|---|
| `0x00` | Language-native (e.g. pickle) | **never** | A client reading this on a payload it did not produce **MUST** fail with an error naming the tag, not a generic decode error. |
| `0x01` | MessagePack | optional | Legacy. A client **MAY** read it and **SHOULD NOT** write it. |
| `0x02` | CBOR ([RFC 8949](https://www.rfc-editor.org/rfc/rfc8949.html)) | **default** | Write this. |
| `0x03` | reserved | — | Tagged JSON, unspecified. |
| `0x04`+ | reserved | — | Future codecs. A protobuf payload codec is explicitly reserved and not claimed. |

An **untagged** payload predates the envelope and is same-SDK legacy. A client
**MUST NOT** sniff for one: a raw CBOR or MessagePack body can begin with any
byte value.

**A call body** (`Job.payload`) is a two-element CBOR array, `[args, kwargs]` —
`args` an array, `kwargs` a map, an empty map when the language has no keyword
arguments. The array is always two elements. Prefer a single object argument
(`[[{…}], {}]`); it maps onto every language's handler-binding model.

**A result body** (`Job.result`) is a bare CBOR value, with no array wrapper.
This is the one that catches people: a payload is `02` then an array, a result is
`02` then whatever the task returned.

```text
f(1, "a"), no kwargs   02 82 82 01 61 61 a0      →  [[1, "a"], {}]
a result of true       02 f5
a result of 2^53       02 1b 00 20 00 00 00 00 00 00
```

Two encoder rules, and **neither is a matter of style**:

- **Definite-length containers only** — `a0` for an empty map, `80` for an empty
  array, never the indefinite forms `bf … ff` / `9f … ff`. Readers **MUST**
  accept both; writers **MUST NOT** emit them.
- **Shortest-form integers.**

Both forms decode identically, so a divergent writer still interoperates — which
is the danger. The `auto:` idempotency key is a hash over these bytes, so a
divergence silently stops idempotent enqueues deduping across clients, and every
call body ends in the kwargs map, so it would shift every payload's key at once.
A streaming CBOR writer left at its defaults is the usual way this happens.

### Enqueueing without a CBOR library

`EnqueueRequest.body` is a `oneof` and a client **MUST** set an arm; setting
none is `INVALID_ARGUMENT` with reason `INVALID_REQUEST`.

- `raw` — the bytes above, reaching storage untouched.
- `structured` — `repeated google.protobuf.Value args` and
  `map<string, google.protobuf.Value> kwargs`, which the server encodes into
  that same envelope through the same encoder.

`WorkflowNodeConfig.body` is the identical `oneof` on a workflow node.

Neither arm is second-class; the stored row is identical. But `structured` is
JSON-shaped, and **it refuses what JSON cannot carry rather than rounding it**:
integers past ±9007199254740991 (`2^53 − 1`), non-finite numbers, byte strings
and CBOR tags. Those are exactly the three cases `wire-vectors.json` marks
`decode_only`. It also normalises object key order, which moves the bytes
without moving the meaning — a client using `structured` **SHOULD** set
`unique_key` itself rather than rely on an `auto:` key matching another
runtime's.

## Errors

**Two different structured errors travel on this wire, and they are not the same
thing.** One says the request failed. The other says the job failed, and rides
inside a response that succeeded.

### A failed request

Every non-`OK` response carries a `google.rpc.Status` whose `details` include
exactly one `google.rpc.ErrorInfo`:

```text
domain   = "flexiq.byteveda.org"
reason   = one value from the closed list below
metadata = map<string, string>, per reason
```

**A client MUST branch on `reason`.** The `google.rpc.Code` is a category and
the message is written for humans — it may be reworded in any release. `reason`
may not.

The closed list, with the code each arrives under:

| Reason | Code | Means |
|---|---|---|
| `UNAUTHENTICATED` | `UNAUTHENTICATED` | No usable credential. One reason for every way of failing, and a message that names none of them. |
| `SCOPE_DENIED` | `PERMISSION_DENIED` | Genuine credential, wrong package. Carries `scope`. Never retryable. |
| `INVALID_REQUEST` | `INVALID_ARGUMENT` | The request is not a shape this service accepts. |
| `MALFORMED_PAYLOAD` | `INVALID_ARGUMENT` | Bytes the client sent could not be decoded. |
| `NO_SUCH_METHOD` | `UNIMPLEMENTED` | The path names no RPC. |
| `JOB_NOT_FOUND` | `NOT_FOUND` | No such job — or a job in another namespace, indistinguishable by design. |
| `DEPENDENCY_NOT_FOUND` | `FAILED_PRECONDITION` | A `depends_on` id names nothing this caller may depend on. |
| `QUEUE_FULL` | `RESOURCE_EXHAUSTED` | Carries `queue`, `pending`, `cap`. |
| `RATE_LIMITED` | `RESOURCE_EXHAUSTED` | A rate limit rejected the call. |
| `TASK_NOT_REGISTERED` | `FAILED_PRECONDITION` | No executor implements the named task. |
| `WORKFLOW_CONSTRUCT_UNSUPPORTED` | `FAILED_PRECONDITION` | Carries `node`, `field`. Resubmitting without that field succeeds. |
| `CONTRACT_TOO_OLD` | `FAILED_PRECONDITION` | Carries `speaks`, `required`. See [Compatibility](#compatibility) — it is never about the client. |
| `JOB_TIMEOUT` | `DEADLINE_EXCEEDED` | |
| `CLAIM_LOST` | `FAILED_PRECONDITION` | The execution claim moved to another owner. Never resend. |
| `STEP_DIVERGED` | `FAILED_PRECONDITION` | A durable step replayed differently from the run it resumes. |
| `STEP_LIMIT_EXCEEDED` | `INVALID_ARGUMENT` | Carries `limit`, `actual`, `allowed`. |
| `STEP_REFUSED` | `FAILED_PRECONDITION` | |
| `LOCK_HELD` | `ABORTED` | Read again and retry. |
| `SETTING_CONFLICT` | `ABORTED` | Read again and retry. |
| `STORAGE_UNAVAILABLE` | `UNAVAILABLE` | Retryable with backoff. |
| `STORAGE_CONSTRAINT` | `INTERNAL` | A write violated a database constraint. It will violate it again. |
| `SERVER_MISCONFIGURED` | `INTERNAL` | |
| `INTERNAL` | `INTERNAL` | A server-side fault with nothing useful to say. |
| `UNKNOWN` | `UNKNOWN` | Nothing above matched. |

Not every reason is reachable from every RPC, and a client **MUST NOT** treat
the code as sufficient: `INVALID_ARGUMENT` covers both a malformed request and a
step over its limit, and only `reason` separates them.

**Metadata values are `map<string, string>`, so every numeric one has a stated
encoding: base-10 ASCII, no grouping, no unit suffix, `-` for negative.** The
width and signedness are per key.

| Key | With | Type |
|---|---|---|
| `queue` | `QUEUE_FULL` | queue name, verbatim |
| `pending`, `cap` | `QUEUE_FULL` | `int64`, jobs |
| `scope` | `SCOPE_DENIED` | one of `produce`, `execute` |
| `speaks`, `required` | `CONTRACT_TOO_OLD` | `uint32`, contract level |
| `limit` | `STEP_LIMIT_EXCEEDED` | one of `step bytes`, `total bytes`, `step count` |
| `actual`, `allowed` | `STEP_LIMIT_EXCEEDED` | `uint64`, in `limit`'s unit |
| `node`, `field` | `WORKFLOW_CONSTRUCT_UNSUPPORTED` | node name; one of `gate`, `cache`, `fan_out`, `fan_in`, `sub_workflow` |
| `index` | any reason, from `EnqueueBatch` | `int32`, 0-based position in the request |

`index` is the one cross-cutting key: it accompanies whatever reason the failing
item raised, because a client that gets `QUEUE_FULL` on a batch needs both facts
at once.

**Every `RESOURCE_EXHAUSTED` additionally carries a `google.rpc.RetryInfo`** —
one second — so `QUEUE_FULL` and `RATE_LIMITED` both tell a client how long to
wait without it inventing a number.

Some errors are deliberately vague. A storage or pool failure has its underlying
message withheld and logged server-side instead; the client is told "the storage
backend is unavailable". `reason` and any metadata still carry the decision.

The JSON facade renders the same value as a body, so a client without a gRPC
library branches on the same string:

```json
{"error": {
  "code": 429,
  "status": "RESOURCE_EXHAUSTED",
  "message": "queue `payments` is full",
  "details": [{"@type": "type.googleapis.com/google.rpc.ErrorInfo",
               "reason": "QUEUE_FULL", "domain": "flexiq.byteveda.org",
               "metadata": {"queue": "payments", "pending": "1001", "cap": "1000"}}]
}}
```

### A failed job

A job that raised carries its error as a string in `Job.error`. **The RPC that
returned it succeeded**; a failed job is data, not a status.

That string is JSON, and its shape is fixed cross-SDK — key order included:

```json
{"errtype":"ValueError","message":"bad value 42","traceback":["...frame...","..."]}
```

| Field | Type | Presence |
|---|---|---|
| `errtype` | string | required — the exception class name, in the raising language's own vocabulary |
| `message` | string | required, may be `""` |
| `traceback` | array of strings | required key, may be `[]` |

### What a client does with what it cannot parse

Two rules, and they point in opposite directions on purpose.

- **A `Job.error` that does not parse MUST be surfaced verbatim.** A string that
  is not a JSON object carrying a string `message` is either a legacy error or
  one the core generated itself — a timeout, a worker-death recovery, an
  expiry, a cancellation are all plain text by design. A client **MUST NOT**
  raise on one; every FlexiQ SDK returns "not structured" and shows the raw
  string, and a client that throws instead loses the only account of why a job
  failed.
- **An `ErrorInfo.metadata` value that will not parse MUST be treated as
  absent.** It is a server bug, and the code and the reason already carry the
  decision. A client **MUST NOT** fail the whole response over one unreadable
  number.

## Authentication

### The credential

Every call carries `authorization: Bearer <token>`. The scheme is matched
case-insensitively. There is no anonymous path except
`/grpc.health.v1.Health/Check` and `/grpc.health.v1.Health/Watch`, which are the
only two public methods on the listener; server reflection and `/metrics` need a
valid credential, though no particular scope.

A token is `fqt_<16 hex characters>.<secret>`. The part before the `.` is a
public id; the server stores only `sha256(secret)`. A client **MUST** treat the
whole string as opaque and **MUST NOT** parse it.

- **Expiry is mandatory.** 90 days by default, 365 days at the outside. There is
  no unlimited-lifetime token, and **nothing on the wire warns a client that its
  own token is expiring** — the server logs it. A client that needs a warning
  **MUST** track the expiry it was given out of band.
- **Revocation takes effect on the very next call.** There is no cache and no
  restart.
- **There is no rotation mechanism and no self-service.** A token is minted by
  an operator, through the dashboard or the `flexiq-server token` CLI. **No gRPC
  credential can mint, widen or revoke a token, including its own** — a producer
  credential that could mint itself a wider one is not a scope.

Every way of failing — missing header, unknown id, wrong secret, wrong scheme,
revoked, expired, wrong namespace — collapses to a single indistinguishable
`UNAUTHENTICATED`. That is deliberate: telling a missing credential from a wrong
one is an oracle for whether a guessed token exists. A client **SHOULD** refresh
its credential and retry once, and **MUST NOT** try to infer which case it hit.

**`flexiq-server` terminates no TLS, on either door.** The bearer token is a
credential, not transport security. A deployment on an untrusted network **MUST**
put a proxy or a service mesh in front of the listener.

### Scopes

There are exactly two, and **they are not a hierarchy**:

| Scope | Opens |
|---|---|
| `produce` | `flexiq.v1` — every RPC in the package, and the JSON facade |
| `execute` | `flexiq.executor.v1` — every RPC in the package |

**A scope is "may call this package", not "may call this RPC".** A token with
`produce` cannot open an executor stream; a token with `execute` cannot enqueue.
The refusal is `PERMISSION_DENIED` with reason `SCOPE_DENIED`, carrying the
scope that was lacking. A credential is granted both only when it genuinely does
both.

The consequence for a client author: a new RPC in a package a client already
calls needs no new grant, and no RPC will ever be individually grantable.

### The namespace

**There is no namespace field on the wire, on either package.** A client cannot
name one, and this is not an omission to be fixed — anything a client could name
is something a client could forge.

- The namespace is a property of the credential, fixed at mint time.
- One `flexiq-server` process serves exactly one namespace, and refuses to mint
  a token for any other.
- `Job.namespace` exists, and is **output-only**: it is told to a client for
  logging, never accepted from one.
- Presenting a token minted for another namespace answers `UNAUTHENTICATED`, not
  `PERMISSION_DENIED`. The other answer would be an existence oracle.
- A read for a job in another namespace is `JOB_NOT_FOUND`, indistinguishable
  from a job that never existed.

The failure this produces is quiet, so it is worth naming: **if enqueues succeed
and nothing ever runs them, check the namespace first.** A job written with no
namespace at all is invisible over this door.

## Compatibility

Three numbers collide in this vocabulary. A client author meets all three and
they govern different things.

### The package version — `v1`

In the package name, and permanent. Both packages are **stable as of 2.0.0**,
the release that first shipped the gRPC role. Five rules, enforced by
`buf breaking` at `WIRE_JSON` on every pull request and self-tested by the
fixtures in `contracts/proto-guard`:

1. Field numbers are never reused and never renumbered.
2. Fields are deprecated in place. On removal, **both the number and the name**
   are reserved.
3. New RPCs, messages, fields and enum values are additive, and never bump the
   package version.
4. A field's meaning and units are frozen with its number.
5. Readers tolerate what they do not know.

Rule 2 is the one worth reading twice. The module is configured
`breaking.use: WIRE_JSON`, not `WIRE`, and the difference is the field *name*:
binary protobuf encodes numbers, so a rename is invisible to it and `WIRE` would
allow one — but the JSON facade publishes those names to clients that have no
`.proto` at all, and to them a rename is a silent outage.

**A client generated against `v1` keeps working across server upgrades.** If a
change ever arrives that none of this can absorb, it is `flexiq.v2`, a new
package served alongside `v1` — never an edit to these files.

### The contract level — which a remote client never sees

`CONTRACT_VERSION` and `MIN_CONTRACT_VERSION` are both `2`. They govern **whether
two builds may share one database**, dialled by the `contract:min_sdk` setting.

| | The package version | The contract level |
|---|---|---|
| **Governs** | the wire shape a client generates against | whether two builds may share one database |
| **Moves when** | never, for an additive change | an older build can no longer read what a newer one writes |
| **A remote client sees it** | yes — it is in the package name | **no. There is no contract-level field in either package.** |

This is the answer to "what does the floor mean to a client that never touches
the database", and it is: **nothing, and that is by construction.**

- There is **no handshake, interceptor or metadata key** on either package that
  carries a client-declared contract version. A client cannot declare one and
  the server does not ask.
- The check, `ensure_contract_supported`, runs **once per process, at storage
  open**, in whatever process holds the database credential — for this door,
  that is `flexiq-server` itself, before it begins serving.
- A client therefore **cannot violate a raised floor, and cannot observe one**.
  An operator who raises `contract:min_sdk` past what a server build speaks gets
  a server that will not start, not clients that are refused.
- `CONTRACT_TOO_OLD` is on the closed reason list because that list is closed,
  not because a client can cause it. Its `speaks` is the **server's** level and
  `required` is its storage's. A client that receives one has learned something
  about the deployment, never about itself, and **MUST NOT** treat it as
  something to fix in its own build.
- A client can neither trigger a migration nor read or set the floor. There is
  no RPC for either.

### `protocol_version` — the third number, and the only one a peer declares

On the executor door only. `HelloFrame.protocol_version` and
`HelloAckFrame.protocol_version` are the **worker frame format** version,
currently `1`. Both sides announce it and **both reject a mismatch** —
`FAILED_PRECONDITION`, never a silent downgrade — and the ack is sent even when
the scheduler is refusing, so both ends can log both numbers.

It has nothing to do with `CONTRACT_VERSION`, and nothing to do with the package
version.

### Capabilities are not a version

Optional behaviour is negotiated, never versioned. Adding a capability does not
bump `protocol_version`, which is the entire point: a scheduler and its
executors must not have to upgrade together. `hello.capabilities` is what a
client implements; `hello_ack.capabilities` is what the scheduler will do on its
behalf. **Send no frame for a behaviour that was not advertised.**

## The delta from an embedded SDK

Everything an in-process SDK can do that a remote client cannot. **None of it is
a gap waiting to be closed** — each line is a recorded decision, stated here so
nobody has to discover it.

Absent from both packages entirely, with no RPC partially implementing any of
them:

| Absent | Because |
|---|---|
| Every admin operation — pausing queues, settings, dead-letter retry and purge, webhook secrets, circuit-breaker internals | An operator surface and a producer surface must not share a credential. They stay behind the dashboard's session and role check. |
| Settings, including the compare-and-set write | Same credential boundary. |
| Migrations, and the contract floor | A storage concern between processes that hold the database credential. |
| Scheduler and retention election | Internal to `flexiq-server`. |
| Topic pub/sub and log streaming | Publishing is producer-shaped, but a subscriber needs lease, ack, nack and cursor operations — a whole second lifecycle. Shipping publish alone would advertise a door that does not open. |
| Worker-registry CRUD | An executor gets a registry entry from its `hello`; there is no register/heartbeat/reap surface. |
| Middleware | Middleware is code in the caller's process. There is no process here. |
| Direct storage access | The point of the door. |
| Durable steps as a callable operation | `step.run` and `step.sleep` exist on this wire **only** as frames inside an already-attached executor stream. There is no unary step RPC, and a producer client cannot reach one. |
| Task registration | The server holds no task registry. **A task name is a string, and enqueuing a name nobody implements succeeds** — the job dead-letters later. |
| Completion notification | No watch, no server stream in `v1`. Poll `GetJob`, or subscribe a webhook. |

Four more hold for an in-process SDK too, but a network client meets them sooner
because it is the one writing the retry loop:

- **No ordering.** `priority` and `scheduled_at` influence dispatch. Nothing
  promises that two jobs enqueued in order run in order.
- **Execution is at-least-once.** `unique_key` dedupes an *enqueue*, never a
  run.
- **A batch is not atomic.**
- **No job id is permanent.** Retention archives and then deletes, so a
  `NOT_FOUND` does not mean the job never existed.

And two that are differences rather than losses:

- **No CPU-parallelism story.** The prefork pool assumes an OS process beside
  the jobs it runs. A remote executor is a network hop from the scheduler with
  the capacity of whatever container it is in. The door buys reach, not cores.
- **`structured` refuses rather than rounds**, and normalises key order. See
  [Enqueueing without a CBOR library](#enqueueing-without-a-cbor-library).

**None of this deprecates embedded mode.** One process, no daemon, straight to
SQLite remains the default and remains why FlexiQ is not a broker-backed queue.
This door is additive, for the cases where nothing in-process can reach the
database credential at all.

What is *not* on the list: retries, timeouts, rate limits, circuit breakers,
priorities, unique keys, debouncing, workflows and durable steps all behave
exactly as they do for an in-process caller, because they are properties of the
job and the scheduler rather than of the door it arrived through.

## Where each fact is defined

This document is normative for the wire. Where a rule originates elsewhere, that
is the file to correct if the two ever disagree:

| Fact | Defined in |
|---|---|
| RPC and message shapes, field numbers, idempotency levels | `contracts/proto/flexiq/**` |
| The payload envelope, durable-step semantics, capability meaning, job status | `crates/flexiq-core/BINDING_CONTRACT.md` |
| Conformance vectors | `contracts/wire-vectors.json` |
| The compiled descriptor reflection serves | `contracts/descriptor.binpb`, at the `buf` version in `contracts/BUF_VERSION` |
| Why any of it is shaped this way | `tasks/specs/2026-09-01-flexiq-v1-proto-design.md` |

Narrative guides for each door — worked examples, `grpcurl` invocations,
operational configuration — are at <https://docs.byteveda.org/flexiq/server>.
They describe this contract; they do not extend it.
