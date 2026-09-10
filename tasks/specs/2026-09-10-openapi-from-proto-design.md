# An OpenAPI document generated from the wire contract

Issue #831. Milestone **Next** — the remote SDK tier, beside #828's normative
contract and #829's Go client.

## The problem

`crates/flexiq-server/src/grpc/facade` already serves the six — now eight —
producer RPCs as JSON over plain HTTP/1.1, and `crates/flexiq-server/README.md`
points at the docs page that shows `curl` doing it. That is a REST API in
everything but description. Without a machine-readable description of it, none
of the generator ecosystem can reach the door: no typed client in a language
this repo does not ship an SDK for, no Postman import, no gateway config, no
reference renderer.

## What makes this hard, and the decision that answers it

The paths are **not** in the protos. `flexiq/v1/producer_service.proto`
declares eight RPCs; the seven-then-nine HTTP bindings that reach them live in
a Rust `const` table, `facade::routes::Binding`. Any generator run over the
protos as they stand today emits `POST /flexiq.v1.ProducerService/Enqueue` for
every method — a document that describes a door nobody serves. The issue calls
this out: *"the JSON facade's actual paths and body shapes are what the spec
must describe… If those diverge, the spec is fiction."*

**D1. The mapping moves into the proto, as `google.api.http`.** Each RPC gains
the standard annotation carrying the path and verb the facade already answers.
The proto becomes the single source of truth; the generator reads the
annotation rather than guessing; and `routes.rs` gains a drift test that fails
when the Rust table and the annotation disagree. Nothing about the spec can
become fiction without a red test.

The alternatives were a bindings file beside the protos (google.api.http
reinvented, understood by nothing outside this repo) and generating out of
`flexiq-server` itself (truest source, but the gate then costs a full
tonic + diesel + bundled-sqlite build and cannot live in a pre-commit hook).

**D2. Additive, not breaking.** Options are not part of `WIRE_JSON`. Adding
`import "google/api/annotations.proto"` costs nothing at the module level
either: `buf.build/googleapis/googleapis` is already a declared dep, pinned in
`buf.lock` for `google/rpc/status.proto`.

**D3. The generator is a workspace member, not a buf plugin.** A remote buf
plugin needs the network at generate time and pins a second tool; a local one
needs a Go toolchain on the pre-commit path. `crates/flexiq-openapi` is
`publish = false` and depends only on `prost`, `prost-types` and `serde_json`,
all already in `Cargo.lock`. This is the same reasoning that rejected `pbjson`
for the facade in #718: one hand-written mapping we control beats a generator
whose assumptions we do not.

**D4. OpenAPI 3.1.0.** JSON Schema 2020-12 alignment is what makes proto3 JSON
expressible — `oneOf` for a oneof, type arrays for absent-vs-null, no
`nullable: true` dialect.

## The bindings

Annotations, one per RPC, mirroring `Binding` exactly:

| RPC | annotation | body |
|---|---|---|
| `Enqueue` | `post: "/v1/jobs"` | `*` |
| `EnqueueBatch` | `post: "/v1/jobs:batchEnqueue"` | `*` |
| `GetJob` | `get: "/v1/jobs/{job_id}"` | — |
| `ListJobs` | `get: "/v1/jobs"` | — |
| `CancelJob` | `post: "/v1/jobs/{job_id}:cancel"` | — |
| `QueueStats` | `get: "/v1/queues/{queue}/stats"` + `additional_bindings { get: "/v1/stats" }` | — |
| `SubmitWorkflow` | `post: "/v1/workflows"` | `*` |
| `GetWorkflowRun` | `get: "/v1/workflows/{run_id}"` | — |

`{job_id}:cancel` is a path-template *verb* (AIP-136) and legal in the
annotation, so the annotation carries the form a client types — which is what
`Binding::path()` already returns. `Binding::pattern()`, the matchit-shaped
form without the suffix, stays a private detail of the router.

`CancelJob` declares no `body`: the handler reads none, and a `body: "*"` would
describe a request field that is discarded.

**D5. `GET /v1/stats` gains its query parameter.** Under `google.api.http`
every request field not bound to the path is a query parameter, so the second
`QueueStats` binding admits `?queue=`. `namespace_stats` parsed no query at
all, which would have made the document advertise a filter the server silently
ignored — a client asking for one queue would have been handed namespace-wide
counts and no error. The handler reads the query instead, making
`GET /v1/stats?queue=x` exactly `GET /v1/queues/x/stats`. The parameter is
refused-on-unknown like every other facade query, so this tightens a surface
that previously ignored anything.

## Schemas

Component schemas are derived from the descriptor by the proto3 JSON mapping —
the same rules `facade/json` implements by hand on both sides:

| proto | JSON Schema |
|---|---|
| `int64` `uint64` `sint64` `fixed64` `sfixed64` | `string`, with the integer pattern |
| `int32` `uint32` and friends | `integer` |
| `float` `double` | `number` |
| `bool` | `boolean` |
| `bytes` | `string`, `contentEncoding: base64` |
| enum | `string`, `enum` of the declared value names |
| message | `$ref` |
| `repeated T` | `array` of `T` |
| `map<K, V>` | `object`, `additionalProperties: V` |
| `oneof` | `oneOf` over one-property objects |
| `google.protobuf.Timestamp` | `string`, `format: date-time` |
| `google.protobuf.Duration` | `string`, the `1.5s` form |
| `google.protobuf.Value` | unconstrained |
| `google.protobuf.Struct` | `object` |
| `google.rpc.Status` | the facade's rendering, not the protobuf one |

Property names are the descriptor's `json_name`, which is the same field
`facade/json/response.rs`'s existing drift test asserts the writer emits. Field
comments become `description`, so the document carries the contract's own prose.

Error responses are the facade's shape, `{"error": {code, status, message,
details}}`, as `components.schemas.Error`; every operation references it under
`default` rather than enumerating a status list that could not be exhaustive.
Authentication is one `bearerAuth` HTTP security scheme, applied globally,
because every `/v1/` path is behind `Scope::Produce`.

## Determinism

The gate is a byte comparison, so the generator may not vary for a fixed
descriptor: `BTreeMap` for every object, declaration order for arrays,
`serde_json` pretty printing at two spaces, one trailing newline. The
descriptor's own byte stability is already the `BUF_VERSION` pin's job.

## Gates

1. **`scripts/proto-check.sh`** regenerates the document to a scratch path and
   compares, exactly as it does the descriptor; `--fix` rewrites both. It
   refuses on a missing cargo the way it refuses on a missing buf.
2. **`routes.rs`** asserts every `Binding` resolves to exactly one annotation
   with the same verb and the same path, and every annotation to exactly one
   `Binding`. `flexiq-openapi` is a dev-dependency of `flexiq-server` so the
   extension decoder exists once.
3. **`ci.yml`** path filters gain `contracts/openapi.json` wherever
   `contracts/descriptor.binpb` appears.

`prost` discards extension fields on decode, so `prost_types::MethodOptions`
will not hand back `google.api.http` (tag 72295728). The generator carries a
minimal mirror of the descriptor types down to `HttpRule` for that one read;
everything else goes through `prost-types` unchanged.

## Out of scope

Serving the document from the gRPC listener, and a test that sweeps every
operation against a live server. The annotation-to-`Binding` drift test is what
holds the document to the router; the live paths were checked by hand once,
against a running `flexiq-server`, before this landed.
