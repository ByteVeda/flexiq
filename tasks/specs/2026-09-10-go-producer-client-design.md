# A Go client over the producer door — design

Issue [#829](https://github.com/ByteVeda/flexiq/issues/829). Milestone: Next.

This is a decision record for `sdks/go`, not a contract. The contract is
[`contracts/REMOTE_SDK_CONTRACT.md`](../../contracts/REMOTE_SDK_CONTRACT.md); where the two
disagree, that one is right and this one is out of date.

## Why this exists

The remote tier needs one implementation that is not written by the same person who wrote the
server, or the contract is untested prose. Go goes first: the largest audience with no current
option, no FFI story competing with it, and a protobuf toolchain that makes the client mostly
generated code plus ergonomics.

It is scoped as a **client, not a port**. It cannot execute tasks — that is
`flexiq.executor.v1`, a separate package behind a separate scope. Shipping the producer half and
saying so is the whole plan.

## Shape

One module at `sdks/go`, module path `github.com/ByteVeda/flexiq/sdks/go/v2`, package `flexiq`.
It links nothing from `crates/` and needs no Rust toolchain to build or test.

| File | Holds |
| --- | --- |
| `client.go` | `Client`, `New`, `Close` |
| `options.go` | dial options and their defaults |
| `auth.go` | the bearer credential |
| `enqueue.go` | `Enqueue`, `EnqueueBatch`, the request and options structs |
| `read.go` | `GetJob`, `ListJobs`, `AllJobs`, `QueueStats` |
| `cancel.go` | `CancelJob` |
| `job.go` | the `Job` read model, `JobStatus`, proto conversion |
| `wire.go` | the payload envelope: tag byte plus CBOR `[args, kwargs]` |
| `errors.go` | `Error`, the closed `Reason` list, status conversion |
| `taskerror.go` | a failed job's recorded error |
| `internal/pb` | generated stubs, committed |
| `tests/` | the whole suite, in its own package |

A new RPC is a new file plus a conversion; nothing above has to move to make room for one.

The tests sit beside the package rather than inside it, so every one of them reaches the client
through its exported API — a surface that is awkward to use is awkward to test, and nothing can
quietly lean on an unexported helper. It cost one export: `ErrorDomain`, which a caller writing
its own interceptor needs anyway.

## Decisions

### The module path carries `/v2`

Not a second module. Go requires a module released above v1 to carry the major in its path, and
the client ships at the repo's own version. The tag is `sdks/go/v2.0.0` — a subdirectory module is
only resolvable at `<subdir>/vX.Y.Z`. Both halves of that are Go's rules rather than ours, and
moving to 3.0.0 means editing `go.mod` as well as cutting a tag.

There is no publish workflow, because there is no registry to push to. This is the sixth tag
namespace and the first with no job behind it.

### Generated stubs are committed, and `internal`

`go get` runs no code generator, so a consumer has to find the stubs already there. Every other
language in this repo generates at build time; Go cannot.

Committing them makes a second copy of the contract, so CI regenerates and fails on a diff. Under
`internal/` they stay out of the public API: the exported surface is hand-written Go, so a
regenerated stub can never be a breaking change for a caller.

Only `flexiq.v1` is generated. Generating the executor package too would ship a client surface
nothing here implements.

### Options are functional for the client, structs for the job

`New` takes `Option` funcs; `EnqueueRequest` and `EnqueueOptions` are plain structs whose zero
values mean "the server's default".

The split is not taste. The job knobs mirror `EnqueueOptions` in the proto one for one, they are
data rather than behaviour, and `EnqueueBatch` needs a value per item — a functional option cannot
be a list element without a wrapper. Extending them is adding a field.

### No automatic retry

`UNAVAILABLE`, `DEADLINE_EXCEEDED` and `CANCELLED` on an `Enqueue` may each mean the write landed
and the connection dropped afterwards, and no field on the wire distinguishes them. A library that
retried on the caller's behalf would double-enqueue by default.

`Error.RetryAfter` carries what the server asked for, and `Error.Retryable` reports whether the
condition clears on its own. Writing the loop, and setting a unique key inside it, stays the
caller's.

### TLS by default, plaintext opt-in

`flexiq-server` terminates no TLS, so the deployment puts a proxy in front of it. The token is a
bearer credential: anything that observes one can replay it. The client verifies the peer unless
`WithInsecureTransport` is passed, and grpc-go's own refusal to attach per-RPC credentials to an
unencrypted connection is what enforces it.

### Both message-size directions are capped

grpc-go caps receiving at 4 MiB and leaves sending unbounded. Left alone, an oversized payload
would cross the network to be refused at the far end. Both directions are set to
`PRODUCER_MAX_MESSAGE_BYTES`, so the send fails locally.

### Sorting stays off in the CBOR encoder

A CBOR map is unordered but its bytes are not, and the contract pins an object argument's keys in
the order the caller wrote them — `single-object-arg` is `order_id` before `amount_cents`, which
is not sorted. A sorting encoder would fail that vector.

The cost is that a Go `map` argument encodes to different bytes on different runs, because Go map
iteration order is unspecified. A struct encodes in declaration order, and that is what the README
and the doc comment tell a caller to pass where the bytes matter.

## Testing

Two layers, no server and no Rust build, both in `tests/`:

1. **The conformance vectors.** `wire_test.go` reads `contracts/wire-vectors.json` out of the tree:
   all nine `encode` cases produced byte-exact, all twelve decoded, both `round_trip_only` cases
   re-encoded to the same bytes. Object arguments are declared as Go structs, and a new object
   vector with no struct behind it fails loudly rather than encoding in map order.
2. **A `ProducerService` double** on a bufconn connection, covering this client's half of the
   contract: the bearer metadata, reason-based error branching, the batch's two failure shapes, an
   unrecognised outcome arm, opaque page-token paging, `include_payload` off by default, an
   unknown `JobStatus` not being terminal, and the local send cap.

What is deliberately not covered here is the server's half: it has its own suite, and duplicating
it against a double would only prove the double agrees with itself.

## Filed, not built

- **#907** — `SubmitWorkflow` and `GetWorkflowRun`. Static graphs only, and they carry the whole
  300-line `workflow.proto` surface; the issue did not ask for them.
- **#908** — an executor client. A separate package, a separate scope, a larger design.
- **#909** — a live end-to-end test against a real `flexiq-server`. The double proves the client's
  half; only a real server proves the pair.
