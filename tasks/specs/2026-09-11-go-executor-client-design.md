# A Go executor client over the dispatch door — design

Issue: [#908](https://github.com/ByteVeda/flexiq/issues/908).
Prerequisite reading: `contracts/REMOTE_SDK_CONTRACT.md`, the executor section;
`contracts/proto/flexiq/executor/v1/executor_service.proto`;
`docs/content/docs/server/custom-executors.mdx`.

Producer half shipped in #829. This is the other half.

## Why this exists

`sdks/go` can submit work and read it back. It cannot run any. A Go service that
wants to *execute* has three options today: rewrite itself in a language with an
SDK shell, poll the producer door (which has no claim operation, so it cannot),
or write the executor door by hand. The third is the one this removes.

The issue that opened #908 said why it is separate from the producer client, and
it is not size: `flexiq.executor.v1` is a second package behind a second scope,
reached by a bidirectional stream with a handshake, a session token, a
capability negotiation and a settling frame per dispatched job. None of that is
an extension of a unary client.

## Scope

Ships: `hello`, the three settling frames, `cancel`, `shutdown`, rotation and
reconnect, heartbeat, and the `lease` and `side_channel` capabilities.

Does not ship: `steps`. A Go port of the durable-step state machine is a local
`StepSequence` — snapshot decode, `step_key` derivation, gapless `seq` counting,
divergence detection, `(job_id, seq)` ack correlation and the two-phase sleep —
and every other SDK got durable steps as its own issue after its executor
landed. Filed as a follow-up. The client therefore **does not advertise
`steps`**, which is the contract's own degradation rule: a capability not
advertised gets no frames, and `steps` is the one that fails rather than
degrades.

Also does not ship: enqueueing. There is no enqueue-shaped RPC in the package,
and the import graph is what says so — see below.

## Shape

A new package, `sdks/go/executor`, import path
`github.com/ByteVeda/flexiq/sdks/go/v2/executor`. It imports `flexiq` for the
payload codec and `TaskError`; `flexiq` does not import it.

```go
w, err := executor.New("queue.internal:50051",
    executor.WithToken(os.Getenv("FLEXIQ_EXECUTE_TOKEN")),
    executor.WithID("go-worker-1"),
    executor.WithSlots(8),
)
if err != nil {
    return err
}
defer w.Close()

w.Handle("billing.charge", func(ctx context.Context, job *executor.Job) (any, error) {
    var charge Charge
    if err := job.Bind(&charge); err != nil {
        return nil, executor.Fatal(err)
    }
    job.Progress(50)
    return Receipt{ID: charge.OrderID}, nil
})

err = w.Run(ctx) // blocks: attach, dispatch, rotate, reconnect
```

| File | Concern |
|---|---|
| `doc.go` | Package documentation |
| `worker.go` | `Worker`, `New`, `Handle`, `Run`, `Close` |
| `options.go` | Functional options and the resolved config |
| `attach.go` | One stream session: handshake, reader, writer, teardown |
| `backoff.go` | The reconnect schedule |
| `job.go` | `Job`, `Call`/`Bind`, `Progress`/`Log`/`Publish` |
| `result.go` | Handler outcome to a settling frame; the task-error JSON |
| `heartbeat.go` | Session token and the unary loop |
| `slots.go` | Slot accounting |
| `errors.go` | `Fatal`, `ErrCancelled`, the attach errors |

## Decisions

### A subpackage, not package `flexiq`

`flexiq.executor.v1` is behind a second scope, and a token that carries
`execute` cannot enqueue. Putting the executor in the same Go package would make
that a sentence in a doc comment. A subpackage makes it an import: nothing in
`executor` reaches `ProducerServiceClient`, and a reviewer can check that with
`go list -deps` rather than by reading.

It also keeps two enum-name spaces apart. #907 already hit `goconst` counting
string literals package-wide and reporting a third `String()` method's
`"UNSPECIFIED"` against an unrelated file; the executor adds a fourth.

A task that fans out holds a second, `produce`-scoped credential and a second
client. That composes — two packages, two tokens — which is what the contract
describes.

### Explicit handlers, not reflected signatures

`func(ctx context.Context, job *Job) (any, error)`. The handler is handed the
job and asks it for what it wants: `job.Call()` for the raw `[]any` and
`map[string]any`, `job.Bind(&v)` to decode the first positional argument into a
struct.

A reflected `Handle("billing.charge", func(ctx, c Charge) error)` reads better at
the call site and costs a registration-time signature validator plus a decode
error that names a reflected type rather than a field. The cross-SDK convention
is a single object argument (`args = [{…}]`, `kwargs = {}`), so `Bind` covers
the common call with no reflection at all.

**Keyword arguments are refused, not dropped.** Go has nothing to bind one to,
which is the same position the Rust SDK is in, and it made the same call:
`decode.rs`'s `reject_kwargs` fails a call carrying any, because passing them
would drop them silently. `Bind` does the same. `Call()` still returns them, for
a handler that wants to read them itself.

### Local timeout enforcement, which core does not do

`JobFrame.timeout` is enforced by the client: the handler's context carries the
deadline, and a job that overruns settles as
`failure{timed_out: true, should_retry: true}` with the error text
`job timed out after {ms}ms`.

This diverges from the reference executor deliberately. Nothing in
`crates/flexiq-core/src/worker/executor.rs` enforces a job timeout — the
scheduler's stale-job reap synthesizes the failure from `started_at +
timeout_ms` and no frame crosses the wire, so an attached executor is never told
its job timed out and keeps running. In Go a deadline is a `context.WithTimeout`
and the job actually stops. The error text is copied from
`scheduler/maintenance.rs` rather than invented, so a timeout reported by the
executor and one synthesized by the reaper read identically.

The reaper is still the backstop for a handler that ignores its context.

### The lease is stamped structurally

Every frame that settles or advances an attempt carries the lease the dispatch
arrived with. The contract names the failure mode and the mitigation in the same
row: a missed echo loses a job's result silently, so echo it where the frame is
built, not at each call site.

One `session.send(jobID, frame)` looks the lease up by job id and stamps it.
There is no path that builds a settling frame without going through it, and the
lease is forgotten when the job settles. It is never inspected, never
constructed, never reused across attempts.

**The acknowledgement does not gate the echo, and must not.** This was found by
the end-to-end suite, not by design, and it is the sharpest edge on this door.

The scheduler decides whether to *check* an executor's frames for a lease from
what `hello` advertised — `Executor.leases` is `capabilities.contains(CAP_LEASE)`
off the incoming frame. But it only advertises `lease` back in `hello_ack` once
`lease_book()` is `Some`, and the book is installed when its scheduler role
starts. Those can happen in either order: an executor that attaches in between
gets an acknowledgement with no `lease` in it and dispatches that carry one.

`frame_is_current` then reads a frame with no lease as `!executor.leases`, which
is `false` — stale. So an executor that followed the acknowledgement literally
would have **every frame about every job dropped**, and the server's own log
would blame a re-dispatch that never happened.

The rule is therefore the dispatch's, not the handshake's: **a job frame that
carried a lease gets it echoed.** Echoing one the scheduler does not check costs
a field it ignores. Withholding one it does check costs the job's result.

### A rotation reconnects immediately; a shutdown does not reconnect

Streams are bounded at `FLEXIQ_GRPC_EXECUTOR_STREAM_MAX_AGE`, 1800s, because a
gRPC stream cannot be load-balanced once started. The scheduler stops matching
work to the stream and drains it before ending one, so the end is clean and
costs no job.

| Cause | Action |
|---|---|
| Clean EOF | Drain, reconnect immediately, log at info |
| `shutdown` frame | Drain in-flight, `Run` returns nil |
| `ALREADY_EXISTS` | Permanent. Another stream holds this id |
| `FAILED_PRECONDITION` | Permanent. Protocol version, or a frame before `hello` |
| Transport error | Reconnect, backoff 250ms→30s jittered, reset on handshake |
| Context cancelled | Drain up to `shutdownDrain`, close, return `ctx.Err()` |

A clean end is not an error and does not consume backoff. A client that treats
one as a failure pages someone every half hour, which is the whole reason the
contract says so twice.

### `hello` first, and the session token arrives before it

The session token is response metadata under `flexiq-attach-session-bin`, and
`tonic` sends it as initial headers when the RPC is entered — before the server
has read `hello`. So `stream.Header()` returns it immediately, and it is
available even for a handshake that is about to be refused. `Heartbeat` carries
it back verbatim and never `executor_id`, which is a name the executor picked
and could be another executor's.

`hello` is still the first frame the client sends. A heartbeat that overtakes it
is read *as* the handshake and refuses the attach.

On a `protocol_version` mismatch the server sends `hello_ack` first and *then*
fails the RPC, so both ends can log both numbers. The client reads the ack,
logs both, and does not reconnect.

### Both message-size directions are raised to 68 MiB

`EXECUTOR_MAX_MESSAGE_BYTES` is `MAX_PAYLOAD_BYTES` (64 MiB) plus 4 MiB of
envelope headroom, compile-time asserted at 68 MiB in
`crates/flexiq-server/src/grpc/limits.rs`, and applied to encoding and decoding
alike. grpc-go defaults to 4 MiB receiving and leaves sending unbounded. A
client that leaves either alone attaches cleanly and fails on its first large
job.

This is why the executor package does not reuse the producer client's
`MaxMessageBytes`, which is 4 MiB on purpose: the two doors have different caps.

### Progress is clamped by the client

The proto says `0-100. Anything else is dropped rather than stored.` The
scheduler does not check it — `apply_progress` passes any `i32` straight to the
sink. The client clamps, because the frame is the client's to get right and a
value the server will store out of range is a dashboard that reads wrong.

### One writer goroutine

`grpc.ClientStream.SendMsg` is not safe for concurrent use. One goroutine owns
the stream and is fed by a channel; a job goroutine never touches it.

Progress coalesces to the latest value per job and logs are bounded drop-oldest,
mirroring core. Neither can block a handler on the socket, which is the point: a
task that only wanted to report progress must never wait on the scheduler to do
it.

### Slot accounting refuses rather than drops

A `slots`-sized semaphore. The scheduler reserves a slot before writing a job
frame and a heartbeat can only shrink its view of free capacity, so it is
designed never to oversend — but a job already in flight when a zero-capacity
heartbeat lands is normal, not a fault. A job arriving with no free slot settles
as `failure{should_retry: true}` naming the reason, exactly as the reference
executor's `decline` does. It is never silently dropped.

## Results and failures

| Handler returns | Frame |
|---|---|
| `(nil, nil)` | `success` with `result` **absent** — the task returned nothing |
| `(v, nil)` | `success` with `result` = tag byte + bare CBOR of `v` |
| `(_, err)` | `failure`, `should_retry: true` |
| `(_, Fatal(err))` | `failure`, `should_retry: false` |
| `(_, ctx.Err())` after a `cancel` frame | `cancelled` |
| deadline exceeded | `failure`, `timed_out: true`, `should_retry: true` |
| panic | `failure`, recovered, stack as `traceback` |

Absent and present-and-empty are different answers and the client keeps them
apart: `result` is `nil` for the first and a non-nil slice for the second.

`should_retry` is the executor's decision. The scheduler never inspects an
error, so `Fatal` is the only way a Go task dead-letters itself.

The error string is the canonical cross-SDK JSON,
`{"errtype","message","traceback"}` in that key order with no extra whitespace.
`errtype` is the Go error's concrete type name; `traceback` is `[]` unless the
failure was a panic, where it is the recovered stack.

## Two additive changes in package `flexiq`

`wire.go` has `EncodeCall`, `DecodeCall` and `DecodeResult` but no
`EncodeResult` — the producer client never had to write a result. The executor
does, for every `success` frame.

`taskerror.go` has `ParseTaskError` but no encoder. The executor writes the
canonical JSON for every `failure` frame.

Both are additive and both belong in `flexiq` rather than duplicated: they are
the same wire, read from one end and written from the other.

## Testing

`sdks/go/tests`, the separate package the producer client's suite already uses,
so every test reaches the client through its exported API. A fake
`ExecutorService` on `bufconn` — no server, no Rust — drives:

- `hello` first, and a session token read from initial headers
- an ack that never comes, against the handshake budget
- a `protocol_version` mismatch: ack read, both numbers logged, no reconnect
- `ALREADY_EXISTS` and `FAILED_PRECONDITION` as permanent
- a clean stream end reconnecting, and a `shutdown` frame not
- the lease echoed on every settling frame, and absent from `hello`
- an unknown `oneof` arm skipped, the stream staying aligned
- progress clamped, logs bounded
- a job with no free slot refused retryably
- `cancel` reaching a handler's context, and the `cancelled` frame after it
- a deadline, a panic, and `Fatal`
- the 68 MiB limit set in both directions

E2E behind `//go:build integration`, reusing the existing harness that owns a
real `flexiq-server`: attach a worker, enqueue through the producer client, run
the job, read the result back through the producer client.

## Filed, not built

- **Durable steps in the Go executor.** The `steps` capability, its snapshot,
  its commit/ack round trip and the two-phase sleep.
- **`crates/flexiq/src/outcome.rs` writes `"traceback": null`.**
  `BINDING_CONTRACT.md` says the key is required and is an array of strings,
  `[]` when the runtime cannot provide frames. The Rust SDK is the one shell
  that writes `null`.
- **`sdks/go/taskerror.go` rejects a null traceback.** It requires all three
  keys non-nil, so a Rust-SDK failure reads as unstructured and loses its
  `errtype`. The contract's fallback rule turns only on `message`.
