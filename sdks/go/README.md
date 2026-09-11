# FlexiQ Go client

A Go client for the **producer door** of a running [`flexiq-server`](../../crates/flexiq-server):
submit work, read it back, cancel it, count it. No database credential, no native binding — a
gRPC library and this module.

```bash
go get github.com/ByteVeda/flexiq/sdks/go/v2
```

```go
import flexiq "github.com/ByteVeda/flexiq/sdks/go/v2"
```

The `/v2` is Go's rule, not a second module: a module released at a major version above 1 carries
that major in its path. The client ships at the repo's own version, which is 2.x.

## Two doors, two packages, two credentials

**The root package cannot execute tasks.** Running work is the executor door,
`flexiq.executor.v1`, which the [`executor`](#running-work-the-executor-door) subpackage opens. The
split is the wire's, not a preference: they are separate packages behind separate scopes, and a
token scoped to `produce` cannot attach while a token scoped to `execute` cannot enqueue.

| Package | Door | Scope | What it does |
| --- | --- | --- | --- |
| `sdks/go/v2` | `flexiq.v1` | `produce` | Enqueue, read, cancel, count, submit workflows |
| `sdks/go/v2/executor` | `flexiq.executor.v1` | `execute` | Attach, run tasks, report results |

A task that fans out to a second stage holds one of each: it runs on the executor door and goes
back through the producer door as an ordinary client.

What neither door has, all of it deliberate rather than missing: middleware, the admin surface,
settings, migrations, pub/sub and the worker registry. See
[the delta from an embedded SDK](../../contracts/REMOTE_SDK_CONTRACT.md#the-delta-from-an-embedded-sdk).
Durable steps are absent from the executor package specifically — see its own section below.

Task registration is absent for a different reason: the server holds no task registry at all.
Enqueuing a name nobody implements succeeds, and the job dead-letters later.

## Quickstart

```go
client, err := flexiq.New("queue.internal:50051", flexiq.WithToken(os.Getenv("FLEXIQ_TOKEN")))
if err != nil {
    return err
}
defer client.Close()

result, err := client.Enqueue(ctx, flexiq.EnqueueRequest{
    Task: "billing.charge",
    Args: []any{charge{OrderID: "ord-0001", AmountCents: 1000}},
    Options: flexiq.EnqueueOptions{
        Queue:     "payments",
        UniqueKey: "charge:ord-0001",
    },
})
if err != nil {
    return err
}

job, err := client.GetJob(ctx, result.Job.ID, flexiq.GetJobOptions{IncludeResult: true})
```

There is no completion notification anywhere on this door — no watch, no server stream. Poll
`GetJob`, or subscribe a webhook on the server side.

## The surface

| Method | What it does |
| --- | --- |
| `Enqueue` | Submit one job |
| `EnqueueBatch` | Submit many, one result per item, no atomicity |
| `GetJob` | Read one job by id |
| `ListJobs` / `AllJobs` | Page through jobs, newest first |
| `CancelJob` | Cancel, and report the state that leaves it in |
| `QueueStats` | Per-status counts for one queue or the namespace |
| `SubmitWorkflow` | Submit a graph of steps, one job per node |
| `GetWorkflowRun` | Read a run and every node it has |

`AllJobs` is a range-over-func iterator that pages for you:

```go
for job, err := range client.AllJobs(ctx, flexiq.ListJobsQuery{Queue: "payments"}) {
    if err != nil {
        return err
    }
    fmt.Println(job.ID, job.Status)
}
```

## Workflows

A workflow is a graph of steps. Submit one and the server pre-enqueues a job per
node, chained by the graph's edges, and the ordinary scheduler advances it — any
worker with workflow tracking enabled, not only whoever submitted it.

```go
submitted, err := client.SubmitWorkflow(ctx, flexiq.SubmitWorkflowRequest{
    Name: "checkout",
    Graph: flexiq.WorkflowGraph{
        Nodes: []flexiq.WorkflowNode{
            {Name: "charge", Task: "billing.charge", Args: []any{order}},
            {Name: "ship", Task: "fulfilment.ship", Options: flexiq.WorkflowNodeOptions{
                Queue:      "fulfilment",
                MaxRetries: 2,
                Condition:  flexiq.EdgeConditionOnSuccess,
            }},
        },
        Edges: []flexiq.WorkflowEdge{{From: "charge", To: "ship"}},
    },
})
if err != nil {
    return err
}

run, err := client.GetWorkflowRun(ctx, submitted.RunID)
if err != nil {
    return err
}
for _, node := range run.Nodes {
    fmt.Println(node.Name, node.Status, node.JobID)
}
```

Poll `GetWorkflowRun` and stop when `run.State.IsTerminal()` says to. As
everywhere else on this door, there is no watch and no server stream.

**Static graphs only.** A node may set `Gate`, `Cache`, `FanOut`, `FanIn` or
`SubWorkflow` — the wire carries all five — but this door refuses a graph that
does, `FAILED_PRECONDITION` with reason `WORKFLOW_CONSTRUCT_UNSUPPORTED`, before
anything is written. Nothing outside a live SDK process can advance a run using
one. The refusal names the node and the field, and one graph is refused for one
node at a time:

```go
if wireErr, ok := flexiq.AsError(err); ok {
    if construct, ok := wireErr.WorkflowConstruct(); ok {
        log.Printf("node %q cannot set %q over this door", construct.Node, construct.Field)
    }
}
```

**A submission is not idempotent, and an ambiguous failure is not retryable.**
There is no `unique_key` equivalent for a workflow, and no way to look a run up
by the name it was submitted under — `GetWorkflowRun` takes a run id and nothing
else. So `UNAVAILABLE`, `DEADLINE_EXCEEDED` and a dropped connection may each
mean the submission landed and the response did not, leaving a run whose id
nobody can recover. Do not retry that automatically: a retry is a second
submission, not a repair.

**There is no version to submit under.** Every submission is version 1 of its
name. Resubmitting a name with a graph that differs from the one version 1
already holds is refused, `INVALID_ARGUMENT` — a run's definition has to
describe the graph that produced its jobs. Submit a materially different graph
under a different name.

## Running work: the executor door

```go
import "github.com/ByteVeda/flexiq/sdks/go/v2/executor"
```

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

if err := w.Handle("billing.charge", func(ctx context.Context, job *executor.Job) (any, error) {
    var c charge
    if err := job.Bind(&c); err != nil {
        return nil, executor.Fatal(err)
    }
    job.Progress(50)
    return receipt{OrderID: c.OrderID}, nil
}); err != nil {
    return err
}

return w.Run(ctx)
```

`Run` blocks. It attaches, dispatches each job to its handler on its own goroutine, and reconnects
on its own. Register every handler first: the handshake advertises the task names, nothing else is
ever sent to this executor, and the list is fixed for the life of a stream.

### What a handler returns

| Returns | Becomes |
| --- | --- |
| `(value, nil)` | A success, with `value` in the cross-SDK result envelope |
| `(nil, nil)` | A success that **returned nothing** — not the same as returning an empty value |
| `(_, err)` | A failure the scheduler retries |
| `(_, executor.Fatal(err))` | A failure it does not retry |
| `(_, ctx.Err())` after a cancel | A cancellation |
| a panic | A failure, recovered, with the stack as the traceback |

**Whether to retry is your decision.** Only the executor can see the exception and the scheduler
never inspects one, so `Fatal` is the only way a Go task dead-letters itself.

The handler's context carries the job's timeout and is cancelled when the scheduler asks for the
job to stop. Honour it — cancellation is cooperative, and nothing here can stop a goroutine that
does not return. A handler that ignores its cancel and finishes settles normally.

### The stream ends, and that is normal

Streams are bounded, 30 minutes by default, because a gRPC stream cannot be load-balanced once it
has started. Before ending one the scheduler stops matching work to it and waits for what the
executor already holds, so a rotation never costs a job.

**A clean end means reconnect; a `shutdown` frame means stop.** `Run` does both for you, logs the
difference, and backs off only for a real transport failure. It returns rather than reconnecting
for the refusals that would repeat verbatim: a duplicate executor id, a protocol version mismatch,
a revoked credential and a wrong scope.

Cancelling the context `Run` was given begins a graceful drain. No new work is accepted, the jobs
already running keep their contexts until the drain budget expires, and their results still reach
the scheduler before the stream closes.

### Capabilities

Optional behaviour is negotiated, never versioned. This client implements two:

| Capability | What it gives you |
| --- | --- |
| `side_channel` | `job.Progress`, `job.Log` and `job.Publish` |
| `lease` | The dispatch lease, echoed on every frame about the attempt |

Both degrade silently when the scheduler does not acknowledge them — the side-channel calls become
no-ops. They are fire and forget either way: nothing answers them, they never settle a job, and one
naming a job this stream is not running is dropped at the far end.

**Durable steps are not implemented.** The `steps` capability is never advertised, so the scheduler
sends no step frames and none go back. That capability is the one that fails rather than degrades —
a durable step that silently did not commit is a step that will re-run a charge — which is exactly
why it is absent rather than half-present.

### An executor cannot enqueue

Not a rule this package applies: there is no enqueue-shaped RPC in `flexiq.executor.v1` at all. A
task that fans out goes back through the producer door as an ordinary client, holding a second,
`produce`-scoped credential of its own.

Nothing an executor sends names a namespace, an owner, an attempt or a resource cap. Anything a
client could name is something a client could forge, so the scheduler applies every one of those
from the dispatch it recorded.

## Credentials

Every call carries `authorization: Bearer <token>`; there is no anonymous path. An operator mints
one:

```bash
flexiq-server token create --name my-service --scope produce
flexiq-server token create --name my-worker  --scope execute
```

One token may carry both scopes, though a producer and an executor are usually separate processes
holding one each. A `produce` token cannot open an executor stream and an `execute` token cannot
enqueue, so a process that does both needs either both scopes or two tokens.

The token is opaque — do not parse it — and it expires, 90 days by default and 365 at the outside.
Nothing on the wire warns you that yours is about to: track the expiry you were given.

**The namespace is a property of the token**, fixed when it was minted. Nothing you send here
names one, because anything a client can name is something a client can forge. If enqueues succeed
and nothing ever runs them, check that the token's namespace is the one your workers drain.

`flexiq-server` terminates no TLS, so this client verifies the peer by default and refuses to send
a token over a plaintext connection. A deployment puts a TLS-terminating proxy or a service mesh in
front of the listener. The exemptions are the hops with no network to observe — a Unix socket, or a
loopback bind — and for those:

```go
client, err := flexiq.New("unix:///run/flexiq.sock",
    flexiq.WithToken(token),
    flexiq.WithInsecureTransport(),
)
```

## Errors

Two different failures travel on this wire and they are not the same thing.

**A failed request** is a `*flexiq.Error`. Branch on the reason, never on the message — the message
is written for humans and may be reworded in any release:

```go
switch {
case errors.Is(err, flexiq.ReasonQueueFull):
    var wireErr *flexiq.Error
    errors.As(err, &wireErr)
    time.Sleep(wireErr.RetryAfter)   // every RESOURCE_EXHAUSTED carries one
case errors.Is(err, flexiq.ReasonUnauthenticated):
    // Refresh the credential and try once. Every way of failing — missing,
    // revoked, expired, wrong namespace — collapses to this one answer on
    // purpose; do not try to infer which.
}
```

**A failed job** is data, and rides inside a response that succeeded:

```go
if taskErr, ok := job.TaskError(); ok {
    log.Printf("%s: %s", taskErr.Type, taskErr.Message)
    if !taskErr.Structured {
        // A timeout, a worker-death recovery, an expiry and a cancellation are
        // plain text by design. taskErr.Message is the raw string.
    }
}
```

## Retries are yours to write

This client does not retry. That is not an omission:

> `UNAVAILABLE`, `DEADLINE_EXCEEDED` and `CANCELLED` on an `Enqueue` may all mean the write landed
> and the connection dropped after it, and no field on the wire distinguishes them.

So a retried enqueue sets `EnqueueOptions.UniqueKey` and reuses the same value. And a unique key is
not an idempotency key — it dedupes against the **active** job only, so once the original completes
or dead-letters the key is released and the same request enqueues a second job. Keep a retry
loop's total deadline inside the job's own life.

## Payloads

Arguments are encoded into the cross-SDK envelope before they go out: one tag byte, then CBOR
`[args, kwargs]`. A worker cannot tell a job enqueued from here from one enqueued by any other SDK.

Go map iteration order is unspecified, so **a `map[string]any` argument encodes to different bytes
on different runs.** Pass a struct where the bytes matter — deriving a unique key by hashing the
payload, or matching what another runtime would have sent:

```go
type charge struct {
    OrderID     string `cbor:"order_id"`
    AmountCents int    `cbor:"amount_cents"`
}
```

`EnqueueRequest.Raw` takes a pre-encoded envelope for a caller that already has one.

## Development

```bash
make            # lists every target
make check      # build, vet, lint and the race suite — what CI runs, in CI's order
make test       # the suite, including the cross-SDK conformance vectors
make lint       # golangci-lint with the committed .golangci.yml
make fmt        # rewrite formatting and import grouping
make generate   # regenerate internal/pb from ../../contracts/proto
make tools      # install the pinned linter into GOBIN
make server     # build the flexiq-server that `make e2e` drives
make e2e        # run the suite against that server, over a real socket
```

The linter version is pinned in two places that must agree: `GOLANGCI_LINT_VERSION` in the
[`Makefile`](Makefile) and in [`ci-go.yml`](../../.github/workflows/ci-go.yml). Formatting is part
of the lint config, so `make lint` covers `gofmt` as well.

`internal/pb` is committed because `go get` runs no code generator. CI regenerates it and fails on
a diff, so it cannot drift from `contracts/proto`.

One file per concern, named after it — `enqueue.go`, `read.go`, `cancel.go`, `errors.go`. A
surface with more than one file's worth in it takes a prefixed family rather than a longer file:
`workflow_graph.go` is the shape you submit, `workflow_submit.go` the call, `workflow_run.go` what
comes back and `workflow_status.go` the enums those carry. The tests mirror the same names.

The suite lives in `tests/`, beside the package rather than inside it. Everything there reaches the
client through its exported API, the same way you do — a surface that is awkward to use is awkward
to test.

Most of it answers a `ProducerService` double on an in-process connection, which pins this client's
half of the contract. The **end-to-end suite** pins the other half — the pair. It is behind
`//go:build integration`, so `make test` stays double-only and needs no Rust toolchain:

```bash
make server     # cargo build -p flexiq-server --features grpc
make e2e
```

`make e2e` starts a real `flexiq-server` on a temporary SQLite file, mints its credentials through
`flexiq-server token create`, runs the suite against a real socket, and tears the process down. It
finds the binary under the workspace `target/`; point `FLEXIQ_SERVER_BIN` at one to override that.
A missing binary **fails** the run rather than skipping it — a suite that skips itself in CI is a
suite that stopped running and said nothing.

`tests/wire_test.go` asserts [`contracts/wire-vectors.json`](../../contracts/wire-vectors.json),
the same file every FlexiQ runtime asserts in its own suite. **A hex string there is never edited
to make a test pass** — a diff to one is a wire-format change, and it breaks every job already
enqueued.

## The contract

[`contracts/REMOTE_SDK_CONTRACT.md`](../../contracts/REMOTE_SDK_CONTRACT.md) is normative for
everything above, and it is worth reading before you rely on any of it. The narrative guide is at
<https://docs.byteveda.org/flexiq/server>.
