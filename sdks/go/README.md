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

## This is a client, not an SDK

**It cannot execute tasks.** A Go worker means the executor door, `flexiq.executor.v1`, which is a
separate package behind a separate scope and a larger design. This module does not open it, and
nothing here is a step towards it that you can finish yourself.

What that leaves out, all of it deliberate rather than missing: task registration, middleware, the
admin surface, settings, migrations, pub/sub, durable steps and the worker registry. They are not
on this door — see [the delta from an embedded SDK](../../contracts/REMOTE_SDK_CONTRACT.md#the-delta-from-an-embedded-sdk).
If you want a queue *and* the workers that drain it in one process, use the Python, Node or Java
SDK instead.

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

`AllJobs` is a range-over-func iterator that pages for you:

```go
for job, err := range client.AllJobs(ctx, flexiq.ListJobsQuery{Queue: "payments"}) {
    if err != nil {
        return err
    }
    fmt.Println(job.ID, job.Status)
}
```

`SubmitWorkflow` and `GetWorkflowRun` are not implemented yet.

## Credentials

Every call carries `authorization: Bearer <token>`; there is no anonymous path. An operator mints
one:

```bash
flexiq-server token create --name my-service --scope produce
```

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
go test -race ./...      # includes the cross-SDK conformance vectors
go vet ./...
gofmt -l .
buf generate             # regenerate internal/pb from ../../contracts/proto
```

`internal/pb` is committed because `go get` runs no code generator. CI regenerates it and fails on
a diff, so it cannot drift from `contracts/proto`.

The suite lives in `tests/`, beside the package rather than inside it. Everything there reaches the
client through its exported API, the same way you do — a surface that is awkward to use is awkward
to test.

`tests/wire_test.go` asserts [`contracts/wire-vectors.json`](../../contracts/wire-vectors.json),
the same file every FlexiQ runtime asserts in its own suite. **A hex string there is never edited
to make a test pass** — a diff to one is a wire-format change, and it breaks every job already
enqueued.

## The contract

[`contracts/REMOTE_SDK_CONTRACT.md`](../../contracts/REMOTE_SDK_CONTRACT.md) is normative for
everything above, and it is worth reading before you rely on any of it. The narrative guide is at
<https://docs.byteveda.org/flexiq/server>.
