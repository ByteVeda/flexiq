// Package flexiq is a Go client for the producer door of a running
// flexiq-server.
//
// It submits work, reads it back, cancels it and counts it. It holds no
// database credential and links no native binding: everything here goes over
// gRPC, against the contract in contracts/REMOTE_SDK_CONTRACT.md.
//
// # What this is not
//
// This is a client, not a port of a FlexiQ SDK. It cannot execute tasks:
// running work is the executor door, flexiq.executor.v1, a separate package
// behind a separate scope. The subpackage
// [github.com/ByteVeda/flexiq/sdks/go/v2/executor] opens that one, and holds a
// credential of its own — a token scoped to produce cannot attach, and a token
// scoped to execute cannot enqueue.
//
// Middleware, the admin surface, settings, migrations and pub/sub are absent
// from both, because they are on neither door.
//
// Task registration is absent for a different reason: the server holds no task
// registry at all. Enqueuing a name nobody implements succeeds, and the job
// dead-letters later.
//
// # Getting started
//
//	client, err := flexiq.New("queue.internal:50051", flexiq.WithToken(os.Getenv("FLEXIQ_TOKEN")))
//	if err != nil {
//		return err
//	}
//	defer client.Close()
//
//	result, err := client.Enqueue(ctx, flexiq.EnqueueRequest{
//		Task: "billing.charge",
//		Args: []any{charge{OrderID: "ord-0001", AmountCents: 1000}},
//		Options: flexiq.EnqueueOptions{
//			Queue:     "payments",
//			UniqueKey: "charge:ord-0001",
//		},
//	})
//
// # Workflows
//
// [Client.SubmitWorkflow] takes a graph of steps and the server pre-enqueues
// one job per node, chained by the graph's edges; [Client.GetWorkflowRun] reads
// the run and every node back. The door executes static graphs only — a node
// setting a gate, a cache, a fan-out, a fan-in or a sub-workflow is refused,
// because nothing outside a live SDK process can advance one.
//
// # Two kinds of failure
//
// A failed request is an [*Error], and it is branched on by reason:
//
//	if errors.Is(err, flexiq.ReasonQueueFull) { ... }
//
// A failed job is data, and rides inside a response that succeeded. Read it
// with [Job.TaskError].
//
// # The namespace
//
// Nothing here names a namespace: it is a property of the token, fixed when an
// operator minted it. If enqueues succeed and nothing ever runs them, check
// that the token's namespace is the one the workers drain.
package flexiq
