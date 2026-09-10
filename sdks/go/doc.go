// Package flexiq is a Go client for the producer door of a running
// flexiq-server.
//
// It submits work, reads it back, cancels it and counts it. It holds no
// database credential and links no native binding: everything here goes over
// gRPC, against the contract in contracts/REMOTE_SDK_CONTRACT.md.
//
// # What this is not
//
// This is a client, not a port of a FlexiQ SDK. It cannot execute tasks. A Go
// worker means the executor door, flexiq.executor.v1, which is a separate
// package behind a separate scope and a larger design; nothing in this module
// opens it. Task registration, middleware, the admin surface, settings,
// migrations and pub/sub are all absent for the same reason — they are not on
// this door.
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
