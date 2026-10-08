// Package admin is a Go client for the operator door of a running
// flexiq-server, flexiq.admin.v1.
//
// It reaches the queue, override, periodic-task, quota and token operations an
// operator performs — the ones infrastructure-as-code drives. It is a separate
// package from the producer client because it is a separate credential: an
// operator token carries the "admin" and "inspect" scopes, and the token
// methods need "tokens" as well, which neither of the others implies.
//
// # Getting started
//
// It dials exactly as the producer client does, from the same options:
//
//	client, err := admin.New("queue.internal:50051", flexiq.WithToken(os.Getenv("FLEXIQ_ADMIN_TOKEN")))
//	if err != nil {
//		return err
//	}
//	defer client.Close()
//
//	queue, err := client.PauseQueue(ctx, "payments")
//
// # Failures
//
// A failed call is a [*flexiq.Error], branched on by reason. A periodic task or
// a token that does not exist — or exists in another namespace, which reads the
// same — is a NOT_FOUND with its own reason:
//
//	if errors.Is(err, flexiq.ReasonPeriodicTaskNotFound) { ... }
//	if errors.Is(err, flexiq.ReasonTokenNotFound) { ... }
//
// A queue, a queue override and a quota have no such reason: reading one that
// was never set answers its empty state.
//
// # The namespace
//
// Nothing here names a namespace. Every call acts on the token's own, and a
// resource in any other is indistinguishable from one that does not exist.
package admin
