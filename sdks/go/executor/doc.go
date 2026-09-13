// Package executor is a Go client for the executor door of a running
// flexiq-server: it attaches, runs the jobs it is given, and reports what
// happened.
//
// It is the other half of [github.com/ByteVeda/flexiq/sdks/go/v2], which
// submits work. The two are separate packages because they are separate doors
// behind separate scopes — a token carrying "execute" cannot enqueue, and a
// token carrying "produce" cannot attach.
//
// # An executor cannot enqueue
//
// There is no enqueue-shaped RPC in flexiq.executor.v1 at all, so this is not a
// restriction this package applies — it is one the wire has. A task that fans
// out to a second stage goes back through the producer door as an ordinary
// client, holding a second, produce-scoped credential of its own.
//
// # Getting started
//
//	w, err := executor.New("queue.internal:50051",
//		executor.WithToken(os.Getenv("FLEXIQ_EXECUTE_TOKEN")),
//		executor.WithID("go-worker-1"),
//		executor.WithSlots(8),
//	)
//	if err != nil {
//		return err
//	}
//	defer w.Close()
//
//	w.Handle("billing.charge", func(ctx context.Context, job *executor.Job) (any, error) {
//		var charge Charge
//		if err := job.Bind(&charge); err != nil {
//			return nil, executor.Fatal(err)
//		}
//		return Receipt{ID: charge.OrderID}, nil
//	})
//
//	err = w.Run(ctx)
//
// [Worker.Run] blocks. It attaches, dispatches jobs to their handlers, and
// reconnects when the scheduler rotates the stream — which it does every half
// hour by default, because a gRPC stream cannot be load balanced once it has
// started. A rotation is not a failure and does not cost a job in flight.
//
// # What a handler returns
//
// A value and no error is a success; a nil value means the task returned
// nothing, which is a different answer from returning an empty one. An error is
// a failure the scheduler will retry, unless it is wrapped in [Fatal]. Whether
// to retry is the executor's decision: only it can see the exception, and the
// scheduler never inspects one.
//
// A handler's context carries the job's timeout and is cancelled when the
// scheduler asks for the job to stop. A handler that returns its context's
// error after a cancel settles the job as cancelled rather than failed.
//
// # Durable steps
//
// A step runs once per job, not once per attempt. Its result is recorded, and
// a later attempt replays that record instead of running the body again — which
// is what stops a retry charging a card twice.
//
//	w.Handle("billing.charge", func(ctx context.Context, job *executor.Job) (any, error) {
//		var order Order
//		if err := job.Bind(&order); err != nil {
//			return nil, executor.Fatal(err)
//		}
//
//		receipt, err := executor.Step(ctx, job, "charge",
//			func(ctx context.Context, key string) (Receipt, error) {
//				return gateway.Charge(ctx, order, key)
//			})
//		if err != nil {
//			return nil, err
//		}
//
//		if err := job.Sleep(ctx, "settlement", 24*time.Hour); err != nil {
//			return nil, err
//		}
//		return receipt, nil
//	})
//
// [Step] is a package function rather than a method because a Go method cannot
// have a type parameter. [StepKeyed] identifies a step by data instead of by
// position, and [Job.Sleep] ends the attempt and reschedules the job.
//
// The key handed to the body is this step's downstream idempotency key, stable
// across every attempt. Memoization closes the replay window; only a key the
// other service dedupes on closes the crash window between a remote call
// succeeding and its row committing.
//
// Unlike the side channel, this capability **fails rather than degrades**: a
// step call against a scheduler that does not offer a step store returns a
// retryable [StepError] rather than running un-memoized. There is no version of
// "your charge step silently lost its memo" that beats a failure naming the
// reason.
//
// A refusal is the task body's to handle: catch it, do something else, return a
// value, and the job is recorded a success. Two are not, and returning normally
// past either does not make them so. [ErrStepDiverged] fails the attempt
// anyway, because the deployed code and the recorded rows disagree.
// [ErrStepSuperseded] settles nothing at all — another attempt owns the job,
// and this one may not write over it.
//
// # What this package does not do
//
// Task registration, middleware, the admin surface, settings and pub/sub are
// absent because they are not on this door — see
// contracts/REMOTE_SDK_CONTRACT.md.
package executor
