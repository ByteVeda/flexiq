# Cancellation in push mode (#846) — design

## The problem, as it actually stands

The issue was written before #843/#845 landed. Reading the code today, the
floor it names — **fence at settle** — already exists: `HttpDispatchTarget`'s
`notify_cancel` abandons an in-flight request, takes the settle marker of an
accepted one, settles the attempt `Cancelled`, and a later `Settle` is refused.

Two things are still wrong, and the first one is worse than the issue says:

1. **Nothing in `flexiq-server` ever calls `notify_cancel`.** `CancelJob` (gRPC
   producer door), the dashboard's cancel route and every SDK's
   `request_cancel` only set `jobs.cancel_requested`. The native dispatcher
   polls that flag; the remote and push dispatchers never read storage. So
   under `flexiq-server` a cancel reaches **neither** an attached executor nor
   a push target — the attach row of the per-topology table ("the scheduler
   sends a `cancel` frame") is not true today either. The API says it worked
   and nothing happens until the job ends by itself.
2. **A push target cannot tell it was cancelled.** Once the attempt settles
   `Cancelled` the accepted entry is removed, so the target's next
   `ExtendLease` / `ReportProgress` / `Settle` is answered `NotHere` — "you
   reached the wrong replica, fix your routing". A target that polls to ask
   "is this still mine?" is told something false.

## Decision

Of the three options the issue lists:

- **Endpoint on the target** — rejected. Most serverless platforms cannot route
  a second request to a running invocation; it would be a contract every
  target has to implement and few can.
- **Poll on the settle path** — **chosen**, cooperative. It composes with #845:
  the RPCs a long-running target already calls become the channel.
- **Fence at settle** — kept as the floor, unchanged. It is what a target that
  never polls gets.

### D1 — A cancel relay in the core `Worker`

`Worker::spawn` gains a fourth thread, started only when a custom dispatcher
was supplied (the built-in `NativeDispatcher` reads the flag itself). Once a
second it:

1. takes the scheduler's in-flight dispatches `(job id, epoch)`;
2. asks storage, in **one** query, which of those ids have
   `cancel_requested` set (`Storage::cancel_requested_among`);
3. calls `dispatcher.notify_cancel` once per newly-cancelled dispatch.

Keyed by `(id, epoch)`, not id: a job that settles and is re-dispatched between
two ticks is a new dispatch and must not inherit the old one's "already
relayed". The relayed set is pruned to what is still in flight each tick, so it
is bounded by `max_in_flight`.

Why the core `Worker`, not `flexiq-server`: the bug is "a dispatcher that does
not read storage never learns of a cancel", which is true of every embedder
that hands `Worker` a remote or push dispatcher. Why storage polling, not a
direct call from `CancelJob`: the cancel may land on another replica, in
another process, or come from an SDK writing to the same database. Storage is
the one place every cancel reaches.

Why a new batch storage method: in-flight is bounded by capacity (tens to
hundreds); N point reads per second per scheduler is a load a batch read
avoids for the price of one method.

### D2 — The target learns "cancelled" from the RPCs it already calls

`HttpDispatchTarget` keeps a small, bounded record of accepted dispatches it
**ended** — job id, lease, and why. When a reporting RPC finds no open
accepted dispatch but an ended one under the **same lease**:

- ended by a cancel → new `SettleRefused::Cancelled`;
- ended any other way (deadline, drain, a settle already won) →
  `SettleRefused::Fenced`, which is what the lease actually earned.

Only a caller holding the right lease learns why; anyone else still gets
`NotHere`/`Fenced` as before. The record is moved into under the same lock
that removes the open entry, so there is no window where a lookup sees neither.
Capacity 1024, oldest first out — enough to answer every target that polls at
any sane interval, small enough not to matter.

On the wire `Cancelled` is `FAILED_PRECONDITION` with `ErrorInfo.reason =
JOB_CANCELLED`, a new reason on the closed list; `Fenced` gains `ErrorInfo`
`CLAIM_LOST`, which it already means. Both **never resend**. A target branches
on the reason — the code alone cannot separate a cancel from a lost race.

The reporting RPCs that were "refused silently" (`ReportProgress`,
`WriteTaskLog`) now answer `JOB_CANCELLED` too, so a target that only reports
progress learns of a cancel as promptly as one that extends its lease.

### D3 — Semantics written down per topology

| Topology | `cancel()` promises |
|---|---|
| Native | The handler observes the storage flag and stops. |
| Attach | Within ~1 s the scheduler sends a `cancel` frame; the executor stops. |
| Push, in-request | Within ~1 s the request is abandoned and the attempt settles `Cancelled`. The target is not told except by the closed connection; its answer is fenced out. |
| Push, accepted (`202`) | Within ~1 s the attempt settles `Cancelled`. The target's next `ExtendLease`/`ReportProgress`/`WriteTaskLog`/`Settle` on the dispatching replica is refused `JOB_CANCELLED`, while that replica still holds the ending among its last 1024 (after that `NotHere`, also `FAILED_PRECONDITION` and a stop) — a target that polls stops then; one that does not runs to completion and its `Settle` is refused. |

"Cancel stops the work" (native, attach) and "cancel prevents the result from
landing, and tells a target that asks" (push) are different promises; the
contract, the docs and the module doc say which is which.

## Not in scope

- A target-side cancel endpoint (rejected above).
- Giving the other settle refusals (`NotHere`, `NotReady`, settle-disabled,
  storage) an `ErrorInfo` — a pre-existing gap, filed separately.
- Cross-replica answers. `JOB_CANCELLED` is a **same-replica** promise, stated
  as such in the push contract's Cancellation section: the ended record is
  process-local, like the open one, so it is bound by the routing rule
  `Settle` already has — reach the replica that dispatched. A report that lands
  elsewhere is refused `NotHere`, which is also a stop and never a resend
  (pinned by `a_settle_for_another_replicas_dispatch_says_so`). Shared state
  would buy a better *message* on a misrouted call, not a different action.
- Shells that never run `Worker`. The relay lives in `Worker::spawn`, which
  `flexiq-server` and the Rust SDK's pool use. The Python, Node and Java shells
  run their own worker loops and already call `notify_cancel` in-process from
  their own `request_cancel`; their `ExecutorClient::spawn` is the *executor*
  side of attach, which receives the relay's effect as a `cancel` frame.
- The Rust SDK's `ShellDispatcher` has no cooperative cancel at all and keeps
  the default no-op `notify_cancel`, so under it the relay costs one indexed
  read a second while jobs are in flight and changes nothing. Giving that shell
  a cancel is its own change.
