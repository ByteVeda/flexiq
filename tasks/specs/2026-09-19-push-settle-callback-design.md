# A settle callback for push work that outlives the request deadline

Issue: [#845](https://github.com/ByteVeda/flexiq/issues/845)
Epic: [#849](https://github.com/ByteVeda/flexiq/issues/849)
Governs: `contracts/PUSH_DISPATCH_CONTRACT.md`, `contracts/proto/flexiq/executor/v1/executor_service.proto`
Precedent: `tasks/specs/2026-09-01-flexiq-v1-proto-design.md` (§8 is binding here)

## Why this document exists

Push dispatch (#843/#844) settles a job on the connection that started it. That
caps a job at the platform's request deadline — 60 minutes on Cloud Run, 15 on
Lambda — and the cap is not ours to raise. Decoupling the report from the
request is the only fix, and it turns the *late* answer from an edge case into
the design's normal failure mode.

The one thing that must be right is the fence. A `Settle` that arrives after its
lease expired and the job was retried elsewhere must be refused, not applied.
Everything below is arranged around making that refusal unavoidable rather than
remembered.

## The evidence

| Fact | Where | What it forces |
|---|---|---|
| `202` is refused outright as `Refusal::Accepted202`, not retried | `worker/http_target/contract.rs:321-323`, `:107-109` | The seam already exists and names this issue. Turning it into a wait is a behaviour change to a shipped contract, so it is opt-in. |
| Every job handed to `run()` yields **exactly one** `JobResult`, never zero, never two | `worker/http_target/mod.rs:15-56`, `attempt.rs:78-141` | A 202 must *defer* the result, not skip it. Anything that emits zero results breaks the invariant the module is built on. |
| `epochs_agree(None, _) == true` — an absent epoch is not a mismatch | `lease.rs:76-81` | Correct for a leaseless pool, wrong for a settle door where the lease **is** the authorisation. The settle path needs a second, strict comparison. |
| `authorize_attempt` defaults to `Authorized`; `Scheduler::authorize_finished` also fails open on a storage error | `storage/traits.rs:857-871`, `scheduler/result_handler.rs:42-60` | Deliberate, and stated. The settle door cannot lean on either: its own check must fail **closed**. |
| `purge_execution_claims` deletes every claim older than a hard-coded hour | `scheduler/maintenance.rs:104` | Any job running past an hour loses its claim row; with the row gone the epoch is absent and the fence agrees with everything. Pre-existing; #845 makes it the normal case. |
| Staleness is `started_at + timeout_ms < now`, on the `jobs` table | `storage/diesel_common/jobs.rs:2160-2183` | Lease extension has to move a durable deadline. Nothing today can. |
| `jobs` and `execution_claims` are not declared joinable in Diesel | `storage/diesel_common/jobs.rs:2186-` | The reaper reads claims in a second indexed query, as `reap_orphaned_jobs` already does. |
| `HttpDispatchTarget::set_claim_owner` is an explicit no-op: "a push target performs no fenced write on the scheduler's behalf" | `worker/http_target/mod.rs:665-673` | Becomes false. The comment is as much a part of the change as the body. |
| `HttpDispatchTarget` has no `SideChannel` | same file | Progress and task logs have nowhere to land until it does. |
| A push deployment has **no inbound listener**: no Service, no containerPort | `deploy/helm/.../values.yaml:152-192`, `push.mdx:309-317` | A settle door means push and the gRPC role become co-dependent. Stated in docs, not discovered. |
| `the_executor_door_is_absent_under_push` asserts the door does not exist under push | `crates/flexiq-server/tests/push_dispatch_e2e.rs:622` | `ExecutorDoor` must become constructible without a `RemoteDispatcher`. |
| The JSON facade serves `flexiq.v1` and a test asserts `flexiq.executor.v1` is never served over HTTP | `grpc/facade/routes.rs:20-23` | §8's "No HTTP binding, ever. Not 'not yet'." holds. Settle is gRPC. |
| No mocking crate anywhere in the workspace | `push_dispatch_e2e.rs`, `http_target_tests.rs` | Tests stay hand-rolled: axum stub end to end, raw TCP for wire cases. |

## Decisions

- **D1** A `202` is a hand-off, not an outcome. Opt-in via `FLEXIQ_PUSH_TARGET_SETTLE`.
- **D2** The deferred result is a long await *inside* `run_one`. The invariant does not move.
- **D3** An accepted dispatch keeps its semaphore permit.
- **D4** Every emission from an accepted dispatch is gated on atomically consuming one durable marker.
- **D5** The settle fence is strict and fails closed — a separate function from `epochs_agree`.
- **D6** One nullable column carries both the deadline and the "awaiting settle" fact.
- **D7** The RPCs carry the existing frames, never copies of their fields.
- **D8** `slept` has no arm on the settle door.
- **D9** The executor door becomes constructible settle-only.
- **D10** The lease fences; the executor-scoped bearer token authenticates. Both, always.

---

## §1 The shape

```
scheduler → POST target                    (job, attempt, lease)
target    → 202 Accepted                   "I have it, I will call back"
   …the request ends; run_one keeps its permit and waits…
target    → ExtendLease(job, lease, +Δ)    (optional, repeatable)
target    → Settle(SuccessFrame{…lease})   → exactly one JobResult
```

### 1.1 Why the wait lives in `run_one` (D2, D3)

`HttpDispatchTarget`'s module doc states the invariant the whole file is built
on: every job handed to `run()` produces exactly one `JobResult`, or is dropped
by the lease re-check because someone else settled it. Never zero, never two.

A 202 that returned from `run_one` without emitting would produce zero, and the
invariant would have to grow an exception. Instead the attempt task stays alive
and swaps what it is waiting on: not the HTTP response, but
`settle-signal | settle-deadline`. One result still comes out of one task, the
final lease re-check still runs verbatim, and the shutdown drain already knows
how to abandon in-flight attempts.

Keeping the permit (D3) follows from the same place. Capacity is the only
backpressure the push path has — there is no `hello`, no announced slots,
nothing else bounding how many jobs the scheduler claims. Releasing the permit
at 202 would let a target that answers 202 instantly have the scheduler claim
every pending job in the queue. Holding it gives
`FLEXIQ_PUSH_TARGET_CAPACITY` one meaning in both modes: **how many jobs may be
outstanding at this target at once.**

The cost is stated rather than hidden: a slot is occupied by work that holds no
connection, so a deployment that uses 202 sizes capacity for concurrent *jobs*,
not concurrent requests.

### 1.2 The race for the marker (D4)

An accepted dispatch can be given up by two things that cannot see each other:

1. a `Settle` reaching the replica that dispatched it,
2. that dispatch being abandoned — the settle deadline passing, a cancel, or a
   shutdown — observed either by the waiting task or, after this process died,
   by the stale-job reaper on any replica.

Both must resolve to exactly one outcome, and the in-process registry cannot
arbitrate: case 2-after-a-crash never touches it. So the arbiter is durable —
each must **atomically consume the settle marker**, and only the one that
consumed it may emit. That is `Storage::claim_settle`, and it is what makes the
settle single-use for the attempt it names, as #845 requires.

**A `Settle` that reaches a different replica is not a third claimant.** It is
refused as `NotHere` before any storage call, and consumes nothing. It could
not be a claimant even if it wanted to be: the waiting attempt holds the
semaphore permit and the result channel, and both live in the process that
dispatched. §5.3 states the routing consequence.

`Scheduler::authorize_finished` resolves its fence from an in-memory
`DispatchRecord` and fails open when there is none. That is harmless here
rather than load-bearing: every settle that reaches storage has already
consumed the marker under stricter rules, and one that reached the wrong
replica never got that far.

---

## §2 The fence

### 2.1 Strict, and why it cannot reuse `epochs_agree` (D5)

```rust
/// Whether a presented lease authorises a settle.
///
/// Strict where [`epochs_agree`] is permissive, and the difference is the
/// point. `epochs_agree` answers "is there evidence this dispatch is stale",
/// so an absence is not evidence and it agrees. Here the lease *is* the
/// authorisation: a claim with no epoch, no claim row at all, or a value that
/// does not match are three different ways of having failed to prove anything,
/// and all three refuse.
pub fn lease_authorizes(claim: Option<i64>, presented: Option<i64>) -> bool {
    matches!((claim, presented), (Some(claim), Some(presented)) if claim == presented)
}
```

Both functions live in `lease.rs`, next to each other, with each doc naming the
other. Two callers with opposite defaults is precisely the kind of thing that
gets "simplified" into one function by a later reader, and the comment is the
only defence.

### 2.2 Status mapping

A refused settle is `FAILED_PRECONDITION`, never `ABORTED`. §4.3 of the proto
design puts `ABORTED` in the retry-with-backoff class, and a settle that lost
its fence must never be resent — resending it is the double execution the fence
exists to refuse. This matches `ClaimLost`'s existing mapping.

| Condition | Code |
|---|---|
| No `outcome` arm set | `INVALID_ARGUMENT` |
| Lease absent, undecodable, or not this claim's epoch | `FAILED_PRECONDITION` |
| No settle marker (never accepted, or already settled) | `FAILED_PRECONDITION` |
| Job unknown in this namespace | `NOT_FOUND` |
| Extension past the per-call ceiling | clamped, not refused — the response carries the deadline actually stored |

`NOT_FOUND` before the fence would be an oracle for job ids; the door is behind
a scoped token and namespace-bound already, so this is diagnostics rather than
disclosure — the same posture `refusal()` takes on the attach handshake.

---

## §3 Storage

### 3.1 The column (D6)

Migration `0019_claim_settle_deadline` adds one nullable column:

```rust
add_column(b, "execution_claims",
    ColumnDef::new(Alias::new("settle_deadline_ms")).big_integer())
```

**Non-null means "this dispatch was accepted out of band and is awaiting a
settle".** Its value is when patience runs out. One column carries both facts
because they are one fact: a dispatch is awaiting a settle exactly as long as
someone is still willing to wait for it.

Nullable and additive, like `m0016_claim_epoch`: a claim written before this
migration has a *missing* marker rather than a wrong one, which reads as "not
awaiting a settle" — the answer it would have given before the column existed.

Redis holds a claim as a single string, `"{owner}:{claimed_at}.{epoch}"`, and
#719 recorded why a fourth `:`-separated field is unsafe there: every reader
takes the owner as everything before the **last** `:`, so a `host:pid` owner
would silently truncate. The deadline therefore rides a **separate key**,
`flexiq:claim_settle:{job_id}`, whose value is the deadline and whose TTL the
scheduler does not rely on for correctness. A separate key also makes the
atomic consume a single `GETDEL`-shaped script rather than a rewrite of the
claim string.

### 3.2 The two methods

```rust
/// Mark a dispatch as awaiting an out-of-band settle, and say how long the
/// scheduler will wait for it.
///
/// Monotonic: never moves a deadline backwards, so a retransmitted accept and
/// a late extension are both safe. Fenced on the same `(owner, attempt, epoch)`
/// triple every other write on a dispatch is fenced on. Returns the deadline
/// actually stored, or `None` when the fence refused — the shape
/// `claim_execution` already uses for "you did not win this".
fn await_settle(
    &self,
    job_id: &str,
    owner: &str,
    attempt: i32,
    epoch: Option<i64>,
    deadline_ms: i64,
    namespace: Option<&str>,
) -> Result<Option<i64>>;

/// Consume the settle marker for `job_id`, if `claimant` is entitled to it.
///
/// The arbiter of §1.2's three-way race, and the reason a settle is single-use:
/// the marker is removed in the same statement that tests it, so exactly one
/// of a local settle, a remote settle and a deadline expiry can win. A caller
/// that gets `Refused` must emit nothing at all — not a failure, not a
/// timeout. Someone else already settled this attempt.
fn claim_settle(
    &self,
    job_id: &str,
    claimant: SettleClaimant,
    namespace: Option<&str>,
) -> Result<SettleGrant>;

/// Whether this backend implements the settle marker.
///
/// `false` refuses `FLEXIQ_PUSH_TARGET_SETTLE=grpc` at boot rather than
/// accepting 202s it cannot fence. Mirrors [`Storage::supports_steps`], and
/// for the same reason: a fence that degrades to "no marker recorded" is a
/// fence that authorises everything.
fn supports_settle(&self) -> bool { false }
```

```rust
pub enum SettleGrant {
    /// The marker was consumed. This caller settles the attempt.
    Granted,
    /// No marker: never accepted, or another racer already took it.
    Refused,
}

/// Who is asking to settle, and on what authority.
///
/// Two claimants with different proofs, and neither can present the other's.
/// A peer holds a lease and no clock the scheduler trusts; the scheduler holds
/// a clock and no lease. Collapsing them into one nullable epoch would make
/// "no lease" the same argument as "the deadline passed", and the reaper would
/// be able to take a marker out from under a target that is still inside its
/// deadline.
pub enum SettleClaimant {
    /// A peer presenting a lease. Granted only on a strict epoch match
    /// ([`lease_authorizes`]).
    Lease(i64),
    /// The scheduler, after the settle deadline passed. Granted only when the
    /// stored deadline is at or before `now`, evaluated inside the same
    /// statement — so an extension that landed a millisecond earlier wins.
    Expired { now: i64 },
}
```

`SettleClaimant::Expired` is what the waiting `run_one` and the stale-job reaper
both present. The reaper holds a `Job` and no epoch, which is exactly why the
epoch cannot be the only currency here.

Both default on the trait to the answer a backend without the column would give
— `await_settle` to `Ok(None)` and `claim_settle` to `Ok(SettleGrant::Refused)`
— because both are **refusals**. This is the one place in the storage layer
where the default must fail closed, and it is the opposite of
`authorize_attempt`'s: that default protects a job from being stuck `Running`
forever, whereas failing open here would let an unfenced settle land. A backend
that cannot evaluate the fence must not be able to authorise one, and a
deployment whose backend does not implement this refuses 202 at config time
(§6) rather than degrading silently.

### 3.3 The reaper

`reap_stale_jobs` grows a return type:

```rust
pub struct StaleJob {
    pub job: Job,
    /// The dispatch was accepted out of band and never settled, rather than
    /// simply running long. The operator-visible difference #845 asks for.
    pub awaiting_settle: bool,
}

fn reap_stale_jobs(&self, now: i64, namespace: Option<&str>) -> Result<Vec<StaleJob>>;
```

The existing SQL predicate is unchanged and stays a correct superset, because
the marker is initialised to the job's *own* deadline and only ever moves
forward — a job whose settle deadline has not passed has necessarily passed
`started_at + timeout_ms` first. A second indexed read over the candidate ids
then both **removes** the ones still inside their settle deadline and **flags**
the ones that had a marker at all. Two queries rather than a join, the shape
`reap_orphaned_jobs` already uses for the same two tables.

The scheduler's message becomes, for a flagged job:

```
push.accepted_not_settled: the target accepted this job and never settled it
within <n>ms
```

keeping the `ACCEPTED_NOT_SETTLED` prefix so the existing regression test's
intent — an operator can grep for this — survives the behaviour change.

Reaping a flagged job consumes its marker on the way through, so the reaper is
a first-class racer in §1.2 rather than an exception to it.

### 3.4 The purge

```rust
// Was: claimed_at < older_than_ms
// Now: claimed_at < older_than_ms AND (settle_deadline_ms IS NULL
//                                      OR settle_deadline_ms < now)
```

Today `purge_execution_claims` drops every claim older than a hard-coded hour
(`maintenance.rs:104`). With the row gone the epoch is absent, `epochs_agree`
agrees with everything, and the fence a long job most needs is the one it does
not have. That hole is pre-existing and this issue makes it the normal case, so
it is closed here for the marked rows.

**The general case is not closed**, and saying so is part of the decision: an
*attached* executor running a job for more than an hour still loses its claim
row and its epoch fence. Fixing that means teaching the purge about live jobs
in `jobs`, which is a join the Diesel backends do not have and a scan the Redis
one would pay for on every sweep. Filed rather than half-done.

---

## §4 The wire

### 4.1 Four RPCs, carrying the frames that already exist (D7, D8)

```proto
rpc Settle(SettleRequest) returns (SettleResponse);
rpc ExtendLease(ExtendLeaseRequest) returns (ExtendLeaseResponse);
rpc ReportProgress(ReportProgressRequest) returns (ReportProgressResponse);
rpc WriteTaskLog(WriteTaskLogRequest) returns (WriteTaskLogResponse);
```

```proto
message SettleRequest {
  // The outcome, as the frame the attach stream already carries. Each one
  // names its own job, task and lease, so this message adds no field of its
  // own — a second copy of the job id here would be free to disagree with the
  // one inside the frame.
  //
  // There is no `slept` arm and there will not be one: a push dispatch has no
  // step session to resume, which is why `x-flexiq-outcome: slept` is refused
  // on the request path too.
  oneof outcome {
    SuccessFrame success = 1;
    FailureFrame failure = 2;
    CancelledFrame cancelled = 3;
  }
}

message SettleResponse {}

message ExtendLeaseRequest {
  string job_id = 1;
  bytes lease = 2;
  // How much longer this attempt needs, measured from now rather than from the
  // current deadline: a target knows how long it still needs, and it does not
  // know what deadline the scheduler is holding.
  google.protobuf.Duration extend_by = 3;
}

message ExtendLeaseResponse {
  // The deadline actually stored, which is not the one proposed when the
  // request was clamped. Echoed for the same reason StepAckFrame echoes
  // wake_at: the caller must plan against what landed, not what it asked for.
  google.protobuf.Timestamp deadline = 1;
}

message ReportProgressRequest { ProgressFrame progress = 1; }
message ReportProgressResponse {}
message WriteTaskLogRequest { TaskLogFrame task_log = 1; }
message WriteTaskLogResponse {}
```

Every message is additive, so `v1` does not move (§2.1). The request/response
pairs are unique per RPC because `RPC_REQUEST_RESPONSE_UNIQUE` at `buf lint`
STANDARD forbids sharing one — which is also why the three empty responses are
three types and not one `Empty`.

A `oneof` with no arm set is `INVALID_ARGUMENT`. "Readers tolerate what they do
not know" applies to a frame *inside* a stream, where skipping is the documented
behaviour; a unary RPC that was asked to settle a job and recognised nothing it
was given has not been asked anything, and answering `OK` would tell a target
its result landed when it did not.

### 4.2 The extension ceiling

One per-call ceiling, declared once in core:

```rust
/// The furthest one ExtendLease may push a settle deadline out.
///
/// Per call, not in total: a target that needs six hours asks six times, and
/// its asking is the liveness signal. An unbounded single call would let one
/// request park a claim past any operator's patience, and a total cap would
/// make a long, healthy job indistinguishable from a wedged one.
pub const MAX_LEASE_EXTENSION: Duration = Duration::from_secs(3_600);
```

Clamped rather than refused, with the stored deadline echoed back. A refusal
would make the correct client behaviour "ask again with a smaller number",
which is a retry loop written into the contract for no benefit.

### 4.3 What the contract says now

`contracts/PUSH_DISPATCH_CONTRACT.md`'s response table replaces the `202` row:

| Status | `x-flexiq-outcome` | Result | Retried? |
|---|---|---|---|
| `202` | — | **Accepted.** The job stays `Running` and the scheduler waits for a `Settle` until the settle deadline. Body ignored. Refused as `Accepted202` when `FLEXIQ_PUSH_TARGET_SETTLE` is `off` | on the deadline, as a timeout |

and the "does not promise" section loses the progress/task-log bullet, gains the
gRPC-role dependency and the credential a target now needs.

---

## §5 The server

### 5.1 A settle-only door (D9)

`ExecutorDoor::new` takes a `RemoteDispatcher` today, which is why
`the_executor_door_is_absent_under_push` holds. It gains a second constructor:

```rust
impl ExecutorDoor {
    /// Serve executors into `dispatcher` — the attach topology.
    pub fn new(dispatcher: RemoteDispatcher, …) -> Self;

    /// Serve only the reporting RPCs, for a push deployment where nothing
    /// attaches. `Attach` and `Heartbeat` answer `FAILED_PRECONDITION`: there
    /// is no dispatcher to attach *to*, and saying so is better than an
    /// `UNIMPLEMENTED` that reads as "this build does not have the feature".
    pub fn settle_only(target: Arc<HttpDispatchTarget>, …) -> Self;
}
```

`the_executor_door_is_absent_under_push` becomes
`the_executor_door_serves_only_settle_under_push`, asserting both halves —
`Attach` refused, `Settle` served. A test that merely stopped asserting absence
would leave the new behaviour unpinned.

### 5.2 Auth (D10)

Two layers, and neither is optional:

- **The bearer token authenticates.** The existing `AuthLayer` over `Routes`
  gates `flexiq.executor.v1` on an executor-scoped token. A push target now
  needs one. Making the lease sufficient on its own would give the executor
  package a second, topology-dependent auth posture, and the one thing #717
  bought was that there is only one.
- **The lease fences.** §2.1, strict, single-use per attempt.

The lease is already redacted in `Debug` (`lease.rs:139-143`) on the stated
grounds that "a log line carrying one hands the next reader the ability to
settle someone else's job" — which was forward-looking when it was written and
is now literally true. The `x-flexiq-lease` request header is already marked
HTTP-header-sensitive (`attempt.rs:359-377`); nothing there changes.

### 5.3 Routing a settle into the result channel

The door holds `Arc<HttpDispatchTarget>` and calls one method:

```rust
/// Deliver an out-of-band outcome for an accepted dispatch.
///
/// Consumes the durable settle marker before waking the waiting attempt, so a
/// settle that lost its fence never reaches `result_tx` at all.
pub fn settle(&self, job_id: &str, lease: &Lease, outcome: SettledOutcome) -> Result<()>;
```

The waiting `run_one` then emits its one `JobResult` down the channel it already
holds. Nothing new touches the scheduler, `handle_result` is unchanged, and the
retry/dead-letter path a settled failure takes is the one push dispatch already
uses.

### 5.4 The side channel

`HttpDispatchTarget` gains a `SideChannel`, installed by the same
`set_*`-on-the-trait shape as `set_lease_book` and `set_claim_owner`.
`ReportProgress` and `WriteTaskLog` resolve their lease against the accepted
registry, then call `update_progress` / `write_task_log` — the same two methods
the attach path's `apply_progress` / `apply_task_log` reach, through the same
`SideChannelPump`, landing in the same columns.

Progress and task logs are **not** gated on the durable consume. They advance an
attempt rather than settling one, so the in-process registry check (does this
lease match the dispatch this replica is holding open?) is the right strictness
— the same call `frame_is_current` makes for the identical frames on the attach
stream. A cross-replica progress report is dropped, which costs a progress
update and nothing else.

### 5.5 `set_claim_owner` stops being empty

```rust
/// Deliberately empty rather than left to the trait default.
///
/// A push target performs no fenced write on the scheduler's behalf …
fn set_claim_owner(&self, _owner: &str) {}
```

`await_settle` is a fenced write on the scheduler's behalf, so this comment
becomes false the moment the feature lands. Both the body and the comment
change; the note that replaces it says *why* a push target now needs the owner,
so the next reader does not re-empty it.

---

## §6 Configuration

| Var | Values | Default | Notes |
|---|---|---|---|
| `FLEXIQ_PUSH_TARGET_SETTLE` | `off` \| `grpc` | `off` | `grpc` requires `FLEXIQ_GRPC_LISTEN` and a storage backend that implements the settle marker. Both checked at boot, by name, the way `UNHONOURED_VARS` already refuses a var it will not honour. |

No duration var. The settle deadline starts at the job's own `timeout_ms` and
moves only by `ExtendLease`, so a second timeout to configure would be a second
policy free to disagree with the first.

Helm: `push.settle` renders the var. The chart already exposes the gRPC
listener, so the co-dependency costs values-file wiring rather than a new
Service. A `push.settle: grpc` with `grpc.enabled: false` fails the render, in
the same style as the existing `terminationGracePeriodSeconds` guard.

Opt-in (D1) because the current behaviour is shipped and documented: a target
that answers 202 by mistake today dead-letters in one attempt with a reason an
operator can read. Flipping that to "hang until the deadline" silently would be
a regression for every existing deployment, and the class of bug it hides —
a framework returning 202 by default — is exactly the one `x-flexiq-outcome`
being mandatory was designed to catch.

---

## §7 Observability

- **`flexiq_push_awaiting_settle`** — a gauge, the size of the accepted
  registry. Process-local and documented as such: the registry lives on the
  replica that dispatched, so this is "accepted here", not a cluster total. A
  cluster total needs a storage count on every scrape, which is a cost per
  scrape for a number an operator reads per incident.
- **The reaper message** (§3.3) is the per-job answer, and the one #845 actually
  asks for: an operator sees "accepted, never settled", not "retried".
- **`flexiq_executors` / `flexiq_executor_slots` stay absent** under push. They
  report attached capacity, and a settle-only door has none. The existing gap
  (push capacity is invisible on `/metrics`, `AppState.dispatcher` is
  `Option<RemoteDispatcher>`) is unchanged and out of scope.

---

## §8 Testing

Hand-rolled, per the workspace's convention — no mocking crate is introduced.

**`crates/flexiq-server/tests/push_dispatch_e2e.rs`** (axum stub):

1. 202 → `Settle(success)` → the result is stored and the job completes.
2. 202 → nothing → at the deadline the job fails with the
   `push.accepted_not_settled` prefix. Replaces
   `a_target_that_returns_202_dead_letters_with_a_reason_an_operator_can_read`,
   whose intent it keeps.
3. 202 → `Settle` under a **stale** lease → `FAILED_PRECONDITION`, job untouched.
4. 202 → `Settle` twice under the same lease → the second is refused.
5. `ExtendLease` past the deadline → the job is not reaped; the response's
   deadline is the clamped one.
6. `FLEXIQ_PUSH_TARGET_SETTLE=off` → 202 still dead-letters, unchanged.
7. `settle: grpc` without `FLEXIQ_GRPC_LISTEN` → refused at boot, by name.
8. `the_executor_door_serves_only_settle_under_push` — `Attach` refused,
   `Settle` served.

**`crates/flexiq-core/tests/rust/http_target_tests.rs`** (raw TCP stub):
the permit is held across an accepted dispatch; capacity still bounds
concurrency when every response is a 202; a shutdown drain abandons an accepted
dispatch to the reaper rather than emitting a second result.

**Unit:** `lease_authorizes` against `epochs_agree` over the same four
`(Option, Option)` inputs, asserting they disagree on exactly the three
absence cases. `await_settle` monotonicity. `claim_settle` single-use.

**Mutation checks**, because a negative test passing for the wrong reason is the
trap #719 recorded: case 3 must fail because `claim_settle` refused, not because
the in-process registry or `dispatch_is_current` refused first — pin it by
settling under a lease the registry *does* hold but storage does not. Case 4's
second refusal must come from the consume, not from the registry entry being
gone.

---

## §9 What this does not promise

- **Cancel still does not stop a push target's work.** #846 owns that. The
  poll-on-settle option it lists composes with this door — a target that calls
  `ExtendLease` could be told "this job is no longer yours" — and the RPC is
  shaped so that answer can be added as a field rather than a new RPC. Nothing
  here implements it.
- **No HTTP settle.** §8 of the proto design: "No HTTP binding, ever. Not 'not
  yet'." A target speaks gRPC to report, and the docs say so plainly rather
  than leaving it to be discovered at integration time.
- **No SDK settle helpers.** The push contract's worked example is deliberately
  framework-free and gains a settle half; four SDK clients are not this branch.
- **No cross-replica progress.** §5.4.
- **The claim-purge hole is closed only for marked rows.** §3.4.
- **Durable steps stay attach-only.** A settle door does not give a push target
  a step session, and `slept` remains refused on both paths.
