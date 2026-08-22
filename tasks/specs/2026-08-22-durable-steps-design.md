# One persistence path for inline steps and workflow nodes

**Date:** 2026-08-22
**Status:** Approved — the contract the #663 sub-issues are reviewed against
**Issue:** [#664](https://github.com/ByteVeda/flexiq/issues/664), part of
[#663](https://github.com/ByteVeda/flexiq/issues/663)
**Governs:** #665 (storage) · #666 (memoization) · #667 (`step.sleep`) · #668
(idempotency key) · #669 (python) · #670 (node) · #671 (java) · #672 (docs)

## Why this document exists

A workflow node and an inline step are both "a named unit whose result is
persisted and replayed". Built separately, they drift into two half-compatible
caches. This document decides where they share code, where they deliberately do
not, and what every sub-issue must honour.

The concern is not hypothetical. The workflow node cache **already has the bug
this epic must not repeat** — see [The evidence](#the-evidence).

## The evidence

Three facts, established by reading the code, that decide most of what follows.

**1. Workflow nodes have no result store of their own.**
`workflow_nodes` (`crates/flexiq-workflows/migrations/m0001_workflow_initial.rs:68`)
carries `result_hash TEXT` and no blob column. A node's result *is* its job's
result: the tracker reads it with `get_job(job_id).result_bytes`
(`sdks/python/flexiq/workflows/tracker/tracker.py:279`). There is no second blob
store to unify with — `jobs.result` is the workflow node result store.

**2. The incremental-run node cache persists a hash but not a value.** A
cache-hit node is created with `job_id: None` and a copied `result_hash`
(`crates/flexiq-python/src/py_queue/workflow_ops/lifecycle.rs:114`), and
`build_workflow_context` only collects results for nodes whose status is
`completed` (`sdks/python/flexiq/workflows/tracker/dag.py:110`). So a cached
node's value is unreachable downstream: the cache remembers *that* a result
existed, not what it was. That is the half-compatible cache #664 exists to
prevent a second of.

**3. An attached executor has no database.** `worker/executor.rs:11` is explicit:
"the executor image carries app code and no database credentials". Every step
read and every step write from an attached executor has to cross the frame
protocol. This is the single largest cost in the epic and the reason §9 exists.

## Decisions

| # | Decision | Rationale |
|---|---|---|
| D1 | **No shared table.** Inline steps get `job_steps`; workflow nodes keep `jobs.result`. | They are not siblings — a node *is* a job, a step lives *inside* one. Writing node results into `job_steps` too would double-write every node result and create the drift the issue fears, in reverse. |
| D2 | **One shared rules module**, `flexiq_core::step`: key derivation, sequence comparison, limits, idempotency-key minting. Pure functions, no I/O. | This is the part that actually drifts. Keeping it in core means Redis, Diesel and all three shells cannot disagree about what a step key *is*. |
| D3 | **One narrow trait surface on `Storage`**, every method defaulted to an error, not to a no-op. | `Storage` is public and published; a required method is a semver break. A default that returns *empty* would silently disable memoization and double-execute — a gate must not fail open. |
| D4 | Step identity is **`name#occurrence`**, with an optional caller-supplied `key` that replaces the occurrence counter. | Occurrence numbering is correct and free for straight-line code and for ordered loops; it is unstable over unordered collections, which is exactly what the explicit key is for. |
| D5 | The **fingerprint is the ordered `(seq, step_key)` list itself**, compared position-by-position at each step. No fingerprint column. | Strictly more precise than a digest, needs no new column, and fails at the first differing position — the earliest point and the best error. A digest is computed only for the message. |
| D6 | **Refuse over the cap, never spill.** 256 KiB per step, 4 MiB per job, 1 000 steps per job, all configurable. | There is no external blob store to spill to; spilling into the same database is not a spill. A refusal names the step and tells the user to store the value elsewhere and memoize the handle. |
| D7 | Step blobs go through the **queue-level serializer**, not the per-task one. | Results already work this way (`app.py:665`). A global `Queue(codec=…)` wraps the queue serializer, so encryption is inherited rather than re-plumbed, and an operator reading a step back gets it with the configuration every other result reader uses. |
| D8 | The durable run key is the job's **origin id**, preserved across `retry_dead`. | `{job_id}:{step_key}` is worthless for a dead-lettered charge that an operator retries three days later — the one case where a downstream idempotency key matters most. |
| D9 | `step.sleep` ends the attempt via `complete_execution` + `reschedule`, and **is itself a step row**. | Free durability (a sleep that already fired is not repeated), free divergence coverage, and it gives the dashboard something to show for an hour-long "pending". |
| D10 | Step rows are deleted **in the job's terminal write**, not by a time-based sweep. | A step memo is execution state with no value after the job ends — keeping it is bloat plus plaintext-at-rest exposure. The post-mortem question ("which step failed") is answered by a small string on the DLQ entry instead. |
| D11 | New hook `on_sleep`; `after` does **not** fire for a slept attempt. | `after(ctx, None, None)` reads as "returned None" to every existing middleware, so OTel would close a span as success and Prometheus would count one. |
| D12 | Inline steps require **`CONTRACT_VERSION` 2** and refuse to run below a floor of 2. | An older worker cannot read `job_steps` and would silently re-run every committed step during a rolling upgrade. That is precisely what the floor exists for. |
| D13 | The epic lands as **one major release (2.0.0)**. | `JobResult` and `ResultOutcome` are not `#[non_exhaustive]`; adding a variant, and adding the attribute, are both breaking. Bundle them once rather than discovering the break at publish time. |
| D14 | **Every step write is fenced on the execution claim**, and every step write is atomic with whatever else it changes. | A job can run in two places at once — `requeue_stuck` says so — so an unfenced write from an abandoned attempt lands in the live one's sequence. And a sleep split across three calls can strand a job `Running` past a crash. |

---

## §1 What is shared and what is not

### 1.1 The two granularities are nested, not parallel

```text
workflow run
  └── node            ── is a job; its result is jobs.result
        └── step      ── is inside a job; its result is job_steps.result
```

A node does not compete with a step for storage, because a node is a job and a
step is a thing inside one. Inline steps inside a workflow node's task body work
on day one with no extra design: the node's job id is the step owner.

This is the answer to "share a table, a trait, or only a serialization
contract": **a trait and a rules module, not a table** (D1, D2, D3).

### 1.2 What lives in `flexiq_core::step` (new module)

Pure, no I/O, no storage, no pyo3 — usable from the core, from
`flexiq-workflows`, and from every shell through its binding.

| Item | Purpose |
|---|---|
| `StepKey::derive(name, occurrence) -> String` | `name#occurrence`. |
| `StepKey::explicit(name, key) -> String` | `name:key`, bypassing the counter. |
| `StepSequence` | Ordered `(seq, step_key)` accumulator; `check(seq, key)` returns `Ok` / `Err(Divergence)`. |
| `StepLimits { max_step_bytes, max_total_bytes, max_steps }` | Defaults 256 KiB / 4 MiB / 1 000. |
| `idempotency_key(run_key, step_key) -> String` | `{run_key}:{step_key}`. |
| `digest(&[String]) -> String` | SHA-256 over `\n`-joined keys, for error messages and the dashboard only. |

Name validation lives here too: a step name must be non-empty, at most 128
bytes, and must not contain `#` or `:` — the two separators the key format
uses. An explicit key is at most 256 bytes and may contain anything, since a
key is only ever compared, never parsed back. A name that would produce an
ambiguous key is a `QueueError::Config`, raised in the shell before any I/O.

### 1.3 The trait surface

Four methods on `Storage`, plus one capability probe. All defaulted, all
namespace-scoped per #614.

```rust
/// Whether this backend implements the step store. `false` disables the
/// inline-step API entirely — it must never degrade to "no memo".
fn supports_steps(&self) -> bool { false }

/// Every committed step for a job, ordered by `seq`. Read ONCE per attempt.
fn get_job_steps(&self, job_id: &str, namespace: Option<&str>)
    -> Result<Vec<JobStep>> { Err(QueueError::Other("steps unsupported".into())) }

/// Commit one step, fenced on the writer still owning the execution claim
/// (§1.4). Enforces the byte and count caps at the boundary and rejects a
/// `seq` that is not exactly `len(existing)`. Idempotent for an identical
/// re-commit; see `StepCommit` below.
fn record_step_result(&self, step: &NewJobStep, owner: &str, namespace: Option<&str>)
    -> Result<StepCommit> { Err(QueueError::Other("steps unsupported".into())) }

/// End the attempt in a sleep: commit the sleep row, release the claim and
/// reschedule the job for `wake_at` — one atomic operation (§7.1).
fn sleep_job(&self, step: &NewJobStep, owner: &str, wake_at: i64, namespace: Option<&str>)
    -> Result<()> { Err(QueueError::Other("steps unsupported".into())) }

/// Drop every step row for a job. Called from the terminal write (§8.4).
fn delete_job_steps(&self, job_id: &str, namespace: Option<&str>)
    -> Result<u64> { Err(QueueError::Other("steps unsupported".into())) }
```

`StepCommit` is `Committed` or `AlreadyCommitted` — the second is what an
identical retransmission gets, and it is a success (§9.2).

The `Unsupported`-by-default choice mirrors `shed_to_dlq`
(`storage/traits.rs:286`) for source compatibility, but inverts its *semantics*:
`shed_to_dlq` may safely degrade to `move_to_dlq`, whereas a step read that
degrades to "no steps recorded" re-runs a charge. The lesson from the contract
floor applies unchanged — **a gate must not fail open**.

`SqliteStorage`, `PostgresStorage` and `RedisStorage` override all five.

### 1.4 Every step write is fenced on the execution claim

A job can be running in two places at once. `requeue_stuck` says so in its own
documentation (`storage/traits.rs:139`): "a still-alive owner may finish the old
attempt, double-executing the job", and the dead-owner reaper reclaims on the
same assumption. Left unfenced, a step write from the abandoned attempt lands
after the new one has started — appending a row the new attempt's sequence never
asked for, or worse, rescheduling a job the new owner is holding.

So the token that already exists is the one used: `execution_claims` is
`(job_id PRIMARY KEY, worker_id, claimed_at)`, and `reclaim_execution` transfers
`worker_id` atomically. A stale owner is exactly an owner whose `worker_id` no
longer matches the claim row.

**Rule:** `record_step_result` and `sleep_job` take the writer's `owner` and, in
the same transaction as the write, require the claim row for `job_id` to still
name it. A mismatch — or no claim row at all — is `QueueError::ClaimLost`, and
the attempt aborts without retrying: another worker owns this job now, and
anything this attempt does from here is a duplicate.

`get_job_steps` is not fenced. It is a read at attempt start, taken by the
worker that just won the claim, and a stale read can only cost a re-run.

This is the counterpart of `finalize_fan_out_parent`'s compare-and-swap
(`flexiq-workflows/src/storage.rs:124`), for the same reason: an operation that
must happen exactly once cannot check its precondition in a separate round trip
from its write.

### 1.5 The workflow node cache is fixed, not replaced

Fact 2 above is a real defect, and the fix is not a second blob store. A
cache-hit node should carry **the base run's `job_id`**, so its value is
fetchable from the archived job it already points at, and `build_workflow_context`
should collect results for `cache_hit` as well as `completed`. That makes the
node cache and the step memo the same shape — a pointer to one durably stored
result blob — without duplicating a byte.

Out of scope for #663; tracked separately. Recorded here because it is the
concrete instance of the drift this document exists to prevent, and because it
is something #665 must not foreclose.

---

## §2 Step identity

### 2.1 Default: `name#occurrence`

`occurrence` is the 0-based count of how many times *this name* has been
requested so far in this attempt. Straight-line code gets `charge#0`,
`receipt#0`. A loop gets `fetch#0`, `fetch#1`, `fetch#2`.

Names are mandatory and positional — never inferred from the callable. An
inferred name changes when a lambda is refactored, which is a divergence the
user never wrote.

### 2.2 The loop problem, and the escape hatch

`name#occurrence` is stable only if the loop yields the same items in the same
order on every attempt. That holds for a list; it does not hold for a set, a
dict before insertion order matters, or an unordered query. Reorder the items
and `fetch#1` now answers a different question — the memo is wrong and the
sequence check cannot see it, because the key sequence is identical.

So a caller may pin identity to the data:

```python
for order in orders:                     # order is arbitrary
    ctx.step.run("fetch", lambda: get(order.id), key=order.id)   # → fetch:1234
```

An explicit `key` replaces the occurrence counter entirely. Two steps that
derive the same explicit key in one attempt are a `QueueError::Config`, not a
silent overwrite.

**A keyed call does not advance the name's occurrence counter.** The two forms
count independently, so

```python
ctx.step.run("fetch", …, key="a")   # fetch:a
ctx.step.run("fetch", …)            # fetch#0   — not fetch#1
ctx.step.run("fetch", …, key="b")   # fetch:b
ctx.step.run("fetch", …)            # fetch#1
```

The alternative — a shared counter — makes *adding a keyed call* shift the key
of every later unkeyed call of the same name, which is a divergence caused by an
edit that changed nothing about the unkeyed steps. A keyed step's identity does
not depend on its position, so its position must not be spent.

Every shell tests all three shapes: keyed only, unkeyed only, and interleaved.

**Guidance for #672:** default to `name#occurrence`; reach for `key=` the moment
a step runs inside a loop over anything whose order is not guaranteed.

### 2.3 `seq` versus the key

`seq` is the position in the attempt's step sequence; `step_key` is the
identity. Both are stored. `seq` drives the position-by-position divergence
check and the `record_step_result` guard; `step_key` is what a memo lookup
matches on. They are not redundant: an explicit key makes the two independent.

---

## §3 The divergence rule

### 3.1 What is fingerprinted

The ordered list of `step_key`s, as they were requested. Nothing else. Not the
closure, not the payload, not the module version — none of which are observable
from where the check runs.

### 3.2 When it is checked

At **every `step.run`/`step.sleep`**, against the snapshot loaded once at attempt
start. For the step at position `i`:

- snapshot has no entry at `i` → new step, run it, commit it;
- snapshot's entry at `i` has the same key → memo hit, return it, run nothing;
- snapshot's entry at `i` has a **different** key → divergence, fail the attempt.

No extra I/O: the snapshot is already in memory (§5.1).

### 3.3 What failure looks like

A `StepDivergenceError` carrying both sequences, the position, and both keys:

```text
StepDivergenceError: step sequence changed for job 018f…c2 at position 2
  recorded: charge#0, notify#0, receipt#0
  running:  charge#0, notify#0, audit#0
  step 2 was 'receipt#0', now 'audit#0'
A memoized result would answer a different question than the step asking for it.
Drain or dead-letter the in-flight jobs of this task before deploying a change
to its step sequence.
```

The attempt fails, is **not** retried (`should_retry = false`), and goes
straight to the DLQ. Retrying cannot help: the code will not change between
attempts, so a retry burns the budget to reproduce the same error. Returning a
wrong memoized result is worse than either.

### 3.4 The two cases that are not failures

**Steps added at the end.** The snapshot runs out; the new steps run and commit.
Normal progress — this is what an attempt that got further looks like.

**Steps removed from the end.** The attempt finishes having consumed fewer steps
than were committed. The orphaned tail's side effects already happened and the
new code has no use for their values. This is a **warning**, logged with both
sequences, not a failure: failing a job whose code legitimately shortened would
be worse than the leak of a value nobody reads. The rows are dropped with the
job (§8.4).

### 3.5 What is not detectable, and must be documented

A step whose key is unchanged but whose *body* changed — a renamed helper, a new
API version behind the same call — replays the old value with no signal. This is
inherent: the closure is not observable. #672 must say so plainly, and give the
rule: **changing what a step does is a new step name.**

---

## §4 The blob cap

### 4.1 The limits

| Limit | Default | Why |
|---|---|---|
| `max_step_bytes` | 256 KiB | One checkpoint, not a data payload. |
| `max_total_bytes` | 4 MiB | The snapshot is loaded whole at attempt start; the per-step cap alone bounds nothing when a loop runs 10 000 times. |
| `max_steps` | 1 000 | A loop of cheap steps returning `None` would slip past a byte cap. |

Configurable on the queue. The hard ceiling on `max_step_bytes` is 1 MiB —
above that, the answer is not a bigger cap.

### 4.2 Refuse, do not spill

A spill needs somewhere to spill *to*. FlexiQ has no blob store, and spilling
into the same database is not a spill — it is the same bytes under a different
key, with the same cost and none of the visibility. A silent spill would also
hide unbounded growth until the disk does the complaining.

So an over-cap step raises, naming the step and the two numbers:

```text
StepResultTooLargeError: step 'render#0' returned 1.4 MiB, over the 256 KiB cap
Store the value where it belongs (object storage, a table of your own) and
memoize the handle instead.
```

### 4.3 Where it is enforced, and on which bytes

**Both** in the shell, before the round trip, and in `record_step_result` at the
storage boundary (#665). The shell check is the good error message; the storage
check is the one that holds when a shell forgets.

Measured on the **encoded** bytes — post-serializer, post-codec — because that
is what is stored. Gzip shrinks them; AES-GCM adds 28 bytes plus padding. A
value that fits before encoding may not fit after, and the error must report the
number that actually failed.

`max_total_bytes` and `max_steps` are checked against the persisted rows inside
`record_step_result`, in the same transaction as the insert.

---

## §5 Serialization, codecs, and the snapshot

### 5.1 One read per attempt

`get_job_steps` is called **once**, at attempt start, before the task body runs.
Never per step. A per-step read would put a database round trip in the middle of
ordinary control flow, and would be wrong anyway: nothing else may write this
job's steps while it is running.

### 5.2 The queue serializer, not the task serializer

The scope line in #666 says "the task's serializer and codec chain". This
document overrides that: **step blobs use the queue-level serializer**, exactly as job
results do (`_serialize_result`, `app.py:665`).

Per-task serializers cover *payloads* only, because a payload has one writer and
one reader that agree by construction. A result has many readers — `JobResult.get`,
the workflow tracker, the dashboard — that carry only queue-level configuration.
A step result is a result. Writing it under a per-task serializer would produce
blobs nothing but that task's own worker can read.

### 5.3 Codecs are inherited, not re-plumbed

`Queue(codec=…)` wraps the queue serializer (`codecs.py:11`), so a deployment on
`AesGcmCodec` gets encrypted step blobs with no extra code. That is the whole
mechanism by which #666's "an encrypted queue writes no plaintext into
`job_steps`" acceptance criterion is met — and the reason D7 is not negotiable.
The test for it should assert on the **raw bytes in the row**, not on a
round trip.

Per-task `codecs=[…]` are payload-only and do **not** apply to step blobs, for
the same reason they do not apply to results.

---

## §6 The downstream idempotency key

### 6.1 The shape

```text
step.idempotency_key == f"{run_key}:{step_key}"        # e.g. 018f…c2:charge#0
```

Deterministic, stable across retries, stable across a sleep/wake, and derived
from nothing that a codec or a serializer touches. #668 must note the contrast
with the `idempotent=True` auto-key, which hashes the serialized payload and
therefore *is* sensitive to a nondeterministic codec.

### 6.2 `run_key` is the origin id, not the job id (D8)

`job_id` alone is wrong at exactly one boundary, and it is the boundary that
matters most. `retry_dead` mints a **new** job id
(`diesel_common/dead_letter.rs:293`), so an operator retrying a dead-lettered
charge would send a fresh idempotency key and charge the customer twice — three
days later, deliberately, through the admin UI.

So:

```text
run_key = job.metadata["__origin_job_id"] ?? job.id
```

`retry_dead` records `__origin_job_id` alongside the `__dlq_retry_count` it
already writes (`dead_letter.rs:267`), preserving an existing value rather than
overwriting it so a twice-retried job keeps the original. The reserved
double-underscore metadata prefix is established precedent.

Costs one small change in `retry_dead` on three backends. #668 owns it.

### 6.3 The tests that matter

Key stability across (a) an ordinary retry, (b) a sleep/wake, and (c) a
`retry_dead`. (c) is the one that would otherwise ship broken.

---

## §7 `step.sleep`

### 7.1 Mechanism

A sleep ends the attempt, doing what `rollback_claim_and_reschedule`
(`scheduler/poller.rs:628`) already does — release the claim, put the job back
to `Pending` at a future `scheduled_at` — plus committing the sleep row.

`reschedule` clears `started_at` (`diesel_common/jobs.rs:1188`), so a sleeping
job is not eligible for `reap_stale_jobs` — it will not be timed out while it
sleeps. That is a property of the existing implementation this design depends
on; #667 needs a test that pins it.

**The three writes are one operation, not three calls.** `sleep_job` (§1.3)
commits the sleep row, deletes the claim and rescheduling the job inside a single
transaction — a Lua script on Redis, as `enqueue_debounced` already does for the
same reason. Only `release_in_flight` stays outside it: that is in-process
bookkeeping with nothing to roll back.

Split across three calls, a crash between the row and the reschedule leaves the
job `Running` with a `sleeping` row whose deadline has not arrived — and the
stale reaper then hands that job to another worker while its own timeout clock
is still running. One transaction removes the window rather than documenting it.

**Recovery is still defined, because a crash can land anywhere.** A committed
sleep row whose `wake_at` is in the future is not a memo hit (§7.3); replaying
into it re-issues the same `sleep_job`, which is idempotent — the row is
identical, so the commit reports `AlreadyCommitted` and only the reschedule
takes effect. A partially-applied sleep heals on the next attempt instead of
needing a repair path of its own.

### 7.2 `reschedule` needs a namespace (#614 gap)

`reschedule` is the one id-addressed `Storage` method that never got a namespace
parameter (`storage/traits.rs:133`). It was safe while only the poller called
it, with a job it had just claimed. A sleep is issued by **task code**, which is
the least trusted caller in the system. #667 adds the namespace parameter.

### 7.3 A sleep is a step row (D9)

`sleep` commits a row with `kind = 'sleep'` and `wake_at` in place of a result
blob.

**A sleep row is a memo hit if and only if `now >= wake_at`.** That is a derived
rule, evaluated by the reader, not a stored status — which is why there is no
`status` column (§10.1) and no operation that would have to move a row from
"sleeping" to "complete". Nothing has to run at the wake moment for the row to
mean the right thing, and there is no state a crash can strand.

A `run` row, by contrast, is unconditionally a memo hit: its presence *is* its
completion, because a step whose closure raised is never committed at all.

Three things fall out of making a sleep a row:

- **Idempotent sleeps.** On wake, the sleep step is a memo hit and returns
  immediately — a job with three sleeps does not restart the first one on the
  third wake.
- **Divergence coverage.** Sleeps take part in the sequence, so moving a sleep
  relative to a step is caught like any other reorder.
- **Dashboard.** The pending job's latest step row says
  `sleeping until 14:32 (after step 3)` instead of an unexplained hour of
  "pending". This is the answer to #667's open question, and it needs no new
  column on `jobs`.

Identity: an unnamed `sleep` gets `sleep#N` from the occurrence counter. A name
is accepted and recommended (`ctx.step.sleep("1h", name="cool_off")`) — the
error messages are unreadable otherwise.

### 7.4 Accounting: what a sleep must not touch

| Counter | Effect of a sleep | Because |
|---|---|---|
| `retry_count` | unchanged | `reschedule`, not `retry`. |
| retry budget (#435) | no token | Only consulted on the failure path (`result_handler.rs:122`). |
| circuit breaker | untouched | Nothing failed. |
| `job_errors` | no row | Nothing failed. |
| `task_metrics` | **no row** | `succeeded=true` would inflate the success count; `false` would inflate failures. |
| `timeout_ms` | not consumed by the sleep | It bounds one attempt; the sleep ends the attempt. Steps *before* the sleep do count against it. |

The metrics gap is a known limitation: per-attempt CPU time is invisible for a
job that sleeps several times. The final success metric still covers the job.
Worth a line in #672; not worth a second metric row shape.

### 7.5 New variants (and their semver cost)

```rust
JobResult::Slept    { job_id, task_name, wake_at, wall_time_ns }
ResultOutcome::Slept { job_id, task_name, queue, wake_at, wall_time_ns }
```

Neither enum is `#[non_exhaustive]` today (`scheduler/mod.rs:108`, `:169`).
Adding a variant is breaking; adding the attribute is breaking. Both go in
together, once, in the 2.0.0 of D13.

A new event `JOB_SLEEPING = "job.sleeping"` joins the taxonomy in all three
shells (`events.py:23`), plus the webhook subscription contract.

### 7.6 Middleware: `on_sleep`, not `after` (D11)

`after(ctx, result, error)` with both `None` is indistinguishable from "the task
returned `None`". OTel contrib would close the span as a success, Prometheus
would increment the success counter, Sentry would clear the scope. Every one of
those is wrong for an attempt that has not finished.

So a new hook:

```python
def on_sleep(self, ctx: JobContext, wake_at: int) -> None:
    """Called when an attempt ends in a step sleep. Pairs with `before`."""
```

**Invariant:** every `before` is matched by exactly one of `after` / `on_sleep`.

The base-class default is a no-op. FlexiQ's own contrib middleware (otel,
sentry, prometheus, in all three shells) implements it in this epic — that is
real work #669/#670/#671 must budget for. Third-party hook middleware that pairs
`before`/`after` will leak whatever `before` opened; #672 documents it, and the
worker logs a one-time warning naming middleware that override `before` but not
`on_sleep`.

### 7.7 The control flow must not be swallowable

A sleep and a divergence both unwind the task body, and user code must not be
able to catch them away — #670 raises this for Node and it applies everywhere.

Two layers:

1. **Language-native.** Python: subclass `BaseException`, so a bare
   `except Exception` misses it, like `KeyboardInterrupt`. Java: subclass
   `Error`, so it escapes `catch (Exception)`.
2. **A latch, checked by the runner.** `ctx.step` sets a flag when it raises a
   control signal. If the handler returns normally with the flag set, the runner
   fails the attempt with *"step control flow was swallowed by the task body"*.
   Language-independent, and it is the only defence available in Node, where
   `try { … } catch { }` catches everything thrown.

---

## §8 Lifecycle

### 8.1 Retry

A retry re-runs the same job id (`storage.retry`, `result_handler.rs:135`), so
the step rows survive and the next attempt replays them. This is the whole
feature and it needs no new identity concept — #663's premise, verified.

### 8.2 Timeout

A timed-out attempt is a failure like any other: steps committed before the
timeout stay committed, and the retry replays them. A long job that would
otherwise time out should sleep, not raise the timeout.

### 8.3 Cancellation

`request_cancel` → the task observes it → `mark_cancelled` archives the job.
Terminal, so the step rows are dropped (§8.4). A cancel *during* a step is
observed between steps, never inside one — `step.run` does not interrupt the
closure it is running.

### 8.4 Retention and the reaper (D10)

Step rows are deleted **in the same write that removes the job from `jobs`**:
`complete`, `complete_batch`, `fail`, `mark_cancelled`, `cancel_job`,
`cascade_cancel`, `move_to_dlq`, `shed_to_dlq`, and the chunked mass-mutation
paths from the Tier-2 scaling work. #665's acceptance criterion — a purged job
leaves no orphan step rows on any backend — is checked against that list, and
`move_to_dlq` is the one that touches both `dead_letter` and `archived_jobs`.

They are **not** deleted by `requeue_stuck` or by the dead-owner reclaim: those
paths exist to let another worker resume the job, which is exactly when the memo
is needed.

This deliberately breaks with `job_errors`, which is swept by age
(`purge_job_errors`) and therefore needs an entry in `RetentionCutoffs` /
`RetentionCounts` and a SCAN sweep on Redis. Step rows need none of that,
because:

- a step memo has no value after the job ends — it is execution state, not
  diagnostic history;
- under `AesGcmCodec` those blobs are ciphertext an operator has no reason to
  keep at rest;
- the fifth Redis SCAN sweep is a cost with no benefit.

**The post-mortem question is answered without them.** `move_to_dlq` records the
last completed step's key and `seq` in the entry's metadata — one short string,
no blobs — so "which step did it die after?" is answerable from the DLQ view.

A defensive TTL on the Redis key (§10) covers the one gap: a backend crash
between the terminal write and the step delete.

---

## §9 Attached executors

This is the section that makes the feature work off-box, and the one most likely
to be skipped.

An executor has no storage (`worker/executor.rs:11`). It cannot call
`get_job_steps`, and the existing side channel (`Progress`, `TaskLog`) is
explicitly fire-and-forget — which is exactly wrong for a step commit, because
"the write may or may not have landed" means "the step may or may not re-run".

### 9.1 Read side: the snapshot rides the dispatch

A new frame, sent immediately before the `Job` frame, to executors that
advertise `CAP_STEPS`:

```text
{"type":"job_steps","job_id":"018f…","payload_len":412}\n<412 bytes>
```

The blob cannot go in the header — headers are capped at `MAX_HEADER_BYTES`
(64 KiB) and a snapshot can be 4 MiB. The length field **must** be named
`payload_len`: `FrameReader::read_or_skip` relies on that name to skip an
unknown frame and stay aligned (`worker/protocol.rs:20`), which is how an
executor that has not been upgraded survives a scheduler that has.

An executor that does not advertise `CAP_STEPS` is never sent the frame; what
happens when it is nonetheless handed a task that calls `step.run` is §9.4.

### 9.2 Write side: the first request/response on the channel

```text
executor  → {"type":"step_commit","job_id":…,"step_key":"charge#0","seq":0,"payload_len":64}
scheduler → {"type":"step_ack","job_id":…,"seq":0,"ok":true}
```

The executor blocks on the ack before returning from `step.run`. This is the
first executor→scheduler frame that expects an answer; both enums are already
`#[non_exhaustive]` (`protocol.rs:122`, `:185`), so the variants are additive.

Correlation is `(job_id, seq)` — one executor runs many jobs concurrently, and a
`job_id` alone is not enough once a job has more than one step in flight (it
cannot, but the pairing should not depend on that). The frame also carries the
executor's `owner` id, which the scheduler passes to `record_step_result` as the
fencing token of §1.4.

A `step_ack` with `ok: false` carries the storage error — a cap violation, a
`seq` conflict, a lost claim — and the executor raises it into the task body at
the `step.run` call site.

**A commit is idempotent, because an ack can be lost.** The connection can drop
between the scheduler's write and the executor seeing the answer, and the
executor's only recourse is to send the frame again. If a retransmission were
treated as a second commit, the durable path would fail exactly when the network
is already failing. So `record_step_result` compares the incoming
`(job_id, seq, step_key)` and a digest of the payload against what is stored:

| Stored row at `seq` | Incoming | Result |
|---|---|---|
| none | anything | `Committed` |
| identical key + digest | retransmission | `AlreadyCommitted`, `ok: true` |
| different key or digest | conflict | `ok: false`, the attempt fails |

Only genuinely conflicting data is refused. That is also what makes the sleep
recovery in §7.1 self-healing.

**The wait is bounded.** The executor waits for an ack up to the job's remaining
`timeout_ms`, or until the connection drops, whichever comes first. Either way
the attempt **fails** — it never proceeds past a step it could not confirm was
durable, because an unconfirmed commit is indistinguishable from one that never
happened, and continuing would re-run the step on the next attempt with the
side effect already applied. The job retries, replays the steps that *are*
committed, and re-runs this one under the same `step.idempotency_key` (§6),
which is the mechanism that makes the re-run safe.

**Sleep** is a terminal frame beside `Success`/`Failure`/`Cancelled`:
`{"type":"slept","job_id":…,"wake_at":…,"task_name":…,"wall_time_ns":…}`. It
carries `owner` too, and the scheduler answers it with a `step_ack` before
treating the attempt as ended — a sleep that could not be persisted is a failed
attempt, not a silent one.

### 9.3 Latency

Every `step.run` costs one round trip on the attached path, where the in-process
and prefork paths cost one local database write. That is the honest price of
durability without credentials, and #672 should state it: steps are
checkpoints, not a loop body.

### 9.4 Refusing rather than degrading

A step-using task dispatched to an executor without `CAP_STEPS` **fails the
attempt** with a clear error. It does not run un-memoized. There is no version
of "your charge step silently lost its memo" that is better than a failure
naming the executor.

The scheduler cannot know in advance which tasks use steps, so the check is at
the first `step.run`: an executor without the capability has no channel to
commit on, and says so.

---

## §10 Backends

### 10.1 Diesel (SQLite, Postgres)

One table, following the `job_errors` shape (`m0001_initial.rs:131`) — text
`job_id`, an index, **no foreign key**, because a job leaves `jobs` for
`archived_jobs` on completion and an FK would break exactly when the feature is
working.

```sql
job_steps(
  id          TEXT PRIMARY KEY,
  job_id      TEXT NOT NULL,
  namespace   TEXT,
  step_key    TEXT NOT NULL,
  seq         INTEGER NOT NULL,
  kind        TEXT NOT NULL DEFAULT 'run',    -- 'run' | 'sleep'
  result      BLOB,
  result_len  INTEGER NOT NULL DEFAULT 0,     -- cheap total-cap check
  wake_at     BIGINT,                         -- kind='sleep'
  created_at  BIGINT NOT NULL
)
UNIQUE INDEX idx_job_steps_job_seq  ON job_steps(job_id, seq)
UNIQUE INDEX idx_job_steps_job_key  ON job_steps(job_id, step_key)
INDEX        idx_job_steps_job_id   ON job_steps(job_id)
```

Placed as `crates/flexiq-core/migrations/m0013_job_steps.rs`; `build.rs`
discovers it. `namespace` is denormalised from the job so the scoped read and
delete are single-table.

**There is no `status` column and no `error` column, deliberately.** A step
whose closure raised is never committed — that is what makes the retry re-run
it — so a committed `run` row is complete by construction, and a `sleep` row's
completeness is `now >= wake_at` (§7.3). Every state is derivable from what is
already stored. A `status` column would be a second source of truth that some
path has to remember to advance, and a schema that could express a failed step
would invite a reader to treat one as a memo hit.

Both unique indexes matter: `(job_id, seq)` is what makes a double commit of the
same position a database error rather than a race, and `(job_id, step_key)`
catches a duplicate explicit key.

### 10.2 Redis

One hash per job, under the same prefix rules as the rest of `redis_backend/`
(`redis_backend/mod.rs:69`):

```text
{prefix}job_steps:{job_id}   HASH
    <seq>          → JSON document          -- decimal seq
    k:<step_key>   → <seq>                  -- uniqueness index
    __total        → running result_len sum
```

A hash, not the list `job_errors` uses, because a step lookup is by position.
The `k:` fields carry what the Diesel side gets from
`UNIQUE(job_id, step_key)` — `HSETNX` on `<seq>` alone would happily accept the
same explicit key at two different positions, which is precisely the collision
§2.2 promises to reject.

**The commit is one Lua script, not `HSETNX` plus `MULTI`.** `MULTI` is not
conditional: it cannot check a constraint and abandon the write when the check
fails, so a rejected commit would still have moved `__total`, and two concurrent
commits could each read a total under the cap and both write. Lua's
single-threaded execution gives the conditional the transaction cannot, in one
round trip — the same reason `enqueue_debounced` decides slide-versus-insert in
a script (three of them, `redis_backend/jobs/enqueue.rs`).

The script, in order: verify the claim still names `owner` (§1.4) · reject a
taken `<seq>` unless the stored document is byte-identical, in which case return
`AlreadyCommitted` (§9.2) · reject a taken `k:<step_key>` · reject when
`HLEN`-derived count would exceed `max_steps` or `__total + result_len` would
exceed `max_total_bytes` · only then write the field, the `k:` index and the new
`__total` together. Every rejection leaves the hash exactly as it found it.

Deleted with `DEL` in the terminal write. A **defensive TTL** covers a crash
between the terminal write and the delete — which is also why Redis needs no
SCAN sweep, unlike `job_errors` (`redis_backend/jobs/errors.rs:110`).

**The TTL is sized from the sleep, not from a constant.** A flat 7-day TTL
refreshed only on commit expires the snapshot of a job sleeping for thirty days,
and the wake attempt then re-runs every committed step — the exact failure the
feature exists to prevent, arriving silently and only for long sleeps. So every
commit sets the TTL to `max(now + 7 days, wake_at + 7 days)`, where `wake_at` is
the latest deadline in the hash. The grace period is the same on both arms, so a
job that never sleeps is unaffected.

Diesel backends need none of this: rows have no TTL, and the terminal write is
their only deletion path.

### 10.3 What this costs `flexiq-workflows`

**Nothing.** D1 means `redis_store.rs` — 1 092 lines with its own key space —
does not change, and neither does `diesel_common.rs`. That is the concrete
payoff of deciding that nodes and steps are nested rather than parallel: the
expensive half of the "shared table" option was always the Redis workflow store,
and this design never touches it.

---

## §11 Contract, semver, and rollout

### 11.1 `CONTRACT_VERSION` → 2 (D12)

The expand-only rule (`BINDING_CONTRACT.md:490`) is satisfied at the schema
level: `job_steps` is a new table and nothing existing changes meaning, so an
older build reads every row it read before.

It is **not** satisfied at the behaviour level. An older worker that claims a
job with committed steps cannot read them and re-runs every one. Silent double
execution during a rolling upgrade is exactly the failure the floor exists to
prevent, so:

- `CONTRACT_VERSION` and `MIN_CONTRACT_VERSION` move to `2`;
- the step API checks the deployment floor once, at attempt start, and refuses
  with a message naming the floor and how to raise it:
  *"inline steps require every worker at contract ≥ 2; raise `contract:min_sdk`
  once every process is upgraded"*;
- the check reads the floor already loaded at open — no extra query.

The upgrade order goes in #672: upgrade every process, then raise the floor,
then deploy tasks that use steps.

### 11.2 The 2.0.0 (D13)

The repo carries one version in `[workspace.package]`, so this is a repo-wide
major. Everything breaking goes in it, once:

| Break | Where |
|---|---|
| `JobResult::Slept` + `#[non_exhaustive]` | `scheduler/mod.rs:108` |
| `ResultOutcome::Slept` + `#[non_exhaustive]` | `scheduler/mod.rs:169` |
| `Storage::reschedule` gains `namespace` | `storage/traits.rs:133` |
| `CONTRACT_VERSION` 1 → 2 | `contract.rs:23` |

Additive, and therefore *not* breaking: the four `Storage` step methods (all
defaulted), the `job_steps` table, the four protocol frames (both enums are
already `#[non_exhaustive]`), `TaskMiddleware.on_sleep`, `JOB_SLEEPING`.

The semver gate runs at publish time, not on the PR
(`.github/workflows/publish-crates.yml:157`), so a branch will look clean right
up until the release is cut. Version with `node scripts/version.mjs --set 2.0.0`
— never by hand.

---

## §12 Review checklist for the sub-issues

Each sub-issue is reviewed against the decision it implements.

**#665 storage** — D1, D3, D6, D10, D14, §1.4, §10. Table shape, both unique
indexes, and no `status` column · five defaulted methods, `Unsupported` not
empty · every write fenced on the claim's `worker_id`, in the write's own
transaction · an identical re-commit returns `AlreadyCommitted`, not a conflict ·
caps enforced in `record_step_result`, on encoded bytes · deletion wired into
every terminal write (`move_to_dlq` included) and into *neither* `requeue_stuck`
nor the dead-owner reclaim · the Redis commit is one Lua script, with the `k:`
uniqueness index and the `wake_at`-sized TTL · contract suite so the Postgres and
Redis legs exercise all of it, including a commit racing a reclaim.

**#666 memoization** — D2, D4, D5, D6, D7, §2, §3, §5. `flexiq_core::step` as
pure functions · one snapshot read per attempt · `name#occurrence` plus the
explicit key, with keyed calls *not* spending an occurrence — tested keyed,
unkeyed and interleaved · position-by-position check, no fingerprint column ·
divergence is `should_retry = false` · the codec test asserts on the raw stored
bytes.

**#667 sleep** — D9, D11, D14, §7. `sleep_job` is one transaction, not three
calls · `reschedule` gains a namespace · the sleep is a step row with `wake_at`,
and a memo hit only once `now >= wake_at` · a test pinning that a sleeping job is
not stale-reaped, and one that kills the process mid-sleep and checks the next
attempt heals · retry count, retry budget, breaker and metrics all untouched ·
`on_sleep` and the `before`-pairing invariant · contrib middleware updated in all
three shells · the swallow latch.

**#668 idempotency key** — D8, §6. `{run_key}:{step_key}` · `run_key` is the
origin id · `retry_dead` writes `__origin_job_id` on three backends · three
stability tests, including across `retry_dead` · the contrast with the
`idempotent=True` auto-key documented.

**#669 / #670 / #671 shells** — §2.1 (names mandatory, positional), §2.2 (the
mixed keyed/unkeyed rule), §4.2 (the error text), §7.6 (`on_sleep` in contrib),
§7.7 (both swallow layers), §9.2 (a commit the executor could not confirm fails
the attempt), §9.4 (refuse without `CAP_STEPS`).

**#672 docs** — the nesting picture from §1.1 above the fold, §3.5 (what
divergence cannot catch), §4.2 (store it elsewhere, memoize the handle), §7.4
(a sleep costs no retry), §9.3 (a step is a checkpoint, not a loop body), §11.1
(upgrade order).

---

## §13 Out of scope

- **Fixing the workflow node cache** (§1.5). Real, adjacent, separately tracked.
- **Steps on a workflow node's task body.** Works by construction — the node is
  a job — and needs no design. Worth one line in #672.
- **Cross-job steps.** A step belongs to one job. A unit of work that outlives a
  job is a workflow node; that is the rule #672 gives for choosing.
- **Step-level retry policy.** `step.run` runs its closure once per attempt; the
  job's retry policy is the only retry. A per-step policy is a plausible later
  feature and would fit the same rows, but nothing here depends on it.
- **A dashboard step timeline** beyond the "sleeping until …" line of §7.3.
