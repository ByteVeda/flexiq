# #719 — a lease token on every dispatched job

Part of #710. Lands before the gRPC executor door (#720) starts issuing jobs, so
the lease is present from that package's first release rather than retrofitted
into it.

## The hole

`ExecutorMessage::Success` and its siblings name a job and nothing else. An
executor that stalls past the reaper's patience has its job requeued and handed
to someone else — and then finishes, and writes its result over the new owner's.

Half the fence already existed, built for durable steps:
`Storage::authorize_attempt(job_id, owner, attempt, ns)` resolves the claim's
owner and the job's `retry_count`, and `Scheduler::authorize_finished` calls it
with the record it took from `release_in_flight`. Three gaps remained.

1. **`requeue_stuck` moves nothing storage can see.** It flips `Running` →
   `Pending` and deletes the claim without touching `retry_count`
   (`diesel_common/jobs.rs`). The dashboard's requeue button therefore produces
   a second dispatch with the *identical* `(owner, attempt)`, and the stalled
   executor's late result authorizes.
2. **The reaper spends the dispatch record.** `reap_stale` synthesizes a
   failure, which runs `release_in_flight`; the real executor's later frame then
   found nothing to fence against and `authorize_finished` **failed open**.
3. **Nothing on the wire identified a dispatch.** Once a job id had been
   dispatched twice, no frame could be attributed to either.

## What was built

One value — the **claim epoch** — minted by storage, checked in two places.

### The epoch (durable)

`execution_claims.epoch`, nullable, migration `m0016_claim_epoch`. A random
non-negative `i64`, minted on every `claim_execution`, `claim_execution_batch`
and `reclaim_execution` and returned to the caller; those three now answer
`Option<i64>` instead of `bool`. `authorize_attempt`, `record_step_result` and
`sleep_job` take it and compare it when *both* sides have one — a claim written
before the column, and a caller holding no lease, are each an absence rather
than a mismatch (`lease::epochs_agree`).

Random rather than monotonic: the comparison is equality, so nothing needs
ordering, and two claims of one job inside the same millisecond must not be able
to collide.

Redis keeps the claim in one string, so the epoch rides the timestamp field —
`"{owner}:{claimed_at}.{epoch}"`. Every reader of that value takes the owner as
everything before the *last* `:`, and a new `:` would silently truncate every
owner containing one (`"host:pid"`, pinned by the contract suite). A `.` inside
the final field moves nothing, and the two forms stay distinguishable because a
legacy value's last field is digits with no dot.

### The lease (on the wire)

`Lease` is that epoch rendered base64url — opaque, since the value is random.
It rides the `job` frame and comes back on every executor→scheduler frame that
**settles or advances an attempt**: `success`, `failure`, `cancelled`, `slept`,
`progress`, `task_log`, `step_commit`. `hello` and `heartbeat` carry none.
`ExecutorMessage::with_lease` and `leased_job` match exhaustively on purpose, so
a frame added later has to answer that question in code rather than inherit a
silent `None` from a wildcard.

Negotiated by `CAP_LEASE`, announced by both sides. Core advertises it for every
shell — echoing a lease is entirely `worker/executor.rs`'s work, so a shell
opt-in would only be a way to forget. `CAP_STEPS` stays opt-in because it needs
a job context the shell builds.

### Where it is checked

- **In memory, at the dispatcher.** `LeaseBook` (owned by `Scheduler`, handed to
  the pool by `WorkerDispatcher::set_lease_book`, exactly as `set_claim_owner`
  already hands over the owner) holds the lease of each job's *current*
  dispatch. `remote.rs` and the prefork pool refuse a frame whose lease is not
  the book's entry, at `error!`, and answer a `step_commit` `Superseded` rather
  than let the child wait out its ack timeout.
- **Durably, at the fence.** `authorize_finished` passes the record's epoch to
  `authorize_attempt`, so a result that outlives the book — or reaches another
  process — is still superseded.

### The retired record

`InFlight` now keeps a bounded map of dispatches whose slot has been freed
(`RETIRED_DISPATCHES = 1024`), and `authorize_finished` falls back to it. That
is gap 2: the reaper settles a stalled job, and the executor that was merely
slow reports afterwards.

## What was rejected

- **A separate Redis key for the epoch.** Unambiguous, but it puts a second key
  in every claim path and a fourth `KEYS` entry in three Lua scripts whose
  numbering is load-bearing (`sleep_job_script` passes *KEYS positions* as
  `ARGV` values).
- **A per-dispatch nonce instead of the claim's epoch.** It closes the same
  aliasing without a migration, but leaves two tokens meaning almost the same
  thing, and the in-memory one cannot answer for a result that reaches another
  process.
- **Putting the lease on `Job` or `JobResult`.** 33 and 117 literal sites, and
  both types are storage/reporting shapes that have no business carrying a
  transport concern. `set_lease_book` is the seam that already existed.

## Not covered, and stated rather than hidden

The **in-process pools** (native-async, classic async, `NativeDispatcher`) hand
a `Job` to a thread and get a `JobResult` back, and a `JobResult` names only a
job — so there is nothing to stamp. They keep the `(owner, attempt, epoch)`
fence, which covers a reclaim and a retry but not a requeue. Closing that would
mean widening either `Job` or the dispatch channel.

## Semver

`Storage`'s three claim methods change return type and four take a new
argument; seven `ExecutorMessage` variants and `SchedulerMessage::Job` gain a
field. Breaking, and the issue authorises it — the release-time
`cargo-semver-checks` gate in `publish-crates.yml` is where it surfaces.

## Verified

`cargo fmt --all --check` · `cargo clippy --workspace --all-targets -D warnings`
· `cargo check --workspace` on default / postgres / redis / native-async ·
`cargo test --workspace` · the contract suite against hosted Postgres and Redis
· `uv run pytest tests/` + `ruff` + `mypy` from `sdks/python/`.

Every new guard is mutation-checked — deleting it reds that test and only that
test:

| Guard deleted | Test that goes red |
|---|---|
| the book comparison in `frame_is_current` | `a_result_under_a_lease_that_is_no_longer_current_is_refused`, `a_step_commit_under_a_stale_lease_is_refused_without_waiting` |
| `epochs_agree` → always true | `a_requeued_jobs_earlier_dispatch_is_fenced_out_by_the_epoch`, `test_the_epoch_separates_two_claims_of_one_attempt` |
| the retired-record fallback | `a_result_whose_dispatch_the_reaper_already_settled_is_still_fenced` |

The step test needed sharpening to earn its entry: dispatched under a lease
naming an epoch storage never wrote, it passed with the dispatcher's check
deleted — the *fence* refused it instead. It now dispatches under the live
claim's own epoch, so only the book can refuse it.
