# WatchJobs — design record (#837)

A server stream on `flexiq.v1.ProducerService` that replaces the `GetJob` poll
loop. Unblocks `fq tail` (#832) and gives the Go client `Wait` /
`EnqueueAndWait`.

## Decisions

**D1. Change source: the event hub, plus a shared re-read.** The issue asked
to reuse either `ResultOutcome` or the core pub/sub rather than add a third
mechanism. The `EventHub` from #848 already carries every transition the
scheduler emits (built from `ResultOutcome`) and the doors' enqueue/cancel. So:

- `flexiq_core::events::EventTap` is an in-process observer on the hub, called
  from `emit` and never blocking. `EventHub::without_sinks()` exists so the gRPC
  role always has a hub, even with no `FLEXIQ_EVENTS_FILE`. The runtime hands
  the same hub to the scheduler.
- The hub only hears **this process**. Another replica, an SDK worker on the
  same database, or a producer-only server with no scheduler would leave a
  watch hanging. The fix is one reconcile task per process: every
  `FLEXIQ_GRPC_WATCH_RECONCILE` seconds (default 5) it reads every id-watched
  job in one `Storage::get_jobs_by_ids` call. That is one blob-free query per
  tick however many clients are watching, not one read per client per poll.
- Rejected: per-backend change feeds (Redis PUBLISH, Postgres NOTIFY). SQLite
  has none, so a re-read is needed anyway, and every write would pay for them.
  They can be added later as a faster path over the same re-read.

User chose this option (2026-09-25).

**D2. Terminal semantics.** An id watch opens with a `SNAPSHOT` per id, read
*after* subscribing to the feed, so nothing emitted during the read is lost.
Events that race the snapshot are ordered by a rank `(attempt, phase)`, and
terminal always wins. A job already terminal sends one item. When every id is
finished the stream ends `OK`.

**D3. Not-found = foreign.** A missing id and another namespace's id both
yield `not_found_job_id`, the same as `GetJob`'s `JOB_NOT_FOUND` position. A
job purged mid-watch (the re-read finds it gone) yields the same.

**D4. `terminal` is the server's call.** `JobTransition.terminal` exists
because `FAILED` is not final while `RETRYING`/`DEAD` is to come, and a client
must not maintain its own terminal-status table.

**D5. Resume.**

- An id watch resumes by reopening; the snapshot covers the gap.
  `resume_cursor` on an id watch is `INVALID_ARGUMENT`.
- A queue watch uses cursors: `(process instance, feed seq)`, base64url,
  opaque. Anything not held by this process is `FAILED_PRECONDITION` /
  `WATCH_CURSOR_EXPIRED`. The client relists and watches from now. This is the
  Kubernetes watch model.
- A queue watch has no re-read, because a queue has no bounded id set.
  Documented as this-process-only.

**D6. Bounds.**

- 100 ids per watch (a wire constant).
- `FLEXIQ_GRPC_WATCH_MAX_PER_TOKEN` (default 16) keyed on `Principal::credential`.
- Ring buffer `FLEXIQ_GRPC_WATCH_BUFFER` (4096) per process.
- `FLEXIQ_GRPC_WATCH_STALL` (30 s): a send that waits longer, or a reader that
  falls out of the ring, ends `RESOURCE_EXHAUSTED` / `WATCH_OVERFLOW`. The final
  status travels beside the item channel, so it is still delivered after the
  items that were queued.
- Shutdown ends every stream `UNAVAILABLE` / `SHUTTING_DOWN`, so the graceful
  listener drain does not wait on them.

**D7. No JSON facade route.** A stream has no request/response mapping. The
facade coverage test exempts streaming RPCs by the descriptor's flag; SSE would
be a separate door.

## Where it lives

- core: `events/tap.rs`, `EventHub::{add_tap, without_sinks}`,
  `Storage::get_jobs_by_ids` (Diesel macro + Redis `MGET`).
- server: `grpc/producer/watch/{feed,cursor,transition,quota,reconcile,stream,mod}.rs`,
  `config/watch.rs`, four new reasons.
- cli: `fq tail` (`commands/tail.rs`, `output/watch.rs`). It reconnects with a
  1–30 s backoff and handles `WATCH_CURSOR_EXPIRED` by following from now.
- go: `watch.go` — `WatchJobs`, `WatchQueue`, `Wait`, `EnqueueAndWait`.
- contract: `REMOTE_SDK_CONTRACT.md`, the "WatchJobs" paragraphs and the
  reason table.
