# Rust docs tier (#920) — authoring brief

Read this before writing a line. Every fact below was read out of
`crates/flexiq/src/**` and `crates/flexiq/tests/**` at `e4340ba7`. Where this
file and the source disagree, **the source is right** — go read it.

Repo root: `/home/ezio/Desktop/Work/personal/taskito-rust-docs`.
Docs root: `docs/`. Content: `docs/content/docs/`.

## The rule that matters

**Never write a Rust symbol you have not seen in the crate.** The crate README's
"Durable steps" and "Periodic tasks" examples are ```` rust,ignore ```` — they do
*not* compile and contain undefined helpers. The known-good sources are:

- `crates/flexiq/examples/quickstart.rs` (runs)
- `crates/flexiq/tests/{queue,worker,steps,periodic,wire_vectors}.rs` (assert)
- `crates/flexiq/tests/ui/*.rs` (the seven things the macro refuses)

If the shell cannot do something, **say so in one sentence and name the escape
hatch** (`queue.storage()`, or `flexiq_core::Worker` directly). Do not invent a
method, do not soften it into "you can also…", and do not tell a Rust reader to
go use another language's SDK — say "the Rust SDK does not wrap this", never
"use the Python SDK".

## The public surface

```rust
use flexiq::{FlexiQ, Outcome, TaskCall, EnqueueOptions, Debounce, Abort,
             WorkerBuilder, Task, StepHandle, current_step, PeriodicSpec};
```

`crates/flexiq/src/lib.rs` is also `pub use flexiq_core::*`, so every core root
export (`Job`, `JobStatus`, `QueueError`, `Storage`, `StorageBackend`,
`TaskError`, `Worker`, `WorkerHandle`, `SchedulerConfig`, `TaskConfig`,
`RetryPolicy`, `RateLimitConfig`, `QueueStats`, `PeriodicTask`, `StepLimits`, …)
is reachable as `flexiq::X` too.

### `FlexiQ` — the queue handle (`src/queue.rs`)

```rust
FlexiQ::open(path: &str) -> Result<Self>          // sqlite file
FlexiQ::in_memory() -> Result<Self>
FlexiQ::from_storage(storage: StorageBackend) -> Result<Self>
.with_namespace(impl Into<String>) -> Self
.namespace() -> Option<&str>
.storage() -> &StorageBackend                     // the escape hatch
.enqueue<T: Task>(call: TaskCall<T>) -> Result<Job>
.enqueue_batch<T: Task>(calls: Vec<TaskCall<T>>) -> Result<Vec<Job>>
.worker() -> WorkerBuilder
.cancel(job_id: &str) -> Result<bool>
.request_cancel(job_id: &str) -> Result<bool>
.get_job(job_id: &str) -> Result<Option<Job>>
.list_jobs(limit: i64, offset: i64) -> Result<Vec<Job>>
.stats() -> Result<QueueStats>
.list_periodic() / .delete_periodic(name) / .pause_periodic(name) / .resume_periodic(name)
```

`open`/`from_storage` call `ensure_contract_supported` — no other shell does this
at open.

### `#[flexiq::task]` (`crates/flexiq-macros`)

Accepted attributes, and nothing else (an unknown key is a compile error that
lists the accepted set): `name`, `queue`, `priority`, `max_retries`,
`retry_backoff_ms`, `retry_max_delay_ms`, `timeout`, `expires`, `result_ttl`,
`idempotent`, `on_excess`, `max_concurrent`, `max_in_flight_per_task`,
`rate_limit`, `retry_budget`, `cron`, `timezone`.

Durations: `500ms` / `30s` / `5m` / `2h` / `1d`, or a bare integer meaning ms.
`rate_limit`/`retry_budget`: `"<count>/<s|m|h>"`, validated at compile time.

Expands to a zero-sized `struct <name>` plus `<name>::call(args…) -> TaskCall<_>`
and `<name>::run(args…)` (the original body, directly callable in a unit test).
Default task name is the **function name**, not the module path.

Refused at compile time: `async fn`, `unsafe fn`, generics, variadics, `self`,
destructuring parameter patterns, a `cron` task that takes parameters,
`timezone` without `cron`, a zero `rate_limit`, an unparseable duration.

### Enqueue options — on both `EnqueueOptions` and `TaskCall<T>`

`.queue(_)`, `.priority(i32)`, `.delay_ms(i64)`, `.max_retries(i32)`,
`.timeout_ms(i64)`, `.unique_key(_)`, `.idempotent()`, `.metadata(_)`,
`.notes(_)`, `.depends_on(iter)`, `.expires_in_ms(i64)`, `.result_ttl_ms(i64)`,
`.namespace(_)`, `.debounce_ms(key, window_ms, max_wait_ms)`, `.debounce(Debounce)`.

Defaults: queue `"default"`, priority 0, max_retries 3, timeout 300_000 ms.
An explicit `.unique_key(..)` beats `.idempotent()`.
`.idempotent()` derives `auto:` + `sha256(name || 0x00 || payload)[..32]` — the
cross-SDK key, pinned by `tests/queue.rs`.

### Worker (`src/pool.rs`)

```rust
queue.worker()
    .register::<charge>()
    .queues(["billing", "default"])
    .num_workers(2)
    .worker_id("w-1")
    .scheduler_config(SchedulerConfig { ..Default::default() })
    .spawn() -> Result<WorkerHandle>          // .shutdown() -> Result<()>
```

`spawn()` refuses a duplicate task name, and refuses **any** periodic on a
namespaced worker. `WorkerBuilder` has **no** `on_outcome` and **no**
`queue_config` — those are on `flexiq_core::Worker` only.

### Durable steps (`src/steps.rs`, `src/task.rs`)

```rust
let mut step = flexiq::current_step();
let receipt: Receipt = step.run("charge", || Ok(charge(&order)))?;
let receipt: Receipt = step.run_keyed("charge", &order, || Ok(charge(&order)))?;
step.sleep_ms("settle", 60_000)?;
step.sleep_until("settle", wake_at_unix_ms)?;
```

A step body runs once per durable run; on replay the stored bytes decode and the
closure is not called. `sleep_*` **ends the attempt** (`Abort::Sleep`) and does
**not** spend a retry — the body restarts from the top next attempt and the
committed steps are memoised. Calling a step method outside a running task is a
fatal error containing `"outside a running task"`; it never panics.

This is the only shell that fences on `(owner, attempt, epoch)` — it sets
`with_epoch`; Python, Node and Java leave it unset.

### Outcomes and errors (`src/outcome.rs`)

`Outcome<T> = Result<T, Abort>`, `Abort::{Fail(TaskError), Sleep(StepSleep)}`.
`From<TaskError> for Abort` and nothing wider — a task body ends
`.map_err(TaskError::retryable)?` or `.map_err(TaskError::fatal)?`. The choice of
retryable vs fatal is the author's and the macro will not guess it.

A failed Rust task writes the contract's `{"errtype","message","traceback"}` JSON,
so a reader in any other shell gets a structured error.

### Periodics (`src/cron.rs`)

`#[flexiq::task(cron = "0 */5 * * * *", timezone = "Europe/Berlin")]` on a
**zero-parameter** function. Six-field cron, seconds first; `timezone` omitted
means UTC. Registered at `spawn()`; re-registering an unchanged declaration
writes nothing.

**Firing is per-scheduler and not leader-elected** — the dedup key is
`periodic:{name}:{now}` off each process's own clock, so two workers that both
registered it can both fire it. Register periodics from one process.

### The wire (`src/encode.rs`, `src/decode.rs`)

Encoding goes through `flexiq_core::wire::encode_call` — the pinned writer.
Decoding is the shell's own `ciborium` reader; core ships none.
Arguments are **positional only** (Rust has no kwargs); a call carrying kwargs is
refused by count, not silently dropped. Struct fields keep declaration order —
sorting them would break `auto:` keys. The serializer is in **binary** mode
(`is_human_readable() == false`), which is why a `Uuid` round-trips.

## What the Rust shell does NOT have

Say it plainly on the page where a reader would look for it:

| Absent | What to say instead |
| --- | --- |
| Typed result read | `decode_result` is `pub(crate)`. `job.result` is `Option<Vec<u8>>`; the crate's own example prints its length. **Do not write `let sum: i64 = …`.** |
| Middleware | No hook surface. `flexiq_core::Worker::on_outcome` is the nearest thing, and `WorkerBuilder` does not expose it. |
| Pluggable serializers | The payload codec is the wire CBOR, fixed. |
| Events, webhooks | Not wrapped. |
| Dependency injection / resources | A task takes its arguments and nothing else. |
| Dashboard | `crates/flexiq-server` serves it; the shell does not. |
| Async tasks | `#[task]` refuses `async fn`; `flexiq_core::Worker::register_async` is the escape hatch. |
| Workflows, mesh | Feature-gated re-exports (`features = ["workflows"]`, `["mesh"]`) of separate crates, not a shell API. |
| Prefork, streaming results | Neither exists. |
| Namespaced periodics | Refused on a namespaced handle — see #918. |
| Debounce in a batch | `enqueue_batch` refuses a call carrying a debounce window. |
| Admin reads (`list_dead`, `requeue_stuck`, `stats_by_queue`, `get_job_steps`) | Not on `FlexiQ`. Reach them through `queue.storage()`. |

## House style

From `docs/README.md` and the three existing tiers:

- Frontmatter is `title` + `description` only. `description` is one sentence,
  quoted, usually an em-dash list of what the page covers.
- Semi-casual, code-first. Show the code, then explain. Short sentences.
- Sentence-case headings below the H1. No "Prerequisites", no "What you'll
  learn", no summary section.
- 1–2 sentences between code blocks, not paragraphs.
- Cross-links inside a tier use `<SdkLink to="guides/core/tasks">…</SdkLink>`
  (no leading slash, no SDK prefix — it resolves per active SDK).
  `<Callout type="info|warn|error">`, `<Cards>`/`<Card>`, `<Tabs items={[…]}>`
  are globally registered; no imports in MDX.
- Markdown tables: escape a union pipe as `int \| None`.
- **Never name another SDK** in a Rust page. "The Rust SDK does not wrap this",
  not "unlike Python".

## Shared-tree tabs

`content/docs/shared/**` fans out to `/rust/**`. Every `<CodeTabs>` block needs a
fourth panel, exactly:

```mdx
  <Tab sdk="rust">

```rust
// …
```

  </Tab>
```

Blank line after the opening tag and before the closing one — match the
surrounding blocks byte for byte. Put the rust tab **last**, after `java`.

Where the Rust shell has no equivalent, the panel is still there and says so in
one line, with the nearest real thing:

```mdx
  <Tab sdk="rust">

The Rust SDK has no middleware hooks. The nearest seam is
`flexiq_core::Worker::on_outcome`, which `WorkerBuilder` does not expose — build
the worker through `flexiq_core::Worker` directly if you need it.

  </Tab>
```

Never `data-parity-exempt`: an exempt block renders an **empty panel** under
`data-sdk="rust"`.
