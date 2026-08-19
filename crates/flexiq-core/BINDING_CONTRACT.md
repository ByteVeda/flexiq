# FlexiQ core — language binding contract

What a new language shell (Python today; Node via napi-rs, Java via UniFFI/JNI next)
must implement or call to reuse this Rust core. The core is **binding-agnostic**:
`flexiq-core`, `flexiq-workflows`, `flexiq-mesh` carry **no** `pyo3` dependency
(enforced in CI — see [Invariant](#invariant)). The Python shell lives in
`crates/flexiq-python`; study it as the reference implementation.

## Invariant
The generic crates must never depend on `pyo3` or any language runtime. CI fails if
`pyo3` appears in the normal dependency tree of `flexiq-core`, `flexiq-workflows`,
or `flexiq-mesh` (`cargo tree` is authoritative — you cannot `use pyo3` without
depending on it). Keep Python/Node/Java specifics in the shell.

## The payload is opaque
`Job.payload` and `Job.result` are `Vec<u8>` blobs. The core **never** interprets
them. Each shell serializes args/kwargs at enqueue and deserializes them in the
worker — using whatever serializer it wants. The Python shell defaults to cloudpickle
(Python-only). **Cross-language constraint:** a job enqueued by one language and run
by another requires both sides to use the wire envelope below.

## Wire envelope (cross-SDK payloads)
A wire payload is one tag byte followed by the codec body. The tag records which
codec produced the body, so any shell can dispatch a decoder (or reject clearly)
without out-of-band configuration:

| Tag    | Body        | Cross-SDK | Notes |
|--------|-------------|-----------|-------|
| `0x00` | native      | **never** — reject with a clear error | Language-native codec (e.g. pickle). Same-language producer/consumer only. |
| `0x01` | msgpack     | optional  | Legacy tagged format; shells MAY read it, SHOULD NOT write it cross-SDK. |
| `0x02` | CBOR (RFC 8949) | **default** | The cross-SDK wire format. |
| `0x03` | reserved    | —         | Tagged JSON (not yet specified). |
| `0x04+`| reserved    | —         | Future (protobuf, …). |

Untagged payloads predate the envelope and are same-SDK legacy; a shell MUST NOT
assume any tag discipline unless the task is configured with a tagged serializer
on both sides. (Sniffing is unsafe: raw msgpack/CBOR bodies can begin with any
byte value.)

**Why CBOR over JSON**: integers survive — JS `Number` is exact only to 2^53−1
while other languages carry 64-bit/unbounded ints; CBOR bignums round-trip them
losslessly. IANA tags also round-trip datetimes (tag 0/1), decimals (tag 4), and
byte strings without a hand-rolled registry. Mature codecs exist everywhere
(`cbor2`, `cbor-x`, `jackson-dataformat-cbor`, `fxamacker/cbor`, …).

**Call body** (`Job.payload`): a 2-element CBOR array `[args, kwargs]` — `args`
an array, `kwargs` a map (empty map when the language has no keyword arguments).
Job-scoped extras belong in the `metadata`/`notes` columns, not the payload.
Convention for cross-SDK tasks: prefer a single object argument
(`args = [ {…} ]`, `kwargs = {}`) — it maps cleanly onto every language's
handler-binding model.

**Result body** (`Job.result`): a bare CBOR value (no array wrapper).

**Cross-SDK rules**:
- A shell reading tag `0x00` on a payload it did not produce MUST fail with an
  error naming the tag, not a generic decode error.
- Producer and consumer of a task MUST be configured with the same wire
  serializer; the tag is a self-check, not a negotiation mechanism.
- Delivery-side semantics (retries, DLQ, acks) are unaffected — the envelope is
  purely a payload contract.
- CBOR maps and arrays MUST carry a definite-length header — `a0` for an empty
  map, `80` for an empty array — never the indefinite-length form (`bf ... ff`,
  `9f ... ff`); readers MUST accept both. Integers MUST use the shortest form
  that holds the value. Both forms decode identically, so a divergent writer
  still interoperates — but the automatic `auto:` idempotency key hashes the
  serialized payload, so it would silently stop idempotent enqueues deduping
  across SDKs. Every call body ends in the kwargs map, so the divergence would
  shift every payload's key at once.

**Test vectors** (hex, `0x02`-tagged CBOR):
- call `f(1, "a")`, no kwargs → `02 82 82 01 61 61 a0` — `[ [1, "a"], {} ]`
- result `true` → `02 f5`
- big int `2^53` → `02 1b 00 20 00 00 00 00 00 00`

## Dispatch call sequence
1. Shell constructs `Storage` (SQLite default; `postgres`/`redis` features) — `storage/traits.rs`.
2. Shell constructs `Scheduler::new(storage, queues, SchedulerConfig, namespace)` — `scheduler/mod.rs`.
3. Shell implements `WorkerDispatcher` — `worker/mod.rs`.
4. `Scheduler.run(job_tx)` polls + claims jobs and sends each `Job` over a
   `tokio::sync::mpsc::Sender<Job>` — `scheduler/poller.rs`, `scheduler/mod.rs`.
5. `WorkerDispatcher::run(job_rx, result_tx)` receives the `Job`, deserializes
   `job.payload`, looks the task up by `job.task_name`, runs it, and sends a
   `JobResult` back over a `crossbeam_channel::Sender<JobResult>`.
6. `Scheduler.handle_result(JobResult)` records the outcome in storage and returns a
   `ResultOutcome` — `scheduler/result_handler.rs`.
7. Shell maps `ResultOutcome` to its own events/middleware (Python: `py_queue/worker.rs`).

```
Storage ─▶ Scheduler.run ──tokio::mpsc<Job>──▶ WorkerDispatcher.run
                                                     │ deserialize payload, run task
                              ◀─crossbeam<JobResult>─┘
Scheduler.handle_result ─▶ ResultOutcome ─▶ shell emits events / middleware
```

## What a shell MUST implement
### `WorkerDispatcher` — `worker/mod.rs`
| Method | Signature | Required |
|--------|-----------|----------|
| `run` | `async fn run(&self, job_rx: tokio::sync::mpsc::Receiver<Job>, result_tx: crossbeam_channel::Sender<JobResult>)` | yes |
| `shutdown` | `fn shutdown(&self)` | yes |
| `notify_cancel` | `fn notify_cancel(&self, job_id: &str)` | optional — in-process pools may no-op; out-of-process (prefork) must deliver a side-channel signal |

Channels: inbound `tokio::sync::mpsc::Receiver<Job>` (async); outbound
`crossbeam_channel::Sender<JobResult>` (sync, cloneable).

## Worker frame protocol (out-of-process executors) — `worker/protocol.rs`

A dispatcher that runs tasks in another process speaks this format over its
stream. The same format serves a pipe (the prefork pool's stdio children) and a
socket, so an executor written in any SDK attaches to any scheduler.

A frame is a JSON header line, then exactly the number of raw payload bytes the
header declares:

```
{"type":"job","id":"018f…","task_name":"resize","payload_len":7,…}\n
<7 raw bytes>
```

The blob is **not** base64-encoded — the bytes on the wire are the wire-envelope
bytes of the section above, unchanged. `MAX_HEADER_BYTES` (64 KiB) and
`MAX_PAYLOAD_BYTES` (64 MiB) bound a desynced or hostile peer.

| Frame | Direction | Payload |
|---|---|---|
| `hello` | executor → scheduler | `{executor_id, sdk, version, tasks[], slots, protocol_version, token?}` |
| `hello_ack` | scheduler → executor | `{scheduler_id, protocol_version, capabilities[]}` |
| `heartbeat` | executor → scheduler | `{free_slots}` |
| `job` | scheduler → executor | `{id, task_name, payload_len, retry_count, max_retries, queue, timeout_ms, namespace, disabled_middleware[], metadata}` + blob |
| `cancel` | scheduler → executor | `{job_id}` |
| `shutdown` | scheduler → executor | — |
| `progress` | executor → scheduler | `{job_id, progress}` |
| `task_log` | executor → scheduler | `{job_id, task_name, level, message, extra_len}` + blob |
| `success` | executor → scheduler | `{job_id, result_len, task_name, wall_time_ns}` + blob |
| `failure` | executor → scheduler | `{job_id, error, retry_count, max_retries, task_name, wall_time_ns, should_retry, timed_out}` |
| `cancelled` | executor → scheduler | `{job_id, task_name, wall_time_ns}` |

Rules:
- `hello` is the first frame on every connection; no `job` may precede its ack.
- Both sides announce `protocol_version` and both reject a mismatch. A version
  is never silently downgraded. The scheduler sends `hello_ack` even when it is
  rejecting, so both ends can log both versions.
- `result_len: null` means the task returned nothing; `0` means it returned an
  empty value. They are distinct. `extra_len` follows the same rule.
- `should_retry` is the executor's decision — only it can see the exception. The
  core never inspects one.
- **Optional behaviour is negotiated, not versioned.** `capabilities` lists what
  the scheduler will do on an executor's behalf; `side_channel` means it applies
  `progress` and `task_log` frames to storage. An executor sends neither frame
  unless it was advertised. Adding one never bumps `protocol_version`, which
  would force scheduler and executors to upgrade together.
- **An unknown frame type is skipped, not fatal.** A reader that cannot name a
  frame reads its declared payload length, discards that many bytes, logs once
  and continues — the stream stays aligned, and a session keeps its in-flight
  jobs. A frame type whose name *is* known but whose header will not parse stays
  an error: that is a desync, not a newer peer.
- **A new frame type must declare its payload length as `payload_len`.** It is
  the only field a peer that predates the frame can find. `result_len` and
  `extra_len` predate this rule and are read as equivalents.
- **The scheduler compares task registries across attached executors.** It
  fingerprints each executor's `tasks[]` on attach — sorted, de-duplicated — and
  warns when one advertises a set no live peer has, naming the difference. This
  is the safety net for a registry that is *discovered* rather than declared: an
  unregistered task name is a fatal, non-retryable failure, so a worker that
  imported part of its task tree dead-letters everything for the rest in
  silence. Nothing extra rides the frame — `tasks[]` is already there and is
  what dispatch routes by, so a fingerprint sent alongside it could only be a
  second copy of the same fact, free to disagree. An executor written without
  this core gets the check for free. A mismatch is **never** a reason to refuse
  an attach: two registries may differ on purpose, and rejecting would turn a
  diagnostic into an outage. An executor advertising no tasks takes no part in
  the comparison.
- `progress` and `task_log` are fire-and-forget: they carry no reply, never
  settle a job, and a scheduler drops one naming a job the sender is not
  running. `progress` is 0–100; a value outside that range is never written —
  SDK boundaries clamp where they can, and the scheduler drops what still
  reaches it rather than failing the job over a progress report.

### Registry fingerprint

One short, comparable value for "what tasks does this peer know how to run". The
scheduler derives it from an executor's `tasks[]` to make the comparison above;
an **in-process** worker, which never speaks this protocol, writes it to
`workers.registry_fingerprint` instead, where the dashboard compares it across
the fleet. Both ends have to produce the same string for the same registry, so
the algorithm is contract material even though it never rides a frame.

64-bit FNV-1a over the sorted, de-duplicated names, each **length-prefixed**,
rendered as sixteen lowercase hex digits:

```
h = 0xcbf29ce484222325
for name in sorted_unique(names):                # sorted by UTF-8 bytes
    for byte in be_u64(len(utf8(name))) + utf8(name):
        h = (h ^ byte) * 0x100000001b3           # mod 2^64
```

| Registry | Fingerprint |
|---|---|
| `[]` | *none* — nothing to compare against; the column stays null |
| `["a"]` | `e6017d3a248deb69` |
| `["invoices.send", "reports.build"]` | `fafd30ef8ebcb7de` |
| `["reports.build", "invoices.send", "reports.build"]` | `fafd30ef8ebcb7de` |
| `["ab", "c"]` | `fe4b6261eea66aa8` |
| `["a", "bc"]` | `e6b0607a88120c30` |
| `["a\nb"]` | `068365c3a2f19d9f` |
| `["a", "b"]` | `9dbd0e0e67e641dc` |

The last four rows are two pairs, and they are the ones worth asserting: each
pair collides under an encoding that concatenates or separates instead of
length-prefixing. Every choice here follows from the same rule as the JSON
headers — an executor must be writable in an SDK's standard library alone:

- **Not cryptographic.** A collision costs a missed warning, never a wrong
  dispatch, so requiring SHA-2 would buy nothing and cost a dependency.
- **Sorted by UTF-8 bytes**, stated rather than implied: JavaScript's
  `Array.prototype.sort` and Java's `String.compareTo` order by UTF-16 code
  units, which disagrees with byte order above the BMP.
- **De-duplicated**, so registering a name twice cannot change the answer.
- **Length-prefixed, not separated.** Any separator can also occur *inside* a
  task name: with a trailing `\n`, `["a\nb"]` and `["a", "b"]` hash the same
  bytes, so two different registries share a fingerprint and the divergence the
  value exists to catch is silently suppressed. A fixed-width length makes the
  encoding injective, and eight big-endian bytes are something every standard
  library can produce.

**An empty registry has no fingerprint.** "Registered nothing" and "does not
report one" are both nothing to compare against, and giving the empty set a value
would make a deliberately inert worker look divergent from every real one. The
same rule holds in storage: the column is null, never `""`.

**Never a gate.** Two registries may differ on purpose — one worker serves
`email`, another serves `video` — so a mismatch is a diagnostic, and refusing an
attach or a registration over one would turn it into an outage. The `workers`
table does not compare rows at all for that reason; the dashboard surfaces the
comparison, and the scheduler makes it only where a fleet is meant to be
interchangeable.

## Task errors (structured, cross-SDK)
When a task raises, the shell reports the failure as a **canonical JSON object**
serialized into `JobResult::Failure.error` (and thus into `jobs.error`,
`job_errors.error`, `dead_letter.error` — the storage layer never interprets it):

```json
{"errtype": "ValueError", "message": "bad value 42", "traceback": ["...frame...", "..."]}
```

- `errtype` — the exception's class name, as idiomatic per language (qualified
  where the language has a notion of it). Required.
- `message` — the human-readable message, verbatim (keeps `error_like`
  substring filters useful). Required, may be empty.
- `traceback` — array of strings, best-effort per shell; `[]` when the
  language/runtime can't provide frames. Required key.

**Fallback rule (readers)**: an error string that does not parse as a JSON
object with a `message` key is a plain legacy/system string and MUST be
surfaced as-is. Core-generated maintenance errors (timeouts, worker-death
recovery, expiry, cancellation) remain plain strings by design.

**Retry semantics**: `retry_on`/`dont_retry_on`-style filtering matches on the
live exception object before formatting — the stored string never drives retry
decisions.

**Test vector** (assert byte-exact in each shell's formatter):
input errtype `BoomError`, message `it broke`, traceback `["frame1", "frame2"]` →
`{"errtype":"BoomError","message":"it broke","traceback":["frame1","frame2"]}`
(JSON with those three keys in that order, no extra whitespace).

## Topic pub/sub (cross-SDK)
A subscription (`topic_subscriptions`) routes a topic's publishes to a subscriber.
Its `mode` column selects the delivery model; the core (`pubsub::publish_to_topic`)
owns both, so a shell only marshals arguments.

| `mode` | On publish | Consumption |
|--------|-----------|-------------|
| `fanout` (default) | one ordinary `jobs` row per active subscriber | the normal dequeue/dispatch path — each delivery is a job |
| `log` | one `topic_messages` row for the whole publish (O(1)) | the shell **pulls**: `read_topic_messages` → process → `ack_topic_cursor` |

A topic may mix both: a publish writes the log row once **and** fans out to any
fan-out subscribers.

**Fan-out `unique_key` salting** — the `jobs` unique index is global, so a shell
that keys a publish MUST salt the key per subscriber or all but one delivery is
deduped away. Salt = `<key>::<topic_len>:<name_len>:<topic><name>` (length
prefixes make it injective). Done in the core; a shell that builds `NewJob` rows
itself must match it.

**Log cursor rules** (`read_topic_messages`/`ack_topic_cursor`):
- Both are **log-subscription only** — a `fanout` subscription (even on a mixed
  topic) reads nothing and acks nothing (`false`). Enforced by a `mode = "log"`
  filter on both backends, so a shell can't accidentally leak the log to a
  fan-out subscriber.
- The **cursor is an opaque, monotonic token** — a shell stores and passes back
  the message `id`, never parses it. Its format differs by backend (UUIDv7 on the
  Diesel backends, a `<ms>-<seq>` stream id on Redis), like the `get_task_logs_after`
  cursor note.
- **Reads are exclusive** of the cursor and ordered oldest-first; the cursor is
  resolved server-side from the subscription row.
- **Ack is a high-water mark**: acking id `X` acks every message `≤ X`. Monotonic —
  acking an older/equal id is a no-op (returns `false`). Like a Kafka offset commit,
  the caller is trusted to pass back an id it actually read.
- Delivery is **at-least-once**: a consumer that reads but dies before acking
  re-reads those messages. The cursor read has no per-message ack (see below).
- **Retention** is min-cursor compaction: a message is dropped once every log
  subscriber on its topic has acked past it (Diesel deletes `id <= min(cursor)`;
  Redis `XTRIM MINID`). A topic with an unread subscriber keeps its backlog.
  Both backends also honor an optional per-message `expires_at` as a TTL safety
  net (Diesel deletes expired rows; Redis `XDEL`s expired stream entries), so a
  stalled or unread cursor can't block reclamation forever.

**Topic registry (declared topics)** (`declare_topic`/`get_topic`/`list_declared_topics`):
- By default a log message is stored only when a `log` subscription already exists
  at publish time (the **late-join boundary**). `declare_topic(name, "log",
  retention_ms)` records the topic so its publishes are retained **even with zero
  subscribers** — a consumer that subscribes later still reads them. Declaration is
  an idempotent upsert on `name`; re-declaring updates `retention_ms` and preserves
  `created_at`. `mode` is `"log"` (the only declarable mode today).
- `retention_ms` bounds a **sub-less** backlog: `publish_to_topic` stamps
  `expires_at = now + retention_ms` on the stored message when the topic has no live
  log subscriber, so the retention sweep reclaims it. Once a log subscriber exists,
  min-cursor compaction governs and the registry lookup is skipped (no extra query
  on the log-subscriber hot path). A shell only marshals `declare_topic`; the core
  owns the publish-time decision.
- Storage: the Diesel backends use a `topics` table (migration `m0006`); Redis uses
  a `topics` hash keyed by name. A shell surfaces `Topic { name, mode, retention_ms,
  created_at }`.
**Per-message ack** (`lease_topic_messages`/`ack_message`/`nack_message`):
- An opt-in **consumption choice** on a `log` subscription (not a registration
  flag): instead of the cursor read, a consumer *leases* messages and acks/nacks
  each individually, so a poison message never blocks its siblings.
- `lease_topic_messages(topic, sub, limit, visibility_ms, now)` returns up to
  `limit` **available** messages oldest-first — never delivered, or a prior lease
  that expired (`now`-relative) or was nacked and never acked — and (re)leases
  each for `visibility_ms`. In-flight (leased, un-expired) messages are skipped.
- `ack_message` ends a delivery (never redelivered); `nack_message` makes it
  available immediately (vs waiting out the timeout). Both return `false` when
  there is no un-acked delivery. An un-acked lease that times out is redelivered.
- Delivery state lives per `(subscription, message)`: Diesel `topic_deliveries`
  table (migration `m0007`); Redis a `pmdeliv:<topic>:<sub>` hash mirroring it.
- **Retention**: on a topic consumed purely per-message (every log sub has
  acked), a message is compacted once every per-message subscriber has acked it;
  a topic that mixes in a cursor subscriber falls back to `expires_at`. Its
  delivery state is dropped with it (Diesel deletes the rows, Redis `HDEL`s the
  fields). Both backends implement this — Diesel via `topic_deliveries`, Redis by
  scanning the `pmdeliv:*` hashes during the purge sweep. A shell only marshals
  the three calls; the core owns the state.

**Test vector** (assert byte-exact in each shell that salts keys itself):
key `evt-42`, topic `orders`, subscription `email` →
`evt-42::6:5:ordersemail`.

## Webhook subscriptions (cross-SDK)
Webhook delivery is a shell concern, but the subscriptions live in the shared
settings KV store, so a queue driven by more than one shell must agree on the
layout or each shell sees only its own hooks.

- **Key** `webhooks:subscriptions` — a single JSON **array** holding every
  subscription. Not one key per hook.
- **Row fields** (snake_case; timestamps Unix ms; timeout in **seconds**):
  `id`, `url`, `events[]` (empty = all), `task_filter` (`null` = all tasks),
  `headers{}`, `secret` (`null` = unsigned), `max_retries`, `timeout_seconds`,
  `retry_backoff`, `enabled`, `description`, `created_at`, `updated_at`.
- **Retry curve**: the Nth wait is `retry_backoff ** N` seconds, N counted from
  zero — default `2.0` gives 1s, 2s, 4s.
- **Tolerant reads, lossless writes**: a shell keeps a row whose fields or event
  names it does not model, and MUST carry those fields through when it rewrites
  the array — every mutation rewrites the whole list, so dropping them would
  destroy another shell's configuration.
- **Delivery log** is separate: key `webhooks:deliveries:<subscription_id>`,
  one JSON array per subscription, newest last.

## Effective retention (cross-SDK)
Retention windows live in `SchedulerConfig`, inside the worker process — a
dashboard elsewhere cannot see them. So the elected retention leader publishes
what it applies to the settings KV on every cleanup sweep, and every shell's
dashboard echoes that document instead of guessing at the defaults.

- **Key** `retention:effective:<namespace>` (unnamespaced queues use `default`).
  The `retention:` prefix is **reserved** (see below), so the published policy
  cannot be spoofed through a dashboard's generic KV endpoints.
- **Document** (snake_case; windows in **milliseconds**, `null` = keep forever):
  `enabled`, `defaulted`, `namespace`, `reported_at` (Unix ms), and `windows` with
  `archived_jobs_ttl_ms`, `dead_letter_ttl_ms`, `task_logs_ttl_ms`,
  `task_metrics_ttl_ms`, `job_errors_ttl_ms`.
- **Only the leader writes it** — a peer's config does not govern the deletes.
  `reported_at` is rewritten every sweep, so it doubles as proof a leader is
  still enforcing the policy.
- **Absent = unreported**, not "retention off": no leader has swept yet. A shell
  surfaces that state distinctly; `enabled: false` is what "off" looks like.
- Shells read it through `scheduler::retention::read_effective_retention_json`
  rather than parsing the key themselves.

## Retention dry-run (cross-SDK)
A read-only preview of what a purge would delete *now*, so an operator can size a
window without deleting anything. Computed in-process against live storage
(unlike the echo above, which is published by a worker), so it always answers.

- Shells call `scheduler::retention::dry_run_json(storage, retention, result_ttl_ms,
  namespace, now)` for explicit windows, or `dry_run_reported_json(storage,
  namespace, now)` to follow the policy the elected cleaner published (falling
  back to the recommended defaults when unreported). The public surface is
  `dry_run_retention()` (Python) / `dryRunRetention()` (Node/Java), each
  accepting optional candidate windows (an empty config = a disabled policy).
  **No-candidate semantics**: a shell whose queue handle carries retention
  config previews that config; a shell where retention is a worker-only option
  previews the *reported* policy — the one that actually governs the deletes.
- **Document** (snake_case; windows in **milliseconds**, `null` = keep forever):
  `enabled`, `defaulted`, `namespace`, `reference_time` (the Unix-ms `now` the
  snapshot was taken at), `windows` (same fields as the echo), `counts` with
  `archived_jobs`, `dead_letter`, `task_logs`, `task_metrics`, `job_errors`, and
  `total`.
- **Counts mirror the purge predicates exactly** (`Storage::count_expired_rows`):
  `archived_jobs`/`dead_letter` always include per-entry-TTL-expired rows plus the
  global window when set; the side tables count only when their window is set.
- **Point-in-time snapshot**: nothing is deleted, and a later purge may differ as
  rows age past their cutoffs. On a freshly-upgraded Redis archive, pre-existing
  per-entry rows may be under-counted until backfill indexes them (the same rows
  the purge indexes lazily).
- **Dashboard** `GET /api/retention/dry-run` returns the same document. The
  dashboard process previews its own (default) windows, which may differ from a
  worker running configured windows elsewhere.

## Reserved settings prefixes (cross-SDK)
The settings KV also backs auth state, webhook subscriptions, and the retention
document above. None of it belongs on the dashboard's generic key/value surface:
reads leak credentials, writes spoof a published policy.

- `settings::RESERVED_SETTING_PREFIXES` is the canonical list, exported by every
  binding (`reserved_setting_prefixes()` / `NativeQueue.reservedSettingPrefixes()`).
  A shell's settings API MUST derive its hide list from it rather than
  hardcoding one — a prefix missed in one shell reopens the hole for all of them.
- A key under a reserved prefix is **absent** to that API: never listed, read,
  written, or deleted through it. The runtime's own readers and writers use the
  `Storage` settings methods, which stay unrestricted.

## Contract level and the floor (cross-SDK)
A deployment outlives individual SDK releases, so the storage carries the lowest
level a process may speak and still join it.

- `contract::CONTRACT_VERSION` is the revision of this shared contract a build
  implements. Bump it only in the change that makes an older build unable to
  read what a newer one writes; an additive change keeps the level (see the
  expand-only rule below).
- The floor lives in the settings KV at `contract:min_sdk`, under the reserved
  `contract:` prefix so no dashboard can read or spoof it.
- **Every shell MUST call `ensure_contract_supported(&storage)` once, on the
  single native open**, before the process does anything else. The check is
  read-only — an unset floor is the permissive default, so a deployment that
  never raises the dial carries no row for it, while a value that will not parse
  is an error rather than a fallback — and fails with
  `QueueError::ContractTooOld`, naming both levels, when this build is below it.
  A *newer* build is always allowed: the floor is a minimum, not the equality
  check the worker handshake uses.
- An attached executor has no storage of its own, so it has no floor to check;
  the scheduler it attaches to was already held to one.
- Raising the floor is an operator's act (`set_min_contract`), done once every
  process is upgraded. A level the writing build cannot itself speak is
  rejected, since that write would lock the operator out of their own storage.

## Applying the schema (cross-SDK)
Opening applies pending migrations by default. A shell MUST also expose the
gated path, for a deployment whose credentials do not permit DDL at runtime:

- an **open option** (`auto_migrate` / `autoMigrate`, default on) that selects
  `SqliteStorage::unmigrated` / `PostgresStorage::unmigrated` instead of the
  migrating constructors — and the matching unmigrated workflow store, or the
  first workflow call would apply the DDL the operator withheld;
- a **`migrate()` method** over `StorageBackend::migrate`, returning the applied
  core versions, the applied workflow versions, the rows the one-time backlog
  sweep archived, and `schemaless` for a backend that has no schema at all;
- a **`migrate` CLI command** that opens *unmigrated* and calls it, so the
  command itself is the only thing that applies DDL.

The contract floor is checked at open, but reading it needs the settings table.
A shell therefore checks it whenever `auto_migrate` is on **or**
`StorageBackend::is_migrated()` reports an existing schema — the only storage
exempt is one that was never migrated, which answers no query anyway.
`StorageBackend::migrate` performs the check itself once the DDL has run, so the
gated path is covered by the one method every shell already calls.

## Schema evolution is expand-only (cross-SDK)
One database is read by shells at different versions during any rolling upgrade,
so a schema change MUST stay readable by the version already running.

- **Add, never repurpose.** New columns are nullable or defaulted; an existing
  column keeps its meaning, its type, and its units forever. Renaming a column
  is an add plus a later drop, never a rename.
- **Drop only after a level bump.** A column no longer written may be dropped
  once `CONTRACT_VERSION` has moved past the last build that read it and the
  floor has been raised to match — three separate releases, not one.
- **Readers tolerate absence.** A field a peer never wrote reads as null and
  MUST NOT fail the read; that is how a worker registered by an older shell
  reports no `sdk_version` rather than a wrong one.
- **The wire envelope follows the same rule** — a reader accepts both the old
  and the new encoding for as long as any supported build emits the old one.

## Types the shell produces / consumes
- **`Job`** — `job.rs`. Fields incl. `id`, `queue`, `task_name`, `payload: Vec<u8>` (opaque),
  `status`, `priority`, `retry_count`, `max_retries`, `timeout_ms`, `unique_key`,
  `metadata`, `notes`, `cancel_requested`, `namespace`. Timestamps are Unix ms.
- **`JobResult`** — `scheduler/mod.rs`. Enum: `Success { result: Option<Vec<u8>>, … }`,
  `Failure { error, retry_count, max_retries, should_retry, timed_out, … }`,
  `Cancelled { … }`. The shell builds this from task execution.
- **`ResultOutcome`** — `scheduler/mod.rs`. Enum the core returns: `Success`, `Retry`,
  `DeadLettered`, `Cancelled`. The core decides retry-vs-DLQ from the retry budget; the
  shell only dispatches events/middleware off it. Every variant carries `wall_time_ns`
  (execution time as the worker measured it, 0 when nothing measured the run) so the shell
  can report a duration without timing the job itself.
- **`SchedulerConfig`** — `scheduler/mod.rs`. Poll interval, `batch_size`, aging,
  reap/cleanup/periodic intervals, `result_ttl_ms`, DLQ auto-retry policy.

## `Storage` trait surface the shell calls — `storage/traits.rs`
Grouped by concern (enumerated, not exhaustive — read the trait):
- **Enqueue / dequeue**: `enqueue`, `enqueue_batch`, `enqueue_unique`, `dequeue`,
  `dequeue_from`, `dequeue_batch`, `dequeue_batch_from`.
- **Completion / retry**: `complete`, `retry`, `reschedule`, `get_job`, `list_jobs`.
- **Exactly-once claims**: `claim_execution`, `complete_execution`, `list_claims_by_worker`.
- **Worker lifecycle**: `register_worker`, `heartbeat`, `unregister_worker`, `reap_dead_workers`.
- **Cancellation**: `request_cancel`, `is_cancel_requested`, `mark_cancelled`.
- **Resilience**: `try_acquire_token` (rate limit), `count_running_by_task`, `stats_by_queue`.
- **Dead-letter**: `move_to_dlq`, `list_dead`, `retry_dead`.
- **Topic pub/sub**: `register_subscription`, `list_subscriptions_for_topic`, `unsubscribe`,
  `publish_message`, `read_topic_messages`, `ack_topic_cursor`, `topic_log_stats`,
  `declare_topic`, `get_topic`, `list_declared_topics`, `lease_topic_messages`,
  `ack_message`, `nack_message` (see
  [Topic pub/sub](#topic-pubsub-cross-sdk)).

## Lifecycle the shell drives
- **Startup**: `register_worker(worker_id, queues, …)`. Worker ID is generated by the shell.
- **Heartbeat**: call `heartbeat(worker_id, resource_health_json)` on an interval
  (Python uses a daemon thread, ~5s). `resource_health` is arbitrary JSON the core stores
  without parsing — schema is the shell's choice.
- **Shutdown**: `unregister_worker(worker_id)`.
- **Cancellation handshake**: producer calls `request_cancel(job_id)`; an in-process
  worker observes it via `is_cancel_requested(job_id)` polling, or an out-of-process
  worker via `notify_cancel`; on observe, the shell calls `mark_cancelled(job_id)`.

## Python assumptions in the core (none structural)
All clean — `payload`/`result`/`task_name` opaque, channels standard, `Storage`
fully abstract, three interchangeable backends. The only Python mentions left are
doc-comments pointing at the Python shell as the *example* binding; no code path
assumes a serializer, a runtime, or a middleware framework.
