# Event egress (#848) — plan

Issue: https://github.com/ByteVeda/flexiq/issues/848 — CloudEvents egress and
broker sinks. Branch `feat/event-egress` off `master` (`405cbea0`), upstream
unset, **never pushed**. No separate spec file: this plan's Design section is
the spec (user-approved decisions below).

User decisions (2026-09-24):
- Emitters: flexiq-server **and** every SDK shell (Python, Node, Java, Rust).
- Sinks: CloudEvents over HTTP + Redis Streams now; Kafka + NATS filed as a
  follow-up issue by the controller at the end.
- `job.enqueued` and pre-run `job.cancelled`: from the server's doors only
  (gRPC producer, triggers, admin/dashboard cancel). Embedded enqueues are
  not seen; the docs say so.
- 2026-09-25, after review: "fix them" — expiry, cascade cancels, periodic
  firings and DLQ auto-retry must emit too (Tasks 12–14). Periodic and
  auto-retry enqueues come from the scheduler, so embedded workers now send
  `job.enqueued` for those two sources only.

## Global Constraints (bind every task)

- **Build**: 13 GB RAM. Every cargo command gets `-j1`; `cargo clippy` gets
  `CARGO_BUILD_JOBS=1` in the env. Never two cargo invocations at once. Prefer
  `cargo test -j1 -p <crate> --lib <filter>` / `--test <name>` while iterating.
- **Python**: from `sdks/python/`, always `uv run …` (`uv run maturin develop`,
  `uv run python -m pytest`, `uv run ruff check`, `uv run mypy flexiq/
  --no-incremental`). Never bare `uv sync`. Lint test files too.
- **Commit identity** — every commit, exactly:
  `git -c user.name="Annika" -c user.email="306827384+stromanni@users.noreply.github.com" -c user.signingkey=/home/ezio/.ssh/git_stromanni_signing.pub commit -m "<msg>"`.
  Conventional prefix (`feat:`, `fix:`, `docs:`, `test:`, `chore:`), subject
  ≤ 60 chars, imperative, no `@` in the subject, **no Co-Authored-By, no
  mention of Claude/Anthropic/AI anywhere**. Body only if the why is not
  obvious, 1–2 sentences. Split distinct changes into separate commits. After
  committing run `git log -1 --format=%an%n%s` to prove it landed (pre-commit
  hooks — cargo fmt, clippy, ruff, mypy — can reject it; fix and recommit,
  never `--no-verify`). Stage explicit paths with `git add <paths> &&` (never
  silence stderr). Never push.
- **Code style**: match surrounding code. Comments 1–2 lines stating the
  *why*. No `unwrap()`/`expect()` in library code (tests fine). No inline
  imports inside functions (Python/TS/Java). Library errors are typed.
  `#![deny(missing_docs)]` is on in flexiq-core: every `pub` item needs a doc
  comment. rustdoc must not link a `pub` item to a private one.
- **SDK independence**: an SDK's code/docs/tests never name a sibling SDK;
  say "cross-SDK contract".
- **Delivery semantics (normative, must be stated in code docs and the
  contract)**: at-most-once overall — an event is lost when its sink's buffer
  is full, when delivery attempts run out, or when the process dies; each
  accepted event may be delivered more than once. Dedupe on the CloudEvents
  `id`, which is `<job_id>:<attempt>:<epoch>:<type>` with `-` for an unknown
  part (see `JobEvent::id`). Never exactly-once.
- **No back-pressure**: `EventHub::emit` never blocks and never does I/O. Full
  buffer ⇒ drop + counter.
- **No payloads by default**: `include_payload` per sink, `false` default; a
  `WARN` naming the sink at hub start when true.
- **HTTP sink = push dispatch's SSRF guard**: `DispatchClient` +
  `EgressPolicy` from a required, non-empty per-sink `allow` list; no proxy;
  no redirects; no "allow private" escape hatch.
- **Event taxonomy** (short names, `EventType`): `job.enqueued`,
  `job.started`, `job.completed`, `job.failed`, `job.retrying`, `job.dead`,
  `job.cancelled`, `job.sleeping`. CloudEvents `type` =
  `org.byteveda.flexiq.` + short name. Default `source` = `/flexiq`.
- **Metric names**: `flexiq_events_delivered_total{sink}`,
  `flexiq_events_dropped_total{sink,reason}` with reason ∈ `buffer_full`,
  `rejected`, `failed`, `shutdown`; gauge `flexiq_events_queued{sink}`.
- **Cargo feature**: flexiq-core `events-http = ["http-target"]` compiles the
  HTTP sink; the Redis Streams sink compiles under the existing `redis`
  feature. The rest of `flexiq_core::events` is always compiled. A config
  naming a kind this build lacks fails at hub start with
  `EventsConfigError::NotCompiled`.

## Design (the spec)

- **One config document, parsed in core** (`flexiq_core::events::config`).
  Server reads it from `FLEXIQ_EVENTS_FILE`; each shell takes the same JSON as
  a worker option. Shape:

  ```json
  {
    "source": "/flexiq",
    "sinks": [
      {"kind": "http", "name": "warehouse", "url": "https://events.example.com/in",
       "allow": ["events.example.com"], "allow_loopback": false,
       "bearer_token_env": "EVT_TOKEN", "hmac_secret_env": "EVT_HMAC",
       "timeout_ms": 10000, "connect_timeout_ms": 5000,
       "filter": {"namespaces": [], "queues": [], "tasks": [], "types": ["job.dead"]},
       "include_payload": false,
       "delivery": {"buffer": 10000, "max_attempts": 5, "max_batch": 1}},
      {"kind": "redis_streams", "name": "stream", "url_env": "EVT_REDIS_URL",
       "stream": "flexiq:events", "max_len": 100000}
    ]
  }
  ```
- **`EventHub`**: one bounded `std::sync::mpsc::sync_channel` + one OS thread
  per sink, so a wedged sink cannot starve another or the host runtime
  (PyO3, napi, JNI, tokio in the server). A sink backend is a *blocking*
  `deliver(&mut self, batch) -> DeliveryResult`; the HTTP backend owns a
  current-thread tokio runtime and `block_on`s the request.
- **Emit sites**: poller dispatch → `job.started`; `handle_result(s)` →
  `job.completed` / `job.failed` then `job.retrying` | `job.dead` /
  `job.cancelled` / `job.sleeping` (`Superseded` emits nothing); rate-limit
  and CoDel sheds → `job.dead` with `reason`; server doors → `job.enqueued`,
  pre-run `job.cancelled`.
- **Payload** rides only where the job row is already in hand (enqueue,
  dispatch), and only if `EventHub::wants_payload()` — no extra read on the
  settle path.

## Task 1: Core event model, config and hub

**Where it fits:** foundation for every other task. flexiq-core only.

**Already drafted in the working tree (uncommitted) — review, keep, finish:**
- `crates/flexiq-core/src/events/event.rs` — `EventType`, `JobEvent`,
  `JobEvent::id`, `JobEvent::to_cloudevent(source, include_payload)`,
  constants `CLOUDEVENTS_TYPE_PREFIX`, `DEFAULT_SOURCE`, tests.
- `crates/flexiq-core/src/events/config.rs` — `EventsConfig::parse`,
  `SinkConfig` (`kind`-tagged: `Http(HttpSinkConfig)`,
  `RedisStreams(RedisSinkConfig)`), `Delivery`, `Filter::admits`,
  `EventsConfigError`, tests. Confirm with a test that `deny_unknown_fields`
  actually refuses unknown keys *inside* a `kind`-tagged variant (serde's
  internally-tagged enums can defeat it); if it does not, fix the shape
  (e.g. deserialize a raw map, split off `kind`, then deserialize the
  variant struct) and keep the tests.

**Build:**
1. `crates/flexiq-core/src/events/mod.rs` — module doc stating the delivery
   semantics (Global Constraints wording) and the no-back-pressure rule;
   re-export the public types. Register `pub mod events;` in `src/lib.rs`
   with a one-line doc, and add root re-exports `EventHub`, `EventType`,
   `JobEvent`, `EventsConfig`, `EventsConfigError`, `SinkStats`.
2. `crates/flexiq-core/src/events/sink/mod.rs` — the backend seam:
   ```rust
   pub(crate) enum DeliveryResult { Delivered, Retry(String), Reject(String) }
   pub(crate) trait SinkBackend: Send + 'static {
       /// Blocking. Sends the whole batch or reports why not.
       fn deliver(&mut self, batch: &[Arc<JobEvent>]) -> DeliveryResult;
   }
   ```
   plus `build_backend(&SinkConfig, source: &str) -> Result<Box<dyn SinkBackend>, EventsConfigError>`
   which, in this task, returns `EventsConfigError::NotCompiled` for `http`
   (feature `events-http`) and `redis_streams` (feature `redis`) — Tasks 2/3
   fill in the compiled arms under `#[cfg]`.
3. `crates/flexiq-core/src/events/hub.rs` — `EventHub`:
   - `EventHub::start(config: EventsConfig) -> Result<EventHub, EventsConfigError>`
     and `EventHub::from_json(&str)`; `pub(crate) fn with_backends(source,
     Vec<(SinkConfig, Box<dyn SinkBackend>)>)` for tests. Logs `warn!` per
     sink with `include_payload` ("events sink '<name>' sends job payloads").
   - `emit(&self, event: JobEvent)`: wrap in `Arc`, for each sink whose
     filter admits it `try_send`; `Full` ⇒ `dropped{buffer_full}`;
     `Disconnected`/after shutdown ⇒ `dropped{shutdown}`. Never blocks.
   - `wants_payload(&self) -> bool` (any sink with `include_payload`).
   - Per-sink thread loop: block for the first event, then drain up to
     `max_batch - 1` more with `try_recv`; deliver with up to `max_attempts`
     attempts; `Retry` ⇒ capped exponential backoff with full jitter (base
     100 ms, cap 10 s, `rand` is a dependency) then retry; `Reject` ⇒ drop
     the batch as `rejected`; attempts exhausted ⇒ `failed`. Each sink sends
     each event's `to_cloudevent(source, sink.include_payload)` — encoding
     lives in the backend, but payload stripping must honour the sink flag.
     `warn!` once per dropped batch with the sink name and reason (no
     payload, no URL credentials in the log).
   - `stats(&self) -> Vec<SinkStats>`; `SinkStats { name, kind, delivered,
     dropped_buffer_full, dropped_rejected, dropped_failed, dropped_shutdown,
     queued }` (atomics inside, plain `u64` snapshot out; `queued` = events
     accepted but not yet delivered or dropped).
   - `render_prometheus(&self) -> String`: the three metric families from
     Global Constraints with `# HELP`/`# TYPE` lines, label values escaped
     (`\\`, `"`, `\n`). Every reason is rendered for every sink, zero
     included, so rate queries have a series from boot.
   - `shutdown(&self, budget: Duration)`: close the channels (senders held
     in `RwLock<Option<SyncSender<…>>>` or equivalent), set a deadline all
     sink loops honour (backoff sleeps are cut short at the deadline, no new
     attempt starts after it), wait for threads up to the deadline, count
     whatever is still buffered at the deadline as `dropped{shutdown}`.
     Idempotent. `Drop` closes channels without waiting.
4. Tests (in-module, fake `SinkBackend`s): filter routing to the right sink
   only; buffer overflow drops and counts without blocking (a backend that
   blocks on a barrier); batch drains up to `max_batch`; `Retry` then
   success counts one delivered; `Reject` counts `rejected` with no retry;
   exhausted retries count `failed`; `include_payload=false` never reaches the
   backend's encoded view; shutdown drains what it can within budget and
   counts the rest; `render_prometheus` output for one sink; `NotCompiled`
   when features are off (cfg-gate that test to the no-feature build).
5. `crates/flexiq-core/Cargo.toml`: add `events-http = ["http-target"]` with
   a comment (HTTP event sink; enabling it compiles the egress guard and
   client, which every shipped SDK artifact will carry for this).

**Verify:** `cargo test -j1 -p flexiq-core --lib events`;
`cargo check -j1 -p flexiq-core --features events-http,redis`;
`CARGO_BUILD_JOBS=1 cargo clippy -p flexiq-core --all-targets --features events-http,redis -- -D warnings`.

**Commit:** `feat(core): event model, config and bounded hub`.

## Task 2: HTTP CloudEvents sink

**Where it fits:** first real backend for Task 1's `SinkBackend`. flexiq-core,
`#[cfg(feature = "events-http")]`.

**Build** `crates/flexiq-core/src/events/sink/http.rs`:
- Construction from `HttpSinkConfig` (inside `build_backend`'s `http` arm):
  - `Allowlist::parse(&allow.join(","))` (check its accepted syntax in
    `src/net/allowlist.rs`), `EgressPolicy::new(allow, allow_loopback)`,
    `DispatchClient::new(Arc::new(policy), connect_timeout)`.
  - Validate the URL with `worker::http_target::validate_target_url(url,
    client.policy())` (it is `pub(crate)`; it enforces scheme, userinfo,
    allowlisted host, cleartext only to loopback). Its errors say "push
    target"; map each `HttpTargetError` variant to an
    `EventsConfigError::Sink { sink, message }` whose message says "url"
    rather than "push target" — do not reword the shared error type.
  - Resolve `bearer_token_env` / `hmac_secret_env` from the environment at
    start; missing or empty ⇒ `EventsConfigError::Sink`. Hold secrets as
    `crate::worker::Secret` (never `Debug`-printable).
  - Own a `tokio::runtime::Builder::new_current_thread().enable_all()`
    runtime for `block_on`.
- `deliver`:
  - `max_batch == 1` ⇒ structured mode, `content-type:
    application/cloudevents+json`, body = one CloudEvent object.
    `max_batch > 1` ⇒ batched mode, `content-type:
    application/cloudevents-batch+json`, body = JSON array (even of one), so
    a receiver sees one content type per sink.
  - `authorization: Bearer <token>` when configured.
  - HMAC when configured: `x-flexiq-event-timestamp: <unix ms>` and
    `x-flexiq-event-signature: v1=<lowercase hex HMAC-SHA256(secret,
    "<timestamp>.<body>")>` via `http::auth::digest::hmac_sha256_hex`. Doc
    comment: deliberately neither the webhook `X-Flexiq-Signature` nor the
    dispatch `x-flexiq-dispatch-*` scheme — different signed bytes under a
    shared name would make one verifier accept the other's messages.
  - Per-request timeout `timeout_ms`. Status: 2xx ⇒ `Delivered`; 408, 429,
    5xx, connect/timeout/IO errors, egress refusal at resolve time ⇒
    `Retry`; any other status ⇒ `Reject`. Do not read the response body
    beyond what reqwest needs; never log the full URL's query or any header
    value.
- Tests: use the in-crate test HTTP helpers (`src/http/testing.rs`) or a
  minimal tokio listener on 127.0.0.1 with `allow_loopback: true` +
  `allow: ["127.0.0.1"]`. Cover: structured body + content type + CE
  attributes; batched array; bearer header; HMAC header verifies
  (recompute in the test); 503 then 200 ⇒ one delivered after retry via the
  hub; 400 ⇒ rejected, not retried; URL not on the allowlist refused at
  start; `http://` to a non-loopback host refused at start; missing secret
  env refused at start; payload absent unless `include_payload`.

**Verify:** `cargo test -j1 -p flexiq-core --features events-http --lib events`;
clippy as Task 1 with `events-http`.

**Commit:** `feat(core): CloudEvents HTTP event sink`.

## Task 3: Redis Streams sink

**Where it fits:** second backend. flexiq-core, `#[cfg(feature = "redis")]`.

**Build** `crates/flexiq-core/src/events/sink/redis_streams.rs`:
- From `RedisSinkConfig`: read `url_env` at start (missing/empty ⇒
  `EventsConfigError::Sink`), `redis::Client::open` (error ⇒ `Sink`, message
  must not echo the URL — it carries a password). Connect lazily; on any
  error drop the connection so the next attempt reconnects. Set connect /
  read / write timeouts of 5 s so a dead Redis cannot hold the sink thread
  forever.
- `deliver`: one pipeline of `XADD <stream> MAXLEN ~ <max_len> * <fields>`
  per event. Fields: `id`, `type` (full CE type), `source`, `time`,
  `namespace`, `queue`, `task`, `event` (the whole structured CloudEvent
  JSON, payload per the sink's flag). Connection/IO errors ⇒ `Retry`;
  a Redis server error reply (e.g. WRONGTYPE) ⇒ `Reject`.
- Tests: a pure unit test of the field list built for an event. A live test
  only if the crate already has a gated Redis test pattern (find how existing
  Redis tests pick up their URL, e.g. an env var, and follow it; skip
  cleanly when unset). No docker.

**Verify:** `cargo test -j1 -p flexiq-core --features redis --lib events`;
`cargo check -j1 -p flexiq-core --features redis,events-http`; clippy with
`redis,events-http`.

**Commit:** `feat(core): Redis Streams event sink`.

## Task 4: Scheduler emit sites + Rust worker hook

**Where it fits:** makes the core scheduler produce events for every runtime.
flexiq-core scheduler + worker runner + Rust SDK (`crates/flexiq`).

**Build:**
- `Scheduler` gets `events: Option<Arc<EventHub>>` and
  `pub fn set_events(&mut self, hub: Arc<EventHub>)` (doc: must be called
  before the scheduler is shared, like the other `register_*`).
- `DispatchRecord` gains `queue: String`; `track_in_flight` takes the queue
  (callers in `poller.rs::finish_dispatch` pass `job.queue`).
- New `crates/flexiq-core/src/scheduler/events.rs` holding the emit helpers
  so `result_handler.rs`/`poller.rs` gain one call each:
  - `job.started` in `finish_dispatch` only after `try_send` returns `Ok`
    (build the event before the job moves; clone `payload` only if
    `hub.wants_payload()`), with `attempt = job.retry_count`, `epoch`.
  - Outcomes: split `handle_result` into an inner fn plus an emitting
    wrapper; `handle_results` routes non-success through the inner fn and
    emits once per `Ok` outcome at the end, so no outcome emits twice and
    none is missed (including the per-job fallback path). Mapping:
    `Success` → `job.completed`; `Retry` → `job.failed` then
    `job.retrying`; `DeadLettered` → `job.failed` then `job.dead`;
    `Cancelled` → `job.cancelled`; `Slept` → `job.sleeping` (with
    `wake_at_ms`); `Superseded` → nothing. Attempt/epoch/queue come from
    the dispatch record (`last_dispatch(job_id)` after release — it is
    retired, not forgotten); for a job with no record (reaper-recovered
    orphan) fall back to the outcome's own `queue` and, for failures, the
    `JobResult::Failure.retry_count`. Carry `error`, `timed_out`,
    `wall_time_ns` where the outcome has them. Namespace =
    `self.namespace`.
  - Sheds: every place the scheduler dead-letters without running the job
    (rate-limit shed via `shed_rate_limited`, CoDel shed in `codel_admit` —
    grep `shed_to_dlq` in `src/scheduler/`) emits `job.dead` with `reason`
    = the shed reason string it already writes.
  - No emit when `events` is `None`; cost when set must be O(1) allocations
    per transition, no storage reads added.
- `flexiq_core::Worker` builder (`src/worker/runner.rs`): add
  `.events(Arc<EventHub>)` which calls `scheduler.set_events`. Do not change
  `on_outcome`.
- Rust SDK `crates/flexiq`: expose the same on its worker builder (find the
  builder that wraps `flexiq_core::Worker`), plus a re-export of
  `EventHub`/`EventsConfig` so a user needs no direct flexiq-core import.
  The Rust SDK does not own hub shutdown; document that the caller calls
  `EventHub::shutdown` after the worker stops.
- Update `crates/flexiq-core/BINDING_CONTRACT.md`'s run-loop section with
  one step: shells may pass an `EventHub` to `Scheduler::set_events`, and
  must call `EventHub::shutdown(budget)` after the scheduler stops.
- Tests (scheduler tests in `src/scheduler/mod.rs` use in-memory SQLite —
  follow their pattern) with a recording fake backend (reuse Task 1's test
  helper; make it `pub(crate)` under `#[cfg(test)]` if needed): success
  emits started + completed with queue, attempt 0, matching epoch;
  retryable failure emits failed + retrying; final failure emits failed +
  dead; superseded emits nothing; slept emits sleeping with wake_at; a
  rate-limit shed emits dead with reason; batch path (`handle_results` with
  mixed results) emits exactly one terminal event per result; no hub ⇒ no
  panic.

**Verify:** `cargo test -j1 -p flexiq-core --lib scheduler`,
`cargo test -j1 -p flexiq-core --lib events`, `cargo test -j1 -p flexiq`,
`cargo check -j1 --workspace` and `--features push-dispatch,redis`, clippy
`--workspace --all-targets --features events-http,redis -- -D warnings`.

**Commits:** `feat(core): emit job lifecycle events from the scheduler`,
`feat(rust): wire event sinks into the worker builder`.

## Task 5: Server — events file, door events, metrics, drain

**Where it fits:** the flexiq-server deployment of the hub. `crates/flexiq-server`.

**Build:**
- `crates/flexiq-server/Cargo.toml`: feature `events-http =
  ["flexiq-core/events-http"]` (comment: the HTTP event sink); add it (and
  keep `redis`) wherever the server image/binary release build lists its
  features (grep Dockerfile(s) and `.github/workflows` for the server's
  `--features`).
- `crates/flexiq-server/src/config/events.rs`, registered in
  `config/mod.rs` like `trigger.rs`: `FLEXIQ_EVENTS_FILE` (path to the JSON
  document; read + `EventsConfig::parse` at boot, any error fails boot with
  the file path in the message) and `FLEXIQ_EVENTS_DRAIN` (shutdown budget,
  same duration syntax/units as the existing drain vars in `config/push.rs`
  — match it; default 5 s). Events are not a role: they do not satisfy the
  "at least one role" check. Config tests like the neighbours'.
- `runtime/mod.rs`: start the hub before spawning roles (so a bad sink fails
  boot), hand `Arc<EventHub>` to the scheduler supervisor
  (`runtime/scheduler.rs` → `Worker` builder `.events(..)`), to the doors,
  and to `/metrics`. On shutdown, after the scheduler/executors drain, call
  `hub.shutdown(drain)` inside `spawn_blocking` (it blocks).
- Door events, only when an event hub is configured:
  - `job.enqueued` after a successful insert in: gRPC producer enqueue
    (`grpc/producer/enqueue.rs`, single + batch, facade routes reuse it —
    check), triggers (`trigger/enqueue.rs`), dashboard enqueue if one exists
    (`dashboard/routes/jobs.rs` — check), admin periodic trigger-now if it
    enqueues (`grpc/admin/periodic.rs` — check). If a unique/idempotent
    enqueue returns an *existing* job, emit nothing. Payload attached only if
    `hub.wants_payload()`; `attempt = 0`.
  - `job.cancelled` when a cancel door actually cancels a *pending* job
    (`grpc/producer/cancel.rs`, dashboard/admin cancel routes — find them):
    only when storage reports the job was cancelled; a running job's cancel
    is emitted later by the scheduler outcome, not here.
  - Thread the hub through the existing shared state structs (`AppState`,
    the gRPC service state, trigger state) as `Option<Arc<EventHub>>`.
- `/metrics`: append `hub.render_prometheus()` at both composition points
  (`grpc/metrics/routes.rs` and `dashboard/probes.rs`, beside
  `trigger::metrics::render()`).
- Tests: config parse/error tests; an e2e test (follow an existing
  `crates/flexiq-server/tests/*.rs` harness) booting with a loopback HTTP
  sink, enqueueing through the gRPC producer or a trigger, running the job
  through an executor if the harness supports it — at minimum assert
  `job.enqueued` arrives with the CloudEvents attributes, and that
  `/metrics` shows `flexiq_events_delivered_total`. A bad events file fails
  boot.

**Verify:** `cargo test -j1 -p flexiq-server --features grpc,http-target,events-http`
(the whole crate once, focused tests while iterating);
`cargo check -j1 -p flexiq-server --features redis,events-http`;
clippy `--workspace --all-targets --all-features -- -D warnings`.

**Commits:** `feat(server): load event sinks from FLEXIQ_EVENTS_FILE`,
`feat(server): emit enqueued and cancelled from the doors`, and a separate
one for the metrics wiring if it stands alone.

## Task 6: Python shell

**Where it fits:** embedded Python workers emit the same events.
`crates/flexiq-python` + `sdks/python`.

**Build:**
- Rust binding: the worker-start path (`crates/flexiq-python/src/py_queue/worker.rs`
  near `Scheduler::new`) accepts an optional events JSON string, builds
  `EventHub::from_json` (error ⇒ Python `ValueError` with the core message),
  `scheduler.set_events`, and calls `hub.shutdown(budget)` when the worker
  loop exits (release the GIL while it blocks). Expose
  `event_sink_stats()` returning a list of dicts (fields of `SinkStats`) from
  the running worker's hub, or empty when none.
- Python API (`sdks/python/flexiq/`): follow how existing worker options
  flow (e.g. `push_dispatch`) from `Queue`/`run_worker` into the binding.
  Add `event_sinks: dict | str | os.PathLike | None` (dict ⇒ `json.dumps`,
  path ⇒ file contents, str ⇒ JSON text) and `event_sinks_drain: float = 5.0`
  seconds. Type stubs (`.pyi`) updated.
- Cargo feature `events-http` added to `crates/flexiq-python/Cargo.toml`
  (forwarding to core) and to every Python build feature list:
  `sdks/python/pyproject.toml` `[tool.maturin] features`,
  `.github/workflows/publish-py.yml` (all four `args:` lines),
  `.github/workflows/ci-python.yml`.
- Tests (`sdks/python/tests/`): invalid document raises `ValueError`; an
  end-to-end test with a stdlib `http.server` on 127.0.0.1 in a thread,
  sink `allow_loopback: true`, `allow: ["127.0.0.1"]`, one task run through
  a worker ⇒ receiver gets `job.started` and `job.completed` CloudEvents for
  that job id with `flexiqqueue`/`flexiqtask`; `event_sink_stats()` shows
  delivered ≥ 2; no `payload_base64` by default.

**Verify:** `uv run maturin develop` (from `sdks/python`), the new tests plus
the worker test module(s) they sit beside, `uv run ruff check flexiq/ tests/`,
`uv run ruff format --check`, `uv run mypy flexiq/ --no-incremental`.

**Commit:** `feat(python): event sinks on the worker`.

## Task 7: Node shell

**Where it fits:** same as Task 6 for `crates/flexiq-node` + `sdks/node`.

**Build:**
- napi binding: worker options (`crates/flexiq-node/src/worker.rs`,
  options struct) gain `events?: string` (JSON) and `eventsDrainMs?: number`;
  build the hub, `set_events`, shut it down off the JS thread when the
  worker stops. `eventSinkStats()` on the worker handle (or the queue — follow
  where the worker's other live stats live).
- TS API (`sdks/node/src/`): `eventSinks?: EventSinksConfig | string` in the
  worker options (object ⇒ `JSON.stringify`), exported `EventSinksConfig`
  type mirroring the document (snake_case keys, as the document is the
  cross-SDK contract).
- `events-http` feature: `crates/flexiq-node/Cargo.toml` and
  `sdks/node/package.json` `build:native` (and any other native build
  script/CI line listing the node features — grep).
- Tests (vitest or whatever `sdks/node` uses): invalid config throws; e2e
  with `node:http` server on 127.0.0.1 receiving `job.started` +
  `job.completed`. Remember `noUncheckedIndexedAccess` in tests.
- `pnpm build:native` (~4 min) is required after the napi change.

**Verify:** from `sdks/node`: `pnpm build:native`, `pnpm build`, the new
test file + worker tests, `pnpm lint`/`typecheck` scripts that exist.

**Commit:** `feat(node): event sinks on the worker`.

## Task 8: Java shell

**Where it fits:** same as Task 6 for `crates/flexiq-java` + `sdks/java`.

**Build:**
- JNI: worker start (`crates/flexiq-java/src/worker.rs` near
  `Scheduler::new`) takes an events JSON string (nullable) + drain millis;
  bad JSON ⇒ `IllegalArgumentException` with the core message; shutdown on
  stop. Stats accessor returning something Java can map (follow how other
  structured values cross JNI in this crate — JSON string is acceptable).
  Anything Rust names by string needs a `jni-config.json` entry (native-image).
- Java API: worker options/builder gains `eventSinks(String json)` /
  `eventSinks(Path)` and `eventSinksDrain(Duration)`; `eventSinkStats()`
  returning a small record list. Javadoc on every public member (`-Xwerror`).
- `events-http` feature: `crates/flexiq-java/Cargo.toml`,
  `sdks/java/build.gradle.kts` cargo command, `.github/workflows/ci-java.yml`,
  `.github/workflows/publish-java.yml` (grep for the feature list).
- Tests (JUnit): invalid config throws; e2e with
  `com.sun.net.httpserver.HttpServer` on 127.0.0.1 receiving `job.started`
  + `job.completed`. Poll an aggregate (received count) with a deadline —
  never rendezvous two workers.

**Verify:** `cargo build -j1 --release --features postgres,redis,workflows,mesh,push-dispatch,events-http -p flexiq-java`
then `./gradlew build` (from `sdks/java`) or the narrower test task.

**Commit:** `feat(java): event sinks on the worker`.

## Task 9: Contract doc

**Where it fits:** the normative statement consumers build against.

**Build:** `contracts/EVENT_EGRESS_CONTRACT.md`, shaped like
`contracts/PUSH_DISPATCH_CONTRACT.md`: scope; the configuration document
(every field, default, validation rule); event taxonomy and when each fires
(including what is *not* seen: embedded-SDK enqueues, pre-run cancels outside
the server doors); CloudEvents attributes + `data` fields; id format and the
dedupe rule; delivery semantics (Global Constraints wording, verbatim
meaning); batching and content types; HTTP status handling; HMAC signing
(signed bytes, headers, a worked example computed by actually running the
code/test); Redis Streams field layout; payload opt-in and its warning;
metrics; SSRF guard rules shared with push dispatch. Link it from
`contracts/REMOTE_SDK_CONTRACT.md` where sibling contracts are listed (if
they are) and from `crates/flexiq-core/README.md` feature list
(`events-http`). Every example must be one you ran.

**Commit:** `docs: event egress contract`.

## Task 10: Helm

**Build:** `deploy/helm/flexiq-server`: an `events:` block in `values.yaml`
(`enabled`, `config` (the JSON document as YAML), `drain`, `secretEnv` for
the `*_env` variables — mirror how `triggers:` keeps definitions in a
ConfigMap and secrets out of it), a ConfigMap template, the volume mount +
`FLEXIQ_EVENTS_FILE`/`FLEXIQ_EVENTS_DRAIN` env on the server container, and
fold `events.drain` into the computed `terminationGracePeriodSeconds` if the
shutdown math in `_validate.tpl`/the deployment template sums drains (check;
if it only covers push, add events' drain to the computed default). Verify
with `helm template` (run multi-flag helm through `bash -c`), enabled and
disabled.

**Commit:** `feat(helm): configure event sinks`.

## Task 11: Docs site + CHANGELOG

**Build:** Fumadocs site in `docs/`. A server-tier guide page for event
egress (config, sinks, filters, semantics, metrics, Helm) and an SDK guide
page with per-SDK tabs for the worker option (follow how an existing
cross-SDK guide uses `<Tab sdk=…>`; Rust tab needs its registry row per the
docs tier rules — read neighbouring pages). Link the contract. Add to nav
(`meta.json`). CHANGELOG entry under Unreleased, then `pnpm --dir docs
sync:changelog` if that script exists. Every command/config example must be
one you actually ran.

**Verify:** `pnpm --dir docs install --frozen-lockfile`,
`pnpm --dir docs typecheck`, `pnpm --dir docs lint`,
`NODE_OPTIONS=--max-old-space-size=8192 pnpm --dir docs build`.

**Commit:** `docs: event egress guides`.

## Review (2026-09-25)

All 11 tasks done via subagent-driven development (implementer + task review
per task), then a whole-branch review and one fix wave. 36 commits off
`405cbea0`, not pushed. Kafka/NATS follow-up filed as #971.

Deviations from this plan, all ruled during review:
- Node `await worker.stop()` waits for the drain, bounded from `stop()`.
- Java drain budget starts after `close()`'s existing handler wait.
- Helm events-only grace default is `max(30, events.drain + 10)`.
- Redis sink retries per the client's `retry_method()`; BUSY stays rejected.
- New public event types are `#[non_exhaustive]`; `crates/flexiq` forwards
  `events-http`.

Follow-up (Tasks 12–14, 2026-09-25): expiry (sweep + dispatch), cascade
cancels, periodic firings and DLQ auto-retry now emit, via storage
`*_reporting` variants (old methods are default wrappers; the sweep reports
per batch). The sweep emits only the scheduler's own namespace; a scheduler
with no namespace announces all of them, the maintenance convention.

Still not emitted (contract "What is not seen"): enqueues and cancels through
an embedded SDK's own API, bulk purges/revokes, direct-to-storage cancels,
workflow node enqueues, and a namespace's expiries when another tenant's
scheduler swept them first.

## Task 12: Storage reports the rows it expires and cascades

**Where it fits:** expiry and cascade cancels happen inside storage, which
holds each full row and throws it away, returning only a count or unit. This
task makes the rows come back. flexiq-core storage only; no emitting yet.

**Build** — follow the `enqueue_unique_reporting` precedent (`storage/traits.rs`),
but give the *old* methods default bodies that call the new ones and discard
the rows, so no caller (scheduler, server, Python/Node/Java/Rust shells)
changes:
- `expire_pending_jobs_reporting(now) -> Result<Vec<Job>>` — the reaper
  sweep (`diesel_common/jobs.rs` `archive_pending_in_batches` /
  `archive_pending_rows`; `redis_backend/jobs/maintenance.rs`). Rows as they
  were before archiving (status Pending is fine; the event says cancelled).
  `expire_pending_jobs` becomes the default wrapper returning `len()`.
- Dispatch-time expiry: the Diesel `dequeue_ordered` / `dequeue_batch_ordered`
  skip-and-archive (`error = "expired before execution"`) and the Redis
  `SELECT_AND_CLAIM` `expired_ids` → `archive_expired` path. Add reporting
  variants of whichever dequeue methods the poller calls (read
  `scheduler/poller.rs` to see which) that also return the expired jobs, e.g.
  a `Dequeued { claimed, expired }` record; the plain methods become default
  wrappers. The Redis Lua script already returns the expired ids; load the
  rows it archives only if they are not already loaded (no new round trip
  per claim on the common no-expiry path — measure it by reading the code).
- Cascade: `cascade_cancel(...) -> Result<Vec<Job>>` returning every
  dependent it cancelled (Diesel BFS `diesel_common/jobs.rs`; Redis
  `redis_backend/jobs/state.rs`). Surface that list through reporting
  variants of its public callers: `move_to_dlq_reporting`,
  `shed_to_dlq_reporting` (keep `shed_to_dlq`'s existing default-to-
  `move_to_dlq` behaviour meaningful for the reporting pair) and
  `cancel_job_reporting -> Result<(bool, Vec<Job>)>`. Old methods = default
  wrappers. Also `resilience/dlq.rs` `DeadLetterQueue` gets a reporting method.
- Wire every new method through `impl_storage!` / `delegate!` in
  `storage/mod.rs` so `StorageBackend` and all three backends expose it.
- Tests: extend the storage contract / backend tests that already cover
  expiry, cascade and dequeue (find them — SQLite in-memory runs locally;
  Postgres/Redis contract suites compile-only locally, `FLEXIQ_REDIS_TEST_URL`
  gating as the crate does) to assert the returned rows: ids, queue,
  task_name, namespace of each expired/cascaded job, including a multi-level
  cascade and a namespace-scoped cancel.

**Verify:** `cargo test -j1 -p flexiq-core --lib storage` (and the crate's
storage contract test target if separate), `cargo check -j1 --workspace
--features postgres` and `--features redis`, clippy `--workspace
--all-targets --all-features -- -D warnings`.

**Commit(s):** e.g. `feat(core): report expired jobs from storage`,
`feat(core): report cascade-cancelled dependents`.

## Task 13: Emit the four missing transitions

**Where it fits:** consumes Task 12's rows. flexiq-core scheduler + server
doors.

**Build:**
- Expiry → `job.cancelled` with `reason` = the storage error string
  (`"expired"` for the sweep, `"expired before execution"` at dispatch),
  `attempt = job.retry_count`, no epoch, the job's own namespace (the sweep
  is unscoped and covers every namespace). Sweep: `scheduler/maintenance.rs`
  switches to `expire_pending_jobs_reporting`. Dispatch: the poller switches
  to the reporting dequeue.
- Cascades → one `job.cancelled` per dependent with `reason` = the cascade
  reason (`"dependency failed"` / `"dependency cancelled"`), from: the
  scheduler's DLQ moves (`result_handler.rs` via the reporting DLQ call),
  both shed paths in `poller.rs`, and the server's cancel doors
  (`crates/flexiq-server/src/events.rs` `cancelled` callers — gRPC
  `CancelJob`, facade, dashboard cancel) via `cancel_job_reporting`. The
  parent's own event stays as today. Embedded-SDK `cancel_job` keeps emitting
  nothing (no hub there, and the parent is not announced either).
- Periodic firings → `job.enqueued` from `scheduler/maintenance.rs`
  (`check_periodic`): switch to `enqueue_unique_reporting` and emit only when
  inserted (a dedup hit emits nothing, matching the doors). `attempt = 0`,
  payload only if `hub.wants_payload()`.
- DLQ auto-retry → `job.enqueued` from `maintenance.rs` (`auto_retry_dlq`)
  with the new id `retry_dead` returns and queue/task/namespace from the
  `DeadJob` entry; `attempt = 0`.
- All helpers live in `scheduler/events.rs` beside `emit_shed` /
  `emit_outcome`; no emit when no hub; no new storage reads.
- Tests (scheduler tests, in-memory SQLite, recording fake sink as Task 4
  did): sweep expiry emits cancelled+reason for each expired job; dispatch-
  time expiry emits; a dead-lettered parent with a two-level dependent chain
  emits one cancelled per dependent; a rate-limit shed parent cascades; a
  periodic tick emits one enqueued and a second tick for the same slot emits
  none; DLQ auto-retry emits enqueued with the new id. Server: extend
  `tests/grpc_events.rs` — cancelling a pending parent emits the parent's and
  each dependent's `job.cancelled`.

**Verify:** `cargo test -j1 -p flexiq-core --lib scheduler`, `--lib events`,
`cargo test -j1 -p flexiq-server --features grpc,http-target,events-http
--test grpc_events`, workspace check + clippy as Task 12.

**Commits:** e.g. `feat(core): emit expiry and cascade cancels`,
`feat(core): emit periodic and auto-retry enqueues`,
`feat(server): emit cascade cancels from the cancel doors`.

## Task 14: Contract, docs and CHANGELOG catch up

**Build:** `contracts/EVENT_EGRESS_CONTRACT.md` — move the four items out of
"What is not seen" into the taxonomy table (who emits, reason strings,
attempt/epoch values); state that embedded workers now send `job.enqueued`
for periodic and auto-retry enqueues only; the dispatch-time vs sweep reason
strings. Update the docs guides (`docs/content/docs/server/operate/events.mdx`,
`docs/content/docs/shared/guides/extend/event-sinks.mdx` — the "An embedded
worker never sends `job.enqueued`" sentence is now wrong) and the CHANGELOG
entry (regenerate the mdx with `pnpm --dir docs sync:changelog`). Every
example real. Docs typecheck, lint, build.

**Commit:** `docs: events for expiry, cascades, periodic and retries`.

## Task 15: A scheduler's events stay inside its own namespace

User decision (2026-09-25): a worker/scheduler with no namespace gets events
for jobs in the **default namespace only**, never other namespaces. Its job
dispatch already works that way; events must match. A sink that wants every
namespace is a future explicit opt-in, not an accident of a missing
namespace.

**Build:**
- One helper in `crates/flexiq-core/src/scheduler/events.rs` deciding whether
  a job row belongs to this scheduler's namespace: `Some(ns)` ↔ row
  namespace `Some(ns)`; `None` ↔ row namespace `None` (the default namespace
  — check whether rows can also carry `Some("default")` and treat it the way
  the dequeue queries do; see `DEFAULT_NAMESPACE`). Use it at every emit
  site that can see another namespace's rows:
  - the expiry sweep in `maintenance.rs` (today `scope.is_none_or(..)`);
  - stale-job reaper timeouts (`reap_stale_jobs` in maintenance, which is
    unscoped for `None` and settles through `handle_result`): find how its
    events get their namespace today — if a `None` scheduler emits other
    namespaces' timeouts (possibly stamped with the wrong namespace), filter
    them and make sure the label is the row's own namespace;
  - periodic firings (`check_periodic`, cluster-wide for `None`);
  - DLQ auto-retry (check whether `list_dead_for_retry` is scoped);
  - cascades and dispatch-time expiry (verify they cannot cross namespaces;
    filter if they can).
  Only the **events** change. The maintenance work itself (who times out,
  fires, retries, archives) stays exactly as it is.
- Tests (scheduler tests, recording fake sink): a `None` scheduler over
  expired / timed-out / periodic / auto-retry work in the default namespace
  and in `tenant-a` emits only the default-namespace events; a `tenant-a`
  scheduler still emits only `tenant-a`'s.
- Contract + both guides + CHANGELOG: replace "one with no namespace
  announces them all" with the new rule; state that no scheduler announces
  another namespace's transitions, so a namespace's maintenance events come
  only from a scheduler of that namespace (and may be missed if another
  namespace's scheduler did the work first). Regenerate the changelog mdx.

**Verify:** `cargo test -j1 -p flexiq-core --lib scheduler` and `--lib
events`, `cargo test -j1 -p flexiq-server --features
grpc,http-target,events-http --test grpc_events`, workspace check
(`redis`, `postgres`), clippy all-features `-D warnings`, docs typecheck +
lint.

**Commits:** `fix(core): keep scheduler events in its own namespace`,
`docs: events stay inside the scheduler's namespace`.
