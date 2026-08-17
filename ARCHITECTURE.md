# Architecture

This document is the map a contributor reads **before** changing FlexiQ. It
explains how the codebase is layered, where each responsibility lives, the
boundaries you must not cross, and the ordered steps for the changes people make
most often.

It is deliberately *not* a tutorial on running FlexiQ (see the
[docs site](docs/content/docs/architecture/overview.mdx)) nor a build/test guide
(see [`CONTRIBUTING.md`](CONTRIBUTING.md)). Three audiences, three documents:

| Document | Audience | Answers |
|---|---|---|
| `docs/content/docs/architecture/*` | end users | *How does FlexiQ behave?* — job lifecycle, failure model, scheduler semantics |
| `CONTRIBUTING.md` | new contributors | *How do I build and test it?* |
| **`ARCHITECTURE.md`** (this file) | **contributors & maintainers** | ***How is the code organized, and where do I add things?*** |

---

## System at a glance

FlexiQ is a polyglot system: **one Rust engine underneath, a thin language SDK shell
on top per host language** — Python via [PyO3](https://pyo3.rs), Node via
[napi-rs](https://napi.rs), Java via [JNI](https://github.com/jni-rs/jni-rs) (with an
FFM fast-path on JDK 22+). Each shell owns ergonomics and extensibility for its
language; Rust owns storage, scheduling, dispatch, rate limiting, and worker
management. A shell talks to the core only through its compiled binding crate
(`flexiq._flexiq` for Python, the `.node` addon for Node, the JNI `.so`/FFM for
Java) — **a shell never reaches into core internals, and the core never imports a
host-language type except at the binding edge.** The `WorkerDispatcher` trait in
`flexiq-core` is binding-free, so a new shell implements one trait against
[`BINDING_CONTRACT.md`](crates/flexiq-core/BINDING_CONTRACT.md).

```text
┌───────────────────────┐ ┌───────────────────────┐ ┌───────────────────────┐
│ PYTHON  sdks/python   │ │ NODE      sdks/node   │ │ JAVA      sdks/java   │
│ Queue = 15 mixins ·   │ │ Queue/Worker · CLI ·  │ │ Queue→DefaultFlexiQ  │
│ async · serializers   │ │ dashboard · events ·  │ │ workflows · DI ·      │
│ interception ·        │ │ middleware · webhooks │ │ proxies · interception│
│ resources · proxies   │ │ resources ·           │ │ codecs · spring ·     │
│ workflows · contrib   │ │ serializers           │ │ processor (@Task)     │
└───────────────────────┘ └───────────────────────┘ └───────────────────────┘
    PyO3 (._flexiq)           napi-rs (.node)           JNI + FFM (.so)
┌───────────────────────┐ ┌───────────────────────┐ ┌───────────────────────┐
│ flexiq-python        │ │ flexiq-node          │ │ flexiq-java          │
│ py_queue/ ·           │ │ JsQueue/JsWorker ·    │ │ backend · dispatcher  │
│ async_worker ·        │ │ dispatcher · convert  │ │ convert · ffi · jvm   │
│ prefork bridge        │ │                       │ │ worker · mesh         │
└───────────┬───────────┘ └───────────┬───────────┘ └───────────┬───────────┘
            └─ all three implement ───┬─ WorkerDispatcher ──────┘
┌─────────────────────────────────────┴─────────────────────────────────────┐
│ RUST CORE        crates/flexiq-core/   (no host language)                │
│ scheduler/ (poll · dispatch · retry · reap · wake)                        │
│ worker.rs (WorkerDispatcher trait) · resilience/ · periodic.rs            │
├───────────────────────────────────────────────────────────────────────────┤
│ STORAGE          crates/flexiq-core/src/storage/                         │
│ Storage trait (traits.rs) ──delegate!──▶ StorageBackend enum              │
│   ├─ sqlite/      ┐ shared logic in diesel_common/ macros                 │
│   ├─ postgres/    ┘                                                       │
│   └─ redis_backend/   (own impl — no Diesel)                              │
└───────────────────────────────────────────────────────────────────────────┘

  WORKFLOWS    crates/flexiq-workflows/ — separate crate, own schema & stores
               (SQLite · Postgres · Redis), surfaced per shell (Python:
               py_queue/workflow_ops/ · Node & Java: bound in their crates)
  MESH         crates/flexiq-mesh/ — optional decentralized work-stealing
               overlay (SWIM gossip · hash ring · TCP steal); `mesh` feature
  NATIVE ASYNC flexiq-python/src/native_async/ — optional native-async pool
               (Python-coupled; behind the `native-async` feature)
```

The dependency arrows point **downward only**. `flexiq-core` knows nothing about
Python, Node, Java, PyO3, napi, or JNI; each binding crate (`flexiq-python`,
`flexiq-node`, `flexiq-java`) depends on `flexiq-core`; each SDK package depends
on its compiled addon. This acyclic shape is the property that keeps the codebase
changeable — guard it.

---

## Layers & responsibilities

> Sections 1–2 describe the **Python SDK** (the original, most complete shell).
> Sections 6–10 cover the shared crates and the **Node** and **Java** SDKs.

### 1. Python API surface — `sdks/python/flexiq/`

The user-facing object is `Queue` in `app.py`. It is intentionally thin: a
constructor, the core `enqueue` / `enqueue_many` path, and serializer/idempotency
helpers. Everything else is composed in through **15 focused mixins**, each owning
one slice of the surface:

| Mixin (`mixins/`) | Owns |
|---|---|
| `decorators.py` | `@task`, `@periodic`, task wrapping, hook registration |
| `inspection.py` | read-only job/queue inspection |
| `operations.py` | enqueue-adjacent operations, cancel, replay |
| `locks.py` | distributed locks |
| `lifecycle.py` | worker start/stop, pause/resume |
| `events.py` | event subscription/emit |
| `middleware_admin.py` | enable/disable middleware |
| `overrides.py` | per-task runtime overrides |
| `predicates.py` | exception-filter predicates |
| `resources.py` | resource (DI) registration |
| `runtime_config.py` | queue-level rate/concurrency limits |
| `settings.py` | misc queue settings |

Async is **physically confined** to `async_support/`. Every `a*` convenience
wrapper lives in `async_support/mixins.py::AsyncQueueMixin`; the dedicated event
loop, contextvars job context, async locks, and `run_maybe_async()` helper live
beside it. No `import asyncio` leaks into the sync layer — when a module boundary
forbids a top-level import, the code is split across modules rather than scoped
inline (see `JobResult(AsyncJobResultMixin)`).

**Must not know about:** Rust internals, specific storage backends, or the
PyO3 struct layout. It talks to `flexiq._flexiq` through its public surface only.

### 2. Feature subsystems — pure-Python packages

These are self-contained packages composed onto the Queue. Each is a leaf the API
layer depends on, not the reverse:

- `interception/` — argument interceptor (runs before serialization) + reconstructor
  (worker side). Strategies: PASS / CONVERT / REDIRECT / PROXY / REJECT.
- `proxies/` — deconstruct/reconstruct non-serializable objects (files, sessions,
  cloud clients) with HMAC signing, schema validation, path allowlists.
- `resources/` — worker-side dependency injection with four scopes
  (WORKER/TASK/THREAD/REQUEST), hot reload, and TOML config.
- `workflows/` — builder DSL, tracker, fan-out, sagas, gates (backed by the
  workflows crate).
- `contrib/` — optional integrations (`otel`, `sentry`, `prometheus`, `flask`,
  `fastapi`, `django/`). All support `task_filter` and custom prefixes/attributes.
- `batching/`, `autoscale/`, `prefork/`, `predicates/`, `dashboard/`.

### 3. PyO3 bindings — `crates/flexiq-python/`

The Python translation layer. `py_queue/` holds `PyQueue` plus partial
`#[pymethods]` blocks split by concern (enabled by PyO3's `multiple-pymethods`
feature): `mod.rs`, `inspection.rs`, `worker.rs`, and `workflow_ops/` (lifecycle,
nodes, fan_out, gates, queries, saga). `async_worker.rs` drives the
`AsyncWorkerPool` with `spawn_blocking` + GIL management. This layer converts
Python values to Rust and back; it holds **no business logic**. It implements the
core's `WorkerDispatcher` trait — the same contract the Node and Java bindings implement.

### 4. Rust core — `crates/flexiq-core/`

The engine. `scheduler/` is split by concern — `poller.rs` (dequeue & dispatch),
`result_handler.rs` (outcome → retry/dead-letter/cancel), `maintenance.rs`
(reap/cleanup/periodic), `wake.rs` (opt-in push dispatch) — with `mod.rs` holding
config and the `run()` loop. `worker.rs` defines the `WorkerDispatcher` trait
(future-proofing for non-Python bindings). `resilience/`, `periodic.rs`,
`job.rs`, and `error.rs` round it out.

**Must not know about:** Python. Keep it that way — it's what lets the core be
tested in pure Rust and reused behind other language bindings.

### 5. Storage — `crates/flexiq-core/src/storage/`

A `Storage` trait (`traits.rs`) with three backends behind a `StorageBackend`
enum wired up by a `delegate!`-style macro in `mod.rs`. The trick that keeps this
DRY: **shared Diesel logic lives in `diesel_common/` macros**
(`impl_diesel_job_ops!`, `impl_diesel_lock_ops!`, and siblings for dead-letter,
archival, logs, metrics, workers, migrations). SQLite and Postgres differ only in
a handful of backend-specific files (locking strategy, upsert). Redis is a
separate hand-written impl (`redis_backend/`, JSON values + sorted sets + Lua
scripts for atomicity) — no Diesel.

The large size of `diesel_common/jobs.rs` is **a feature, not a smell**: it is one
macro that erases SQLite/Postgres duplication. Don't "flatten" it into per-backend
copies.

### 6. Workflows crate — `crates/flexiq-workflows/`

A separate crate with its own schema (`workflow_definitions`, `workflow_runs`,
`workflow_nodes`) and migration path, kept apart so workflow state evolves
independently of the job queue. It now ships stores for **all three backends** —
`sqlite_store.rs`, `postgres_store.rs`, `redis_store.rs` — selected at runtime via
the `WorkflowStorageBackend` enum (`workflow_ops/mod.rs::workflow_storage`), with
Postgres/Redis behind cargo features.

### 7. Native async — `flexiq-python/src/native_async/`

Optional (`native-async` feature). Python-specific binding code (was the separate
`flexiq-async` crate; folded in because every line is Python-coupled).
`NativeAsyncPool` dual-dispatches: async tasks run on the Python event loop, sync
tasks via `spawn_blocking`. `PyResultSender` bridges the Python executor back to
the Rust scheduler.

### 8. Node SDK — `sdks/node/` + `crates/flexiq-node/`

The second language shell, peer to Python. `crates/flexiq-node/` is a
[napi-rs](https://napi.rs) binding crate (`JsQueue`/`JsWorker`, `dispatcher.rs`,
`convert/`) that implements the same core `WorkerDispatcher` trait — the Rust↔JS
seam, holding no business logic. `sdks/node/` is the TypeScript package (dual
ESM/CJS via tsup): `Queue`/`Worker`, serializers, a standalone `flexiq` CLI, a
dashboard server reusing the existing React SPA, and Node-native events/middleware,
webhooks, and resource DI. Python-idiom features (proxies, interception) get Node
**equivalents**, not 1:1 ports. The DB stays the source of truth, so a Python and a
Node worker can share one queue.

### 9. Java SDK — `sdks/java/` + `crates/flexiq-java/`

The third language shell, peer to Python and Node. `crates/flexiq-java/` is a
[JNI](https://github.com/jni-rs/jni-rs) binding crate (`backend.rs`, `dispatcher.rs`,
`convert.rs`, `queue/`, `workflows/`, `worker.rs`, `mesh.rs`) that implements the same
core `WorkerDispatcher` trait, with an FFM fast-path (`ffi.rs`, `ffi_c.rs`) that
bypasses JNI on JDK 22+. `sdks/java/` is the Gradle project (package
`org.byteveda.flexiq`, baseline JDK 17): a call on `Queue`/`FlexiQ` routes
`DefaultFlexiQ → core.CoreFacade → spi.QueueBackend → internal.JniQueueBackend →
JNI`, so the JNI seam is the shell's single point of contact with Rust. Feature
packages mirror the other shells — `workflows/`, `resources/` (DI), `proxies/`,
`interception/`, `serialization/` (payload codecs), `middleware/`, `webhooks/`,
`autoscale/`, `contrib/`. Extra Gradle subprojects: `:processor` (compile-time
`@TaskHandler` processing, GraalVM-clean), `:test-support` (JNI-free
`InMemoryQueueBackend`), `:spring`, `:graalvm-smoke`. Python-idiom features get Java
**equivalents**, not 1:1 ports; the DB stays the shared source of truth.

### 10. Mesh — `crates/flexiq-mesh/`

Optional (`mesh` feature). A decentralized work-stealing overlay — SWIM gossip
(UDP), a consistent-hash ring, a local deque, and TCP work-stealing — composed
behind `MeshNode`. It is an **optimization only**: the DB remains the source of
truth, and a mesh bridge keeps the scheduler and dispatcher unaware of mesh logic.
Gossip failure degrades cleanly to DB-only dispatch.

---

## Dependency & boundary rules

These invariants are what make the codebase easy to change. Treat a PR that
violates one as a design regression, not a style nit.

1. **Dependencies point downward only.** SDK → its binding crate → core → storage.
   `flexiq-core` must not depend on any binding crate (`flexiq-python`,
   `flexiq-node`, `flexiq-java`) or host-language type.
2. **Each binding crate is its language's only seam.** A shell touches Rust solely
   via its compiled addon's public surface — no reaching into struct internals. Keep
   the surface contract in sync: `_flexiq.pyi` for Python, the generated `.d.ts` for
   Node, the `spi.QueueBackend` interface for Java. A new language implements
   `WorkerDispatcher` against `BINDING_CONTRACT.md`.
3. **Asyncio is confined to `async_support/`** (plus the narrow, documented
   exceptions: `app.py` uses only `iscoroutinefunction`; `contrib/fastapi.py`).
   No inline `import asyncio` to dodge a boundary — split the module instead.
4. **Shared storage logic lives in `diesel_common/` macros**, never copy-pasted
   per backend. Backend files hold only genuine SQLite/Postgres/Redis differences.
5. **Locks are table-based, not advisory** — advisory locks are connection-scoped
   and interact badly with pooling.
6. **All three backends move together.** A storage change isn't done until SQLite,
   Postgres, and Redis all implement it (CI runs all three).

---

## Extension recipes — "where do I add X"

These are the ordered touch-points for the common changes. The skills under
`.claude/skills/` (`storage-impl`, `python-api`, `rust-python-check`) carry the
detailed versions.

### Add a Storage method
1. Declare it on the `Storage` trait — `storage/traits.rs`.
2. Implement shared logic in the relevant `diesel_common/` macro (covers SQLite +
   Postgres at once).
3. Add any backend-specific delta in `sqlite/` and `postgres/`.
4. Implement it for Redis in `redis_backend/`.
5. Wire it through the `delegate!` macro in `storage/mod.rs`.
6. Expose it on each shell that needs it: PyO3 in `crates/flexiq-python/src/py_queue/`
   (then add the signature to `sdks/python/flexiq/_flexiq.pyi`), napi in
   `crates/flexiq-node/src/queue/`, and/or JNI in `crates/flexiq-java/src/queue/`.
7. Test: a Rust test in `storage/sqlite/tests.rs` + the contract suite (runs against
   all three backends in CI).

### Add a Python feature / Queue method
- Find the mixin that owns the concern (`mixins/`) and add the method there — do
  **not** grow `app.py`. New concern → new mixin, composed onto `Queue`.
- Async counterpart (if any) goes in `async_support/mixins.py` as an `a*` wrapper.
- Test under the matching `tests/<area>/` subdirectory.

### Add a contrib integration
- New module under `contrib/` following the existing middleware shape: support
  `task_filter`, configurable prefixes/namespaces, and an extra-attributes
  callable. Keep instance-based state (no module-level singletons — see Prometheus).

### Add a serializer or proxy handler
- Serializer: implement the `Serializer` protocol (`serializers.py`); per-task use
  via `@queue.task(serializer=...)`.
- Proxy handler: implement the `ProxyHandler` protocol and register it
  (`proxies/`), following the built-in handlers in `proxies/handlers/`.

---

## Why this scales

The codebase is large (≈25k LOC Python, ≈37k LOC Rust across 6 crates, ≈12k LOC Java,
≈7k LOC Node/TS) but not heavy: no god-objects, an acyclic dependency graph, one
binding seam per language, and duplication erased by macros. The biggest files are deduplicating
macros (`diesel_common/jobs.rs`) or inherently complex state machines (saga, scheduler)
— large because the *problem* is, not because concerns are tangled. Drift comes from
eroding the six boundary rules above; hold them and the design scales with the
contributor count.
