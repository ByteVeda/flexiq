# Redis dispatch round trips (#959, #960, #961)

Epic #958. Branch `perf/redis-dispatch-round-trips` off `master` (`d311a748`). **Never push.**
Research notes (file:line map): `/tmp/redis-perf-facts.md` — read the section named in your task.

## Decisions (made with the user, binding)

1. **#960 — one Lua script selects AND claims up to N jobs in one round trip.** Jobs with
   `has_deps` keep the current Rust slow path. Expired jobs are *reported* by the script and
   archived by Rust (existing `archive_job_immediately`). The job document is patched by
   exact-token string swap, **never** `cjson.encode` (empty `[]` → `{}` corruption,
   `steps.rs:27-30`).
2. **#959 — `SchedulerConfig::batch_size` becomes `Option<usize>`, default `None`.** `None` →
   backend picks: Redis `8`, SQLite/Postgres `1` (no behaviour change there). An explicit
   `Some(n)` is always honoured (clamped `>= 1`). Shells pass `None` when the user set nothing.
   Public type change is accepted (Rust SDK users see it).
3. **#961 — ship `push-dispatch` compiled into every shipped artifact; default-on for Redis
   only.** SQLite/Postgres stay polling unless the user opts in. A user can opt out on Redis
   explicitly (`push_dispatch=false`).
4. Verification: Redis contract suite on hosted Redis + new round-trip / bounded-shutdown tests,
   plus (controller, at the end) a local, uncommitted bench run.

## Global Constraints

- **Cargo: one job, never two cargo invocations at once.** Always `-j1` on
  `cargo build/check/test`; `CARGO_BUILD_JOBS=1` for `cargo clippy`, `uv run maturin develop`,
  `pnpm build:native`, and gradle. The untracked `.cargo/config.toml` says `jobs = 4` — ignore
  it, pass `-j1` explicitly. Prefer `cargo test -j1 -p flexiq-core --features redis --test rust <filter>`.
- **Redis tests use the hosted Redis, never docker:**
  `FLEXIQ_REDIS_TEST_URL="$(cat /tmp/flexiq-redis-url)"` (credential file, never commit it or
  paste the URL into code, commits, or reports).
  It is remote (~high latency) and persistent: a count-only failure is suspect stale state —
  rerun once. `test_dequeue_batch_archives_expired_jobs` is a known flake; rerun once.
- **Commit identity (all five flags, every commit):**
  `git -c user.name=kartikeya -c user.email=67143288+kartikeya-27@users.noreply.github.com -c user.signingkey=/home/ezio/.ssh/git_kartik_signing.pub -c gpg.format=ssh -c commit.gpgsign=true commit ...`
  No `Co-Authored-By`, no Claude/Anthropic/AI mention. Conventional prefix (`perf:`, `feat:`,
  `test:`, `docs:`, `build:`), subject ≤ 60 chars, imperative, no `@`. One logical change per
  commit — split a task into several commits when it has several changes. Stage explicit paths
  with `git add <paths> && git commit ...` (never silence `git add` stderr). After committing,
  `git log -1 --format='%h %an %s'` to prove it landed (a hook rejection still exits 0 in a chain).
  Pre-commit hooks run fmt/clippy/ruff/mypy — fix, never `--no-verify`.
- **Code style:** no `unwrap()`/`expect()` in library code, `FlexiQError` variants, no inline
  (function-scoped) imports, comments 1-2 lines stating the *why*, match surrounding density.
  Never name a sibling SDK inside an SDK's code/docs (say "cross-SDK contract").
- **Python:** `uv run` always, from `sdks/python/`; ruff + mypy on tests too. Never bare `uv sync`.
- Error strings are an interface — grep `match=`/`toThrow` before rewording any.
- Do not dispatch subagents. Do not push.

---

## Task 1: Fold candidate selection into the Redis claim script (#960)

**Files:** `crates/flexiq-core/src/storage/redis_backend/jobs/dequeue.rs` (main),
`crates/flexiq-core/tests/rust/storage_tests.rs` (Redis tests, `redis_*` fns invoked from
`redis_storage_tests` ~:4208).

Read first: `/tmp/redis-perf-facts.md` §5, §6, §7; `dequeue.rs` in full; `helpers.rs`
(`namespace_segment`, `debounce_index_key`, `job_debounce_index_key`, `archive_job_immediately`);
`pubsub.rs:454-473` (`reindex_pubsub_best_effort`); `job.rs:52-65` (`JobStatus::wire_name`).

### Behaviour

New `SELECT_AND_CLAIM_SCRIPT` (Lua, `redis::Script`, invoked once per queue):

- KEYS: `queue_pending_zset`, `pending_status_set` (`jobs:status:0`), `running_status_set`
  (`jobs:status:1`).
- ARGV: `key_prefix` (the storage prefix, so the script can build `job:<id>` and the debounce
  index key), `now` (ms), namespace mode + value (must distinguish `None` = "only jobs without a
  namespace" from `Some(ns)`), `max` (claims wanted), `scan_limit`.
- Steps: `ZRANGEBYSCORE queue -inf +inf LIMIT 0 scan_limit`; for each id in score order, stop
  once `max` claimed:
  - `GET job:<id>`; missing → `ZREM` from the queue zset (stale), continue.
  - `cjson.decode` (decode only). Skip unless status is the wire name `"Pending"` and
    `scheduled_at <= now`. Namespace filter identical to today (`cjson.null` ⇒ no namespace).
  - `expires_at` non-null and `now > expires_at` → do NOT claim; append the id to an `expired`
    list returned to Rust; continue.
  - `has_deps == true` → do NOT claim; append the raw document to a `deferred` list; continue.
  - Claim: guard `SISMEMBER pending_status_set id` (0 → skip). Patch the document by exact
    token swap: `"status":"Pending"` → `"status":"Running"` and `"started_at":null` →
    `"started_at":<now>`, each must replace **exactly one** occurrence (use plain `string.find`
    with `plain=true` and splice; Lua patterns treat `"`/`:` fine but be explicit). If either
    count ≠ 1, do not claim — append the document to `deferred` (Rust slow path handles it).
    Then `SET job:<id> patched`, `SREM pending`, `SADD running`, `ZREM queue`, and if the
    decoded `debounce_key` is non-null, `ZREM` the debounce index key built exactly as
    `debounce_index_key(namespace, key)` builds it (mirror `namespace_segment` in Lua —
    read it and replicate byte-for-byte). Append the patched document to `claimed`.
- Return `{claimed_docs, expired_ids, deferred_docs}`.

Rust side (`dequeue_batch`):

- `scan_limit = (max * 4).clamp(100, 400)` (keeps single-job `dequeue`'s old 100 window).
- One script invocation → deserialize `claimed` docs with `serde_json` (they are the Running
  jobs; `started_at == Some(now)`). For each `expired` id: load, set Cancelled/completed_at/
  error `"expired before execution"`, `ZREM` + `archive_job_immediately(..., JobStatus::Pending)`
  — the exact existing semantics. For each `deferred` doc (budget remaining): run the existing
  dependency check + `claim_pending` path (keep `claim_pending` and `CLAIM_JOB_SCRIPT` for this).
  Call `reindex_pubsub_best_effort(conn, &job, Running)` for every claimed job.
- Preserve score order in the returned `Vec` for the fast path. Deferred (has_deps) claims are
  appended after fast-path claims — **Ruling: acceptable** (dependency jobs were never strictly
  ordered against peers; document it in a comment).
- `dequeue(queue, now, ns)` becomes `dequeue_batch(queue, now, ns, 1).map(|v| v.into_iter().next())`
  — one code path. `dequeue_from` / `dequeue_batch_from` unchanged in shape.
- **Delete** the TOCTOU sentence on `dequeue_batch`'s doc comment ("Shares `dequeue`'s TOCTOU
  window …") and rewrite the doc to describe the single-round-trip select+claim.
- Remove now-dead code; no `#[allow(dead_code)]` left behind.

### Tests (Redis, add `redis_*` fns and register them in `redis_storage_tests`)

Existing generic suite must pass unchanged (`test_dequeue`, `test_dequeue_batch`,
`test_dequeue_batch_archives_expired_jobs`, `test_dispatch_order_*`, debounce tests,
`redis_debounce_index_never_outlives_its_job`). Add:

1. `redis_claim_preserves_empty_payload` — enqueue a job with `payload: vec![]`, dequeue, then
   reload it via `get_job`: payload is `[]`, status Running, `started_at == Some(now)`.
2. `redis_claim_respects_priority_then_schedule_order` — mixed priorities/scheduled_at, one
   `dequeue_batch(max=N)` returns them in score order.
3. `redis_claim_skips_future_and_foreign_namespace` — future `scheduled_at` and other-namespace
   jobs are left Pending; `None` namespace does not claim namespaced jobs and vice versa.
4. `redis_claim_defers_dependency_jobs` — a `has_deps` job with an incomplete dep is not
   claimed; after the dep completes (archived Complete) it is claimed.
5. `redis_select_and_claim_is_one_round_trip` — prove the fast path issues no `ZRANGEBYSCORE`/
   `MGET` from the client: take `INFO commandstats` before/after a `dequeue_batch(max=4)` over 4
   plain jobs and assert `cmdstat_zrangebyscore`/`cmdstat_mget` calls did not grow and
   `cmdstat_evalsha` (or `eval`) grew by exactly 1 (use a fresh connection for INFO; tolerate
   the INFO call itself). If commandstats is unavailable on the hosted Redis (`INFO` restricted),
   fall back to asserting via a `redis::Script` hash check and note it in the report.

Run: `FLEXIQ_REDIS_TEST_URL=… cargo test -j1 -p flexiq-core --features redis --test rust redis_storage_tests`
plus `cargo test -j1 -p flexiq-core --test rust` (SQLite unaffected) and
`CARGO_BUILD_JOBS=1 cargo clippy -p flexiq-core --features redis --all-targets -- -D warnings`.

Commits: `perf(redis): select and claim jobs in one script` (+ tests may be the same commit or a
separate `test(redis): …` commit).

---

## Task 2: Backend-aware default dispatch batch (#959)

**Files:** `crates/flexiq-core/src/scheduler/mod.rs`, `scheduler/poller.rs`,
`crates/flexiq-python/src/py_queue/{mod.rs,worker.rs}`, `crates/flexiq-python/src/py_queue/workflow_ops/test_helpers.rs`,
`sdks/python/flexiq/_flexiq.pyi`, `sdks/python/flexiq/app.py`, `crates/flexiq-node/src/worker.rs`,
`crates/flexiq-java/src/worker.rs`, plus every `SchedulerConfig { batch_size: … }` literal
(grep the workspace incl. tests, examples, doctests in READMEs/`*.md` compiled as doctests,
`crates/flexiq`, `crates/flexiq-server`). Research map: `/tmp/redis-perf-facts.md` §1.

### Behaviour

- `pub batch_size: Option<usize>` — doc: `None` lets the backend choose (Redis 8, else 1);
  `Some(n)` is honoured, clamped to ≥ 1.
- `pub const REDIS_DEFAULT_BATCH_SIZE: usize = 8;` (named, documented why: one selection script
  serves several claims on a network hop; local backends gain nothing and a bigger batch only
  widens claim→dispatch).
- `Scheduler::batch_size(&self) -> usize` resolves it once from `self.storage` (match on
  `StorageBackend::Redis` under `#[cfg(feature = "redis")]`). Both reads (`tick_dispatch`,
  `try_dispatch_batch`) use it. Resolve at construction or per call — per call is fine if cheap.
- Python: `scheduler_batch_size: int | None = None` in `app.py`, `_flexiq.pyi`, PyO3 signature
  `scheduler_batch_size=None` → `Option<usize>`; map `Some(n) → Some(n.max(1))`. Update the
  kwarg's docstring. Node/Java: `options.batch_size.map(|b| (b.max(1)) as usize)` into the
  config (no longer default-then-override). Server and Rust SDK: `SchedulerConfig::default()`
  now yields `None` → they get the backend default automatically.
- Existing tests that set `batch_size: 8` → `Some(8)`; `rate_limited_scheduler(…, batch_size)`
  → `Some(batch_size)`.

### Tests

- Unit tests in `scheduler/mod.rs`: SQLite + `None` → 1 (and `tick_dispatch` takes the single
  path); SQLite + `Some(8)` → 8; `Some(0)` → 1; Redis + `None` → 8 (gate on
  `FLEXIQ_REDIS_TEST_URL` / `#[cfg(feature = "redis")]`, constructing `RedisStorage` only —
  resolution must not need a live connection; if `RedisStorage::new` connects eagerly, skip
  gracefully like `redis_storage_tests`).
- Python: a test that `Queue()` without the kwarg passes `None` and `Queue(scheduler_batch_size=4)`
  is accepted (find existing tests of `scheduler_batch_size` first and extend them).
- All existing dispatch tests pass: `cargo test -j1 -p flexiq-core --lib scheduler`,
  `cargo test -j1 -p flexiq-core --test rust`, `cargo check -j1 --workspace` and with
  `--features redis`, `--features postgres`; Python: `CARGO_BUILD_JOBS=1 uv run maturin develop`
  then `uv run python -m pytest tests/ -q -x` (full suite once, before committing),
  `uv run ruff check flexiq/ tests/`, `uv run mypy flexiq/ --no-incremental`.
- Node/Java: `cargo check -j1 -p flexiq-node --features redis` and `-p flexiq-java --features redis`.

Commits (split): `perf(scheduler): pick dispatch batch per backend` (core + literal sites),
`feat(python): let the backend pick the dispatch batch` (python shell), and one for node+java
shell mapping if it is not trivially part of the core commit (a shell that fails to compile
without the change belongs in the core commit — then say so in the report).

---

## Task 3: Wake Redis workers on enqueue instead of polling (#961)

**Files:** `crates/flexiq-core/src/scheduler/mod.rs` (`run`, `enable_push_dispatch`, new
opt-out), `scheduler/wake.rs`, `storage/redis_backend/listener.rs`, shells:
`crates/flexiq-python/src/py_queue/{mod.rs,worker.rs}`, `sdks/python/flexiq/{app.py,_flexiq.pyi}`,
`crates/flexiq-node/src/{config.rs,worker.rs}`, `crates/flexiq-java/src/{convert.rs,worker.rs}`,
`crates/flexiq-core/src/worker/runner.rs` (turnkey `Worker` used by Rust SDK + server),
`crates/flexiq-server/Cargo.toml`. Build/ship wiring: `sdks/python/pyproject.toml:62`,
`.github/workflows/publish-py.yml` (every `--features` list), `sdks/node/package.json:93`
(`build:native`) and any other node native build command in CI/workflows,
`sdks/java/build.gradle.kts:219`, `.github/workflows/publish-java.yml:107`,
`docker/scheduler.Dockerfile:64-65`, `crates/flexiq/Cargo.toml` default features if it has a
`default = [...]` list. Grep `.github/workflows` for every `--features` that builds a shell and
add `push-dispatch` where the artifact ships. Research map: `/tmp/redis-perf-facts.md` §2, §3, §4.

### Behaviour

- Core decides the default so every shell gets it: in `Scheduler::run` (push-dispatch build),
  if no wake source was installed and the user did not opt out, and the storage is Redis,
  install `WakeSource::for_storage` (we are inside the runtime there) and run `run_push`.
  SQLite/Postgres: unchanged (polling unless `enable_push_dispatch()` was called).
- New `pub fn disable_push_dispatch(&self)` (both cfg variants; the non-feature one is a no-op)
  that records the opt-out. Shell tri-state: `Some(true)` → enable, `Some(false)` → disable,
  `None` → nothing (core default). Python kwarg becomes `push_dispatch: bool | None = None`
  (pyi, app.py, PyO3 `Option<bool>`), docstrings updated: "default: on for Redis".
- `flexiq-server`: add `push-dispatch = ["flexiq-core/push-dispatch"]` and turn it on in the
  shipped Dockerfile feature list. Turnkey `Worker` (runner.rs) needs no toggle — core default
  covers Redis.
- Shipped builds compile `push-dispatch` (Python wheel, Node addon, Java native, server image).
- **Bounded shutdown:** the listener must stop promptly when the scheduler stops. Keep the 1 s
  `BLPOP` backstop; `run_push` returning drops the receiver, the listener notices within one
  `BLPOP` timeout. Add a test proving it (below). If the reconnect-backoff branch can exceed the
  bound, tighten it (check `tx.is_closed()` before and after each sleep).
- Rename log prefixes `push-dispatch:` stay as is (operators grep them).

### Tests

- `scheduler/mod.rs` (or `tests/rust/`), gated `#[cfg(all(feature = "push-dispatch", feature = "redis"))]`
  and on `FLEXIQ_REDIS_TEST_URL` (skip gracefully when unset), using
  `RedisStorage::with_prefix(url, "<unique uuid prefix>:")` for isolation (never FLUSHDB here):
  1. `redis_worker_wakes_on_enqueue_by_default` — scheduler on Redis with no explicit enable;
     start `run`, enqueue a ready job, assert it is dispatched well before the push fallback
     timer (`PUSH_FALLBACK_INTERVAL` 2 s) would fire — i.e. the wake path did it. Account for
     remote latency: assert < 1.5 s, not a tight number.
  2. `redis_push_opt_out_keeps_polling` — `disable_push_dispatch()` → `take_wake_source` stays
     `None` / the poll loop runs (assert via an observable, not timing).
  3. `redis_listener_shutdown_is_bounded` — build a dedicated multi-thread runtime, run the
     scheduler on Redis with push active, notify shutdown, then **drop the runtime** and assert
     the whole stop+drop finishes within 3 s (budget = failure deadline, not a delay).
  4. SQLite default stays polling: `take_wake_source()` is `None` after construction and
     `run` does not install one (unit test, no Redis).
- Run: `cargo test -j1 -p flexiq-core --features push-dispatch,redis <filters>` with the
  Redis URL; `cargo test -j1 -p flexiq-core --features push-dispatch --lib scheduler`;
  `cargo check -j1 --workspace --features push-dispatch,postgres,redis`;
  `cargo check -j1 -p flexiq-server --features redis,push-dispatch`;
  Python rebuild (`CARGO_BUILD_JOBS=1 uv run maturin develop --features push-dispatch` or the
  pyproject default once changed) + pytest subset touching `push_dispatch` + ruff + mypy.

Commits (split): `feat(scheduler): wake Redis workers on enqueue by default`,
`feat: tri-state push_dispatch option in the shells` (may split per shell if large),
`build: compile push-dispatch into shipped artifacts`.

---

## Task 4: Keep the ready-notify off the Redis enqueue critical path

**Why:** with `push-dispatch` now compiled into shipped builds, every Redis enqueue calls
`RedisStorage::notify_job_ready` (`redis_backend/mod.rs:110-128`), which checks out a connection
and runs `LPUSH`+`LTRIM` as its own round trip — one extra network hop per enqueue that did not
exist in shipped builds before. Enqueue throughput is a published number.

**Files:** `crates/flexiq-core/src/storage/redis_backend/{mod.rs,jobs/enqueue.rs}`,
`crates/flexiq-core/src/storage/mod.rs` (`notify_if_ready` ~:1575-1650).

### Behaviour

- Fold the `LPUSH <notify_key> 1` + `LTRIM <notify_key> 0 15` into the pipeline each Redis enqueue
  path already sends (enqueue, enqueue_batch, unique/unique-batch reporting, debounced), only
  when the job is ready now (`scheduled_at <= now`, same rule as `notify_if_ready`), so a Redis
  enqueue costs the same number of round trips as before the feature was compiled in. Then make
  `StorageBackend::notify_if_ready` skip the Redis arm (the enqueue already notified); retry /
  sleep `signal_scheduled` keep calling `notify_job_ready` (those are not enqueue pipelines).
- If an enqueue path does not use a pipeline/script where the push can ride along, leave that
  path on `notify_job_ready` and list it in the report.
- The notify stays best-effort: it must never turn a successful enqueue into an error. Inside a
  `MULTI` pipeline an `LPUSH` on a list key cannot fail for type reasons unless the key is
  corrupted — note this in a comment.

### Tests

- Redis (hosted URL, unique prefix): after `enqueue` of a ready job the notify list has length
  ≥ 1; after `enqueue` of a future job it is unchanged; `enqueue_batch` with one ready job
  pushes. Existing Redis suite passes.
- `cargo test -j1 -p flexiq-core --features push-dispatch,redis --test rust redis_storage_tests`,
  `cargo check -j1 -p flexiq-core --features redis` (feature off must still compile — the
  pipeline addition is `#[cfg(feature = "push-dispatch")]`).

Commit: `perf(redis): notify workers inside the enqueue pipeline`.

---

## Task 5: Docs

**Files:** docs pages listed in `/tmp/redis-perf-facts.md` §1 "Docs mentioning default 1"
(execution-model, batch-enqueue, benchmark example, troubleshooting, deployment, python queue
symbol page, python upgrading page only if it states a *current* default — leave historical
changelog lines alone), and push-dispatch pages:
`grep -rln "push.dispatch\|push_dispatch\|pushDispatch" docs/content` (python/node/java
worker/queue reference, rust installation/overview, server operate). Also
`crates/flexiq-core/BINDING_CONTRACT.md` if it documents `batch_size` or push dispatch.

- State: dispatch batch default = backend-chosen (Redis 8, SQLite/Postgres 1), explicit value
  honoured; push dispatch is compiled into shipped packages and on by default for Redis
  (opt out with `push_dispatch=false` / `pushDispatch: false` / Java equivalent), opt-in for
  SQLite/Postgres; the "requires building with the `push-dispatch` feature" caveats go away for
  shipped packages.
- Never put a command in docs that was not run. Use `<Tab sdk=…>` patterns already on the page.
- Verify: `pnpm --dir docs install --frozen-lockfile` (if needed), `pnpm --dir docs typecheck`,
  `pnpm --dir docs lint`. (`build` is heavy — skip unless the page structure changed.)

Commit: `docs: backend-chosen dispatch batch and Redis push`.
