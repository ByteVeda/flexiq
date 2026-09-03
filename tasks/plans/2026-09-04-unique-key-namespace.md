# #773 — storage: unique_key dedupes across namespaces

Branch `fix/unique-key-namespace-scope`, off `master` at `dc5ceab3`. **Not pushed.**
Issue: https://github.com/ByteVeda/flexiq/issues/773

## The hole

`unique_key` dedup reads across the namespace boundary every other id-addressed
`Storage` member (`get_job`, `cancel_job`, `request_cancel`, debounce) already
keeps. Tenant B sending its own `unique_key` in its own namespace gets back
**tenant A's job** instead of getting its own — a cross-tenant read, and the
job B asked for is silently never created.

Three places carry the bug, because they all reach the same unscoped query or
key:

- `crates/flexiq-core/src/storage/diesel_common/jobs.rs` — the duplicate
  lookup in `enqueue_unique_reporting` (twice: the initial check and the
  post-`UniqueViolation` race re-read) and in `enqueue_unique_batch_reporting`
  (once). All three filter on `unique_key` + `status` only.
- `crates/flexiq-core/src/storage/redis_backend/jobs/enqueue.rs` — the pointer
  key is `jobs:unique:<uk>`, namespace absent from both the key and the store
  script's race re-check.
- `crates/flexiq-core/migrations/m0001_initial.rs` — `idx_jobs_unique_key`, a
  partial unique index on `(unique_key)` (`WHERE unique_key IS NOT NULL AND
  status IN (0, 1)`). This is what actually enforces the identity on Postgres,
  where the read-then-write isn't serialized by a whole-DB lock the way
  SQLite's `BEGIN IMMEDIATE` serializes it — so the query-side fix alone isn't
  enough, the index has to agree.

**The trap:** rebuilding the index as `UNIQUE (namespace, unique_key)` silently
stops deduping the *default* namespace — `namespace` is nullable, and both
SQLite and Postgres treat two NULLs as distinct inside a unique index, so every
unnamespaced job gets its own slot and `enqueue_unique` inserts every time.
Looks fine in any test that sets a namespace, breaks exactly where most
deployments run. Needs `COALESCE(namespace, '')` in the index, and both
dialects have to agree with the query that reads it. `m0010_debounce` hit the
identical trap and sidestepped it by leaving that index non-unique (the
invariant lives in the write transaction, not the index); we don't have that
option because Postgres genuinely needs the DB-enforced constraint — that's
what the existing `UniqueViolation`-then-retry code is already built around.

## The fix

**1. Migration `m0017_unique_key_namespace`** — drop `idx_jobs_unique_key`,
recreate it over `(COALESCE(namespace, ''), unique_key)`, same partial
predicate. `sea_query`'s `Index` builder has no expression-column support, so
the create side goes through the `raw_ddl` escape hatch (`m0004` precedent) —
one literal, since `CREATE UNIQUE INDEX IF NOT EXISTS ... WHERE ...` with a
`COALESCE` column is valid, identical SQL on both SQLite and Postgres. The drop
goes through `sea_query`'s `Index::drop().if_exists()`, portable as-is.

**2. Diesel lookups** (`diesel_common/jobs.rs`) — all three `unique_key`
lookups gain `.into_boxed()` (already the pattern used elsewhere in this file,
e.g. `complete`) plus a namespace filter matching the exact scheme
`sqlite/jobs.rs::lock_debounce_candidates` already uses:
`Some(ns) => .filter(jobs::namespace.eq(ns))`,
`None => .filter(jobs::namespace.is_null())`. Not `job_in_namespace` (that
helper's `caller.is_none_or(...)` treats `None` as "no filter, matches
anything" — the right semantics for a scoped *read*, wrong here: we need the
row's namespace to equal the enqueuing job's namespace exactly, including
None-equals-None).

**3. Redis pointer key** (`redis_backend/jobs/{enqueue.rs,helpers.rs}`) — mirror
`debounce_index_key`'s injective namespace segment (`-` for default,
`<len>:<ns>` otherwise — length-prefixed so a `:` inside `ns` can't collide two
different pairs). Factor that segment computation out of `debounce_index_key`
into a shared `namespace_segment` helper, add `unique_key_key(namespace, uk)`
next to it, and use it both where the pointer is written
(`enqueue_unique_reporting`) and where it's released
(`release_unique_key`, which needs a new `namespace` parameter — both call
sites, `helpers.rs::delete_archived_job` and `state.rs::complete`, already have
`job.namespace` in scope). Old `jobs:unique:*` keys (no namespace segment) are
orphaned by the rename; they expire with the jobs they used to point at, so no
migration needed — just a note.

**4. Contract-suite test** (`tests/rust/storage_tests.rs`, runs on all three
backends via `run_storage_tests`) — one new
`test_unique_key_dedup_is_namespace_scoped`: the same key in two namespaces
yields two distinct, non-deduplicated jobs; the same key twice in one
namespace dedupes (`false` then `true`); and the same for the default
namespace (`None`), which is the case the NULL trap breaks. Also fixes
`redis_complete_preserves_reused_unique_key`'s raw key construction
(`rkey(s, &["jobs", "unique", shared])` → needs the `-` default-namespace
segment inserted, matching `redis_debounce_index_size`'s established style for
rebuilding a namespaced key by hand in a test).

**5. Proto doc** — `contracts/proto/flexiq/v1/job.proto`'s `EnqueueOptions.unique_key`
documents the cross-namespace exception from #772; it becomes false once this
lands, so the paragraph comes out (replaced with the same "scoped to the
caller's namespace" phrasing `depends_on`'s doc already uses on the same
message). `producer_service.proto`'s "one exception, documented on
EnqueueOptions.unique_key" clause in the service-level doc comment goes too.
`traits.rs::enqueue_unique`'s doc gains one clause noting the scope, for
symmetry with `enqueue`'s dependency-boundary doc.

## Files touched

- `crates/flexiq-core/migrations/m0017_unique_key_namespace.rs` (new)
- `crates/flexiq-core/src/storage/diesel_common/jobs.rs`
- `crates/flexiq-core/src/storage/redis_backend/jobs/enqueue.rs`
- `crates/flexiq-core/src/storage/redis_backend/jobs/helpers.rs`
- `crates/flexiq-core/src/storage/redis_backend/jobs/state.rs`
- `crates/flexiq-core/src/storage/traits.rs`
- `crates/flexiq-core/tests/rust/storage_tests.rs`
- `contracts/proto/flexiq/v1/job.proto`
- `contracts/proto/flexiq/v1/producer_service.proto`

Not touched, deliberately: `crates/flexiq-core/BINDING_CONTRACT.md`'s
"Fan-out `unique_key` salting" note (`jobs` unique index is global) — that's
about per-*subscriber* salting so a fan-out publish doesn't dedupe its own
deliveries against each other, orthogonal to the tenant boundary. No SDK
(Python/Node/Java) docstring repeats the cross-namespace caveat — grepped, only
the two proto files carry it.

## Verification

- `cargo check --workspace -j2`, `--features postgres -j2`, `--features redis -j2`
- `cargo test --workspace -j2` — exercises `sqlite_storage_tests` (always) and
  compiles/attempts `postgres_storage_tests` / `redis_storage_tests` (skip
  gracefully without `FLEXIQ_POSTGRES_TEST_URL` / a reachable Redis — neither
  is available in this sandbox, so full three-backend behavioral confirmation
  happens in CI's three Rust jobs, not here)
- New test run once red (pre-fix, reproduces the issue on SQLite) and once
  green (post-fix)
