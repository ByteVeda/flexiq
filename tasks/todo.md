# #773 — storage: unique_key dedupes across namespaces

Branch `fix/unique-key-namespace-scope`, off `master` at `dc5ceab3`. Plan:
`tasks/plans/2026-09-04-unique-key-namespace.md`. **Not pushed.**

## Done

- [x] Add `test_unique_key_dedup_is_namespace_scoped` to
      `tests/rust/storage_tests.rs`, confirm it fails on SQLite pre-fix
      (reproduces #773).
- [x] `crates/flexiq-core/migrations/m0017_unique_key_namespace.rs` — drop +
      recreate `idx_jobs_unique_key` over `(COALESCE(namespace, ''), unique_key)`.
- [x] `diesel_common/jobs.rs` — scope the 3 unique_key lookups (initial check,
      race re-read, batch check) by namespace, via a shared
      `find_active_by_unique_key` helper. SQLite contract test green.
- [x] `redis_backend/jobs/{helpers.rs,enqueue.rs,state.rs}` — namespace the
      `jobs:unique:*` pointer key, thread `namespace` through
      `release_unique_key`, fix `redis_complete_preserves_reused_unique_key`'s
      raw key. `cargo check`/`clippy --features redis` clean; no local
      redis-server here, so `redis_storage_tests` compiles and skips (full
      run happens in CI's Redis Cloud job).
- [x] Confirm `cargo check --workspace --features postgres` is clean.
- [x] `traits.rs` doc + `job.proto` / `producer_service.proto` — drop the
      cross-namespace exception language now that it's fixed. Regenerated
      `contracts/descriptor.binpb` via `./scripts/proto-check.sh --fix`.
- [x] Final full check pass + review section below.

## Review

Fixed on both backends that were affected — the Diesel `idx_jobs_unique_key`
partial index and its three query sites (initial check, race re-read, batch
check), and the Redis `jobs:unique:*` pointer key. Both now scope by
namespace, `None` treated as its own namespace (not "match anything"), via
the same encoding pattern each backend already used for the equivalent
debounce case (`lock_debounce_candidates`'s `Some/None` filter match on
Diesel, `debounce_index_key`'s length-prefixed segment on Redis).

`test_unique_key_dedup_is_namespace_scoped` reproduces #773 verbatim (two
tenants sending the same key; confirmed red pre-fix, green post-fix on
SQLite) and also covers the default-namespace NULL trap the issue calls out
— a naive `(namespace, unique_key)` unique index would look correct in any
test that sets a namespace and silently stop deduping the common case.

Verified: `cargo check --workspace` / `--features postgres` / `--features
redis` all clean; `cargo clippy --features redis` clean; `cargo fmt` clean;
sqlite contract suite green; redis/postgres contract suites compile and
skip gracefully (no `FLEXIQ_REDIS_TEST_URL`/`FLEXIQ_POSTGRES_TEST_URL` or
local redis-server in this sandbox — full three-backend run happens in
CI's three Rust jobs, per project convention). `./scripts/proto-check.sh`
clean after regenerating `contracts/descriptor.binpb`.

Not pushed, per instruction. Four commits on `fix/unique-key-namespace-scope`,
authored as the active gh account (pratyush618).
