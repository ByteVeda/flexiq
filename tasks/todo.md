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
- [ ] `redis_backend/jobs/{helpers.rs,enqueue.rs,state.rs}` — namespace the
      `jobs:unique:*` pointer key, thread `namespace` through
      `release_unique_key`, fix `redis_complete_preserves_reused_unique_key`'s
      raw key.
- [ ] Confirm the new test passes on SQLite; `cargo check --workspace` /
      `--features postgres` / `--features redis`.
- [ ] `traits.rs` doc + `job.proto` / `producer_service.proto` — drop the
      cross-namespace exception language now that it's fixed.
- [ ] Final full check pass + review section below.

## Review

(filled in once implementation is verified)
