# `flexiq.admin.v1` (#836) — plan

Design: `tasks/specs/2026-09-23-admin-service-design.md`.
Branch `feat/admin-service` off `master` (`8f27a50d`), upstream unset, **not
pushed**. Commits authored as stromanni. One cargo job at a time (`-j1`,
`CARGO_BUILD_JOBS=1` for clippy). Every commit's staged tree must compile
(pre-commit clippy `--all-targets --all-features`).

**Gate:** the proto (spec § "The proto") is reviewed by the user before step 7.
Approved 2026-09-23.

## Phase 1 — namespace the core

- [x] 1. Queue pause (D2): `m0020`, `diesel_common/queue_state.rs`, Redis
      per-namespace set with legacy default key, poller, dashboard, TUI, shells.
      Contract test + scheduler unit test.
- [x] 2. Workers (D3): `m0021`, `WorkerRegistration::namespace`,
      `list_workers(namespace)`, 4 registration sites, 8 consumers.
- [x] 3. DLQ (D4): namespaced `purge_dead`/`purge_dead_by_task`, `get_dead`.
- [x] 4. Overrides (D5): `flexiq_core::overrides`, server store, Python / Node /
      Java stores + worker-start apply, `BINDING_CONTRACT.md`, vector tests.
- [x] 5. Throughput (D6): `Storage::queue_throughput` + `QueueStats::add`.
- [x] 6. `periodic::{periodic_job, next_run}`; `check_periodic` uses them.

## Phase 2 — the door

- [x] 7. Proto, descriptor, OpenAPI (both packages, named request bodies), Go
      stubs.
- [x] 8. `inspect` / `admin` scopes; gate by `INSPECT_METHODS` (drift-tested
      against the descriptor, unknown method → `admin`) and facade verb.
- [x] 9. `grpc/admin/*`, registered on the listener; not-found reasons.
- [x] 10. Facade: 22 admin bindings, generic custom-verb dispatch, drift tests,
      metrics labels `package.Service/Method`.
- [x] 11. e2e `tests/grpc_admin.rs` (8) + facade admin section.
- [x] 12. `fq` admin subcommands; `grpc_cli.rs` parity + e2e. Durations are
      `*-ms` flags, the CLI's existing rule.
- [x] 13. Docs: admin.mdx, tokens, grpc, limits, contract, index, cli pages,
      REMOTE_SDK_CONTRACT, README, CHANGELOG (+ synced page). Every curl, grpcurl
      and fq example run against the real binary.
- [x] 14. Verify: fmt; clippy `--workspace --all-targets --all-features -D
      warnings`; check default/postgres/redis/native-async; flexiq-core (lib
      524, rust 105, namespace_scoping 27, …), flexiq, flexiq-tui, flexiq-cli
      100, flexiq-openapi 19, flexiq-server `grpc,http-target` 715; Python
      affected suites 758 passed; Node 790; Java 654; Go vet + test; docs
      typecheck, lint, build (`NODE_OPTIONS=--max-old-space-size=8192`, as CI).
      PG/Redis contract suites compile-only locally — first run is CI.
- [ ] 15. Memory + follow-up issues (ask first).

## Deviations from the reviewed proto

- `Queue.paused_at` dropped: the Redis backend stores a set of names, no time.
- `QueueThroughput.dead` added beside `completed`/`failed`/`cancelled`.
- `Worker.threads` named `concurrency`; `DeadLetter.dlq_retry_count` named
  `replay_count`.
- `DeleteDeadLetter` / `DeletePeriodicTask` answer `{}` and `NOT_FOUND` when
  absent (AIP), instead of `deleted: bool`.
- `PurgeDeadLetters` filter is a oneof (`failed_before` | `task_name`): storage
  has one method per filter.
- `TaskOverride` / `QueueOverride` carry an output-only `update_time`; the set
  requests name the body field (`task_override`, `queue_override`), since
  `override` is a Rust keyword.
- Rate limit units are `s|m|h` — what `RateLimitConfig::parse` accepts.
- The gate classifies admin gRPC methods from a const list drift-tested against
  the descriptor, not a runtime descriptor decode (no `expect` on the hot path).

## Review
