# `flexiq.admin.v1` (#836) — plan

Design: `tasks/specs/2026-09-23-admin-service-design.md`.
Branch `feat/admin-service` off `master` (`8f27a50d`), upstream unset, **not
pushed**. Commits authored as stromanni. One cargo job at a time (`-j1`,
`CARGO_BUILD_JOBS=1` for clippy). Every commit's staged tree must compile
(pre-commit clippy `--all-targets --all-features`).

**Gate:** the proto (spec § "The proto") is reviewed by the user before step 7.

## Phase 1 — namespace the core

- [ ] 1. Queue pause (D2): `m0020_queue_state_namespace`, `schema.rs`,
      `models.rs`, `diesel_common/queue_state.rs` (UPDATE-else-INSERT), delete
      the SQLite/Postgres pair, Redis per-namespace set with legacy default key,
      trait + both forwarding sites, `poller.rs::active_queues`, dashboard
      routes, TUI (`None`), Python/Node/Java bindings. Contract test
      `test_pause_resume_queue_is_namespace_scoped` + `namespace_scoping.rs`
      case.
- [ ] 2. Workers (D3): `m0021_worker_namespace`, `WorkerRegistration::namespace`,
      `WorkerInfo.namespace`, `list_workers(namespace)` in diesel macro + Redis,
      4 registration sites, 8 consumers (probe behaviour change noted). Contract
      test for isolation.
- [ ] 3. DLQ (D4): `purge_dead`/`purge_dead_by_task` gain `namespace`
      (`None` = all), new `get_dead`. Callers: dashboard, TUI, 3 shells.
      Contract tests.
- [ ] 4. Overrides (D5): `flexiq_core::overrides` key/prefix builders + vector
      test; server `dashboard/stores/overrides.rs` uses them with
      `state.namespace`; Python/Node/Java dashboard stores + worker-start apply
      use the namespaced layout; `BINDING_CONTRACT.md` section; shared vector
      test in each SDK suite.
- [ ] 5. Throughput (D6): `Storage::queue_throughput` — diesel macro, Redis,
      forwarding, contract test.
- [ ] 6. Periodic job builder (D7): hoist `scheduler::periodic_job`; `check_periodic`
      uses it. Existing periodic tests stay green.

## Phase 2 — the door

- [ ] 7. Proto: `admin_service.proto`; `scripts/proto-check.sh --fix` (buf
      1.72.0) → descriptor + openapi; `flexiq-openapi` walks both packages;
      producer service comment reworded (D11). Go: `buf.gen.yaml` path +
      regenerated stubs.
- [ ] 8. Reasons + scopes: `DEAD_LETTER_NOT_FOUND`, `PERIODIC_TASK_NOT_FOUND`;
      `Scope::{Inspect, Admin}`; gate: descriptor-derived admin map, facade
      `/v1/admin/` before `/v1/`; `ScopeArg`; dashboard `SCOPE_HELP`. Gate +
      scope tests.
- [ ] 9. `grpc/admin/{mod,convert,queues,dead_letters,workers,periodic,overrides}.rs`
      — `AdminService` impl delegating to free fns, `on_storage` blocking
      helpers; registered on the listener. `pb.rs` `pub mod admin`.
- [ ] 10. Facade: admin bindings, generalised `{param}:verb` dispatch, drift
      tests over both packages, JSON writers + response field-set drift test.
- [ ] 11. e2e `tests/grpc_admin.rs`: every RPC; scope matrix (produce ✗,
      inspect reads only, admin writes only); cross-namespace = NOT_FOUND /
      invisible for every resource; pause stops dispatch in one namespace only;
      trigger enqueues the periodic job shape; facade round-trip per route.
- [ ] 12. `fq`: `pb.rs` admin module, subcommands, output renders, unit tests;
      `grpc_cli.rs` parity + e2e.
- [ ] 13. Docs: new `server/operate/admin.mdx`, `tokens.mdx` scopes,
      `cli.mdx` (drop seven "cannot do" rows, document commands),
      `REMOTE_SDK_CONTRACT.md` (scopes table, admin surface MAY grading),
      `grpc/mod.rs` doc, `crates/flexiq-server/README.md`, changelog (incl. the
      D2 breaking note and D3 probe change).
- [ ] 14. Verify: fmt; clippy `--all-targets --all-features -D warnings`;
      `cargo test -j1` for `flexiq-core` (lib, `rust`, `namespace_scoping`),
      `flexiq-server --features grpc`, `flexiq-cli`; `cargo check` postgres /
      redis / native-async; Python wheel + `tests/` subset (pause, dlq,
      overrides, workers); Node `build:native` + affected tests; Java
      `./gradlew test` affected; `golangci-lint run`; docs typecheck/lint/build.
      PG/Redis contract suites: compile-only locally, first run is CI.
- [ ] 15. Memory + follow-up issues (ask first).

## Review
