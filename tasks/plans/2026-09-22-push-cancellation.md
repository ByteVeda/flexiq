# Cancellation in push mode (#846) — plan

Design: `tasks/specs/2026-09-22-push-cancellation-design.md`.
Branch `feat/push-cancellation`, not pushed. Commits authored as stromanni.

- [x] 1. `Storage::cancel_requested_among(ids, namespace)` — trait, diesel
      macro (SQLite + Postgres), Redis, delegate, forwarding. Tests beside the
      existing cancel tests.
- [x] 2. `Scheduler::in_flight_dispatches()` → `Vec<(String, Option<i64>)>`.
- [x] 3. `worker/cancel_relay.rs` — `CancelRelay::tick`, unit-tested with a
      recording dispatcher over in-memory SQLite.
- [x] 4. `Worker::spawn` runs the relay thread when a dispatcher is supplied;
      joined on shutdown.
- [x] 5. `HttpDispatchTarget`: ended-dispatch record + `SettleRefused::Cancelled`;
      every reporting method consults it on a miss. Unit + e2e tests.
- [x] 6. `flexiq-server`: `JOB_CANCELLED` reason; `settle_refusal` attaches
      `ErrorInfo` for `Cancelled` and `Fenced`. Go `ReasonJobCancelled`.
- [x] 7. e2e: `storage.request_cancel` alone (no direct `notify_cancel`) settles
      an accepted push dispatch `Cancelled`, and `ExtendLease` then answers
      `Cancelled`.
- [x] 8. Docs: push contract cancel section, REMOTE_SDK reason row, module doc
      table, `/server/custom-executors` + `/server/operate/push`, CHANGELOG.
- [ ] 9. Verify: `cargo test -j1` targeted, clippy (`CARGO_BUILD_JOBS=1`),
      feature checks (postgres, redis, http-target, grpc), docs build checks.
