# #719 — a lease token on every dispatched job

Branch `feat/dispatch-lease-token`, off `master` at `85fcf88a`. Plan:
`tasks/plans/2026-09-03-dispatch-lease-token.md`. **Not pushed.**

## Done

- [x] `crates/flexiq-core/src/lease.rs` — `Lease`, `LeaseBook`,
      `mint_claim_epoch`, `epochs_agree`, and the doc block that explains why the
      claim and not `(owner, attempt)`.
- [x] `migrations/m0016_claim_epoch.rs` + `schema.rs` + `NewExecutionClaimRow` —
      nullable `execution_claims.epoch`.
- [x] `claim_execution` / `claim_execution_batch` / `reclaim_execution` return
      the epoch (`Option<i64>`); sqlite, postgres, redis, `diesel_common`,
      `traits.rs`, both blocks in `storage/mod.rs`.
- [x] `authorize_attempt` / `record_step_result` / `sleep_job` take the epoch;
      `resolve_step_fence` and the Redis `fence()` compare it, and the
      re-assert paths carry it.
- [x] Redis claim value is `"{owner}:{claimed_at}.{epoch}"` — the epoch rides
      the timestamp field so no owner-parsing site moves.
- [x] Scheduler: `DispatchRecord.epoch`, the `LeaseBook`, the bounded retired-
      dispatch map, `authorize_finished`'s fallback, `error!` on a superseded
      result, and the epoch through `poller.rs` / `maintenance.rs`.
- [x] Wire: `CAP_LEASE`, `lease` on the `job` frame and the seven executor
      frames, `with_lease` / `leased_job`, `write_job_leased`.
- [x] `worker/executor.rs` stamps every outgoing frame about a job and
      advertises `CAP_LEASE` for every shell.
- [x] `worker/remote.rs` refuses a stale frame — result, side channel and step
      commit — and fences step writes on the dispatch's epoch.
- [x] Prefork: pool-side book + refusal, `CAP_LEASE` in the child handshake,
      and `_stamp_lease` in `sdks/python/flexiq/prefork/child.py`.
- [x] Tests: 3 in the storage contract suite, 4 in `remote_tests.rs`, 3 in
      `executor_tests.rs`, 2 in `scheduler/mod.rs`, 2 in `protocol.rs`, 5 unit
      tests in `lease.rs`, 9 in `tests/worker/test_prefork_lease.py`. Every new
      guard mutation-checked.
- [x] Docs: `BINDING_CONTRACT.md` (frame table, the lease section, the
      capabilities bullet) and `modules/executor.mdx`.

## Review notes

- **In-process pools are not covered, on purpose.** They exchange `Job` for
  `JobResult`, and a `JobResult` names only a job — there is nothing to stamp.
  They keep the `(owner, attempt, epoch)` fence, which covers a reclaim and a
  retry but not a requeue. Called out in `py_queue/worker.rs` and in the plan.
- **Breaking.** Three `Storage` return types, four signatures, eight enum
  variants. The issue authorises it; the gate is at release, not per PR.
- The step-commit refusal test was sharpened after its first mutation check
  passed for the wrong reason — see the plan's table.
