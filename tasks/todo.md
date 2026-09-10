# #880 — the node concurrent-steps case flakes on macOS

Branch `fix/node-concurrent-steps-test-flake` off `master` at `5aeccb98`. One test file:
no `src/`, no Rust, no docs change.

## The failure

`Node SDK Smoke (macos-15)` on run 34492507551 failed
`test/worker/steps.test.ts:190` — `expect(await waitFor(() => dead.length > 0)).toBe(true)`
returned `false`. No `job.dead` event inside the 20s budget, which every other `waitFor` in
the file shares, so the budget is not the problem.

## Why it can pass, and therefore fail

The case asserts "a step started while another is uncommitted has no position to take" —
`sequence.rs:339`'s `check_issuable`. It set that up with

```ts
await Promise.all([steps.run("charge", () => 1), steps.run("receipt", () => 2)]);
```

Two `run` calls made back to back only *look* concurrent. Each start crosses to the core
through `JsStepSession::begin_run`, which is a `#[napi]` async fn dispatching to
`spawn_blocking`, so the two starts are two tasks on the blocking pool rather than two
statements. With instant callbacks (`() => 1`) a loaded runner can let the first start, run
and *commit* before the second start is picked up. `receipt` then takes position 2
legitimately, the body returns `"done"`, the job completes, and `dead` stays empty until the
budget expires.

`sequence.rs` is not wrong. The test was depending on scheduling instead of on the rule.

## Plan

- [x] Read the issue, the failing job and `check_issuable`, and confirm the diagnosis against
      the napi binding rather than assuming it
- [x] Add a `deferred()` helper beside `waitFor`
- [x] Hold `charge` open inside its callback until `receipt` has been asked for and answered,
      so the second start is guaranteed to reach the sequence while the first is uncommitted
- [x] Reproduce the wrong-reason pass locally, so the diagnosis is measured rather than argued
- [x] `pnpm build:native`, then run the file and the whole node suite
- [x] `pnpm typecheck` and `pnpm lint`
- [ ] Commit, push, open the PR

## Shape of the fix

`charge`'s callback resolves `charging` — which only runs once its `beginRun` returned a
`Run` decision, so `state.pending` is set — and then parks on `held`. The body awaits
`charging` before asking for `receipt`, and `receipt`'s promise resolves `held` on settle, so
nothing deadlocks when `receipt` rejects (which is the expected outcome). `Promise.all`
attaches handlers to both up front, so `charge`'s later settlement cannot surface as an
unhandled rejection.

The latch keeps the *first* signal, so the dead letter still carries `still uncommitted`.

## Not in scope

The Python twin (`tests/core/test_steps.py:766`) forces the same overlap with an
`asyncio.sleep(0.05)` in the step body. Time-based rather than a handshake, but it has not
flaked and the issue does not name it.

## Review

The diagnosis was measured, not argued. A throwaway probe reran the **old** shape with the
starvation made explicit — a 300 ms delay before the second `run` — and the job **completed**:

```text
PROBE dead= 0 completed= 1
```

Zero dead letters, one completion. That is the macOS failure exactly: `receipt` arrives after
`charge` has committed, takes position 2 legitimately, and the case's `waitFor` then spends
its whole 20s budget waiting for an event that is never coming. The probe was deleted before
committing.

With the handshake the case is deterministic: 15 consecutive runs of it alone, all green,
each settling in about a second rather than near the budget.

- `pnpm exec vitest run test/worker/steps.test.ts` → 24 passed
- `pnpm test` → 102 files, 780 passed, 6 skipped, 0 failed
- `pnpm typecheck` → clean · `pnpm lint` → exit 0 (one pre-existing warning in
  `executorAttach.test.ts`, untouched here)

The dashboard cases need `static/dashboard/index.html`, so a worktree with no
`dashboard/node_modules` fails all ten on collection. `pnpm -C dashboard install` and
`pnpm run build:dashboard` fix that; CI builds the SPA itself and never sees it.

No `src/` and no Rust change: `check_issuable` was always right, and the test is what moved.
