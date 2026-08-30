# #738 — bound before, after, onError and onSleep

## Problem
`timeout_ms` bounds the handler, not the middleware around it. A `before`,
`after`, `onError` or `onSleep` that blocks — an exporter flushing to an
unreachable collector — holds the attempt open past the task's own limit, on
every shell. Worst on the sleep path: a slept attempt is already Pending and
unclaimed, so nothing reaps it and the leaked worker slot accumulates until the
hook returns.

## Design
One policy, one knob per shell, three mechanisms. A hook that overruns is never
a job failure: log and continue.

Hooks cannot be moved off the dispatch thread — java's `FlexiQObservation.before`
opens a Micrometer `Observation.Scope` (ThreadLocal) *for the handler*, python's
`SentryMiddleware.before` does `push_scope()`, and python passes the per-thread
`current_job` proxy. So the deadline is enforced without moving the call:

- node: `Promise.race` — stop awaiting, detach the hook with `.catch`.
- java: shared daemon scheduler interrupts the dispatch thread; disarm under a
  lock so no interrupt lands after the hook returns; clear the flag afterwards.
- python: one process-wide daemon watchdog thread; warn only. Prefork is already
  bounded by `prefork/watchdog.rs`, which SIGKILLs on `timeout_ms`.

Budget is per hook call and comes from its own knob, not `timeout_ms` — a task
with no timeout still wants bounded hooks. `0` disables.

| SDK | knob | default |
|---|---|---|
| python | `Queue(middleware_timeout=5.0)` seconds | 5.0 |
| node | `new Queue({ middlewareTimeoutMs: 5000 })` | 5000 |
| java | `FlexiQ.builder().middlewareTimeout(Duration)` | 5s |

Scope is the four execution hooks. Python has three (its `after` carries the
error, so it has no `onError`). `onEnqueue` and the outcome hooks stay out.

## Tasks
- [x] python: `flexiq/hook_deadline.py` — watchdog + `hook_deadline()` guard
- [x] python: knob on `Queue.__init__`, wired at the three `task_lifecycle` sites
- [x] python: tests
- [x] node: `src/middleware-deadline.ts` — `withHookDeadline()`
- [x] node: `middlewareTimeoutMs` through `QueueOptions` → worker + executor
- [x] node: tests
- [x] java: `worker/HookDeadline.java` — arm/disarm interrupt
- [x] java: `middlewareTimeout` through `FlexiQ.Builder` → `DefaultFlexiQ` → bridge
- [x] java: tests
- [x] docs: one section in `shared/guides/extensibility/middleware.mdx`
- [x] checks: cargo (unchanged), ruff/mypy/pytest, biome/tsc/vitest, gradle test

## Review

Shipped as designed. What the implementation added to the plan:

- **The knob has to reach three constructors, not one.** Node's callback is
  built by both `worker.ts` and `executor.ts`, so `middlewareTimeoutMs` rides
  `WorkerStartParams` *and* `ExecutorStartParams`; java's bridge is built by
  `Worker.Builder` and `Executor.Builder`, so both grew a
  `middlewareTimeout(Duration)` and `FlexiQ.Builder`'s value is nullable —
  `null` means "leave the worker's own default", which keeps the 5s literal in
  one place (`HookDeadline.DEFAULT_TIMEOUT_MILLIS`).
- **Java runs `after` for a hook whose `before` was interrupted.** Java has no
  `completed_mw` list, so the whole chain gets its pairing hook. That is the
  right answer for the same reason python keeps one: a middleware that got
  partway through setting something up is owed the chance to take it down.
- **`_FakeQueue` in `tests/worker/test_native_async.py` needed the attribute.**
  It declares `__slots__` precisely so a new lifecycle dependency fails loudly
  rather than silently — which is what happened, 14 red tests deep.
- **Only the overrun is swallowed.** A hook that throws on its own still
  behaves as it did: node rejects out of `withHookDeadline`, java rethrows
  after disarming. Both are pinned by a test, because the obvious
  implementation (catch everything) would have quietly broken a `before` that
  means to reject a job.
