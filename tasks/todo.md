# A rate limit below one silently never dispatches (#917)

Plan: `tasks/plans/2026-09-11-rate-limit-count-floor.md`

`RateLimitConfig::parse` accepted `"0/s"`, `"-5/s"` and `"NaN/s"` and handed them
to the backends, which gate on `tokens >= 1.0`. The task never dispatched and
nothing said why. Decision: **reject** in `parse`, matching the rule #916 already
applies to a `#[task]` literal.

- [x] 1. The count floor in `RateLimitConfig::parse`
- [x] 2. Python's queue-level rate limit raises instead of dropping a bad value
- [x] 3. The three shells say a rate needs a count of at least one
- [x] 4. The floor documented where the grammar is
- [x] 5. Whole-repository verification

## Review

`RateLimitConfig::parse` now returns `None` for a count that is not finite or is
below `1.0`, so a rate that could never release a job fails the worker start
instead of leaving a task that looks merely idle. The rule lives in `parse`, one
layer below every SDK, and matches what `flexiq-macros` already enforced on a
`#[task]` literal — the macro's `expect("a rate the macro already validated")`
only holds while the two agree.

One fail-open widened by that change, fixed with it: Python registered a
queue-level rate through `.and_then(RateLimitConfig::parse)`, so a `None` was
dropped and the queue ran **unthrottled**. `"0/s"` on a queue would have flipped
from "never dispatch" to "no limit" — the one direction a throttle must not fail
in. The task-level `parse_rate` helper grew a `scope` argument rather than a
second copy, mirroring Node's `parse_rate_spec`.

The three shells share one wording, since `(expected e.g. '100/m')` reads as
nonsense when the value refused is `"0/s"`. Error strings are an interface: the
two exact-match assertions in `sdks/node/test/worker/rateScope.test.ts` moved
with it.

### Behaviour change

`RateLimitConfig::parse("0/s")` went from `Some` to `None` on a published crate.
Deliberate, and stated in each SDK's rate-limiting guide and the `#[task]` option
table rather than left to be discovered.

### Left alone, on purpose

The dashboard/REST override stores validate a rate by shape only (non-empty,
`contains('/')`) in four independent places. An operator can still push `"0/s"`
through them; a worker catches it at the next parse. Putting the numeric rule
there would be a fifth copy of the grammar, against this issue's own instruction
that the rule belongs in `parse`. Worth its own issue.

### Verification

Run on a fresh worktree off `origin/master`, every shell rebuilt against the new
core — a stale native artifact survives a checkout and would have run the new
tests against the old Rust.

- `cargo test --workspace -j2` — 1260 passed, 0 failed across 56 suites.
- `cargo test -p flexiq --test macro_ui` — trybuild green, `zero_rate_limit.rs`
  included: the #916 fixture still gets the message it asserts.
- `cargo check --workspace` for default, `postgres`, `redis` and
  `native-async`; `cargo fmt --all --check` clean; clippy `--all-targets
  --all-features` clean via the pre-commit hook on each commit.
- Python, after `maturin develop`: `pytest tests/` — 1587 passed, 16 skipped.
  `ruff check flexiq/ tests/` and `mypy flexiq/ --no-incremental` clean.
- Node, after `pnpm build:native` and a dashboard build: `vitest run` — 781
  passed, 6 skipped, 0 failed. `biome ci src test` and `tsc --noEmit` clean.
- Java: `./gradlew build` — BUILD SUCCESSFUL, 705 tests, 0 failures, 0 errors.
