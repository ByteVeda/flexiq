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

- `cargo test --workspace -j2 --no-fail-fast` — exit 0, no failures.
- `cargo check --workspace` for default, `postgres`, `redis`, `native-async`.
- `cargo fmt --all --check` clean.
- Python: `pytest tests/core/test_rate_limit.py` green after `maturin develop`,
  `ruff check flexiq/ tests/`, `mypy flexiq/ --no-incremental`.
- Node: `vitest run test/worker/rateScope.test.ts test/worker/taskConfig.test.ts
  test/validation.test.ts` green after `pnpm build:native`, `biome ci` and `tsc
  --noEmit` clean.
- Java: `./gradlew test --tests '*TaskPolicyConfigTest*'` green.
