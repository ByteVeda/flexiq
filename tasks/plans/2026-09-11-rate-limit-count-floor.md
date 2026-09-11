# A rate limit below one silently never dispatches (#917)

## The bug

`RateLimitConfig::parse` accepts counts that can never release a job and hands
them to the backends unchanged:

```rust
RateLimitConfig::parse("0/s")    // Some(RateLimitConfig { max_tokens: 0.0, .. })
RateLimitConfig::parse("-5/s")   // Some(..)
RateLimitConfig::parse("NaN/s")  // Some(..)
```

Every backend consumes a token behind `row.tokens >= 1.0`
(`storage/sqlite/rate_limits.rs`, and the Postgres and Redis twins), so a zero or
negative count denies every acquisition and a `NaN` loses the comparison
outright. The task never dispatches, and nothing anywhere says why — it looks
like a task that is merely idle.

Found by review on #916.

## The decision: reject, not clamp

The issue offers two defensible answers. This takes **reject**: `parse` returns
`None` for a non-finite count or one below `1.0`.

- Every SDK boundary already turns `None` into an error at worker start, so a
  dead rate becomes actionable instead of silent. That is the whole complaint.
- #916 already shipped exactly this rule one layer up, in `flexiq-macros`'s
  `rate()`, with a `trybuild` fixture behind it. Clamping in `parse` would leave
  the macro rejecting what the API accepts — two rules for one grammar.
- Clamping gives a caller who wrote `"0/s"` meaning *stop everything* one job per
  second instead. A silent dispatch is worse than a loud refusal to start.

The cost is a behaviour change on a published crate: `parse("0/s")` went from
`Some` to `None`. Documented rather than hidden.

The rule lives in `parse`, not in each caller.

## Steps

- [x] 1. The floor in `RateLimitConfig::parse`, worded to match `flexiq-macros`
- [x] 2. Python's queue-level rate limit raises instead of dropping a bad value
- [x] 3. The three shells say a rate needs a count of at least one
- [x] 4. The floor documented where the grammar is
- [x] 5. Whole-repository verification

### 1. The floor

`crates/flexiq-core/src/resilience/rate_limiter.rs` — after the count parses,
reject `!count.is_finite() || count < 1.0`. The comment names the backend
comparison that makes it dead, and the macro's
`expect("a rate the macro already validated")` that panics if the two rules ever
drift apart. The macro's unit set is a strict subset of core's, so macro ⊆ core
still holds.

### 2. The one fail-open the change would widen

`crates/flexiq-python/src/py_queue/worker.rs` registered queue-level rate limits
through `.and_then(RateLimitConfig::parse)` — a `None` was silently dropped and
the queue ran with **no** rate limit. Before this change `"0/s"` on a queue meant
"never dispatch"; after it, it would have meant "unthrottled". A silent flip in
the dangerous direction, caused by this change, so it is fixed with it.

The existing task-level `parse_rate` helper grows a `scope` argument rather than
gaining a second copy, mirroring Node's `parse_rate_spec`, which has named
`"task"` vs `"queue"` since it was written. The task-level message is unchanged.

Node already errors on both scopes; Java exposes no queue-level rate.

### 3. The message

`(expected e.g. '100/m')` is misleading when the value rejected is `"0/s"` —
that *is* like `100/m`. One wording covers the malformed and the sub-one case in
all three shells: `expected a count of at least 1 over a unit, as in '100/m'`.

Error strings are an interface: the two exact-match assertions in
`sdks/node/test/worker/rateScope.test.ts` move with it. Python, Java and the
other Node assertions match loosely.

### 4. Docs

The floor was stated nowhere. One line in each SDK's rate-limiting guide, and in
the `#[task]` option table in `flexiq-macros`'s crate docs. `contracts/` does not
carry this grammar.

## Left alone, on purpose

The dashboard/REST override stores validate a rate with a shape check only —
non-empty and `contains('/')` — in four independent places (`flexiq-server`'s
`dashboard/stores/overrides.rs`, and the Python, Node and Java twins). An
operator can still push `"0/s"` through them; it is caught when a worker next
parses the config. Putting the numeric rule there would be a fifth copy of the
grammar, against this issue's own instruction that the rule belongs in `parse`.
Worth its own issue.

Pre-existing and untouched: the macro accepts `s|m|h|second|minute|hour` while
core also accepts `sec|min|hr`. Harmless while macro ⊆ core.
