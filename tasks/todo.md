# A rate limit below one silently never dispatches (#917)

Plan: `tasks/plans/2026-09-11-rate-limit-count-floor.md`

`RateLimitConfig::parse` accepted `"0/s"`, `"-5/s"` and `"NaN/s"` and handed them
to the backends, which gate on `tokens >= 1.0`. The task never dispatched and
nothing said why. Decision: **reject** in `parse`, matching the rule #916 already
applies to a `#[task]` literal.

- [ ] 1. The count floor in `RateLimitConfig::parse`
- [ ] 2. Python's queue-level rate limit raises instead of dropping a bad value
- [ ] 3. The three shells say a rate needs a count of at least one
- [ ] 4. The floor documented where the grammar is
- [ ] 5. Whole-repository verification

## Review
