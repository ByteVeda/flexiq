# docs: durable steps, and when to use a workflow instead (#672)

Part of #663. Shipping code is merged in all three shells (#669-#671); this is
the reader-facing page.

## Placement

One shared page: `docs/content/docs/shared/guides/core/steps.mdx` →
`/python|/node|/java` + `/guides/core/steps`. Core, not Workflows: a step is
something you write inside a task body, and the page's job is to explain why it
is *not* the workflows section.

Nav: `"steps"` after `"execution-model"` in each SDK's `guides/core/meta.json`.

## Outline (the issue's must-cover list, in order of a reader's questions)

- [x] Above the fold: steps vs workflow DAGs, with a decision table and the rule.
- [x] What a step is: runs once per job, not once per attempt.
- [x] Memoization is not exactly-once — the crash window drawn, then
      `idempotency_key` as the actual fix.
- [x] Identity: `name#occurrence`, and the keyed escape hatch for loops.
- [x] Divergence: the real error text, what fails and what does not, and the
      rule for changing a step's body.
- [x] `step.sleep`: ends the attempt, holds no slot, costs no retry, deadline
      fixed by the first commit.
- [x] The caps, and what to do with a large result.
- [x] Where steps refuse (attached executor, test mode, concurrent steps) and
      why their signals must not be swallowed.

## Accuracy notes (verified against source, not the design spec)

- Caps are **not** configurable — every shell passes `StepLimits::default()`.
  The design doc says "configurable on the queue"; the code does not.
- Error name is `StepLimitExceededError`, not the spec's
  `StepResultTooLargeError`.
- Exact messages copied from `crates/flexiq-core/src/error.rs`.
- Test-mode inlining is Python-only (`queue.test_mode()`).
- `job.sleeping` event + `on_sleep` middleware hook exist in all three shells.

## Verify

`pnpm --dir docs check:parity`, `typecheck`, `lint`, `build`.
