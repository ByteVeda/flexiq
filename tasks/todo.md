# Durable steps in the Go executor client (#929)

Design: `tasks/specs/2026-09-13-go-executor-steps-design.md`
Plan: `tasks/plans/2026-09-13-go-executor-steps.md`

PR #928 shipped the Go executor with `lease` and `side_channel` and left
`steps` out on purpose: it is the one capability that fails rather than
degrades. Go links no Rust, so the rules the other shells hand to
`flexiq_core::step` across their FFI have to be reimplemented here and stay
byte-compatible with the original.

- [x] 1. `internal/step`: identity and caps
- [x] 2. `internal/step`: the snapshot codec, strict in every direction
- [x] 3. `internal/step`: the sequence walk, its keys and the run key
- [x] 4. The executor wiring: the surface, the ack registry, the settlement
- [x] 5. The bufconn round trip, five assertions mutation-checked
- [x] 6. E2E against a real server
- [x] 7. Docs: the Go README and the `/server` executor page

## Review

`executor.Step` / `executor.StepKeyed` are package functions because a Go method
cannot have a type parameter, and `job.Sleep` / `job.SleepUntil` are methods
because they return no value. The body is handed its downstream idempotency key
rather than being offered an accessor for it — memoization closes the replay
window and only that key closes the crash window, so the signature is where it
belongs.

Three things this cost more thought than expected:

- **A Go body can swallow an error**, and the first cut latched every refusal
  for it. The governing design is narrower: §7.7 of
  `tasks/specs/2026-08-22-durable-steps-design.md` says the latch exists for a
  **sleep and a divergence**, and the Node plan spells out that it only *bites*
  on a swallowed divergence. So an ordinary refusal the body caught and returned
  past is taken at its word — only that code knows whether the work is done —
  while a divergence fails the attempt whatever the body does. Superseded still
  sends **no frame at all**, and a swallowed sleep still writes `slept`, because
  the claim is gone either way.
- **`seq` is the number of rows already stored, not the walk's position.** A
  keyed hit claims a row out of order and leaves the cursor behind. Mutating it
  to the cursor reddens exactly one test, which is the point of having it.
- **A damaged snapshot is retryable; a hole in the recorded `seq` is not.** The
  first is a fact about this dispatch, the second about rows that will not heal.
  Neither may ever read as "no steps recorded" — that answer re-runs a charge.

The wait for an ack is bounded by the caller's context, the *job's* context and
`WithStepAckTimeout`, whichever comes first, and the waiter is registered before
the commit is sent. A stream ending closes every waiter at once.
