# #666 — step memoization, the sequence check and the blob cap

Reviewed against `tasks/specs/2026-08-22-durable-steps-design.md` §12: D2, D4, D5,
D6, D7, §2, §3, §5.

#665 landed the persistence half. Nothing yet *decided* what to run: the storage layer
commits whatever `seq` and `step_key` a caller invents. This is the decision half, in the
core so the three shells (#669–#671) drive one implementation instead of three.

## Deviations from the letter of the issue, all with spec backing

- **"the task's serializer and codec chain" → the queue's, and it stays in the shell.**
  §5.2 already overrides the issue on task-vs-queue. The chain itself is shell code, so a
  codec trait in the core would mean a Rust→shell callback per step and would have no
  implementor. The core is deliberately **byte-transparent** instead: it commits exactly
  the bytes it is handed and returns exactly the bytes it read, which is what lets
  `Queue(codec=…)` encrypt step blobs with no extra plumbing. The test asserts on the raw
  stored row (§5.3); the end-to-end encryption test belongs to #669, where a codec exists.
- **No `digest()`.** §1.2 gives it to error messages and the dashboard; the dashboard
  timeline is out of scope (§13) and the divergence error carries the sequences
  themselves, so it would cost a published crate a `sha2` dependency for a display aid
  with no caller. The "fingerprint" the issue asks for is the ordered key list — §3.1 and
  §12 ("no fingerprint column") — and that is what `StepSequence` is.
- **`idempotency_key`** stays out: §6 and §12 give it to #668, along with the
  `__origin_job_id` half of it that does not exist yet.

## Commits

1. **`refactor(core): split the step rules into a module`**
   - `src/step.rs` → `src/step/{mod,limits,failure}.rs`, one concern per file. No
     behaviour change.

2. **`feat(core): derive and validate step keys`**
   - `step/key.rs` — `StepKey::derive` (`name#occurrence`) and `StepKey::explicit`
     (`name:key`), with the §1.2 validation: a name is non-empty, ≤128 bytes and holds
     neither separator; a key is non-empty and ≤256 bytes and may hold anything, because
     a key is only ever compared, never parsed back. Refused as `QueueError::Config`,
     before any I/O.
   - Errors quote back a bounded prefix of the offending value — a name built from a
     payload is exactly the case that fails, and pasting all of it helps nobody.

3. **`feat(core): check the step sequence for divergence`**
   - `step/sequence.rs` — `StepSequence`, `StepDecision{Memoized,Run}`, `PendingStep`.
     Position-by-position against the snapshot (§3.2): absent → run, same key and kind →
     memo, anything else → diverge. `kind` is part of the match, so a `run` replaying onto
     a recorded `sleep` is caught.
   - **Keyed calls do not spend an occurrence** (§2.2): two independent counters, so
     adding a keyed call cannot shift the key of a later unkeyed one.
   - **A keyed step is matched by its key, wherever it sits; an unkeyed one by
     position.** §3.2 describes the positional check and §2.3 says an explicit key makes
     identity and position "independent" — the second rule is the one that governs a keyed
     step, because §2.2's own motivating example is a loop whose order is not guaranteed,
     and a positional check would dead-letter it on the first reorder. The walk skips
     recorded steps a keyed hit already claimed, so an unkeyed step after a reordered
     keyed one still lines up; a keyed step the recorded run never saw takes the next free
     `seq` (the stored count), not the walk's position; and a recorded step this attempt
     never asked for is the §3.4 warning, whether it sits at the end or in the middle.
   - A key issued twice in one attempt, a step started while another is uncommitted, and a
     hole in the snapshot are all refused: each of the three would otherwise return a
     value that answers a different question.
   - `error.rs` — `StepSequenceDiverged(Box<StepDivergence>)`. Boxed because six fields of
     payload made it the crate's widest variant and every `Result` in the crate carries
     that width (clippy's `result_large_err` is denied here). Its `Display` is the §3.3
     text: both sequences, the position, both keys, and what to do about it. Long
     sequences are windowed around the divergence — an error nobody can read is not louder
     for being longer.
   - `classify_step_failure` maps it to `Permanent`, and `StepFailure::should_retry()`
     gives the shells that mapping once instead of three times.

4. **`feat(core): drive step memoization against storage`**
   - `step/session.rs` — `StepSession<S: Storage>`. `load` reads `get_job_steps`
     **once** (§5.1) and refuses a backend without a step store: it must never degrade to
     "no memo". `run` is the Rust-native form (memo hit → the closure never runs);
     `begin_run` + `commit_run` is the split form for a shell whose closure cannot cross
     into Rust. `finish` warns about a shortened sequence (§3.4) rather than failing it.
   - `owner` and `attempt` (`job.retry_count`) ride every commit, so the #665 fence
     applies unchanged.
   - Caps are pre-checked against the snapshot totals so the error names the step without
     a round trip (§4.3); storage still holds the same line inside its transaction.

5. **`test(core): cover the step session on every backend`**
   - Two cases in the contract suite, so the Postgres and Redis legs run them: a memo hit
     that returns the stored bytes verbatim across a reload (Redis stores step results
     base64 — a shell-visible byte difference would surface here), and a changed sequence
     that fails before the closure runs and commits nothing.

## Acceptance (from the issue)

| Criterion | Test |
|---|---|
| memo hit skips the closure | `a_memoized_step_never_runs_its_closure`, `test_step_session_memoizes_across_attempts` |
| a changed sequence fails loudly | `a_reordered_sequence_names_both_sequences`, `a_diverged_sequence_fails_before_the_closure_runs`, `test_step_session_refuses_a_changed_sequence` |
| codecs are applied | `an_encoded_result_is_stored_verbatim` — asserts the raw row, not a round trip |
| an over-cap result is rejected | `an_over_cap_result_names_the_step`, plus the count and total-byte caps |

## Left for the later issues

`begin_sleep` and the `Sleep` memo rule (#667) · `idempotency_key` and `__origin_job_id`
(#668) · the shells' own validation, error text and `ctx.step` surface (#669–#671) ·
`digest` if a dashboard timeline ever wants one.
