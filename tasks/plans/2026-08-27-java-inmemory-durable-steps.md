# #737 — durable steps under `InMemoryFlexiQ`

Part of #663. Follows #671, which shipped `JobContext.current().step()` on the
Java shell and deliberately left `InMemoryFlexiQ` refusing.

## The decision the issue defers

Implement, with a narrower contract stated — not "document the refusal".

Every fidelity requirement the issue lists is reachable in pure Java except
one: **derive identity through the core**. `flexiq-test` is published as a
JNI-free artifact, and the Java shell exposes no `deriveStepKey` across JNI, so
that call cannot exist here. Its *intent* — the harness must not drift — is
served instead by a behavioural parity test in CI: one shared test body run
against `InMemoryFlexiQ` and against a real temp-SQLite worker, asserting
identical outcomes. That proves behaviour rather than string equality, which is
the stronger claim.

The rules live **package-private inside `test-support`**, so production code can
never reach a second copy of them.

## Contract

Modelled:

- both identity forms and their validation (empty / oversize name, `#` and `:`
  in a name, empty / oversize key)
- an occurrence spent only once the step is *accepted*, and never by a keyed step
- memo replay across attempts
- divergence at the point the step is asked for — positional, keyed, and
  kind-mismatch — permanent, never spending the retry budget
- one step at a time; a duplicate explicit key in one attempt refused
- the three caps, measured on encoded bytes
- `step.sleep`: committed once, deadline fixed by the first commit, `Elapsed`
  and `Resume` arms, the attempt ended, no retry spent
- the `(owner, attempt)` fence — a lost claim is `StepSupersededError`
- `runKey()` stable across an operator's dead-letter retry
- the orphaned-tail warning

Not modelled, and said so in javadoc and docs:

- no transaction: one process, and the fence is a field check rather than a row
  condition
- no cross-process concurrency
- no step retention — rows die with the job
- workflows, as before

## Build order

1. `StepKeys` — port of `key.rs`. Unit tests.
2. `StepRecord` + `StepSequence` — port of `sequence.rs`. Unit tests, RED-checked.
3. `InMemoryStepSession` — the snapshot, the fence, the caps, both sleeps.
4. Backend wiring: step store, `JobRec.owner`, `openStepSession`, `sleepJob`,
   the 7-arg `onJob`, `__origin_job_id` on `retryDead`.
5. `StepHarnessParityTest` — one body, two backends.
6. Docs + memory.
