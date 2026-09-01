# #711 — design the `flexiq.v1` proto and what it must not promise

## Problem
Every other sub-issue of #710 is blocked on rules that do not exist yet. Field
numbers are permanent from the first release, so a service written before the
rules are written *is* the rules, inferred from whatever happened to ship.

Seven things #711 names must be decided: package and layout, the versioning
rule, how the proto relates to `BINDING_CONTRACT.md`, the error model, where the
namespace comes from, what is deliberately absent, and which RPCs are
`NO_SIDE_EFFECTS`.

## Deliverable
`tasks/specs/2026-09-01-flexiq-v1-proto-design.md` — one document, the shape of
`tasks/specs/2026-08-22-durable-steps-design.md` (#664, landed as `cce67e89`):
evidence read out of the code, a numbered decision table, one section per
decision area, and a review checklist the sub-issues are read against.

## Plan
- [x] Read #710 and every sub-issue (#712–#721) — what each one is entitled to assume.
- [x] Establish the evidence from the code, with `file:line` for every claim:
      the three meanings of `namespace: None`, the absent `QueueError` → status
      mapping, `classify_step_failure` as the only existing retry classifier,
      `QueueFull`'s Display as a load-bearing FFI wire, capability negotiation
      versus version bumps, the contract floor, the server's three roles.
- [x] Confirm what `buf lint` STANDARD forces on the names #714/#720 sketch.
- [x] Write the document.
- [x] Fact-check every citation and stress-test the decisions against the code.
- [x] Commit as one `docs:` commit, on a branch, unpushed.

## Decisions the document has to land
1. Two packages — `flexiq.v1` and `flexiq.executor.v1` — so "the JSON facade
   serves `flexiq.v1`" is a package rule, not a per-RPC allowlist.
2. `v1` is permanent; deprecate in place, reserve number *and* name, and name
   what would force a `v2` (nothing planned does).
3. The proto never restates `BINDING_CONTRACT.md`; a `CONTRACT_VERSION` bump
   never moves a wire field, and vice versa.
4. A `google.rpc.Code` + closed `ErrorInfo.reason` table for every `QueueError`
   variant, pinned against `classify_step_failure` so the two cannot drift.
5. The namespace comes from the credential, never from a request body — and the
   NULL namespace is not addressable over the wire, because `None` means three
   different things inside `Storage`.
6. The absent surface, stated as a rule rather than an omission.
7. `NO_SIDE_EFFECTS` on the four reads only; `GET` iff `NO_SIDE_EFFECTS`.

## Review

24 decisions, §1–§12, and a §11 checklist with a "fails if" line per sub-issue.
The evidence section is what most of it hangs off — in particular E3, which was
not in the issue: `namespace: None` means "only the NULL rows" to a dequeue,
"any namespace" to `get_job`, and "no filter" to a listing. That is why the wire
carries no namespace at all and why the NULL namespace is unaddressable.

Four things the fact-check changed, none of them cosmetic:

1. **`ClaimLost` moved off `ABORTED` to `FAILED_PRECONDITION`.** §4.3 puts
   `ABORTED` in the retry-with-backoff class, so a claim loss on that code would
   have told a generic client to resend the one frame the `(owner, attempt)`
   fence exists to refuse. A code whose retry class depends on reading `reason`
   is a code a middlebox gets wrong.
2. **D8 now scopes agreement to the arms `classify_step_failure` names.** That
   function is total — `_ => Retryable` — and pinning the fallthrough would have
   made `Timeout`, `Worker`, `Scheduler` and `Other` retryable RPCs. The two
   functions answer different questions: "may this attempt run again" versus
   "may this client resend".
3. **`Storage(DatabaseError(other kind))` had no row.** Added, as `UNAVAILABLE`,
   matching `classify_step_failure`; §4.4 now says why that one nested match
   keeps a wildcard when the outer one may not.
4. **§5.4 is new.** One process runs one scheduler on one `FLEXIQ_NAMESPACE`, so
   a #717 token minted for another namespace would accept enqueues nothing ever
   dequeues — a success response and no work. Tokens are validated against the
   server's own namespace at mint time.

Rejected: the review read `buf lint`'s `STANDARD` category as a typo for
`DEFAULT`. `DEFAULT` was renamed in buf v1.40.0 and #712 says so explicitly;
§1.2 now carries the note so the next reader does not "fix" it either.

### Second round (PR #768 review), 8 findings, all valid

1. `ErrorInfo.metadata` is `map<string, string>`, so §4.1 now states the encoding
   — base-10 ASCII `int64`, with a table of every key, its reason and its unit.
2. `QueueError::Storage` wraps `diesel::result::Error::NotFound` too, which the
   table mapped to `UNAVAILABLE`. Absence is normalised by `.optional()` before
   it becomes an error, so a raw `NotFound` at the boundary is a missing
   `.optional()` — `INTERNAL`, never `UNAVAILABLE` and never `NOT_FOUND`.
3. §4.3 asked one question where it needed two. Retryability is a property of the
   code *and* the method: `UNAVAILABLE` does not promise the write did not land,
   so `Enqueue`/`EnqueueBatch`/`SubmitWorkflow` are not automatically retryable.
   `CancelJob` is, being `IDEMPOTENT`, which the old text left out.
4. D9's sanitisation was written per variant, but `RedisBackend::conn`
   stringifies a `RedisError` into `QueueError::Other`
   (`redis_backend/mod.rs:86`) — the one backend whose errors name a host. §4.5
   now sanitises by provenance and `Other` carries a fixed message.
5. `Job.payload` needed explicit presence like `result`: §7.1 permits `raw = ""`,
   so plain `bytes` cannot separate "not requested" from "empty".
6. `EnqueueBatch`'s per-item result was a bare `Job`, dropping `deduplicated` and
   reporting rolled-back items as enqueued on a transactional backend. It is an
   `EnqueueResponse`, and a transactional backend fails the whole RPC with the
   failing index rather than lying per item.
7. The lease token covered five frames; `cancelled` and `slept` are attempt
   outcomes too. The rule is now "does this frame settle or advance an attempt",
   so a later frame inherits it.
8. `max_decoding_message_size` measures the serialized message, so setting it to
   `MAX_PAYLOAD_BYTES` rejects a maximum payload. 64 MiB payload, 68 MiB message,
   and the acceptance test sends the maximum rather than something under it.
