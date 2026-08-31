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
