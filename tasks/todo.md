# A settle callback for push work that outlives the request deadline (#845)

Design: `tasks/specs/2026-09-19-push-settle-callback-design.md`
Plan: `tasks/plans/2026-09-19-push-settle-callback.md`

Push dispatch settles a job on the connection that started it, so a job is
capped at the platform's request deadline — 60 minutes on Cloud Run, 15 on
Lambda. A `202` becomes a hand-off: the request ends, the target keeps working,
and it reports later through a `Settle` RPC on the executor door.

**The thing that must be right** is the fence. A `Settle` arriving after its
lease expired and the job was retried elsewhere must be refused, not applied.
Three racers can settle an accepted dispatch — a local `Settle`, a `Settle` on
another replica, and the deadline passing — and each must first atomically
consume one durable marker. Exactly one wins; the rest emit nothing.

Two decisions carry the rest:

- **The 202 wait lives inside `run_one`.** The attempt task keeps its semaphore
  permit and swaps what it awaits. So "exactly one `JobResult` per job, never
  zero, never two" survives verbatim, and `FLEXIQ_PUSH_TARGET_CAPACITY` keeps
  one meaning in both modes.
- **One nullable column** — `execution_claims.settle_deadline_ms` — is both the
  extension mechanism and the "accepted, never settled" marker, because those
  are one fact.

## Tasks

- [ ] 1. Proto: four RPCs carrying the existing frames + the contract's `202` row
- [ ] 2. Storage: migration `m0019`, the records, `await_settle`/`claim_settle`/
      `supports_settle`, `lease_authorizes` beside `epochs_agree`
- [ ] 3. Storage: SQLite, Postgres and Redis impls + the `delegate!` block
- [ ] 4. Reaper honours the marker; the purge stops deleting a live one
- [ ] 5. Dispatcher accepts a 202 and waits
- [ ] 6. The settle-only executor door
- [ ] 7. `FLEXIQ_PUSH_TARGET_SETTLE` + the chart
- [ ] 8. Progress and task logs from a push target
- [ ] 9. Tests, including both mutation checks
- [ ] 10. Docs

## Found on the way in

`purge_execution_claims` drops every claim older than a hard-coded hour
(`scheduler/maintenance.rs:104`). With the row gone the epoch is absent,
`epochs_agree` agrees with everything, and a job running past an hour has no
epoch fence at all. Pre-existing; #845 makes it the normal case. Closed here
for marked rows only — the general case (a long *attached* job) needs a join
the Diesel backends do not have, and is filed rather than half-done.

## Review
