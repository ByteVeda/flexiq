# A BullMQ migration guide (#863)

Plan: `tasks/plans/2026-09-14-bullmq-migration-guide.md`

`node/operate/migration.mdx` already exists, titled "Migrating from BullMQ", but
it is a 65-line concept table: no job options, no `QueueEvents`, no flows beyond
one row, no sandbox, and no list of what is lost. The issue asks for the
concrete version. Rewrite that page in place — the nav, the skeleton, the
`operate` landing card and `about/comparison`'s `SdkLink` all already point at
it, so nothing else moves.

- [x] 1. Verify every FlexiQ claim against a native build (probes, not reading)
- [x] 2. Verify every BullMQ claim against the BullMQ 5 and 6 packages and docs
- [x] 3. Write the guide: Queue/Worker, job options, the processor, results,
      events, periodic, flows, limits, sandbox, steps, languages, what is lost
- [x] 4. Run every snippet on the page — FlexiQ ones against the build, BullMQ
      ones through `tsc` against bullmq 5.81.5 and 6.3.6
- [x] 5. Fix the neighbouring pages the guide contradicts, one commit each
- [x] 6. `check:parity`, `check:search`, `lint`, `typecheck`, `build`,
      `version.mjs --check`

## Review

Two premises in the issue were out of date by the time it was picked up, and
the page says so rather than repeating them:

- **"BullMQ is Redis by definition."** BullMQ 6 (2026-07-30) ships a PostgreSQL
  backend. "No Redis" no longer separates the two; "no server at all" — the
  SQLite default — does.
- **"A BullMQ job cannot be produced from Python."** BullMQ has official ports
  (Python, Elixir, Rust, .NET, a producer-only PHP client) that share its Lua
  scripts, so it can. The honest contrast is one implementation versus one
  client per language.

Behaviour the probes found that no page stated correctly:

- A Node timeout neither stops the handler nor aborts `currentJob().signal`,
  and it frees the slot, so a `concurrency: 1` worker ran three timed-out
  handlers at once. Three pages promised the signal aborts.
- `runWorker({ concurrency })` exists and defaults to unbounded; the workers
  page said `runWorker()` takes no concurrency.
- `purgeCompleted` / `purgeDead` take a Unix-ms cutoff, not an age, despite the
  `olderThanMs` name.
- Rate-limit buckets are keyed by queue or task name without the namespace.
- `maxRetries` / `timeoutMs` are stamped at enqueue from the *enqueuing*
  process's registry, so a producer that never registered the task stamps
  3 / 5 min; periodic jobs are hard-coded to 3 / 5 min whatever the task says.
- The Node addon installs no `log` logger, so the core's warnings (the default
  retention announcement among them) never print in Node.

The last four are reported, not fixed here; the guide states the behaviour as
it is. A second-reader review caught the enqueue-time stamping, `job.failed`
not firing on a timeout, `job.enqueued` firing on a dedup, and the
namespace-wide `uniqueKey`; each was confirmed by a probe or the source before
the page changed.

The guide pushed the search index to 322 KB against a 320 KB budget that
content alone had already reached; the budget moved to 330 in its own commit.
