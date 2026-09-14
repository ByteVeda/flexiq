# BullMQ migration guide (#863)

## Where it goes

`docs/content/docs/node/operate/migration.mdx`, rewritten in place. It is
already the page every entry point links to — the `operate` meta, the section
skeleton, the `operate` landing card and `about/comparison.mdx`'s `SdkLink` —
so the rewrite needs no nav, redirect or skeleton change.

## What the page must answer

A BullMQ user has working code. Each section is "this line becomes that line",
then the difference in behaviour, then a link to the full guide page.

1. The three defaults that bite first — retries, timeout, retention
2. `Queue` / `Worker` / processor → `Queue` / `task()` / `runWorker()`
3. Job options: `attempts`, `backoff`, `delay`, `priority`, `jobId`,
   `removeOnComplete`/`removeOnFail`, `lifo`, `deduplication`, `timeout`
4. Inside the processor: `job.*` → `currentJob()`, the special errors
5. Waiting on a result: `waitUntilFinished` → `queue.result`
6. `QueueEvents` → the event taxonomy, and where each event fires
7. Repeatable jobs / job schedulers → `registerPeriodic`
8. Flows → workflows (direction flips; results are not passed)
9. `concurrency`, `limiter`, global concurrency → worker, queue and task limits;
   namespaces isolate, they do not limit
10. Sandboxed processors → the attached executor, and why it is not the same
11. Durable steps (no BullMQ primitive; BullMQ's "step job" pattern)
12. One engine, several languages (and BullMQ's own ports, stated honestly)
13. No Redis required; what the Redis backend does and does not do
14. What is lost
15. Moving incrementally + checklist

## Verified against a native build (probes in /tmp/bullmq-verify)

- A 5-field cron is rejected: `invalid cron expression '0 9 * * *'`.
- Job defaults: `maxRetries` 3, `timeoutMs` 300000, `priority` 0.
- A timeout dead-letters the attempt but the handler keeps running and
  `currentJob().signal` is **not** aborted — `timeouts.mdx` says it is.
- Default retries = 4 attempts; `retryOn → false` dead-letters after 1.
- Events per failed attempt: `job.failed` then `job.retrying`/`job.dead`.
- `uniqueKey` returns the pending job's id; a new job once that one finishes.
- Priority: higher first, default 0, negatives run after the default.
- `runWorker({ concurrency: 3 })` caps that worker at 3.
- `worker.stop()` returns at once; a running handler finishes in the background
  and its result lands only if the process stays up.
- Workflow steps get only their own `args`; `fanIn` gets the child results; an
  `on_success` step after a failure is skipped, `always` runs.
- `step.sleep` leaves the job `pending` with `retryCount` unchanged.

## Decisions

- **Honest over tidy.** The sandbox has no FlexiQ equivalent, namespaces are not
  a limit, and BullMQ *does* have a Python port. The page says so.
- Contradicted neighbours are fixed in their own commits, not silently left.
