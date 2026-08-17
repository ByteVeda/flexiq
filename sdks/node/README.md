# taskito (Node.js)

[![npm version](https://img.shields.io/npm/v/@byteveda/taskito.svg)](https://www.npmjs.com/package/@byteveda/taskito)
[![License](https://img.shields.io/npm/l/@byteveda/taskito.svg)](https://github.com/ByteVeda/taskito/blob/master/LICENSE)

Rust-powered task queue for Node.js — no broker required. A thin
[napi-rs](https://napi.rs) shell over the Taskito Rust core, peer to the Python
SDK. Enqueue work and run workers in the same process or across processes that
share storage (SQLite, PostgreSQL, or Redis).

Part of the [taskito](https://github.com/ByteVeda/taskito) project (Rust core + native
SDKs for Python and Node). Full guides at the [Node docs](https://docs.byteveda.org/taskito/node).

## Install

```bash
pnpm add @byteveda/taskito
```

Requires Node.js >= 18. Ships as dual ESM + CommonJS. A prebuilt native binary is
installed automatically for your platform via an optional per-platform package
(`@byteveda/taskito-<os>-<arch>`) — linux x64/arm64 (gnu + musl), macOS x64/arm64, and
Windows x64. On a platform without a prebuilt, build from source with the Rust
toolchain + napi-rs CLI (`pnpm build:native`).

## Quickstart

```ts
import { Queue } from "@byteveda/taskito";

const queue = new Queue({ dbPath: "taskito.db" });

// Register a task with optional per-task config.
queue.task("add", (a: number, b: number) => a + b, {
  maxRetries: 3,
  retryBackoff: { baseMs: 1000, maxMs: 60_000 },
  timeoutMs: 30_000,
  maxConcurrent: 4,
  circuitBreaker: { threshold: 5, windowMs: 60_000, cooldownMs: 30_000 },
});

// Producer.
const id = queue.enqueue("add", [2, 3], { priority: 5 });

// Worker.
const worker = queue.runWorker({ queues: ["default"] });

// Await the result.
const result = await queue.result(id); // 5
worker.stop();
```

## Backends

```ts
new Queue();                                              // SQLite at .taskito/taskito.db (default)
new Queue({ dbPath: "data/taskito.db" });                 // SQLite at a custom path (dirs created)
new Queue({ backend: "postgres", dsn: process.env.PG_URL, schema: "taskito" });
new Queue({ backend: "redis", dsn: "redis://localhost", prefix: "taskito" });
```

SQLite defaults to `.taskito/taskito.db` and creates the parent directory
automatically. Postgres isolates its tables in the `schema` (default
`"taskito"`); Redis isolates its keys under `prefix`. Override either to share
or separate state.

Scan-heavy methods (`stats*`, `listJobs`, `deadLetters*`, `purge*`,
`listWorkers`, `getMetrics`, `getJobErrors`) are async and run off the event
loop. Single-row operations (`enqueue`, `getJob`, `cancelJob`, …) are sync and
block for one storage round-trip — negligible on SQLite, but on Postgres/Redis
that is a network hop, so keep them off latency-critical request paths.

## Enqueue options

`priority`, `maxRetries`, `timeoutMs`, `delayMs` (delayed run), `uniqueKey`
(idempotency — a duplicate enqueue is a no-op while the first job is
pending/running), `metadata`, `namespace`.

## Cancellation

Cancellation is cooperative. A running task reads its context via `currentJob()`:

```ts
import { currentJob } from "@byteveda/taskito";

queue.task("download", async (url: string) => {
  const { signal } = currentJob() ?? {};
  const res = await fetch(url, { signal });
  return res.text();
});

queue.requestCancel(jobId); // aborts the task's signal
```

`cancelJob(id)` cancels a still-pending job. Tasks may report progress via
`currentJob()?.setProgress(0–100)`.

## Inspection & management

```ts
await queue.stats();        // { pending, running, completed, failed, dead, cancelled }
await queue.statsByQueue("default");
await queue.statsAllQueues();
await queue.listJobs({ status: "failed", limit: 50 });
await queue.getJobErrors(id);
await queue.getMetrics(3600_000, "add");

await queue.deadLetters();  // dead-letter entries
queue.retryDead(deadId);    // re-enqueue
queue.deleteDead(deadId);
await queue.purgeDead(olderThanMs);
await queue.purgeCompleted(olderThanMs);

queue.pauseQueue("default");
queue.resumeQueue("default");
queue.listPausedQueues();
```

## Serializers

Args and results are serialized with a pluggable `Serializer` (default
`JsonSerializer`; `MsgpackSerializer` for compact binary). The Rust core treats
payloads as opaque bytes.

```ts
import { Queue, MsgpackSerializer } from "@byteveda/taskito";
new Queue({ dbPath: "taskito.db", serializer: new MsgpackSerializer() });
```

## CLI

A standalone `taskito` command (no Python) operates the queue from the terminal:

```bash
# Connect with --db <path> (or --backend/--dsn for postgres/redis).
taskito --db taskito.db enqueue add '[2,3]'
taskito --db taskito.db stats
taskito --db taskito.db jobs --status failed
taskito --db taskito.db dlq list
taskito --db taskito.db dlq retry <deadId>
taskito --db taskito.db pause default
taskito --db taskito.db cancel <jobId>

# Run a worker from a module that exports a configured Queue.
taskito run ./app.js --queues default,emails
```

`--json` on any read command prints machine-readable output.

## Events & middleware

Subscribe to job lifecycle events, or register middleware around execution:

```ts
queue.on("job.completed", (e) => console.log("done", e.jobId));
queue.on("job.dead", (e) => alertOps(e));

queue.use({
  before: (ctx) => log.info("start", ctx.taskName),
  after: (ctx, result) => log.info("ok", ctx.taskName),
  onError: (ctx, err) => log.error("threw", ctx.taskName, err),
  onRetry: (e) => metrics.inc("retry", e.taskName),
  onDeadLetter: (e) => alertOps(e),
});
```

Events: `job.completed`, `job.retrying`, `job.dead`, `job.cancelled`. `before`/
`after`/`onError` wrap execution (awaited); the outcome hooks fire after the core
decides the result.

## Distributed locks

TTL-bounded, owner-scoped locks backed by the queue's storage — coordinate
across processes without a separate lock server. A held lock auto-extends at
`ttlMs / 3` so a slow section never loses it.

```ts
// Scoped helper — acquires, runs, releases (throws if held elsewhere).
await queue.withLock("report:2026-06", async () => {
  await rebuildReport();
});

// Manual handle with explicit release.
const lock = queue.lock("resource", { ttlMs: 30_000 });
if (lock.acquire()) {
  try {
    // ... critical section
  } finally {
    lock.release();
  }
}
```

`Lock` also implements `Symbol.dispose`, so on Node 20.4+ you can use `using lock =
queue.lock("resource")` for automatic release at block exit.

`lock.extend(ms)`, `lock.info()`, and `lock.ownerId` round out the API. Expired
locks are reaped by the worker's maintenance loop.

## Periodic (cron) tasks

Schedule a registered task on a cron expression. A running worker enqueues it
when due (the scheduler's maintenance loop drives this).

```ts
queue.task("digest", (date: string) => sendDigest(date));

// cron is 6/7-field, seconds first: sec min hour day-of-month month day-of-week
queue.registerPeriodic("daily-digest", "digest", "0 0 9 * * *", {
  args: ["2026-06-16"],
  timezone: "America/New_York",
});
```

Returns the next fire time (Unix ms). Re-registering the same name replaces it.

## Webhooks

Deliver job events to HTTP endpoints — HMAC-SHA256 signed, retried with backoff,
persisted across restarts:

```ts
const hook = queue.webhooks.create({
  url: "https://hooks.example.com/jobs",
  events: ["job.dead", "job.completed"], // omit for all
  secret: process.env.WEBHOOK_SECRET,    // signs X-FlexiQ-Signature: sha256=...
  taskFilter: ["send_email"],            // optional
});

queue.webhooks.list();
queue.webhooks.delete(hook.id);
```

Deliveries fire from the worker process (where events originate). The dashboard
exposes `/api/webhooks` for managing them.

## Workflows

Orchestrate multi-step DAGs. Each step is a registered task; `after` declares
dependencies. Steps are pre-enqueued with `depends_on` chains, so the core
scheduler runs them in topological order — and a worker advances the run as each
step settles.

```ts
const handle = queue.workflows
  .define("etl")
  .step("extract", "extractTask", { args: ["s3://bucket/in"] })
  .step("transform", "transformTask", { after: "extract" })
  .step("load", "loadTask", { after: "transform", maxRetries: 5 })
  .submit();

queue.runWorker(); // advances workflow nodes by default

const run = await handle.wait();      // resolves when terminal
console.log(run.state);               // "completed" | "failed" | ...
handle.nodes();                       // per-step status
queue.workflows.list({ state: "running" });
```

If a step dead-letters, the run fails and remaining steps are skipped
(fail-fast). Per-step options: `after`, `args`, `queue`, `maxRetries`,
`timeoutMs`, `priority`. Workers that never process workflow steps can skip the
per-job bookkeeping with `runWorker({ advanceWorkflows: false })`. Requires the
addon built with the `workflows` cargo feature (enabled by `build:native`).

### Fan-out / fan-in

A `fanOut` step expands at runtime into one child job per item, each running the
same task. The items come from the array result of a predecessor (`itemsFrom`,
defaulting to the sole predecessor) — each item is passed to the child task as
its single argument. An optional `fanIn` step collects the children's results
into an array and runs a combiner task over it.

```ts
queue.task("listFiles", () => ["a.csv", "b.csv", "c.csv"]); // returns the items
queue.task("processFile", (file: string) => file.length);  // runs once per item
queue.task("summarize", (sizes: number[]) => sizes.reduce((a, b) => a + b, 0));

const handle = queue.workflows
  .define("batch")
  .step("list", "listFiles")
  .fanOut("process", { after: "list", task: "processFile", itemsFrom: "list" })
  .fanIn("collect", { after: "process", task: "summarize" })
  .submit();

queue.runWorker();
const run = await handle.wait();
// handle.nodes(): "process" carries fanOutCount; children are "process[0]", "process[1]", …
```

The worker-side tracker drives expansion from the outcome stream and
reconstructs the plan from storage, so submission and execution may run in
different processes. Fan-out is fail-fast: if any child dead-letters, the parent
and run fail and downstream steps are skipped. An empty item list completes the
fan-out immediately and runs the fan-in with `[]`. Children and the combiner each
run on the fan-out step's `queue` / `maxRetries` / `timeoutMs` / `priority`.

### Conditions

A step's `condition` gates it on its predecessors' outcomes: `on_success`
(default — all predecessors completed), `on_failure` (a predecessor failed — an
error handler), or `always`. A step whose condition isn't met is `skipped`, and
the skip propagates downstream.

```ts
queue.workflows
  .define("with-handler")
  .step("risky", "risky")
  .step("recover", "rollback", { after: "risky", condition: "on_failure" })
  .step("notify", "notifyOk", { after: "risky", condition: "on_success" })
  .submit();
```

### Approval gates

A `gate` step pauses the run (`waiting_approval`) until resolved out-of-band.
Resolve from any process (it reads the plan from storage); an optional timeout
auto-resolves per `onTimeout`.

```ts
const handle = queue.workflows
  .define("publish")
  .step("build", "build")
  .gate("review", { after: "build", timeoutMs: 86_400_000, onTimeout: "reject" })
  .step("ship", "ship", { after: "review" })
  .submit();

queue.workflows.approveGate(handle.runId, "review"); // or rejectGate(..., reason)
```

### Sub-workflows

A `subWorkflow` step runs a child workflow as a node; the parent advances when
the child finalizes (child failure fails the parent node). Build the child with
`.build()` (don't submit it directly).

```ts
const child = queue.workflows.define("child").step("a", "taskA").build();

queue.workflows
  .define("parent")
  .step("prep", "prep")
  .subWorkflow("sub", { after: "prep", workflow: child })
  .step("finish", "finish", { after: "sub" })
  .submit();

queue.workflows.children(handle.runId); // the spawned child run(s)
```

### Saga compensation

Give a step a `compensate` task and, if the run fails, the tracker rolls back
each completed compensable step in reverse-dependency order, passing the step's
result to its compensator. The run ends `compensated`, or `compensation_failed`
if a rollback itself fails.

```ts
queue.workflows
  .define("checkout")
  .step("reserve", "reserve", { compensate: "unreserve" })
  .step("charge", "charge", { after: "reserve", compensate: "refund" })
  .step("ship", "ship", { after: "charge" }) // if this fails → refund, then unreserve
  .submit();
```

## Dashboard

A web dashboard (the same React UI the Python SDK serves) runs over the queue —
no Python required. Build the SPA assets once, then serve:

```bash
pnpm build:dashboard          # builds the SPA into static/dashboard (one-time)
taskito --db taskito.db dashboard --port 8787
```

Or programmatically:

```ts
import { Queue, serveDashboard } from "@byteveda/taskito";

const queue = new Queue({ dbPath: "taskito.db" });
const server = serveDashboard(queue, { port: 8787 });
// ... server.close() to stop
```

It serves the SPA plus the `/api/*` REST contract (stats, jobs, dead-letters,
queues, metrics, workers, webhooks, workflow runs, cancel/retry/pause/resume)
over the queue. Auth runs open (localhost); the metrics and workers panels
populate from live job history and running workers.

## Mesh (work-stealing overlay)

Workers can form a decentralized mesh — SWIM gossip for peer discovery plus
consistent-hash placement and TCP work-stealing — so idle nodes pull work from
busy ones. The database stays the source of truth; the mesh only optimizes
dispatch locality. Requires the addon built with the `mesh` cargo feature
(`build:native` enables it).

```ts
queue.runWorker({
  queues: ["default"],
  mesh: {
    port: 7946,                       // UDP gossip; TCP steal binds port + 1
    seeds: ["10.0.0.2:7946"],         // peers to join (empty = standalone)
    steal: true,
    encryptionKey: process.env.MESH_KEY, // optional XOR-encrypt gossip
  },
});
```

Other tunables: `bindAddr`, `advertiseAddr` (NAT), `affinityWeight`,
`localBuffer`, `stealBatch`, `stealThreshold`, `virtualNodes`, `stealRateLimit`.

## Contrib integrations

Optional integrations live under the `taskito/contrib/*` subpaths. Each requires its
framework as a peer dependency you install yourself; none are pulled in by the main
package or exported from the `taskito` barrel.

### Observability

```ts
import { otelMiddleware } from "@byteveda/taskito/contrib/otel"; // peer: @opentelemetry/api
import { prometheusMiddleware, PrometheusStatsCollector } from "@byteveda/taskito/contrib/prometheus"; // peer: prom-client

queue.use(otelMiddleware());        // one span per execution: taskito.execute.<task>
queue.use(prometheusMiddleware());  // taskito_jobs_total, _job_duration_seconds, _active_workers, _retries_total

const collector = new PrometheusStatsCollector(queue); // polls queue depth + DLQ size
collector.start();
// expose `await register.metrics()` from your HTTP server
```

OTel options: `tracerName`, `attributePrefix`, `spanName(ctx)`, `extraAttributes(ctx)`,
`taskFilter(name)`. Prometheus options: `namespace`, `register`, `taskFilter`, `buckets`
(metrics for one namespace are built once per registry, so multiple middlewares are safe).

```ts
import { sentryMiddleware } from "@byteveda/taskito/contrib/sentry"; // peer: @sentry/node
queue.use(sentryMiddleware()); // call Sentry.init(...) yourself first
```

The exception (with its stack) is captured from `onError` and reported when the job
dead-letters — one event per dead job, tagged with task/job/queue. Set `captureRetries`
to also report each intermediate failure as a warning. Other options: `tagPrefix`,
`level`, `extraTags(event)`, `taskFilter`.

### Web frameworks

`taskitoRouter` / the Fastify plugin expose a JSON API (enqueue + inspection); a separate
helper mounts the dashboard (SPA + `/api/*`) into your app.

```ts
import { taskitoRouter, taskitoDashboard } from "@byteveda/taskito/contrib/express"; // peer: express
app.use("/tasks", taskitoRouter(queue));   // POST /enqueue, GET /stats, /jobs/:id, ...
app.use("/admin", taskitoDashboard(queue)); // dashboard SPA + /api/*

import { taskitoFastify, taskitoDashboardPlugin } from "@byteveda/taskito/contrib/fastify"; // peer: fastify
app.register(taskitoFastify, { queue, prefix: "/tasks" });
app.register(taskitoDashboardPlugin, { queue, prefix: "/admin" });
```

Both routers take `includeRoutes` / `excludeRoutes` (route names: `enqueue`, `stats`,
`queue-stats`, `job`, `job-errors`, `job-result`, `cancel`, `dead-letters`, `retry-dead`)
and `resultTimeoutMs`.

NestJS exposes an injectable service:

```ts
import { TaskitoModule, TaskitoService } from "@byteveda/taskito/contrib/nest"; // peers: @nestjs/common, reflect-metadata

@Module({ imports: [TaskitoModule.forRoot(queue)] })
export class AppModule {}

// constructor(private readonly tasks: TaskitoService) {}
// this.tasks.enqueue("add", [2, 3]); this.tasks.queue gives the full API
```

## Development

```bash
pnpm install
pnpm build       # napi build (native addon) + tsup (dual esm/cjs + .d.ts)
pnpm typecheck
pnpm lint
pnpm test
```

The native crate lives at `crates/taskito-node`; this package builds it into
`native/` and wraps it with a typed TypeScript API. Postgres/Redis backends are
compiled in via `--features postgres,redis`.

## Not yet covered

End-to-end interop test coverage for the shared cross-SDK wire contract.
