# FlexiQ benchmark harness

One scenario, run against FlexiQ, Celery, Dramatiq, RQ and BullMQ, producing a
committed results artifact. The chart on the documentation landing page is
generated from that artifact and from nothing else, so every number the project
publishes has a script in this directory behind it.

The results are in [`results/latest.json`](results/latest.json), and the page
that reads them is [About → Benchmarks](../docs/content/docs/about/benchmarks.mdx).

## Running it

```bash
# The plumbing, in about a minute, with no network and no services.
uv run --project bench python bench/run.py --smoke

# The published run. Bring your own Redis; the URL is never stored here.
REDIS_URL=redis://localhost:6379 \
  uv run --project bench python bench/run.py --out bench/results/latest.json
```

Node entrants need their dependencies installed once:

```bash
pnpm --dir bench/node install --frozen-lockfile
```

Useful flags: `--only <id> [<id> …]` to run a subset, `--jobs`, `--concurrency`,
`--payload-bytes`, `--warmup-jobs`, `--idle-window`, `--drain-timeout` to
override the scenario. `--smoke` is a 20-job SQLite-only run that writes no
artifact.

## The scenario

[`scenario.json`](scenario.json) is the definition, and both the Python harness
and the Node adapters read it — there is no second copy to drift.

| | |
|---|---|
| Jobs | 500 measured, after 50 warmup |
| Payload | 256 bytes, `{"i", "t", "pad"}` |
| Concurrency | 4, in each engine's own unit |
| Work | decode the payload, record a latency, return |
| Measured | **to completion**, not to enqueue |

The task body does nothing but observe, deliberately. Anything heavier would
measure the handler rather than the queue underneath it.

### The three metrics, reported separately

- **Enqueue throughput** — one producer, serial, no batch API. FlexiQ has
  `enqueue_many` and most of the others do not, so using it would measure a
  feature rather than a queue.
- **End-to-end latency** — submission to completion, in percentiles. Every
  handler appends `index latency completed_at` to one file; `O_APPEND` writes
  under `PIPE_BUF` are atomic on Linux, so a forked Celery child, a Dramatiq
  thread and a Node worker can share a sink without coordinating. FlexiQ's own
  `created_at`/`completed_at` columns could answer this from the database, but
  the other four have no equivalent, and a number read one way for FlexiQ and
  another way for everyone else is not a comparison.
- **Idle cost** — CPU and RSS across the worker process tree with the queue
  empty. A poller and a blocking consumer look identical on a drain time and
  very different on a small instance.

### "Concurrency 4" is not one thing

Each row in the artifact carries its own `concurrency_model`, because the
number means something different to each entrant:

| Entrant | What 4 means |
|---|---|
| FlexiQ (Python) | 4 worker threads (`workers=4`) |
| FlexiQ (Node) | 4 concurrent jobs in one process |
| Celery | 4 prefork processes (`-c 4`, the default pool) |
| Dramatiq | 1 process × 4 threads |
| RQ | 4 worker processes; RQ forks a child per job by default |
| BullMQ | 4 concurrent jobs in one process |

Every entrant runs with its own defaults. None of them was tuned, including
FlexiQ — a benchmark that tunes one entrant is a benchmark that entrant wrote.

## Reading the published numbers honestly

The committed run crosses a **network** to reach Redis: a hosted instance about
36 ms away. That is a floor under every Redis-backed entrant on every command,
so it is a property of the link as much as of the engines. It is recorded in
the artifact (`backends.redis.rtt_ms`) rather than left as a footnote, and the
comparison is same-backend for exactly that reason — all five Redis entrants
pay the identical round trip.

The SQLite rows are a **different deployment**, not a faster one: a local file
against a network service. They are in the artifact because no-broker is what
FlexiQ is for, not so the local number can stand in for the networked one.

Re-run it against a colocated Redis and every absolute figure will move. That
is the point of the harness being in the repository.

## Pins

Every dependency is pinned exactly — [`pyproject.toml`](pyproject.toml) with
[`uv.lock`](uv.lock), [`node/package.json`](node/package.json) with
`pnpm-lock.yaml`, and Python fixed at 3.12.

The `flexiq` pin deliberately does **not** follow the workspace version, and
`bench/` is deliberately absent from `SNIPPETS` in `scripts/version.mjs`: the
committed numbers were produced by the pinned release, and a version bump that
silently re-pointed the harness would leave the published chart describing a
build nobody ran. To move the pin, re-run the harness and commit the new
artifact — never the other way round.

## Layout

```
run.py                 the one command
scenario.json          the definition, shared with the Node side
harness/
  scenario.py          the scenario and the payload padding
  machine.py           machine fingerprint, Redis probe (RTT, version, eviction policy)
  latency.py           the shared sink and the percentiles
  idle.py              process-tree CPU/RSS sampler
  process.py           worker subprocesses: own session, stack-scoped teardown, logs on disk
  redis_scope.py       run-unique names, keyspace diff, cleanup, the baseline check
  report.py            the artifact and the markdown table
  adapters/            one file per entrant, over a shared run template
  workers/             the worker and producer entry points each framework's CLI imports
node/
  shared.mjs           the Node half of the sink, the payload and the producer loop
  bullmq.mjs flexiq.mjs
results/               committed artifacts
```

## Running against a shared Redis

The harness assumes it might not own the server. Every run gets an id; the
queue and task names carry it, so workers never see another run's backlog. The
keyspace is snapshotted before each entrant and diffed after, and whatever is
new and recognisably a queue's is unlinked — which stays correct when an engine
reshapes its keys, unlike a hand-written list of patterns. Job ids are also
removed from containers that existed beforehand, since a name diff cannot see a
membership.

`DBSIZE` returning to its baseline is then asserted, not assumed, and the check
lands in the artifact under `cleanup`. Nothing here ever calls `FLUSHDB`.
