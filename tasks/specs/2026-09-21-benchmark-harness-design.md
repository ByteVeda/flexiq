# A reproducible benchmark harness with published numbers

Issue: [#824](https://github.com/ByteVeda/flexiq/issues/824)
Epic: [#827](https://github.com/ByteVeda/flexiq/issues/827) — "the front door still sells 1.0"
Governs: `bench/`, `bench/results/latest.json`, `docs/content/docs/about/benchmarks.mdx`

## Why this document exists

The project sold speed and shipped no benchmark: no `benches/`, no criterion
target, no harness anywhere in the tree. The epic's rule is the binding one —
*no number goes on the landing page that a reader cannot reproduce from a
script in this repository* — and it decides almost every question below.

The thing that must be right is not the magnitude of any figure. It is that a
stranger who disagrees with a figure can re-run the exact thing that produced
it, and that the figure does not flatter us by construction. A benchmark FlexiQ
wins on every axis reads as a benchmark FlexiQ wrote, so the losses are part of
the deliverable, not a risk to it.

## The evidence

| Fact | Where | What it forces |
|---|---|---|
| No local `redis-server`, no local Postgres server, no passwordless sudo, docker off-limits | this machine; [[test-backends]] | The published run crosses a network to a hosted Redis. Measured: 8.6.2, **34–41 ms** round trip. |
| The hosted instance is **shared** and holds ~1750 `flexiq:*` keys from past contract suites | `SCAN` on it | Never `FLUSHDB`. Isolation by name, cleanup by diff, `DBSIZE` asserted. |
| `SELECT 3` and `SELECT 0` are the **same keyspace** there (both report the same `DBSIZE`, a key `SET` in one is visible in the other) | probed directly | No logical-database partition to hide in. Free-tier Redis Cloud runs `databases 1`. |
| Its `maxmemory-policy` is `volatile-lru`, not `noeviction` | `INFO memory` | A durability caveat on the whole run. Recorded in the artifact and stated on the page. |
| The Python shell exposes no Redis key prefix; `RedisStorage::with_prefix` exists only in core | `crates/flexiq-python/src/py_queue/mod.rs`, `redis_backend/mod.rs:37` | Isolation cannot be a prefix without an SDK change. It is a per-run **queue and task name** instead. |
| `Storage::stats` is global, not per-queue | `storage/traits.rs:254` | The drain test cannot be `stats()`: stale rows from old contract runs would count. |
| Every SDK has `created_at`/`completed_at` in Unix ms; Celery, Dramatiq, RQ and BullMQ have no equivalent | `crates/flexiq-core/src/job.rs:72-135` | The published latency cannot come from the database. One measurement path, or it is not a comparison. |
| `O_APPEND` writes below `PIPE_BUF` are atomic on Linux | POSIX | A forked Celery child, a Dramatiq thread and a Node worker can share one sink with no lock and no coordinator. |
| `scripts/polyglot_e2e.py` already solved multi-process orchestration: own session per worker, `ExitStack` teardown, logs on disk, monotonic deadline, fail fast on a dead worker | that file | Copy the shape rather than re-derive it. |
| `scripts/version.mjs` rewrites `flexiq==X.Y.Z` in every registered snippet | `scripts/version.mjs:116-133` | The bench pin must **not** be registered — see D6. |
| `uv.lock` is gitignored repo-wide | `.gitignore:8` | Needs an explicit `!bench/uv.lock`, or the harness's whole premise is unlocked. |
| Biome in `docs/` reformats what it owns, and `--check` compares bytes | `docs/biome.json` | The generated TS module is excluded there, or the staleness gate fails on formatting. |
| BullMQ 6 makes `ioredis` optional and, under native ESM, refuses to build a client from an option bag | its `redis-connection.js` | `ioredis` is a direct pin, and a client instance is constructed per Queue/Worker. |
| `rq worker` spells the flag `--logging_level`; `rq worker-pool` spells it `--logging-level` | RQ 2.12 CLI | Not a typo in `rq_py.py`. |
| Two of five warmup jobs hung indefinitely under `rq worker-pool` over the 36 ms link | observed, run `20260921223116807a` | Four separate `rq worker` processes instead — also closer to how RQ is actually supervised. |

## Decisions

- **D1** The head-to-head is **same-backend only**. FlexiQ runs on the same
  remote Redis as its four competitors, so the round trip is a common floor
  rather than a thumb on the scale.
- **D2** The SQLite rows are reported as a **separate deployment**, never as a
  head-to-head win, and the chart keeps them in their own group with their own
  scale.
- **D3** Latency comes from one uniform sink for every entrant. FlexiQ's own
  columns are a cross-check and never the published number.
- **D4** Every entrant runs its own **defaults**, FlexiQ included, and each row
  carries its `concurrency_model` verbatim — "4" is four different things.
- **D5** Enqueue is serial everywhere. `enqueue_many` would measure a feature.
- **D6** The `flexiq` pin does not track master. The artifact describes the
  release that produced it; a version bump that silently re-pointed the harness
  would leave the chart describing a build nobody ran.
- **D7** Cleanup is a **keyspace diff**, not a list of key patterns: snapshot
  before, snapshot after, unlink what is new and recognisably a queue's. It
  stays correct when an engine reshapes its keys.
- **D8** `DBSIZE` back to baseline is a **checked postcondition**, in the
  artifact under `cleanup`, and `<=` rather than `==` — an engine's own
  maintenance can retire rows a previous run left behind.
- **D9** A failing entrant is recorded and the run continues. Thirty minutes of
  measurement should not be lost to the fourth of eight entrants.
- **D10** The chart is hand-rolled SVG/CSS off the generated module. No
  charting dependency: every neighbour in `diagrams/` is raw, and series
  colours are resolved per theme because a bar needs 3:1 in both.

## The shape

```
bench/run.py  ──►  per entrant: warmup ▸ submit ▸ drain ▸ idle
                        │                                  │
                        └── one latency sink ──────────────┘
                                   │
                     bench/results/latest.json   (committed)
                                   │
                    scripts/sync-benchmarks.mjs
                                   │
                    docs/app/lib/benchmark-data.ts   (generated)
                           │                  │
                  landing BenchmarkFold   about/benchmarks
```

Nothing downstream of the artifact is typed by hand, including the machine in
the caption. `--check` on the sync script is the staleness gate.

### Phases, and why in that order

Warmup is untimed but **not separate**: same queue, same handler, indices below
the threshold the summary filters on. A second timed phase would drift out of
step with the first; an index threshold cannot. Drain is measured from the
producer's own clock to the last completion recorded in the sink, so the number
is a property of the run rather than of how often the harness polled. Idle is
sampled last, with the queue empty and the workers still up — which is the cost
of being available, and the axis a poller loses.

## What this is not

**Not a claim about FlexiQ's Redis backend in production.** It is a claim about
that backend across a 36 ms link, where its round trips per job are multiplied
harder than a blocking-pop design's. The page says so, and the harness exists
so that a reader with a colocated Redis can say otherwise with evidence.

**Not a gate.** `bench.yml` is `workflow_dispatch`, gates nothing, and commits
nothing. A benchmark as a required check turns shared-runner noise into a red
PR.

**Not an SDK change.** Exposing a Redis key prefix through the shells would
have made isolation easier and would have put cross-SDK parity debt on a
documentation issue.
