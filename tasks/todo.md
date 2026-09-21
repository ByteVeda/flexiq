# A reproducible benchmark harness with published numbers (#824)

Design: `tasks/specs/2026-09-21-benchmark-harness-design.md`
Epic: #827 — "the front door still sells 1.0"

The project sold speed and shipped no benchmark. The epic's rule governs: *no
number goes on the landing page that a reader cannot reproduce from a script in
this repository.*

**The thing that must be right** is not any magnitude. It is that the numbers
are reproducible from the tree, that every entrant is measured the same way,
and that the losses are published — a benchmark FlexiQ wins on every axis reads
as a benchmark FlexiQ wrote.

Three decisions carry the rest:

- **The head-to-head is same-backend.** The only Redis available is ~36 ms
  away, so FlexiQ runs on it too and the round trip is a common floor. SQLite
  is reported as a separate deployment, never as a head-to-head win.
- **One measurement path.** Every handler in every language appends to the same
  sink. FlexiQ's own `created_at`/`completed_at` are a cross-check only — the
  other four have no equivalent.
- **Defaults everywhere, and `concurrency_model` on every row.** "4" is four
  prefork processes, one process with four threads, four forking workers, or
  four concurrent jobs in one process, depending on who is asked.

## Items

- [x] `bench/` harness: scenario, sink, idle sampler, process lifecycle, report
- [x] Adapters: FlexiQ (Python/Node × SQLite/Redis), Celery, Dramatiq, RQ, BullMQ
- [x] Redis hygiene: per-run names, keyspace diff cleanup, `DBSIZE` postcondition
- [x] Pinned deps — `bench/uv.lock` un-ignored, `bench/node/pnpm-lock.yaml`, Python 3.12
- [x] `bench-ruff` / `bench-mypy` pre-commit hooks
- [x] `scripts/sync-benchmarks.mjs` → generated `docs/app/lib/benchmark-data.ts` (+ `--check`)
- [x] `BenchmarkChart` / `BenchmarkNotes` / `BenchmarkTable` in the diagrams barrel
- [x] `BenchmarkFold` on the docs landing page
- [x] `about/benchmarks` page + nav entry
- [x] README section; `about/comparison` loses its unbacked "lower latency"
- [x] `bench.yml` — `workflow_dispatch`, colocated Redis, gates nothing
- [x] The published run, committed as `bench/results/latest.json`
- [x] Memory + skills updated

## Review

**What shipped.** A `bench/` project with its own pins, eight entrants over one
scenario, three metrics reported separately, and a committed artifact that the
landing chart and the docs page are both generated from. `--check` on the sync
script is the staleness gate; nothing downstream of the artifact is hand-typed.

**What the numbers say.** Against a Redis 36 ms away, Dramatiq and BullMQ win
the drain outright, Celery and RQ sit in the middle, and FlexiQ's Redis backend
is last by a wide margin — it spends more round trips per job, and the link
multiplies them. FlexiQ also loses on idle, because its scheduler polls where
RQ and BullMQ block. Both losses are on the page, with the reason.

**Two bugs the harness found in itself.** A producer that submits and then
declines to exit hangs the run with no output — every producer subprocess now
has a timeout. And `rq worker-pool` left two of five warmup jobs hung
indefinitely over the high-latency link; four separate `rq worker` processes do
not, and are closer to how RQ is supervised anyway.

**Deliberately not done.** No Redis key prefix was added to the SDK shells,
though it would have made isolation easier — that is cross-SDK parity debt on a
documentation issue. No engine tuning, for anyone. And the run was not made a
required check: a benchmark on a shared runner turns noise into a red PR.
