# A reproducible benchmark harness — implementation plan

Issue: [#824](https://github.com/ByteVeda/flexiq/issues/824)
Epic: [#827](https://github.com/ByteVeda/flexiq/issues/827) — the last of its six children.
Branch: `bench/reproducible-harness`, off `origin/master` at `5acbbb1a`.

Commits authored as **pratyush618**. The branch was written to the remote
through the GitHub API rather than `git push`, which was unavailable in the
environment this was built in; the API carried the same authorship, so the
remote series matches the local one in both tree and author. Those commits are
unsigned, which is what `git push` would have given them here anyway.

## Why

The README sells a Rust core and no number in the tree supports it. Worse, the
docs already publish a comparison table nobody measured —
`docs/content/docs/shared/more/examples/benchmark.mdx` claims ~55,000/s enqueue
against Celery's ~5,000/s and a 3.4 ms p99, sourced to "public benchmarks and
community reports". That table is the claim #824 objects to, and it is the first
thing a sceptical reader finds.

## Shape

`bench/`: one scenario file, one orchestrator, one runner per system, one
results artifact. Five systems on their defaults — FlexiQ (SQLite and Redis),
Celery, Dramatiq, RQ, BullMQ — plus two tuned FlexiQ rows that are named
explicitly or not run at all.

Four measurements reported apart: enqueue throughput, completion throughput
(the drain measured to completion rather than to enqueue, as the issue asks),
service latency percentiles from a separate paced phase, and idle cost.

Measurement is uniform by construction: every worker writes one
`[seq, enqueued_ns, completed_ns]` record to a shared Redis list, and no
framework's own result backend is read. FlexiQ on SQLite pays that Redis hop
too.

## The methodology bug the first run found

The first full run reported a p50 of 68 seconds for FlexiQ and 0.4 ms for
BullMQ. Both figures were real and neither meant what it looked like: every job
was submitted in a four-second burst, so a percentile over that round measures
the backlog in front of each job, not the job. It was the completion column in
different units, and BullMQ scored well only because it drained fast enough
never to queue.

#824 asks for latency reported *separately* from throughput, which that cannot
be. The harness grew a second, paced load phase — submitted below every
system's capacity so nothing queues — and a saturation guard that flags any row
where that assumption broke. Those numbers were discarded and the run redone.

Published in `bench/results/`. The headline is not flattering and ships anyway:
at its defaults FlexiQ drains far slower than BullMQ or Celery. Verified
outside the harness through FlexiQ's own `stats()` before publishing — 135.6
jobs/s default, 624 jobs/s with the two documented knobs, no sink involved —
so the loss is the product's, not the harness's. FlexiQ wins idle RSS and is
near the top on enqueue.

**The first explanation of that loss was wrong and has been corrected.** It
blamed `scheduler_batch_size = 1`, which is the knob the docs recommend first
and which turns out to be nearly irrelevant: at a 50 ms poll, raising it from 1
to 64 moves 131 jobs/s to 150. The actual constraint is the dispatch channel,
`num_workers * 2` jobs wide (`worker/runner.rs:230`), drained once per poll
interval — because `dispatch_wake` only fires when in-flight work reaches
`max_in_flight` (`scheduler/mod.rs:735`), which the Python binding sets to
`num_workers + async_concurrency`, ~104, and which an eight-slot channel cannot
reach. The timer is the only thing that ever wakes the scheduler, so throughput
is about `2 * num_workers / poll_interval`:

| workers | poll | model | measured |
|---|---|---|---|
| 1 | 50 ms | 40/s | 37.1/s |
| 4 | 50 ms | 160/s | 131.1/s (batch 1) · 150.0/s (batch 64) |
| 8 | 50 ms | 320/s | 231.8/s |
| 4 | 10 ms | 800/s | 380.7/s (batch 1) · 603.2/s (batch 64) |

That looks like a bug rather than a tuning default: the "only wake when
saturated" guard in `release_in_flight` is keyed to a limit the pool never hits,
because it saturates at the channel instead. Worth its own issue.

One fairness defect was found and fixed during bring-up: FlexiQ logs two lines
per job at its default INFO, where the other four were already levelled to
WARNING by their CLI flags. `FLEXIQ_LOG_LEVEL=WARNING` is now set for every run.
It moved the number by less than the run-to-run noise, but it was measuring the
logging module.

## Build order

1. Harness — scenario, sink, machine fingerprint, process supervision, report
   schema, `run.py`, pinned `requirements.txt`, `README.md`.
2. The six runner files plus the BullMQ package and its lockfile.
2b. The paced phase, the saturation guard, and the rate pacing in all six
   runners (see above — this landed after the first run was thrown away).
3. The measured run, committed on its own so a reviewer can read the numbers
   without the harness around them.
4. The docs chart: a hand-rolled SVG section on the docs index, fed by
   `scripts/sync-bench.mjs` in the shape `scripts/sync-changelog.mjs` already
   established, so `results/latest.json` stays the only source of truth.
5. `benchmark.mdx` and the README: the invented table replaced by the measured
   one, losses included.
6. `ci-bench.yml`: the smoke scenario only. CI runners cannot produce a
   defensible measurement and the workflow says so.

## Verification

```bash
bench/.venv/bin/python bench/run.py --scenario bench/scenario.smoke.toml
bench/.venv/bin/python bench/run.py --label "..." --publish
pnpm --dir docs typecheck && pnpm --dir docs lint && pnpm --dir docs build
```

Plus: the sink record count must equal the job count for every system (a lost or
duplicated job invalidates that row's percentiles — `run.py` enforces it), and
every figure rendered in the docs must be traceable to `results/latest.json`.
