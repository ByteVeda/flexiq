# bench

One scenario, run against FlexiQ and against four other task queues, on your
machine, from one command. The committed output lives in [`results/`](results/);
the chart on the docs index and the comparison table in
[the benchmark page](../docs/content/docs/shared/more/examples/benchmark.mdx)
are rendered from it and from nothing else.

> **The numbers in `results/` were measured on a shared cloud VM.** That is
> written into the file, and it is not a formality: a virtualised host with
> neighbours you cannot see has noisy tails and a throughput ceiling that is not
> yours. Reproduce before you decide anything. The whole point of this directory
> is that you can.

## Run it

```bash
# A Redis. Every system needs one — the four brokers, and the completion sink
# that FlexiQ-on-SQLite writes to as well.
redis-server --port 6399 --save '' --appendonly no --daemonize yes

python3 -m venv bench/.venv
bench/.venv/bin/pip install -r bench/requirements.txt
pnpm --dir bench/runners/bullmq install --frozen-lockfile

# Does it all still work? ~2 minutes, measures nothing.
bench/.venv/bin/python bench/run.py --scenario bench/scenario.smoke.toml

# The real thing. ~40 minutes.
bench/.venv/bin/python bench/run.py --label "what this machine is"
```

`--systems flexiq-sqlite,celery` runs a subset. `--systems everything` adds the
tuned FlexiQ rows described below. `--publish` also writes `results/latest.json`,
which is what the docs read.

## What is measured

The scenario is [`scenario.toml`](scenario.toml) — job counts, payload size,
concurrency, the paced rate, warmup, timeouts — and every runner reads that same
file.

**Two load phases, because one cannot answer both questions.** The *burst* phase
submits every job as fast as the producer can and measures how quickly a backlog
clears. The *paced* phase submits at a fixed rate well under every system's
measured capacity, so nothing queues, and measures what one caller waits.

That split is not a refinement; it is the difference between a meaningful p99
and a meaningless one. In the burst phase the twenty-thousandth job waits behind
the nineteen thousand in front of it, so its "latency" is the drain time
restated — and a system fast enough never to build a backlog scores a
near-perfect p99 for reasons that have nothing to do with its latency. The first
run of this harness reported exactly that and the numbers were thrown away.

| Axis | Phase | What it is |
|---|---|---|
| **Enqueue throughput** | burst | Jobs submitted per second, one call per job, producer side — what a request handler waits for before it can return |
| **Completion throughput** | burst | Jobs finished per second — measured to completion, not to enqueue: the rate a backlog actually clears |
| **Service latency** | paced | Enqueue → completion, p50 / p90 / p99 / max, with nothing queued. What the job's *subject* waits |
| **Queueing delay** | burst | The same measurement under load. Reported as what it is, because the contrast with the paced figure is instructive |
| **Idle cost** | — | RSS and CPU of the worker with an empty queue, sampled **before it has run a single job** — provisioning cost, not memory retained after work |

The paced rate has to sit below the *slowest* system's capacity or the phase
measures queueing again. It does not take that on trust: the last tenth of the
run is compared against the first, and a tail that has grown by more than half
sets `saturated` on that row. Every surface that prints these figures marks such
a row rather than passing queueing delay off as service time.

Batch enqueue is reported as a **second, separate figure** where the system has
a bulk producer API, and as an explicit `"no bulk producer API"` where it does
not. Celery and Dramatiq have none; quoting FlexiQ's batch figure against
Celery's per-job figure would be comparing a bulk insert to a loop.

## The fairness rules

These are the decisions that could have been made dishonestly. They are written
down so you can disagree with them specifically.

**One sink, and everybody pays for it.** Every worker, whatever it drains, does
exactly one extra thing per job: `RPUSH` a `[seq, enqueued_ns, completed_ns]`
record onto a Redis list. No framework's own result backend is consulted,
because five result backends are five different definitions of "done". FlexiQ on
SQLite has to reach Redis for this too, and is not given that back.

**Defaults, except for three things.** Every system runs the configuration you
get out of the box, with three exceptions, applied to all of them:

1. *Log level is WARNING.* Celery, Dramatiq, RQ and FlexiQ all log at least one
   line per job at their default level. Left alone, a chunk of what looks like
   queue overhead is the logging module formatting timestamps, and the
   chattiest default loses a benchmark about something else. (FlexiQ needs
   `FLEXIQ_LOG_LEVEL`; `logging.basicConfig` cannot reach its handler.)
2. *Result storage is off.* RQ and BullMQ keep finished jobs by default; Celery
   would need a result backend configured to keep anything. The sink is the only
   record of a completion, for everyone.
3. *Concurrency is the scenario's.* See below — this is the leakiest of the
   three.

Celery's gossip, mingle and heartbeat stay **on**, because they are on for
everyone who types `celery worker`, and their cost is part of what this
measures.

**Concurrency is not one thing.** `concurrency = 4` means four jobs in flight,
and each project gets there its own way:

| System | Four jobs in flight means |
|---|---|
| FlexiQ | 4 worker threads inside one process, in the Rust core |
| Celery | 4 prefork child processes (`-c 4`) |
| Dramatiq | 4 worker processes, 1 thread each (its own default is per-core processes × 8 threads) |
| RQ | 4 single-job worker processes (`rq worker-pool -n 4`) |
| BullMQ | 1 Node process, 4 jobs interleaved on one event loop |

For a body that does one Redis write, these are comparable. For a CPU-bound
body they are not — an event loop cannot run two job bodies at once — and this
harness does not pretend otherwise.

**The task body is a no-op.** It checks the payload is there and writes the sink
record. What is left is the framework's own cost, which is the thing under
comparison. Your real handler will dominate all of this; that is the most
important sentence on this page.

**Two FlexiQ configurations, and only FlexiQ gets two.** `flexiq-sqlite` and
`flexiq-redis` are the defaults — `scheduler_batch_size = 1` and a 50 ms poll.
`flexiq-sqlite-tuned` and `flexiq-redis-tuned` apply the two knobs FlexiQ's own
documentation tells you to reach for first (`scheduler_batch_size = 64`,
`scheduler_poll_interval_ms = 10`). That asymmetry is a disclosure, not a thumb
on the scale: FlexiQ's default loses this benchmark badly, a table showing only
the tuned number would hide that, and a table showing only the default would
hide that the knob exists. No equivalent tuning was applied to the other four.
**If you know the documented knob that moves one of their numbers, that is a
pull request this harness wants.**

Of those two knobs it is the poll interval that does nearly all the work, and
not for the reason the names suggest. FlexiQ's scheduler fills a dispatch
channel holding `num_workers * 2` jobs, then sleeps out the poll interval before
looking again; nothing wakes it when a worker frees up, because the completion
wake is keyed to `max_in_flight` (104 under the stock Python binding) and a
channel of eight never reaches it. Throughput therefore lands near
`2 * num_workers / poll_interval` however deep the backlog. Batching the *claim*
cannot move a constraint that lives in the channel: at a 50 ms poll, raising
`scheduler_batch_size` from 1 to 64 moves 131 jobs/s to 150, where dropping the
poll to 10 ms on its own gives 381.

## What this does not measure

Durability guarantees, at-least-once semantics under worker loss, retry
behaviour, scheduling, priorities, workflows, multi-machine fan-out, memory
under sustained load, or anything about a payload that is not 256 bytes of
ASCII. Two queues can hit identical numbers here and be entirely different
propositions in production. This measures the cost of moving a job from one
process to another and back, and nothing else.

One thing the idle column is *not*: memory after a worker has drained a
backlog. It is sampled at startup, so it is what a freshly provisioned worker
costs while waiting, not what one holds onto afterwards. A second sample taken
after the burst would answer that and would be a genuinely useful addition —
it is not in here yet, and the column should not be read as if it were.

## The results file

`results/<date>-<machine>.json`, schema in
[`harness/report.py`](harness/report.py). It records the scenario, the machine
(CPU, cores, RAM, kernel, interpreter, Redis version), the git commit and
whether the tree was dirty, and then per system: the exact installed version of
every library, the concurrency model in words, the configuration, and every
axis. A system whose run failed still gets a row, carrying the reason — a
missing row would read as modesty.

`run.py` refuses to write a file that does not validate, and refuses a drain
that returned the wrong number of records: percentiles over a truncated drain
are the flattering half of a distribution.

## Adding a system

One file under [`runners/`](runners/), with three subcommands — `worker`,
`enqueue`, `versions` — and an entry in `systems()` in [`run.py`](run.py).
`_common.py` handles the argument parsing, the log levelling and the sink;
a runner should contain its queue's API and nothing else that could be accused
of favouring it. Copy [`runners/rq_runner.py`](runners/rq_runner.py), which is
the shortest.
