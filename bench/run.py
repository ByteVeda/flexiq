#!/usr/bin/env python3
"""Run the scenario against every queue and write one results file.

    python bench/run.py                      # the published run
    python bench/run.py --scenario bench/scenario.smoke.toml   # does it still work
    python bench/run.py --systems flexiq-sqlite,celery         # just these two

Prerequisites are a Redis on hand, the Python dependencies from
`bench/requirements.txt`, and `pnpm install` under `runners/bullmq`. See
`bench/README.md` — including the part about what these numbers are not.

Each system is measured alone, in this order, with the worker up throughout:

    reset → start worker → settle → idle sample → warmup → burst → paced

The worker keeps draining while the producer enqueues, because that is what a
queue does; an enqueue figure taken against an idle system is a figure nobody
will ever see again.

Two load phases, and the difference between them is the point. The **burst**
submits as fast as the producer can and measures enqueue throughput and the
rate a backlog clears — its latency percentiles are dominated by queueing, so
they are reported as what they are. The **paced** phase submits well below
every system's measured capacity, so no backlog forms and its percentiles are
service latency: what one caller actually waits. Reporting only the first would
make the p99 column a restatement of throughput.
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
import time
from dataclasses import dataclass
from pathlib import Path

BENCH_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(BENCH_ROOT))

import redis  # noqa: E402

from harness import machine, report, scenario as scenario_mod, sink  # noqa: E402
from harness.process import IdleCost, Worker, WorkerDied, run_json  # noqa: E402

#: Redis logical databases, one per system, so that a leftover backlog cannot
#: cross from one measurement into the next. The sink keeps db 0 to itself.
SINK_DB = 0


@dataclass(frozen=True)
class System:
    """One row of the results table, and how to drive it."""

    name: str
    label: str
    language: str
    backend: str
    #: Redis logical db this system's broker owns; None for the embedded one.
    db: int | None
    #: argv prefix for its three subcommands, relative to `bench/`.
    argv: list[str]
    env_url_var: str | None
    #: How this system was configured, in words, for the results file. Every
    #: row carries one so that "defaults" is a claim the file makes explicitly
    #: rather than something a reader has to assume.
    configuration: str = "defaults"
    #: Environment this system needs on top of the shared block.
    extra_env: tuple[tuple[str, str], ...] = ()

    def command(self, *args: str) -> list[str]:
        return [*self.argv, *args]


def systems(python: str) -> dict[str, System]:
    """The five, in the order the table reads best: the pair, then the field."""
    module = [python, "-m"]
    return {
        s.name: s
        for s in [
            System("flexiq-sqlite", "FlexiQ · SQLite (WAL)", "python", "sqlite",
                   None, [*module, "runners.flexiq_sqlite"], None),
            System("flexiq-redis", "FlexiQ · Redis", "python", "redis",
                   6, [*module, "runners.flexiq_redis"], "BENCH_FLEXIQ_REDIS_URL"),
            System("celery", "Celery · Redis", "python", "redis",
                   2, [*module, "runners.celery_runner"], "BENCH_CELERY_BROKER"),
            System("dramatiq", "Dramatiq · Redis", "python", "redis",
                   3, [*module, "runners.dramatiq_runner"], "BENCH_DRAMATIQ_URL"),
            System("rq", "RQ · Redis", "python", "redis",
                   4, [*module, "runners.rq_runner"], "BENCH_RQ_URL"),
            System("bullmq", "BullMQ · Redis", "node", "redis",
                   5, ["node", "runners/bullmq/driver.mjs"], "BENCH_BULLMQ_URL"),
            # The same engine with the two knobs its own documentation names.
            # Out of `--systems all` on purpose: the default rows are the
            # comparison, these two are what the knobs are worth.
            System("flexiq-sqlite-tuned", "FlexiQ · SQLite (tuned)", "python", "sqlite",
                   None, [*module, "runners.flexiq_sqlite"], None,
                   "scheduler_batch_size=64, scheduler_poll_interval_ms=10",
                   (("BENCH_FLEXIQ_TUNING", "tuned"),)),
            System("flexiq-redis-tuned", "FlexiQ · Redis (tuned)", "python", "redis",
                   6, [*module, "runners.flexiq_redis"], "BENCH_FLEXIQ_REDIS_URL",
                   "scheduler_batch_size=64, scheduler_poll_interval_ms=10",
                   (("BENCH_FLEXIQ_TUNING", "tuned"),)),
        ]
    }


#: `--systems all` means the comparison: every system on its defaults. The
#: tuned FlexiQ rows are named explicitly or not at all.
DEFAULT_SET = ("flexiq-sqlite", "flexiq-redis", "celery", "dramatiq", "rq", "bullmq")


def bullmq_worker_argv() -> list[str]:
    """BullMQ is the one system whose worker is not its driver."""
    return ["node", "runners/bullmq/worker.mjs"]


class Failed(RuntimeError):
    """One system did not complete the scenario. The others still run."""


def base_env(redis_url: str, workdir: Path, all_systems: dict[str, System]) -> dict[str, str]:
    """Environment shared by every runner process.

    Every system's URL is exported to every runner, which costs nothing and
    means a runner is never guessing where its broker is — the defaults baked
    into the runner files are there for a human debugging one by hand, and
    should never be what a published number was measured against.
    """
    env = dict(os.environ)
    env["BENCH_SINK_URL"] = f"{redis_url}/{SINK_DB}"
    env["BENCH_WORKDIR"] = str(workdir)
    env["PYTHONPATH"] = str(BENCH_ROOT) + os.pathsep + env.get("PYTHONPATH", "")
    # Unbuffered, so a worker that dies has actually written the reason by the
    # time we read its log.
    env["PYTHONUNBUFFERED"] = "1"
    # FlexiQ's own logger, levelled the same way `celery -l WARNING` and
    # `rq -l WARNING` are on the command lines below. It defaults to INFO and
    # writes two lines per job — received, then succeeded — which on a no-op
    # body is a measurable share of the work, and a share none of its rivals
    # is paying once they are quiet. `logging.basicConfig` cannot reach it:
    # `flexiq.log_config.configure` installs its own handler when the worker
    # starts, and this variable is what that function reads.
    env.setdefault("FLEXIQ_LOG_LEVEL", "WARNING")
    for system in all_systems.values():
        if system.env_url_var and system.db is not None:
            env[system.env_url_var] = f"{redis_url}/{system.db}"
    return env


def reset(client: redis.Redis, system: System, redis_url: str, workdir: Path) -> None:
    """Leave nothing of the last run for this one to inherit."""
    sink.flush(client, system.name)
    if system.db is not None:
        redis.Redis.from_url(f"{redis_url}/{system.db}").flushdb()
    for stale in workdir.glob(f"{system.name}.db*"):
        stale.unlink()


def measure(
    system: System,
    scenario: scenario_mod.Scenario,
    client: redis.Redis,
    env: dict[str, str],
    logs: Path,
) -> dict[str, object]:
    """Drive one system through the whole scenario and return its row."""
    env = dict(env, BENCH_SYSTEM=system.name, **dict(system.extra_env))
    probe = run_json(system.command("versions"), BENCH_ROOT, env, timeout=120)
    row: dict[str, object] = {
        "label": system.label,
        "language": system.language,
        "backend": system.backend,
        "configuration": system.configuration,
        "versions": probe["versions"],
        "concurrency_model": probe["concurrency_model"],
    }

    worker_argv = (
        bullmq_worker_argv() if system.name == "bullmq"
        else system.command("worker", "--concurrency", str(scenario.concurrency))
    )
    worker_env = dict(env, BENCH_CONCURRENCY=str(scenario.concurrency))

    with Worker(worker_argv, BENCH_ROOT, worker_env, logs / f"{system.name}.log") as worker:
        time.sleep(scenario.worker_settle_seconds)
        worker.check_alive()

        idle: IdleCost = worker.idle_cost(scenario.idle_sample_seconds)
        row["idle"] = idle.as_dict()

        # Warmup: same code path as the measured round, results thrown away.
        # Its job is to have every connection pool, prefork child and page of
        # SQLite already where the measured round expects them.
        run_json(
            system.command("enqueue", "--jobs", str(scenario.warmup_jobs),
                           "--payload-bytes", str(scenario.payload_bytes),
                           "--seq-offset", "0", "--mode", "per-job"),
            BENCH_ROOT, env, timeout=scenario.drain_timeout_seconds,
        )
        sink.wait_for(client, system.name, scenario.warmup_jobs, scenario.drain_timeout_seconds)
        sink.flush(client, system.name)
        worker.check_alive()

        enqueue = run_json(
            system.command("enqueue", "--jobs", str(scenario.jobs),
                           "--payload-bytes", str(scenario.payload_bytes),
                           "--seq-offset", "1000000", "--mode", "per-job"),
            BENCH_ROOT, env, timeout=scenario.drain_timeout_seconds,
        )
        sink.wait_for(client, system.name, scenario.jobs, scenario.drain_timeout_seconds)
        records = sink.read_all(client, system.name)
        worker.check_alive()

        if len(records) != scenario.jobs:
            raise Failed(
                f"{system.name}: sink holds {len(records)} records for {scenario.jobs} jobs — "
                "a duplicate or a lost job makes these percentiles meaningless"
            )
        row["drain"] = sink.summarise(records).as_dict()
        row["enqueue"] = {"per_job": {k: enqueue[k] for k in ("jobs", "seconds", "per_second")}}

        if probe["supports_batch"]:
            sink.flush(client, system.name)
            batch = run_json(
                system.command("enqueue", "--jobs", str(scenario.jobs),
                               "--payload-bytes", str(scenario.payload_bytes),
                               "--seq-offset", "2000000", "--mode", "batch"),
                BENCH_ROOT, env, timeout=scenario.drain_timeout_seconds,
            )
            sink.wait_for(client, system.name, scenario.jobs, scenario.drain_timeout_seconds)
            row["enqueue"]["batch"] = {k: batch[k] for k in ("jobs", "seconds", "per_second")}
        else:
            row["enqueue"]["batch"] = None
            row["enqueue"]["batch_declined"] = "no bulk producer API"

        # The paced phase. Submitted under capacity so that nothing queues and
        # the percentiles describe one job rather than the backlog in front of
        # it.
        sink.flush(client, system.name)
        paced_timeout = scenario.drain_timeout_seconds + (
            scenario.latency_jobs // max(scenario.latency_rate_per_second, 1)
        )
        run_json(
            system.command("enqueue", "--jobs", str(scenario.latency_jobs),
                           "--payload-bytes", str(scenario.payload_bytes),
                           "--seq-offset", "3000000", "--mode", "per-job",
                           "--rate", str(scenario.latency_rate_per_second)),
            BENCH_ROOT, env, timeout=paced_timeout,
        )
        sink.wait_for(client, system.name, scenario.latency_jobs, scenario.drain_timeout_seconds)
        row["latency"] = sink.paced(
            sink.read_all(client, system.name), scenario.latency_rate_per_second
        )
        worker.check_alive()

    return row


def failed_row(system: System, reason: str) -> dict[str, object]:
    """A system that did not finish still gets a row, saying so.

    Dropping it would leave a table whose absences look like modesty. The row
    carries the reason into the results file, where a reader can weigh it.
    """
    row: dict[str, object] = {
        "label": system.label,
        "language": system.language,
        "backend": system.backend,
        "configuration": system.configuration,
        "versions": {},
        "concurrency_model": "unknown — the run failed",
    }
    for axis in report.AXES:
        row[axis] = None
        row[f"{axis}_declined"] = reason
    return row


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--scenario", type=Path, default=BENCH_ROOT / "scenario.toml")
    parser.add_argument("--systems", default="all",
                        help="comma-separated names, 'all' (every system on its defaults) "
                             "or 'everything' (adds the tuned FlexiQ rows)")
    parser.add_argument("--redis-url", default=os.environ.get("BENCH_REDIS", "redis://127.0.0.1:6399"),
                        help="Redis to use, without a database number")
    parser.add_argument("--python", default=sys.executable,
                        help="interpreter the Python runners use — the one with bench/requirements.txt in it")
    parser.add_argument("--out", type=Path, default=None,
                        help="results file; defaults to results/<date>-<machine>.json")
    parser.add_argument("--label", default=None,
                        help="what this machine is, in words, e.g. '4-core shared cloud VM'")
    parser.add_argument("--notes", default="",
                        help="anything a reader needs to weigh these numbers")
    parser.add_argument("--publish", action="store_true",
                        help="also write results/latest.json, which the docs read")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    scenario = scenario_mod.load(args.scenario)
    registry = systems(args.python)

    if args.systems == "all":
        chosen = list(DEFAULT_SET)
    elif args.systems == "everything":
        chosen = list(registry)
    else:
        chosen = args.systems.split(",")
    unknown = [name for name in chosen if name not in registry]
    if unknown:
        raise SystemExit(f"unknown system(s): {', '.join(unknown)}; have {', '.join(registry)}")

    client = sink.connect(f"{args.redis_url}/{SINK_DB}")
    try:
        client.ping()
    except redis.RedisError as err:
        raise SystemExit(
            f"no Redis at {args.redis_url}: {err}\n"
            "Every system here needs one — the four brokers, and the completion sink "
            "that FlexiQ-on-SQLite writes to as well. See bench/README.md."
        ) from None

    workdir = BENCH_ROOT / ".work"
    logs = workdir / "logs"
    shutil.rmtree(workdir, ignore_errors=True)
    logs.mkdir(parents=True, exist_ok=True)
    env = base_env(args.redis_url, workdir, registry)

    results: dict[str, object] = {}
    failures: list[str] = []
    for name in chosen:
        system = registry[name]
        print(f"→ {system.label}", flush=True)
        reset(client, system, args.redis_url, workdir)
        try:
            results[name] = measure(system, scenario, client, env, logs)
        except (Failed, WorkerDied, sink.DrainTimeout, RuntimeError) as err:
            print(f"  failed: {err}", file=sys.stderr, flush=True)
            failures.append(name)
            results[name] = failed_row(system, str(err).splitlines()[0][:400])

    doc = {
        "schema": report.SCHEMA,
        "scenario": scenario.as_dict(),
        "machine": machine.fingerprint(
            label=args.label or f"unlabelled ({machine.slug()})",
            notes=args.notes,
        ),
        "sink": {"kind": "redis-list", "url": f"{args.redis_url}/{SINK_DB}"},
        "systems": results,
    }

    out = args.out or BENCH_ROOT / "results" / f"{time.strftime('%Y-%m-%d')}-{machine.slug()}.json"
    report.write(doc, out)
    if args.publish:
        report.write(doc, BENCH_ROOT / "results" / "latest.json")

    print(report.render_summary(doc))
    print(f"written: {out}")
    if failures:
        print(f"\nFAILED: {', '.join(failures)}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
