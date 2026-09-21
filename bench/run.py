#!/usr/bin/env python3
"""Run the FlexiQ cross-runtime benchmark and write a results artifact.

One scenario — N jobs, one fixed payload, one fixed worker concurrency,
measured to completion — against FlexiQ, Celery, Dramatiq, RQ and BullMQ.
Enqueue throughput, end-to-end latency percentiles and idle cost are reported
separately, because an engine that wins one can lose another and a single
number would hide it.

    # everything that needs no network
    uv run --project bench python bench/run.py --smoke

    # the published run
    REDIS_URL=redis://… uv run --project bench python bench/run.py \\
        --out bench/results/latest.json

The Redis URL is never read from a file in this repository. Every Redis-backed
entrant shares one server, so the network round trip is a floor under all of
them equally — `bench/README.md` says what that does to the numbers.
"""

from __future__ import annotations

import argparse
import secrets
import sys
import time
from pathlib import Path
from typing import Any

from harness import machine, report
from harness.adapters import ADAPTER_IDS, RunContext, select
from harness.process import BenchError, tail
from harness.redis_scope import RedisScope, RunNames
from harness.scenario import Scenario

BENCH_ROOT = Path(__file__).resolve().parent
DEFAULT_OUT = BENCH_ROOT / "results" / "latest.json"

#: A run small enough to prove the plumbing without touching the network.
SMOKE = {"jobs": 20, "warmup_jobs": 5, "idle_window_s": 3.0}
SMOKE_RUNTIMES = ("flexiq-sqlite", "flexiq-node-sqlite")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--jobs", type=int, help="measured jobs per runtime")
    parser.add_argument(
        "--concurrency", type=int, help="worker concurrency, in each engine's unit"
    )
    parser.add_argument("--payload-bytes", type=int, dest="payload_bytes")
    parser.add_argument("--warmup-jobs", type=int, dest="warmup_jobs")
    parser.add_argument("--idle-window", type=float, dest="idle_window_s")
    parser.add_argument("--drain-timeout", type=float, dest="drain_timeout_s")
    parser.add_argument(
        "--only",
        nargs="+",
        metavar="ID",
        help=f"run a subset: {', '.join(ADAPTER_IDS)}",
    )
    parser.add_argument(
        "--smoke",
        action="store_true",
        help="tiny local-only run that proves the harness works; writes nothing by default",
    )
    parser.add_argument("--out", type=Path, help=f"artifact path (default {DEFAULT_OUT})")
    parser.add_argument(
        "--scenario",
        type=Path,
        default=BENCH_ROOT / "scenario.json",
        help="scenario definition to load",
    )
    return parser.parse_args(argv)


def build_scenario(args: argparse.Namespace) -> Scenario:
    scenario = Scenario.load(args.scenario)
    if args.smoke:
        scenario = scenario.replace(**SMOKE)
    return scenario.replace(
        jobs=args.jobs,
        concurrency=args.concurrency,
        payload_bytes=args.payload_bytes,
        warmup_jobs=args.warmup_jobs,
        idle_window_s=args.idle_window_s,
        drain_timeout_s=args.drain_timeout_s,
    )


def run_id() -> str:
    """Unique per run, and short enough to live inside a queue name."""
    return f"{time.strftime('%Y%m%d%H%M%S')}{secrets.token_hex(2)}"


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    scenario = build_scenario(args)
    wanted = args.only or (list(SMOKE_RUNTIMES) if args.smoke else None)
    adapters = select(wanted)

    redis_url = machine.redis_url_from_env()
    needs_redis = [adapter.id for adapter in adapters if adapter.needs_redis]
    if needs_redis and not redis_url:
        print(
            f"error: {', '.join(needs_redis)} need Redis. Set REDIS_URL, or narrow the run "
            "with --only / --smoke.",
            file=sys.stderr,
        )
        return 2

    redis_target = probe = None
    if redis_url and needs_redis:
        print(f"probing Redis ({len(needs_redis)} entrants need it) …", file=sys.stderr)
        probe = machine.probe_redis(redis_url)
        redis_target = probe.to_dict()
        print(f"  {probe.provider} {probe.version}, rtt {probe.rtt_ms}", file=sys.stderr)

    identifier = run_id()
    names = RunNames(identifier)
    run_root = BENCH_ROOT / "runs" / identifier
    scope = RedisScope(redis_url, identifier) if redis_url and needs_redis else None
    baseline = scope.dbsize() if scope else None

    runtimes: list[dict[str, Any]] = []
    for adapter in adapters:
        ctx = RunContext(
            adapter_id=adapter.id,
            scenario=scenario,
            run_dir=run_root / adapter.id,
            names=names,
            bench_root=BENCH_ROOT,
            redis_url=redis_url,
        )
        print(
            f"→ {adapter.id} ({scenario.jobs} jobs, concurrency {scenario.concurrency})",
            file=sys.stderr,
        )
        before = scope.snapshot() if scope and adapter.needs_redis else None
        try:
            result = adapter.measure(ctx)
        except BenchError as exc:
            print(f"  failed: {exc}", file=sys.stderr)
            print(tail(ctx.run_dir / "worker.log"), file=sys.stderr)
            runtimes.append({"id": adapter.id, "engine": adapter.engine, "error": str(exc)})
            result = None
        finally:
            if scope is not None and before is not None:
                job_ids = result.get("job_ids", []) if result else []
                swept = scope.cleanup(before, [str(i) for i in job_ids])
                print(f"  cleaned {swept['deleted']} keys", file=sys.stderr)
        if result is not None:
            result.pop("job_ids", None)
            runtimes.append(result)
            print(f"  {report.table({'runtimes': [result]}).splitlines()[-1]}", file=sys.stderr)

    cleanup = scope.verify(baseline) if scope is not None and baseline is not None else None
    if scope is not None:
        scope.close()
    if cleanup is not None and not cleanup["clean"]:
        print(f"warning: Redis not back to baseline: {cleanup}", file=sys.stderr)

    artifact = report.build(
        run_id=identifier,
        scenario=scenario.to_dict(),
        machine=machine.describe_machine(),
        redis=redis_target,
        runtimes=runtimes,
        cleanup=cleanup,
    )
    print(report.table(artifact))

    out = args.out or (None if args.smoke else DEFAULT_OUT)
    if out is not None:
        report.write(out, artifact)
        print(f"\nwrote {out}", file=sys.stderr)
    return 1 if any("error" in runtime for runtime in runtimes) else 0


if __name__ == "__main__":
    raise SystemExit(main())
