"""The artifact, and the table a reader sees first.

One JSON file per run, committed. Its job is to be enough on its own: the
scenario, the machine, the backends and every runtime's three metrics, with the
caveats written into the file rather than left to whoever quotes it.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

#: Bump when a consumer would break — `scripts/sync-benchmarks.mjs` checks it.
SCHEMA_VERSION = 1

SQLITE_BACKEND = {
    "kind": "local",
    "engine": "SQLite (WAL)",
    "location": "local disk, same host as the workers",
}


def notes_for(machine: dict[str, Any], redis: dict[str, Any] | None) -> list[str]:
    """The caveats, derived rather than remembered.

    A caveat that has to be re-typed for each run is a caveat that eventually
    is not.
    """
    notes = [
        "Every entrant runs its workers and its producer as separate processes, "
        "submits serially, and is measured to completion rather than to enqueue.",
        "Concurrency 4 means a different thing to each entrant — see "
        "`concurrency_model` on every row.",
        "Each entrant runs with its own defaults. No tuning was applied to any of them.",
    ]
    if redis and redis.get("kind") == "remote":
        rtt = redis.get("rtt_ms", {})
        notes.append(
            f"Redis is remote ({redis.get('provider')}, {redis.get('region')}), "
            f"{rtt.get('avg')} ms average round trip. Every Redis-backed entrant pays "
            "that floor on every command, so the absolute numbers are a property of "
            "this link as much as of the engines. A colocated Redis moves all of them."
        )
        notes.append(
            "The SQLite rows are a different deployment, not a faster one: a local "
            "file against a network service. They are here because no-broker is what "
            "FlexiQ is for, not so the local number can stand in for the networked one."
        )
    if redis and redis.get("maxmemory_policy") not in (None, "unknown", "noeviction"):
        notes.append(
            f"The Redis server's eviction policy is `{redis['maxmemory_policy']}`, not "
            "`noeviction`. Under memory pressure it may drop queued jobs — that affects "
            "every Redis-backed entrant equally, but it is a durability caveat on the "
            "run rather than a performance one."
        )
    load = machine.get("load_avg_at_start") or [0]
    if load[0] > 1.0:
        notes.append(
            f"The host was not idle at the start of the run (1-minute load average "
            f"{load[0]}). Treat the absolute figures as a floor."
        )
    return notes


def build(
    *,
    run_id: str,
    scenario: dict[str, Any],
    machine: dict[str, Any],
    redis: dict[str, Any] | None,
    runtimes: list[dict[str, Any]],
    cleanup: dict[str, Any] | None,
) -> dict[str, Any]:
    backends: dict[str, Any] = {"sqlite": SQLITE_BACKEND}
    if redis:
        backends["redis"] = redis
    return {
        "schema": SCHEMA_VERSION,
        "run_id": run_id,
        "generated_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": "bench/run.py",
        "scenario": scenario,
        "machine": machine,
        "backends": backends,
        "runtimes": runtimes,
        "cleanup": cleanup,
        "notes": notes_for(machine, redis),
    }


def write(path: Path, artifact: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(artifact, indent=2) + "\n")


def table(artifact: dict[str, Any]) -> str:
    """A markdown table of the run, losses included, in the artifact's own order."""
    header = (
        "| runtime | backend | enqueue/s | drain/s | p50 ms | p95 ms | p99 ms | "
        "idle CPU % | idle RSS MB |\n"
        "|---|---|---:|---:|---:|---:|---:|---:|---:|"
    )
    rows = []
    for runtime in artifact["runtimes"]:
        if "error" in runtime:
            rows.append(f"| {runtime['id']} | — | failed: {runtime['error'][:60]} |")
            continue
        latency = runtime["latency_ms"]
        rows.append(
            f"| {runtime['id']} | {runtime['backend']} "
            f"| {runtime['enqueue']['per_second']} | {runtime['drain']['per_second']} "
            f"| {latency['p50']} | {latency['p95']} | {latency['p99']} "
            f"| {runtime['idle']['cpu_pct']} | {runtime['idle']['rss_mb']} |"
        )
    return "\n".join([header, *rows])
