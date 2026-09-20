"""The results file, its schema, and the table everything else is rendered from.

One artifact is the source of truth for every published number: the docs chart,
the comparison table and the README all read this file or something generated
from it. A figure that is not in here is a figure nobody measured.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

#: Bumped when the shape below changes incompatibly. `scripts/sync-bench.mjs`
#: refuses a file whose schema it does not know rather than rendering a chart
#: out of fields that moved.
SCHEMA = 1

#: Every axis a system must report, or explicitly decline with a reason.
AXES = ("enqueue", "drain", "latency", "idle")


class InvalidResults(ValueError):
    """The results document is not something anything downstream may render."""


def validate(doc: dict[str, Any]) -> None:
    """Fail before writing, not after publishing.

    Everything checked here has a downstream consumer that would otherwise fail
    silently: a missing `machine.label` renders a chart with no machine on it,
    and a system whose `enqueue` is absent renders as a gap that reads like a
    zero.
    """
    if doc.get("schema") != SCHEMA:
        raise InvalidResults(f"schema must be {SCHEMA}, got {doc.get('schema')!r}")

    for key in ("scenario", "machine", "sink", "systems"):
        if key not in doc:
            raise InvalidResults(f"missing top-level key {key!r}")

    if not doc["machine"].get("label"):
        raise InvalidResults("machine.label is required — a number needs a machine")

    systems = doc["systems"]
    if not systems:
        raise InvalidResults("no systems measured")

    for name, system in systems.items():
        for key in ("label", "language", "backend", "configuration", "concurrency_model"):
            if not system.get(key):
                raise InvalidResults(f"{name}: missing {key!r}")
        # Present but possibly empty: a system whose run failed never got as far
        # as reporting its versions, and that row still has to be writable.
        if not isinstance(system.get("versions"), dict):
            raise InvalidResults(f"{name}: versions must be an object")
        for axis in AXES:
            if axis not in system:
                raise InvalidResults(f"{name}: missing axis {axis!r}")
            if system[axis] is None and not system.get(f"{axis}_declined"):
                raise InvalidResults(
                    f"{name}: {axis} is null with no {axis}_declined reason — "
                    "an unmeasured axis has to say why"
                )
        enqueue = system.get("enqueue") or {}
        if "batch" in enqueue and enqueue["batch"] is None and not enqueue.get("batch_declined"):
            raise InvalidResults(f"{name}: null batch figure with no reason")


def write(doc: dict[str, Any], path: Path) -> None:
    validate(doc)
    path.parent.mkdir(parents=True, exist_ok=True)
    # `ensure_ascii=False`: a label reads as "FlexiQ · SQLite" in the file
    # rather than as an escape sequence. The file is UTF-8 either way; this
    # is about whether a human opening it can read the row names.
    path.write_text(
        json.dumps(doc, indent=2, sort_keys=False, ensure_ascii=False) + "\n"
    )


def read(path: Path) -> dict[str, Any]:
    doc = json.loads(path.read_text())
    validate(doc)
    return doc


def _cell(value: object, unit: str = "") -> str:
    """One table cell. `None` is an em dash — never a zero, never a blank."""
    if value is None:
        return "—"
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return str(value)
    # A decimal below a thousand, none above: three decimals on a 3,430 ms
    # percentile is precision this harness does not have.
    return f"{value:,.0f}{unit}" if abs(value) >= 1000 else f"{value:,.1f}{unit}"


def render_table(doc: dict[str, Any]) -> str:
    """The comparison table, markdown, generated — never typed by hand.

    Column order puts the axes the issue asks to see separately side by side,
    so that a system winning throughput and losing p99 cannot be quoted as
    winning without the loss coming along in the same row.
    """
    header = (
        "| System | Config | Enqueue (jobs/s) | Completion (jobs/s) | p50 | p99 | p99 burst | Idle RSS | Idle CPU |\n"
        "|---|---|---:|---:|---:|---:|---:|---:|---:|\n"
    )
    rows = []
    for system in doc["systems"].values():
        enqueue = system.get("enqueue") or {}
        per_job = (enqueue.get("per_job") or {}).get("per_second")
        drain = system.get("drain") or {}
        paced = system.get("latency") or {}
        latency = paced.get("latency_ms") or {}
        idle = system.get("idle") or {}
        rows.append(
            "| {label} | {config} | {enq} | {done} | {p50} | {p99} | {queued} | {rss} | {cpu} |".format(
                label=system["label"],
                config="defaults" if system["configuration"] == "defaults" else "tuned",
                enq=_cell(per_job),
                done=_cell(drain.get("completion_per_second")),
                p50=_cell(latency.get("p50"), " ms"),
                p99=_cell(latency.get("p99"), " ms"),
                queued=_cell((drain.get("latency_ms") or {}).get("p99"), " ms"),
                rss=_cell(idle.get("rss_mb_mean"), " MB"),
                cpu=_cell(idle.get("cpu_percent"), "%"),
            )
        )
    return header + "\n".join(rows) + "\n"


def render_summary(doc: dict[str, Any]) -> str:
    """What `run.py` prints when it finishes."""
    machine = doc["machine"]
    scenario = doc["scenario"]
    head = (
        f"\n{scenario['name']}: {scenario['jobs']:,} jobs · "
        f"{scenario['payload_bytes']} B payload · concurrency {scenario['concurrency']}\n"
        f"on {machine['label']} ({machine['cpu_count']} cores, {machine['cpu_model']})\n\n"
    )
    return head + render_table(doc)
