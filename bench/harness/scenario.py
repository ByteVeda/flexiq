"""The scenario, loaded once and handed to every runner unchanged."""

from __future__ import annotations

import tomllib
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass(frozen=True)
class Scenario:
    """One measurable workload, identical for every system under test."""

    name: str
    jobs: int
    payload_bytes: int
    concurrency: int
    warmup_jobs: int
    latency_jobs: int
    latency_rate_per_second: int
    idle_sample_seconds: int
    worker_settle_seconds: int
    drain_timeout_seconds: int

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


def load(path: Path) -> Scenario:
    """Read a scenario file, rejecting anything the runners cannot honour.

    Validation is not politeness here. A scenario with `concurrency = 0` starts
    five workers that drain nothing and reports a timeout as if it were a
    property of the queues, so every field is checked before a single process
    is spawned.
    """
    with path.open("rb") as handle:
        raw = tomllib.load(handle)

    try:
        table = raw["scenario"]
    except KeyError:
        raise ValueError(f"{path}: no [scenario] table") from None

    fields = {f for f in Scenario.__dataclass_fields__}
    missing = fields - table.keys()
    if missing:
        raise ValueError(f"{path}: missing {', '.join(sorted(missing))}")
    unknown = table.keys() - fields
    if unknown:
        raise ValueError(f"{path}: unknown key(s) {', '.join(sorted(unknown))}")

    scenario = Scenario(**table)
    for field in fields - {"name"}:
        value = getattr(scenario, field)
        if not isinstance(value, int) or value < 1:
            raise ValueError(f"{path}: {field} must be a positive integer, got {value!r}")
    if not scenario.name:
        raise ValueError(f"{path}: name must not be empty")
    return scenario


def payload(size: int) -> str:
    """The job body's filler: `size` deterministic ASCII characters.

    A string rather than bytes because Celery's default serialiser is JSON and
    would need a base64 detour that none of the others pay — the point is that
    every system moves the same `size` characters, not that we prove one
    serialiser awkward. Deterministic, because a random blob per job hands each
    compressing transport a different problem; not a repeated character,
    because that hands them all a free win.
    """
    if size < 1:
        raise ValueError("payload_bytes must be positive")
    tile = "".join(chr(33 + (i % 94)) for i in range(94))
    return (tile * (size // len(tile) + 1))[:size]
