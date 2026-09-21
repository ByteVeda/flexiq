"""The scenario — one definition, shared by every runtime.

The point of a single file here is that no adapter can quietly measure
something else: the pad length, the job count and the concurrency all leave
this module and reach the Node side through the environment rather than being
restated in JavaScript.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path

#: The payload every runtime carries. ``t`` is the producer's wall clock in
#: milliseconds at enqueue; the handler subtracts it from its own clock, which
#: is the only latency measurement five frameworks and two languages can agree
#: on. ``pad`` brings the encoded body up to ``payload_bytes``.
PAYLOAD_KEYS = ("i", "t", "pad")


def pad_length(payload_bytes: int) -> int:
    """Characters of padding that bring the encoded payload to ``payload_bytes``.

    Measured against a worst-case index and a full-width millisecond clock, so
    the body is the stated size for the whole run rather than only for job 0.
    Returns 0 when the envelope alone already exceeds the target.
    """
    envelope = json.dumps({"i": 999_999, "t": 1_774_083_600_000.123, "pad": ""})
    return max(0, payload_bytes - len(envelope))


@dataclass(frozen=True)
class Scenario:
    """N jobs, one fixed payload, one fixed concurrency, measured to completion."""

    jobs: int
    payload_bytes: int
    concurrency: int
    warmup_jobs: int
    idle_window_s: float
    drain_timeout_s: float

    @classmethod
    def load(cls, path: Path) -> Scenario:
        raw = json.loads(path.read_text())
        return cls(
            jobs=int(raw["jobs"]),
            payload_bytes=int(raw["payload_bytes"]),
            concurrency=int(raw["concurrency"]),
            warmup_jobs=int(raw["warmup_jobs"]),
            idle_window_s=float(raw["idle_window_s"]),
            drain_timeout_s=float(raw["drain_timeout_s"]),
        )

    def replace(self, **overrides: int | float | None) -> Scenario:
        """A copy with the non-``None`` overrides applied."""
        given = {k: v for k, v in overrides.items() if v is not None}
        return Scenario(**{**asdict(self), **given})  # type: ignore[arg-type]

    @property
    def pad(self) -> str:
        return "x" * pad_length(self.payload_bytes)

    @property
    def total_jobs(self) -> int:
        """Warmup jobs run through the same path; only the measured ones count."""
        return self.warmup_jobs + self.jobs

    def to_dict(self) -> dict[str, int | float | str]:
        return {**asdict(self), "task": "decode payload, record latency, return"}
