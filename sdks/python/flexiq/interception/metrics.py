"""Interception performance metrics."""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Any

from flexiq.interception.strategy import Strategy


@dataclass
class InterceptionMetrics:
    """Thread-safe accumulator for interception statistics."""

    total_intercepts: int = 0
    total_duration_ms: float = 0.0
    strategy_counts: dict[str, int] = field(default_factory=lambda: {s.value: 0 for s in Strategy})
    max_depth_reached: int = 0
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def record(
        self,
        duration_ms: float,
        strategies: dict[str, int],
        max_depth: int,
    ) -> None:
        """Record metrics from a single interception pass."""
        with self._lock:
            self.total_intercepts += 1
            self.total_duration_ms += duration_ms
            for key, count in strategies.items():
                self.strategy_counts[key] = self.strategy_counts.get(key, 0) + count
            self.max_depth_reached = max(self.max_depth_reached, max_depth)

    def to_dict(self) -> dict[str, Any]:
        """Return metrics as a plain dict."""
        with self._lock:
            avg = self.total_duration_ms / self.total_intercepts if self.total_intercepts else 0
            return {
                "total_intercepts": self.total_intercepts,
                "total_duration_ms": round(self.total_duration_ms, 2),
                "avg_duration_ms": round(avg, 2),
                "strategy_counts": dict(self.strategy_counts),
                "max_depth_reached": self.max_depth_reached,
            }
