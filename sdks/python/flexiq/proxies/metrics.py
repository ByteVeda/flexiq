"""Proxy reconstruction performance metrics."""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Any


@dataclass
class HandlerMetrics:
    """Per-handler reconstruction metrics."""

    name: str
    total_reconstructions: int = 0
    total_errors: int = 0
    total_cleanup_errors: int = 0
    total_checksum_failures: int = 0
    total_duration_ms: float = 0.0
    max_duration_ms: float = 0.0
    _durations: list[float] = field(default_factory=list)

    @property
    def avg_duration_ms(self) -> float:
        if not self._durations:
            return 0.0
        return sum(self._durations) / len(self._durations)

    @property
    def p95_duration_ms(self) -> float:
        if not self._durations:
            return 0.0
        sorted_d = sorted(self._durations)
        idx = int(len(sorted_d) * 0.95)
        return sorted_d[min(idx, len(sorted_d) - 1)]


class ProxyMetrics:
    """Thread-safe accumulator for proxy reconstruction metrics."""

    def __init__(self) -> None:
        self._handlers: dict[str, HandlerMetrics] = {}
        self._lock = threading.Lock()

    def _get_handler(self, name: str) -> HandlerMetrics:
        if name not in self._handlers:
            self._handlers[name] = HandlerMetrics(name=name)
        return self._handlers[name]

    def record_reconstruction(self, handler_name: str, duration_ms: float) -> None:
        with self._lock:
            h = self._get_handler(handler_name)
            h.total_reconstructions += 1
            h.total_duration_ms += duration_ms
            h.max_duration_ms = max(h.max_duration_ms, duration_ms)
            h._durations.append(duration_ms)

    def record_error(self, handler_name: str) -> None:
        with self._lock:
            self._get_handler(handler_name).total_errors += 1

    def record_cleanup_error(self, handler_name: str) -> None:
        with self._lock:
            self._get_handler(handler_name).total_cleanup_errors += 1

    def record_checksum_failure(self, handler_name: str) -> None:
        with self._lock:
            self._get_handler(handler_name).total_checksum_failures += 1

    def to_list(self) -> list[dict[str, Any]]:
        """Return metrics as a list of dicts."""
        with self._lock:
            return [
                {
                    "handler": h.name,
                    "total_reconstructions": h.total_reconstructions,
                    "total_errors": h.total_errors,
                    "total_cleanup_errors": h.total_cleanup_errors,
                    "total_checksum_failures": h.total_checksum_failures,
                    "total_duration_ms": round(h.total_duration_ms, 2),
                    "avg_duration_ms": round(h.avg_duration_ms, 2),
                    "max_duration_ms": round(h.max_duration_ms, 2),
                    "p95_duration_ms": round(h.p95_duration_ms, 2),
                }
                for h in self._handlers.values()
            ]
