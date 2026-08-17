"""Lightweight counters for predicate outcomes."""

from __future__ import annotations

import threading
from dataclasses import dataclass, field


@dataclass
class PredicateMetrics:
    """Per-queue counters for predicate outcomes.

    All counters are atomic under a single lock — predicate evaluation is
    not on the hot path of dispatch, so contention is negligible.
    """

    _allowed: int = 0
    _denied: int = 0
    _deferred: int = 0
    _cancelled: int = 0
    _errors: int = 0
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def record_allowed(self) -> None:
        with self._lock:
            self._allowed += 1

    def record_denied(self) -> None:
        with self._lock:
            self._denied += 1

    def record_deferred(self) -> None:
        with self._lock:
            self._deferred += 1

    def record_cancelled(self) -> None:
        with self._lock:
            self._cancelled += 1

    def record_error(self) -> None:
        with self._lock:
            self._errors += 1

    def snapshot(self) -> dict[str, int]:
        with self._lock:
            return {
                "allowed": self._allowed,
                "denied": self._denied,
                "deferred": self._deferred,
                "cancelled": self._cancelled,
                "errors": self._errors,
            }

    def reset(self) -> None:
        with self._lock:
            self._allowed = 0
            self._denied = 0
            self._deferred = 0
            self._cancelled = 0
            self._errors = 0
