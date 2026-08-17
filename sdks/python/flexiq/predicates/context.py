"""Predicate evaluation context."""

from __future__ import annotations

import time
import weakref
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from flexiq.app import Queue


@dataclass
class PredicateContext:
    """Per-evaluation context passed to :meth:`Predicate.evaluate`.

    Carries job metadata, a weak reference to the owning :class:`Queue` so
    system-state recipes can read queue depth or pause status, and a
    per-evaluation memo dict so composed predicates that hit the same
    storage call only pay for it once.

    Instances are short-lived: one per predicate evaluation. Holding a
    reference past a single call is undefined.
    """

    task_name: str
    queue: str
    priority: int = 0
    retry_count: int = 0
    args: tuple[Any, ...] = ()
    kwargs: dict[str, Any] = field(default_factory=dict)
    job_id: str | None = None
    scheduled_at: datetime | None = None
    created_at: datetime | None = None
    payload_size: int = 0
    extras: dict[str, Any] = field(default_factory=dict)

    _queue_weakref: Any = None  # weakref.ref[Queue] | None
    _state_cache: dict[tuple[Any, ...], Any] = field(default_factory=dict)

    @classmethod
    def for_enqueue(
        cls,
        *,
        task_name: str,
        queue: str,
        priority: int | None,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        payload_size: int,
        delay_seconds: float | None,
        extras: dict[str, Any] | None,
        queue_ref: Queue | None,
    ) -> PredicateContext:
        """Build a context for an enqueue-time evaluation."""
        now = datetime.now(timezone.utc)
        scheduled = now if not delay_seconds else _add_seconds(now, delay_seconds)
        return cls(
            task_name=task_name,
            queue=queue,
            priority=priority or 0,
            retry_count=0,
            args=tuple(args),
            kwargs=dict(kwargs),
            job_id=None,
            scheduled_at=scheduled,
            created_at=now,
            payload_size=payload_size,
            extras=dict(extras or {}),
            _queue_weakref=weakref.ref(queue_ref) if queue_ref is not None else None,
        )

    @classmethod
    def for_dispatch(
        cls,
        *,
        task_name: str,
        queue: str,
        priority: int,
        retry_count: int,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        job_id: str,
        payload_size: int,
        extras: dict[str, Any] | None,
        queue_ref: Queue | None,
    ) -> PredicateContext:
        """Build a context for a worker-dispatch-time evaluation."""
        now = datetime.now(timezone.utc)
        return cls(
            task_name=task_name,
            queue=queue,
            priority=priority,
            retry_count=retry_count,
            args=tuple(args),
            kwargs=dict(kwargs),
            job_id=job_id,
            scheduled_at=now,
            created_at=now,
            payload_size=payload_size,
            extras=dict(extras or {}),
            _queue_weakref=weakref.ref(queue_ref) if queue_ref is not None else None,
        )

    def now(self) -> datetime:
        """Wall-clock time, timezone-aware (UTC)."""
        return datetime.now(timezone.utc)

    def monotonic(self) -> float:
        """Process-local monotonic clock for short timing windows."""
        return time.monotonic()

    def queue_size(self, queue_name: str | None = None) -> int:
        """Pending job count in ``queue_name`` (defaults to this job's queue).

        Memoised within a single predicate evaluation, so composed
        predicates sharing the same call do not re-hit storage.
        """
        name = queue_name or self.queue
        key = ("queue_size", name)
        if key in self._state_cache:
            return int(self._state_cache[key])
        queue = self._resolve_queue()
        if queue is None:
            self._state_cache[key] = 0
            return 0
        try:
            stats = queue._inner.stats_by_queue(name)
            value = int(stats.get("pending", 0))
        except Exception:
            value = 0
        self._state_cache[key] = value
        return value

    def queue_paused(self, queue_name: str | None = None) -> bool:
        """Whether ``queue_name`` is paused (defaults to this job's queue)."""
        name = queue_name or self.queue
        key = ("queue_paused", name)
        if key in self._state_cache:
            return bool(self._state_cache[key])
        queue = self._resolve_queue()
        if queue is None:
            self._state_cache[key] = False
            return False
        try:
            paused = name in queue._inner.list_paused_queues()
        except Exception:
            paused = False
        self._state_cache[key] = paused
        return paused

    def stats(self) -> dict[str, int]:
        """Backend-wide stats (memoised per evaluation)."""
        key: tuple[Any, ...] = ("stats",)
        if key in self._state_cache:
            return dict(self._state_cache[key])
        queue = self._resolve_queue()
        if queue is None:
            self._state_cache[key] = {}
            return {}
        try:
            value = dict(queue._inner.stats())
        except Exception:
            value = {}
        self._state_cache[key] = value
        return value

    def _resolve_queue(self) -> Queue | None:
        ref = self._queue_weakref
        if ref is None:
            return None
        return ref()  # type: ignore[no-any-return]


def _add_seconds(dt: datetime, seconds: float) -> datetime:
    from datetime import timedelta

    return dt + timedelta(seconds=seconds)
