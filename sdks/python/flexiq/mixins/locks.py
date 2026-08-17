"""Distributed locking methods for the Queue."""

from __future__ import annotations

from typing import Any

from flexiq.locks import DistributedLock


class QueueLockMixin:
    """Distributed locking methods for the Queue."""

    _inner: Any

    def lock(
        self,
        name: str,
        ttl: float = 30.0,
        auto_extend: bool = True,
        owner_id: str | None = None,
        timeout: float | None = None,
        retry_interval: float = 0.1,
    ) -> DistributedLock:
        """Return a sync distributed lock context manager.

        Args:
            name: Lock name (unique across the cluster).
            ttl: Lock TTL in seconds. Auto-extended at ttl/3 intervals.
            auto_extend: Whether to auto-extend the lock in a background thread.
            owner_id: Unique owner identifier. Auto-generated if not provided.
            timeout: Max seconds to wait for acquisition. None = fail immediately.
            retry_interval: Seconds between retries when timeout is set.
        """
        return DistributedLock(
            inner=self._inner,
            name=name,
            ttl=ttl,
            owner_id=owner_id,
            auto_extend=auto_extend,
            timeout=timeout,
            retry_interval=retry_interval,
        )
