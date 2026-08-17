"""Async distributed lock for flexiq."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from flexiq.locks import DistributedLock, LockNotAcquiredError

if TYPE_CHECKING:
    from types import TracebackType

    from flexiq._flexiq import PyQueue


class AsyncDistributedLock:
    """Async context manager variant of DistributedLock.

    Usage::

        async with queue.alock("my-resource", ttl=30):
            # critical section
            ...
    """

    def __init__(
        self,
        inner: PyQueue,
        name: str,
        ttl: float = 30.0,
        owner_id: str | None = None,
        auto_extend: bool = True,
        timeout: float | None = None,
        retry_interval: float = 0.1,
    ) -> None:
        self._lock = DistributedLock(
            inner=inner,
            name=name,
            ttl=ttl,
            owner_id=owner_id,
            auto_extend=auto_extend,
            timeout=timeout,
            retry_interval=retry_interval,
        )

    @property
    def name(self) -> str:
        """The lock name."""
        return self._lock.name

    @property
    def owner_id(self) -> str:
        """The unique owner identifier."""
        return self._lock.owner_id

    async def acquire(self) -> bool:
        """Try to acquire the lock."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._lock.acquire)

    async def release(self) -> bool:
        """Release the lock."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._lock.release)

    async def extend(self, ttl: float | None = None) -> bool:
        """Extend the lock's TTL."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._lock.extend, ttl)

    async def __aenter__(self) -> AsyncDistributedLock:
        if self._lock._timeout is not None:
            deadline = asyncio.get_event_loop().time() + self._lock._timeout
            while True:
                if await self.acquire():
                    self._lock._start_extend()
                    return self
                if asyncio.get_event_loop().time() >= deadline:
                    raise LockNotAcquiredError(
                        f"Could not acquire lock '{self.name}' within {self._lock._timeout}s"
                    )
                await asyncio.sleep(self._lock._retry_interval)
        else:
            if not await self.acquire():
                raise LockNotAcquiredError(f"Could not acquire lock '{self.name}'")
            self._lock._start_extend()
            return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self.release()
