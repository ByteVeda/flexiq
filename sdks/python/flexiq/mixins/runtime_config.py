"""Queue-level runtime configuration: rate limits, concurrency, type registration.

These are runtime knobs callers tune after queue construction — distinct
from the decorator-level ``@queue.task(rate_limit=...)`` (which configures
one task) and from the storage-backed dashboard settings managed by
:class:`~flexiq.mixins.settings.QueueSettingsMixin`.

Type registration lives here too because, conceptually, it is a runtime
adjustment to the argument-interception subsystem — not part of the task
decorator itself.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from flexiq.enums import coerce_enum
from flexiq.interception.strategy import Strategy

if TYPE_CHECKING:
    from flexiq.interception import ArgumentInterceptor


class QueueRuntimeConfigMixin:
    """Rate limits, concurrency caps, and custom type registration."""

    _queue_configs: dict[str, dict[str, Any]]
    _max_pending: dict[str, int]
    _interceptor: ArgumentInterceptor | None

    def register_type(
        self,
        python_type: type,
        strategy: Strategy | str,
        *,
        resource: str | None = None,
        message: str | None = None,
        converter: Callable | None = None,
        type_key: str | None = None,
        proxy_handler: str | None = None,
    ) -> None:
        """Register a custom type with the interception system.

        Args:
            python_type: The type to register.
            strategy: A :class:`~flexiq.interception.strategy.Strategy` or its
                string: ``"pass"``, ``"convert"``, ``"redirect"``, ``"reject"``,
                or ``"proxy"``.
            resource: Resource name for ``"redirect"`` strategy.
            message: Rejection reason for ``"reject"`` strategy.
            converter: Converter callable for ``"convert"`` strategy.
            type_key: Key for the converter reconstructor dispatch.
            proxy_handler: Handler name for ``"proxy"`` strategy.
        """
        if self._interceptor is None:
            raise RuntimeError(
                "Interception is disabled; set interception='strict' or "
                "'lenient' to use register_type()"
            )
        strat = coerce_enum(Strategy, strategy, param="strategy")
        self._interceptor._registry.register(
            python_type,
            strat,
            priority=15,
            redirect_resource=resource,
            reject_reason=message,
            converter=converter,
            type_key=type_key,
            proxy_handler=proxy_handler,
        )

    def set_queue_rate_limit(self, queue_name: str, rate_limit: str) -> None:
        """Set a rate limit for an entire queue.

        Args:
            queue_name: Queue name (e.g. ``"default"``).
            rate_limit: Rate limit string, e.g. ``"100/m"``, ``"10/s"``.
        """
        self._queue_configs.setdefault(queue_name, {})["rate_limit"] = rate_limit

    def set_queue_concurrency(self, queue_name: str, max_concurrent: int) -> None:
        """Set a maximum number of concurrent jobs for a queue.

        Args:
            queue_name: Queue name (e.g. ``"default"``).
            max_concurrent: Maximum number of jobs running simultaneously
                from this queue.
        """
        self._queue_configs.setdefault(queue_name, {})["max_concurrent"] = max_concurrent

    def set_queue_max_pending(self, queue_name: str, max_pending: int) -> None:
        """Set an opt-in admission cap on a queue's pending backlog.

        Once the queue holds ``max_pending`` pending jobs, ``enqueue`` and
        ``enqueue_many`` raise :class:`~flexiq.exceptions.QueueFullError`. The
        check is a non-atomic count-then-insert (brief overshoot is possible
        under concurrent producers). Enforced producer-side, so it applies even
        when no worker is running.

        Args:
            queue_name: Queue name (e.g. ``"default"``).
            max_pending: Maximum pending jobs allowed before enqueue is rejected.
                Must be non-negative; ``0`` admits nothing.
        """
        if max_pending < 0:
            raise ValueError("max_pending must be non-negative")
        self._max_pending[queue_name] = max_pending

    def set_queue_codel(self, queue_name: str, target_ms: int, interval_ms: int) -> None:
        """Enable opt-in CoDel load shedding on a queue.

        Under sustained overload — when a job's wait past its eligibility stays
        above ``target_ms`` for a full ``interval_ms`` — the dispatcher sheds the
        stalest jobs to the dead-letter queue (reason prefixed ``codel:``)
        instead of running them stale. A transient spike is never shed, and CoDel
        entries are excluded from DLQ auto-retry. Takes effect at ``run_worker``.

        Args:
            queue_name: Queue name (e.g. ``"default"``).
            target_ms: Acceptable steady-state wait (ms) past eligibility.
            interval_ms: Window the wait must stay above target before shedding.
        """
        if target_ms <= 0 or interval_ms <= 0:
            raise ValueError("target_ms and interval_ms must be positive")
        config = self._queue_configs.setdefault(queue_name, {})
        config["codel_target_ms"] = target_ms
        config["codel_interval_ms"] = interval_ms

    def set_queue_dispatch_order(self, queue_name: str, order: str) -> None:
        """Set a queue's dispatch order for same-priority jobs.

        ``"fifo"`` (default) runs oldest-first — the fair ordering a durable
        queue is expected to keep. ``"lifo"`` runs newest-first, a freshness
        lever for workloads that would rather run recent work than clear a stale
        backlog in order under overload. Priority always dominates; this only
        breaks ties. Takes effect at ``run_worker``.

        Note: honored on SQLite and PostgreSQL. The Redis backend is FIFO-only
        (its single-score index can't express per-priority LIFO without a
        second sorted set); ``"lifo"`` is accepted but ignored there.

        Args:
            queue_name: Queue name (e.g. ``"default"``).
            order: ``"fifo"`` or ``"lifo"``.
        """
        if order not in ("fifo", "lifo"):
            raise ValueError("order must be 'fifo' or 'lifo'")
        self._queue_configs.setdefault(queue_name, {})["dispatch_order"] = order
