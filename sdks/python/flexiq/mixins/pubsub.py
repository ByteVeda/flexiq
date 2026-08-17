"""Topic pub/sub: fan-out subscribers (one job each) or log subscribers (pull)."""

from __future__ import annotations

import json
import math
import threading
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from flexiq.enums import coerce_enum
from flexiq.mixins._log_consumer import ConsumerErrorAction, LogConsumerThread
from flexiq.notes import validate_and_encode_notes
from flexiq.result import JobResult

if TYPE_CHECKING:
    from flexiq._flexiq import PyQueue
    from flexiq.serializers import Serializer
    from flexiq.task import TaskWrapper


@dataclass(frozen=True)
class TopicMessage:
    """One message pulled from a log topic.

    ``id`` is the cursor token to pass to :meth:`ack_topic`. ``args``/``kwargs``
    are the decoded publish payload; ``metadata``/``notes`` are the caller's.
    """

    id: str
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    metadata: dict[str, Any] | None
    notes: dict[str, Any] | None
    created_at: int


class QueuePubSubMixin:
    """Publish/subscribe over topics.

    A subscriber is a normal task plus a registry row mapping
    ``(topic, subscription_name) -> (task_name, queue)``. ``publish()`` fans a
    message out as one ordinary job per active subscription, so every task
    feature — retries, DLQ, middleware, rate limits — applies per subscriber,
    and a failing subscriber never affects its siblings.

    Deliveries use the queue-level serializer (there is one payload for all
    subscribers); per-task serializer overrides don't apply to topic tasks.
    """

    _inner: PyQueue
    _serializer: Serializer
    _subscription_configs: list[dict[str, Any]]
    _log_consumer_configs: list[dict[str, Any]]

    def subscriber(
        self,
        topic: str,
        name: str | None = None,
        queue: str = "default",
        durable: bool = True,
        **task_kwargs: Any,
    ) -> Callable[[Callable[..., Any]], TaskWrapper]:
        """Decorator registering ``fn`` as an independent subscriber of ``topic``.

        The function becomes a normal task (``task_kwargs`` are forwarded to
        :meth:`task`), and the subscription is written to storage when
        ``run_worker()`` starts — or via :meth:`declare_subscriptions` in a
        producer-only process. ``durable=False`` ties the subscription to one
        worker process: it only registers inside ``run_worker()`` and is
        reaped automatically once its worker stops heartbeating.

        Args:
            topic: Topic to subscribe to.
            name: Stable subscription identity. Defaults to the task name;
                re-registering the same ``(topic, name)`` updates the routing
                target instead of duplicating.
            queue: Queue the subscriber's delivery jobs go to.
            durable: Persist across restarts (default). ``False`` = ephemeral.
            **task_kwargs: Any :meth:`task` option (``max_retries``,
                ``timeout``, ``middleware``, ``idempotent``, ...).
        """

        if "codecs" in task_kwargs:
            raise ValueError(
                "subscriber() does not accept per-task codecs: publish() encodes one "
                "shared payload with the queue serializer, so a per-subscriber codec "
                "chain could never be applied on the producer side"
            )

        def decorator(fn: Callable[..., Any]) -> TaskWrapper:
            wrapper: TaskWrapper = self.task(queue=queue, **task_kwargs)(fn)  # type: ignore[attr-defined]
            config = {
                "topic": topic,
                "subscription_name": name or wrapper.name,
                "task_name": wrapper.name,
                "queue": queue,
                "durable": durable,
            }
            # Replace, don't append: re-decorating the same (topic, name) (module
            # reloads, test fixtures) must not accumulate stale declarations.
            identity = (config["topic"], config["subscription_name"])
            self._subscription_configs[:] = [
                c
                for c in self._subscription_configs
                if (c["topic"], c["subscription_name"]) != identity
            ]
            self._subscription_configs.append(config)
            return wrapper

        return decorator

    def subscribe_log(self, topic: str, name: str) -> None:
        """Register a durable **log** subscription (a named cursor over ``topic``).

        Unlike :meth:`subscriber`, a log subscription has no handler: the topic's
        publishes are stored once each and this consumer pulls them with
        :meth:`read_topic`, advancing its cursor with :meth:`ack_topic`. Writes
        immediately to storage, so a producer must have registered it (or called
        this) before the publishes it wants to see.
        """
        self._inner.register_subscription(
            topic=topic,
            subscription_name=name,
            task_name="",
            queue="default",
            durable=True,
            owner_worker_id=None,
            mode="log",
        )

    def declare_topic(self, name: str, *, retention: float | None = None) -> None:
        """Declare a **log** topic so its publishes are retained even with no
        subscriber (removing the late-join boundary).

        Without a declaration, a log message is stored only when a log
        subscription already exists at publish time. Declaring the topic makes
        every publish durable; a consumer that subscribes later still sees them.

        ``retention`` (seconds) bounds a sub-less backlog: each stored message
        expires that long after it was published, so the retention sweep can
        reclaim it. ``None`` keeps messages until a subscriber consumes them.
        Idempotent — re-declaring updates ``retention`` and keeps the topic.
        """
        retention_ms = None if retention is None else int(retention * 1000)
        self._inner.declare_topic(name, "log", retention_ms)

    def list_declared_topics(self) -> list[dict[str, Any]]:
        """List declared topics. Each entry: ``name``, ``mode``, ``retention_ms``
        (``None`` if unbounded), ``created_at`` (Unix ms)."""
        return [
            {"name": row[0], "mode": row[1], "retention_ms": row[2], "created_at": row[3]}
            for row in self._inner.list_declared_topics()
        ]

    def log_consumer(
        self,
        topic: str,
        name: str | None = None,
        *,
        poll_interval: float = 1.0,
        batch_size: int = 100,
        on_error: ConsumerErrorAction | str = ConsumerErrorAction.RETRY,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Decorator: register ``fn`` as a managed consumer of log ``topic``.

        Registers a durable log subscription (like :meth:`subscribe_log`) and,
        when :meth:`run_worker` starts, spawns a daemon thread that pulls
        messages, invokes ``fn(*args, **kwargs)`` per message, and advances the
        cursor — the read/ack loop callers otherwise hand-write. The handler may
        be sync or async; a producer-only process that never runs a worker still
        registers the subscription so its publishes are retained.

        Run a given ``(topic, name)`` consumer in **one** worker process — the
        cursor is a single high-water mark, so parallel workers would double-read.
        Use distinct ``name`` values for independent consumers.

        Args:
            topic: Log topic to consume.
            name: Stable subscription identity. Defaults to the handler name.
            poll_interval: Seconds to wait after an empty poll before re-reading.
            batch_size: Max messages pulled per poll.
            on_error: :class:`ConsumerErrorAction` (or its string) —
                ``"retry"`` (default) leaves the failed message un-acked so the
                batch re-reads; ``"skip"`` acks past it and continues.
        """
        on_error_action = coerce_enum(ConsumerErrorAction, on_error, param="on_error")
        # A non-finite/non-positive interval spins on empty polls; a non-positive
        # batch size never makes progress.
        if not (poll_interval > 0 and math.isfinite(poll_interval)):
            raise ValueError(
                f"poll_interval must be a finite positive number, got {poll_interval!r}"
            )
        if batch_size <= 0:
            raise ValueError(f"batch_size must be a positive integer, got {batch_size!r}")

        def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
            sub_name = name or fn.__name__
            self.subscribe_log(topic, sub_name)
            config = {
                "topic": topic,
                "name": sub_name,
                "handler": fn,
                "poll_interval": poll_interval,
                "batch_size": batch_size,
                "on_error": on_error_action,
            }
            # Replace, don't append: re-decorating the same (topic, name) (module
            # reloads, test fixtures) must not spawn duplicate consumer threads.
            identity = (topic, sub_name)
            self._log_consumer_configs[:] = [
                c for c in self._log_consumer_configs if (c["topic"], c["name"]) != identity
            ]
            self._log_consumer_configs.append(config)
            return fn

        return decorator

    def _build_log_consumer_threads(self, stop_event: threading.Event) -> list[LogConsumerThread]:
        """One :class:`LogConsumerThread` per registered consumer, sharing a
        single stop event so ``run_worker`` can drain them all at once."""
        return [
            LogConsumerThread(self, config, stop_event) for config in self._log_consumer_configs
        ]

    def read_topic(self, topic: str, name: str, limit: int = 100) -> list[TopicMessage]:
        """Pull up to ``limit`` messages after a log subscription's cursor.

        Oldest first, exclusive of the cursor. Decodes each payload with the
        queue serializer. Returns an empty list when the consumer is caught up.
        Delivery is at-least-once: process, then :meth:`ack_topic` the last id.
        """
        return self._decode_topic_messages(self._inner.read_topic_messages(topic, name, limit))

    def _decode_topic_messages(
        self, rows: list[tuple[str, bytes, str | None, str | None, int]]
    ) -> list[TopicMessage]:
        """Decode native ``(id, payload, metadata, notes, created_at)`` rows into
        :class:`TopicMessage`, unpacking the payload with the queue serializer."""
        messages = []
        for msg_id, payload, metadata, notes, created_at in rows:
            args, kwargs = self._serializer.loads(payload)
            messages.append(
                TopicMessage(
                    id=msg_id,
                    args=args,
                    kwargs=kwargs,
                    metadata=json.loads(metadata) if metadata is not None else None,
                    notes=json.loads(notes) if notes is not None else None,
                    created_at=created_at,
                )
            )
        return messages

    def ack_topic(self, topic: str, name: str, cursor: str) -> bool:
        """Advance a log subscription's cursor to ``cursor`` (a message id).

        A high-water mark: acking an id acks everything up to and including it.
        Monotonic — acking an older id is a no-op. Returns False if nothing moved.
        """
        return self._inner.ack_topic_cursor(topic, name, cursor)

    def lease_topic(
        self, topic: str, name: str, limit: int = 100, visibility: float = 30.0
    ) -> list[TopicMessage]:
        """Lease up to ``limit`` messages for **per-message** consumption.

        Unlike :meth:`read_topic`'s cursor, each returned message is leased for
        ``visibility`` seconds and tracked individually: :meth:`ack_message` it
        when done, or :meth:`nack_message` to redeliver it now. A lease that
        expires un-acked is redelivered, so one poison message no longer blocks
        its siblings. Oldest first; in-flight (leased, un-expired) messages are
        skipped.
        """
        return self._decode_topic_messages(
            self._inner.lease_topic_messages(topic, name, limit, int(visibility * 1000))
        )

    def ack_message(self, topic: str, name: str, message_id: str) -> bool:
        """Ack one leased message — the delivery is done and never redelivered.
        Returns False when there was no un-acked delivery to ack."""
        return self._inner.ack_message(topic, name, message_id)

    def nack_message(self, topic: str, name: str, message_id: str) -> bool:
        """Nack one leased message — make it available for redelivery now (vs
        waiting out the visibility timeout). Returns False when there was no
        un-acked delivery to nack."""
        return self._inner.nack_message(topic, name, message_id)

    def topic_log_stats(self) -> list[dict[str, Any]]:
        """Lag snapshot per log subscription.

        Each entry: ``topic``, ``subscription``, ``cursor`` (``None`` if unread),
        ``lag`` (un-acked messages), ``oldest_unacked_age_ms`` (``None`` if
        caught up).
        """
        return [
            {
                "topic": row[0],
                "subscription": row[1],
                "cursor": row[2],
                "lag": row[3],
                "oldest_unacked_age_ms": row[4],
            }
            for row in self._inner.topic_log_stats()
        ]

    def declare_subscriptions(self) -> None:
        """Write pending durable subscriptions to storage.

        Called automatically at ``run_worker()`` startup. Call it explicitly
        in a producer-only process (one that imports subscriber modules but
        never runs a worker) so ``publish()`` sees the subscriptions.
        Ephemeral subscriptions are skipped — they need an owning worker.
        """
        for config in self._subscription_configs:
            if config["durable"]:
                self._register_subscription(config, owner_worker_id=None)

    def publish(
        self,
        topic: str,
        *args: Any,
        idempotency_key: str | None = None,
        metadata: dict[str, Any] | None = None,
        notes: dict[str, Any] | None = None,
        priority: int | None = None,
        delay_seconds: float | None = None,
        max_retries: int | None = None,
        timeout: int | None = None,
        expires: float | None = None,
        result_ttl: int | None = None,
        **kwargs: Any,
    ) -> list[JobResult]:
        """Publish a message to ``topic``.

        Every active subscription receives an independent job carrying the
        same ``(args, kwargs)`` payload (at-least-once per subscriber).
        Returns one :class:`JobResult` per delivery — empty when the topic
        has no active subscribers, which is a valid pub/sub no-op.

        ``idempotency_key`` dedupes per subscriber: republishing the same key
        yields no new deliveries, and a subscription added later still gets
        its own copy. Each delivery's ``notes`` carry ``topic`` and
        ``subscription`` for filtering.
        """
        payload = self._serializer.dumps((args, kwargs))
        self._check_payload_size(topic, len(payload))  # type: ignore[attr-defined]
        py_jobs = self._inner.publish(
            topic=topic,
            payload=payload,
            idempotency_key=idempotency_key,
            metadata=json.dumps(metadata) if metadata is not None else None,
            notes=validate_and_encode_notes(notes) if notes is not None else None,
            priority=priority,
            delay_seconds=delay_seconds,
            max_retries=max_retries,
            timeout=timeout,
            expires=expires,
            result_ttl=result_ttl,
        )
        return [JobResult(py_job=py_job, queue=self) for py_job in py_jobs]  # type: ignore[arg-type]

    def unsubscribe(self, topic: str, name: str) -> bool:
        """Remove a subscription. Returns False if none matched.

        Also drops any matching local declaration so a later worker start
        doesn't re-register what was just removed.
        """
        self._subscription_configs[:] = [
            c
            for c in self._subscription_configs
            if (c["topic"], c["subscription_name"]) != (topic, name)
        ]
        return self._inner.unsubscribe(topic, name)

    def pause_subscription(self, topic: str, name: str) -> bool:
        """Stop deliveries without unregistering. Returns False if unknown."""
        return self._inner.set_subscription_active(topic, name, False)

    def resume_subscription(self, topic: str, name: str) -> bool:
        """Resume a paused subscription. Returns False if unknown."""
        return self._inner.set_subscription_active(topic, name, True)

    def list_subscriptions(self, topic: str | None = None) -> list[dict[str, Any]]:
        """List subscriptions — all of them, or one topic's active ones."""
        rows = self._inner.list_subscriptions(topic)
        return [
            {
                "topic": row[0],
                "name": row[1],
                "task_name": row[2],
                "queue": row[3],
                "active": row[4],
                "durable": row[5],
            }
            for row in rows
        ]

    def list_topics(self) -> list[str]:
        """Distinct topics that currently have at least one subscription."""
        seen: dict[str, None] = {}
        for row in self._inner.list_subscriptions(None):
            seen.setdefault(row[0], None)
        return list(seen)

    def topic_stats(self, topic: str | None = None) -> list[dict[str, Any]]:
        """Backlog/lag snapshot per subscription, optionally filtered to a topic.

        Each entry: ``topic``, ``subscription``, ``task_name``, ``queue``,
        ``active``, ``durable``, ``pending``, ``running``, ``dead``, and
        ``oldest_pending_age_ms`` (``None`` when the subscription has no pending
        backlog). Computed live off indexed columns — safe to poll.
        """
        stats = [
            {
                "topic": row[0],
                "subscription": row[1],
                "task_name": row[2],
                "queue": row[3],
                "active": row[4],
                "durable": row[5],
                "pending": row[6],
                "running": row[7],
                "dead": row[8],
                "oldest_pending_age_ms": row[9],
            }
            for row in self._inner.topic_backlog_stats()
        ]
        return stats if topic is None else [s for s in stats if s["topic"] == topic]

    def _delivery_task_defaults(self) -> dict[str, tuple[int, int, int]]:
        """Per-task ``(priority, max_retries, timeout_ms)`` from this process's
        task registry. Persisted on the subscription row at registration so a
        producer-only process (which never loads the task) still applies each
        subscriber's own settings. Tasks not registered here get queue defaults."""
        return {
            config.name: (config.priority, config.max_retries, config.timeout * 1000)
            for config in self._task_configs  # type: ignore[attr-defined]
        }

    def _register_subscription(self, config: dict[str, Any], owner_worker_id: str | None) -> None:
        priority, max_retries, timeout_ms = self._delivery_task_defaults().get(
            config["task_name"], (None, None, None)
        )
        self._inner.register_subscription(
            topic=config["topic"],
            subscription_name=config["subscription_name"],
            task_name=config["task_name"],
            queue=config["queue"],
            durable=config["durable"],
            owner_worker_id=owner_worker_id,
            priority=priority,
            max_retries=max_retries,
            timeout_ms=timeout_ms,
        )

    def _declare_worker_subscriptions(self, worker_id: str) -> None:
        """Flush all subscriptions at worker startup, owning the ephemeral ones."""
        for config in self._subscription_configs:
            owner = None if config["durable"] else worker_id
            self._register_subscription(config, owner_worker_id=owner)
