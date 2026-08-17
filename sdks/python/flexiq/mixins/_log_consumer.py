"""Managed log-topic consumer: a daemon thread that pulls, invokes, and acks.

One thread per registered ``log_consumer``. It polls the log subscription's
cursor with the same ``read_topic``/``ack_topic`` API a caller would use by
hand, so the delivery guarantee is identical (at-least-once, cursor-based). The
thread is owned by ``run_worker`` and stopped when the worker drains.
"""

from __future__ import annotations

import enum
import logging
import threading
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from flexiq.async_support.helpers import run_maybe_async

if TYPE_CHECKING:
    from flexiq.mixins.pubsub import QueuePubSubMixin, TopicMessage


logger = logging.getLogger("flexiq")


class ConsumerErrorAction(str, enum.Enum):
    """What a managed log consumer does when its handler raises."""

    RETRY = "retry"
    """Leave the message un-acked so the next poll re-reads the batch."""

    SKIP = "skip"
    """Ack past the failed message and continue."""


class LogConsumerThread(threading.Thread):
    """Poll one log subscription and drive its handler until asked to stop.

    Loop: read a batch after the cursor, invoke the handler per message, then
    advance the cursor to the last successfully handled message. A non-empty
    batch loops immediately to drain a backlog; an empty one waits
    ``poll_interval`` seconds (interruptibly) before polling again.
    """

    def __init__(
        self,
        queue: QueuePubSubMixin,
        config: dict[str, Any],
        stop_event: threading.Event,
    ) -> None:
        super().__init__(daemon=True, name=f"flexiq-log-consumer-{config['name']}")
        self._queue = queue
        self._topic: str = config["topic"]
        self._name: str = config["name"]
        self._handler: Callable[..., Any] = config["handler"]
        self._poll_interval: float = config["poll_interval"]
        self._batch_size: int = config["batch_size"]
        self._on_error: ConsumerErrorAction = config["on_error"]
        self._stop_event = stop_event

    def run(self) -> None:
        while not self._stop_event.is_set():
            try:
                messages = self._queue.read_topic(self._topic, self._name, self._batch_size)
            except Exception:
                logger.exception("log_consumer %s/%s: read failed", self._topic, self._name)
                self._stop_event.wait(self._poll_interval)
                continue

            if not messages:
                self._stop_event.wait(self._poll_interval)
                continue

            last_acked, retry_failure = self._drain_batch(messages)
            if last_acked is not None:
                try:
                    self._queue.ack_topic(self._topic, self._name, last_acked)
                except Exception:
                    logger.exception("log_consumer %s/%s: ack failed", self._topic, self._name)
            # A retry-mode failure made no progress past the poison message, so
            # wait one interval before re-reading rather than hot-looping on it.
            # Otherwise keep draining the backlog immediately.
            if retry_failure:
                self._stop_event.wait(self._poll_interval)

    def _drain_batch(self, messages: list[TopicMessage]) -> tuple[str | None, bool]:
        """Invoke the handler per message; return ``(ack_id, retry_failure)``.

        ``retry`` stops at the first failure and acks only the run of successes
        before it (``retry_failure=True``), so the failed message re-reads next
        poll. ``skip`` acks past a failure too, moving the cursor forward.
        ``ack_id`` is ``None`` when nothing should be acked yet.
        """
        last_acked: str | None = None
        for message in messages:
            if self._stop_event.is_set():
                break
            try:
                run_maybe_async(self._handler(*message.args, **message.kwargs))
            except Exception:
                logger.exception(
                    "log_consumer %s/%s: handler failed on message %s",
                    self._topic,
                    self._name,
                    message.id,
                )
                if self._on_error is ConsumerErrorAction.RETRY:
                    return last_acked, True
            last_acked = message.id
        return last_acked, False
