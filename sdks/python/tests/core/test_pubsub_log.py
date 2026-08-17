"""Log topics (S28): one stored message per publish, pulled via a cursor."""

import threading
import time
from typing import Any

from flexiq import Queue, TopicMessage

PollUntil = Any  # the conftest fixture's runtime type


class TestLogPublish:
    def test_publish_stores_one_message_per_call(self, queue: Queue) -> None:
        queue.subscribe_log("events", "analytics")

        # No fan-out jobs; one stored message per publish regardless of readers.
        assert queue.publish("events", 1, kind="a") == []
        assert queue.publish("events", 2, kind="b") == []

        msgs = queue.read_topic("events", "analytics")
        assert [(m.args, m.kwargs) for m in msgs] == [((1,), {"kind": "a"}), ((2,), {"kind": "b"})]
        assert all(isinstance(m, TopicMessage) for m in msgs)

    def test_read_is_empty_before_any_publish(self, queue: Queue) -> None:
        queue.subscribe_log("events", "analytics")
        assert queue.read_topic("events", "analytics") == []

    def test_late_subscriber_misses_earlier_publishes(self, queue: Queue) -> None:
        # A log subscription only sees messages published after it registered.
        queue.publish("events", "early")  # no subscriber yet → nothing stored
        queue.subscribe_log("events", "late")
        queue.publish("events", "seen")
        msgs = queue.read_topic("events", "late")
        assert [m.args for m in msgs] == [("seen",)]


class TestCursor:
    def test_ack_advances_and_is_monotonic(self, queue: Queue) -> None:
        queue.subscribe_log("events", "c")
        for i in range(3):
            queue.publish("events", i)

        msgs = queue.read_topic("events", "c")
        assert [m.args[0] for m in msgs] == [0, 1, 2]

        # Ack through the middle message: the next read starts after it.
        assert queue.ack_topic("events", "c", msgs[1].id) is True
        remaining = queue.read_topic("events", "c")
        assert [m.args[0] for m in remaining] == [2]

        # Acking an older cursor never rewinds.
        assert queue.ack_topic("events", "c", msgs[0].id) is False
        assert [m.args[0] for m in queue.read_topic("events", "c")] == [2]

    def test_unacked_read_is_at_least_once(self, queue: Queue) -> None:
        queue.subscribe_log("events", "c")
        queue.publish("events", "x")
        # Reading without acking (e.g. a crash mid-process) re-delivers.
        assert [m.args for m in queue.read_topic("events", "c")] == [("x",)]
        assert [m.args for m in queue.read_topic("events", "c")] == [("x",)]

    def test_limit_bounds_the_page(self, queue: Queue) -> None:
        queue.subscribe_log("events", "c")
        for i in range(5):
            queue.publish("events", i)
        first = queue.read_topic("events", "c", limit=2)
        assert [m.args[0] for m in first] == [0, 1]
        queue.ack_topic("events", "c", first[-1].id)
        assert [m.args[0] for m in queue.read_topic("events", "c", limit=2)] == [2, 3]


class TestMixedTopic:
    def test_log_and_fanout_subscribers_coexist(
        self, queue: Queue, run_worker: threading.Thread, poll_until: PollUntil
    ) -> None:
        seen: list[int] = []
        lock = threading.Lock()

        @queue.subscriber("events", name="worker")
        def handle(n: int) -> None:
            with lock:
                seen.append(n)

        queue.declare_subscriptions()
        queue.subscribe_log("events", "log")

        # One publish: the fan-out subscriber runs its job...
        queue.publish("events", 7)
        poll_until(lambda: seen == [7], timeout=30, message="fan-out subscriber should run")

        # ...and the same publish stored one log message for the log subscriber.
        assert [m.args for m in queue.read_topic("events", "log")] == [(7,)]


class TestManagedConsumer:
    """A ``log_consumer`` daemon thread pulls, invokes the handler, and acks."""

    @staticmethod
    def _worker(queue: Queue) -> threading.Thread:
        thread = threading.Thread(target=queue.run_worker, daemon=True)
        thread.start()
        return thread

    def test_managed_consumers_in_one_worker(self, queue: Queue, poll_until: PollUntil) -> None:
        # One worker hosts all four consumers, so their poll loops share a single
        # startup instead of a heavy worker lifecycle per test (slow on CI).
        lock = threading.Lock()
        sync: list[int] = []
        async_seen: list[int] = []
        retry: list[int] = []
        skip: list[int] = []
        retry_failed = threading.Event()

        @queue.log_consumer("t-sync", "c", poll_interval=0.05)
        def on_sync(n: int) -> None:
            with lock:
                sync.append(n)

        @queue.log_consumer("t-async", "c", poll_interval=0.05)
        async def on_async(n: int) -> None:
            with lock:
                async_seen.append(n)

        @queue.log_consumer("t-retry", "c", poll_interval=0.05, on_error="retry")
        def on_retry(n: int) -> None:
            with lock:
                retry.append(n)
            if n == 1 and not retry_failed.is_set():
                retry_failed.set()
                raise RuntimeError("boom")

        @queue.log_consumer("t-skip", "c", poll_interval=0.05, on_error="skip")
        def on_skip(n: int) -> None:
            with lock:
                skip.append(n)
            if n == 1:
                raise RuntimeError("always poison")

        thread = self._worker(queue)
        try:
            for i in range(3):
                queue.publish("t-sync", i)
                queue.publish("t-retry", i)
                queue.publish("t-skip", i)
            queue.publish("t-async", 42)

            poll_until(lambda: sorted(sync) == [0, 1, 2], timeout=30, message="sync handler ran")
            poll_until(lambda: async_seen == [42], timeout=30, message="async handler awaited")
            poll_until(
                lambda: retry.count(1) == 2 and 2 in retry,
                timeout=30,
                message="retry re-reads the failure then completes",
            )
            poll_until(lambda: sorted(skip) == [0, 1, 2], timeout=30, message="skip past poison")
        finally:
            queue.shutdown()
            thread.join(timeout=5)

        assert retry.count(0) == 1  # acked before the failure, never redelivered
        assert skip.count(1) == 1  # poison attempted once, then acked past
        stats = {(s["topic"], s["subscription"]): s for s in queue.topic_log_stats()}
        assert stats[("t-sync", "c")]["lag"] == 0
        assert stats[("t-skip", "c")]["lag"] == 0

    def test_shutdown_stops_consumer_thread(self, queue: Queue, poll_until: PollUntil) -> None:
        handled: list[int] = []

        @queue.log_consumer("events", "c", poll_interval=0.05)
        def handle(n: int) -> None:
            handled.append(n)

        thread = self._worker(queue)
        queue.publish("events", 1)
        # Wait until the consumer has actually run before shutting down, so the
        # request can't race an unstarted worker (which would block forever).
        poll_until(
            lambda: handled == [1], timeout=30, message="consumer should run before shutdown"
        )
        queue.shutdown()
        # SQLite is opened with ``busy_timeout = 5000``, so one contended query
        # during the drain can block for 5s on its own — a 5s budget can't bound
        # a shutdown. Wait past that, well under the 30s drain timeout.
        thread.join(timeout=20)
        assert not thread.is_alive()


class TestLogStats:
    def test_lag_reflects_unacked(self, queue: Queue) -> None:
        queue.subscribe_log("events", "c")
        for i in range(3):
            queue.publish("events", i)

        (stat,) = queue.topic_log_stats()
        assert stat["topic"] == "events"
        assert stat["subscription"] == "c"
        assert stat["cursor"] is None
        assert stat["lag"] == 3

        msgs = queue.read_topic("events", "c")
        queue.ack_topic("events", "c", msgs[-1].id)
        (stat,) = queue.topic_log_stats()
        assert stat["lag"] == 0
        assert stat["oldest_unacked_age_ms"] is None


class TestTopicRegistry:
    def test_declared_topic_retains_without_subscribers(self, queue: Queue) -> None:
        queue.declare_topic("events")
        # No subscriber yet, but declared → the publish is retained.
        assert queue.publish("events", 1) == []
        assert queue.publish("events", 2) == []
        # A log subscriber that joins later still sees the earlier publishes.
        queue.subscribe_log("events", "late")
        assert [m.args for m in queue.read_topic("events", "late")] == [(1,), (2,)]

    def test_undeclared_topic_keeps_late_join_boundary(self, queue: Queue) -> None:
        queue.publish("events", "early")  # not declared, no sub → dropped
        queue.subscribe_log("events", "late")
        queue.publish("events", "seen")
        assert [m.args for m in queue.read_topic("events", "late")] == [("seen",)]

    def test_list_declared_topics_and_retention_round_trip(self, queue: Queue) -> None:
        queue.declare_topic("orders", retention=1.5)
        queue.declare_topic("events")
        topics = {t["name"]: t for t in queue.list_declared_topics()}
        assert topics["orders"]["mode"] == "log"
        assert topics["orders"]["retention_ms"] == 1500
        assert topics["events"]["retention_ms"] is None

        # Idempotent re-declare updates retention without adding a row.
        queue.declare_topic("orders", retention=2.0)
        topics = {t["name"]: t for t in queue.list_declared_topics()}
        assert topics["orders"]["retention_ms"] == 2000
        assert len(queue.list_declared_topics()) == 2


class TestPerMessageAck:
    def test_lease_ack_nack(self, queue: Queue) -> None:
        queue.subscribe_log("events", "w")
        for i in range(3):
            queue.publish("events", i)

        # Lease 2 with a long visibility; a second lease sees only msg 2.
        batch = queue.lease_topic("events", "w", limit=2, visibility=60)
        assert [m.args[0] for m in batch] == [0, 1]
        assert [m.args[0] for m in queue.lease_topic("events", "w", visibility=60)] == [2]

        # Ack 0 (done); nack 1 (redeliver now). Acking 0 again is a no-op.
        assert queue.ack_message("events", "w", batch[0].id) is True
        assert queue.nack_message("events", "w", batch[1].id) is True
        assert queue.ack_message("events", "w", batch[0].id) is False

        # Only the nacked msg 1 comes back (0 acked, 2 in-flight).
        assert [m.args[0] for m in queue.lease_topic("events", "w", visibility=60)] == [1]

    def test_visibility_timeout_redelivers(self, queue: Queue) -> None:
        queue.subscribe_log("events", "w")
        queue.publish("events", 1)

        assert [m.args[0] for m in queue.lease_topic("events", "w", visibility=0.1)] == [1]
        # Within the window it is not re-leased.
        assert queue.lease_topic("events", "w", visibility=0.1) == []
        time.sleep(0.15)
        # Past the timeout, an un-acked message redelivers.
        assert [m.args[0] for m in queue.lease_topic("events", "w", visibility=0.1)] == [1]
