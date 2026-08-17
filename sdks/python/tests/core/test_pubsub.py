"""Topic pub/sub: fan-out, per-subscriber isolation, subscription lifecycle."""

import threading
from typing import Any

import pytest

from flexiq import Queue

PollUntil = Any  # the conftest fixture's runtime type


class TestSubscriptionRegistry:
    def test_declare_and_list(self, queue: Queue) -> None:
        @queue.subscriber("orders", name="email")
        def send_email(order_id: int) -> str:
            return f"email {order_id}"

        @queue.subscriber("orders", name="analytics")
        def track(order_id: int) -> str:
            return f"track {order_id}"

        queue.declare_subscriptions()

        subs = queue.list_subscriptions("orders")
        assert {s["name"] for s in subs} == {"email", "analytics"}
        assert all(s["active"] and s["durable"] for s in subs)
        assert queue.list_topics() == ["orders"]

    def test_subscriber_name_defaults_to_task_name(self, queue: Queue) -> None:
        @queue.subscriber("orders")
        def handle(order_id: int) -> None: ...

        queue.declare_subscriptions()
        (sub,) = queue.list_subscriptions("orders")
        assert sub["name"] == sub["task_name"] == handle.name

    def test_redeclare_updates_instead_of_duplicating(self, queue: Queue) -> None:
        @queue.subscriber("orders", name="email", queue="mail")
        def send_email(order_id: int) -> None: ...

        queue.declare_subscriptions()
        queue.declare_subscriptions()

        subs = queue.list_subscriptions()
        assert len(subs) == 1
        assert subs[0]["queue"] == "mail"

    def test_unsubscribe(self, queue: Queue) -> None:
        @queue.subscriber("orders", name="email")
        def send_email(order_id: int) -> None: ...

        queue.declare_subscriptions()
        assert queue.unsubscribe("orders", "email") is True
        assert queue.unsubscribe("orders", "email") is False
        assert queue.list_subscriptions("orders") == []

    def test_pause_resume(self, queue: Queue) -> None:
        @queue.subscriber("orders", name="email")
        def send_email(order_id: int) -> None: ...

        queue.declare_subscriptions()
        assert queue.pause_subscription("orders", "email") is True
        assert queue.publish("orders", 1) == []
        assert queue.resume_subscription("orders", "email") is True
        assert len(queue.publish("orders", 2)) == 1


class TestPublish:
    def test_publish_without_subscribers_is_noop(self, queue: Queue) -> None:
        assert queue.publish("ghost-topic", 1, key="value") == []

    def test_fan_out_delivers_to_every_subscriber(
        self, queue: Queue, run_worker: threading.Thread, poll_until: PollUntil
    ) -> None:
        received: dict[str, int] = {}
        lock = threading.Lock()

        @queue.subscriber("orders", name="email")
        def send_email(order_id: int) -> None:
            with lock:
                received["email"] = order_id

        @queue.subscriber("orders", name="analytics")
        def track(order_id: int) -> None:
            with lock:
                received["analytics"] = order_id

        queue.declare_subscriptions()
        results = queue.publish("orders", 42)
        assert len(results) == 2

        poll_until(lambda: len(received) == 2, message="both subscribers should run")
        assert received == {"email": 42, "analytics": 42}

    def test_failing_subscriber_does_not_affect_sibling(
        self, queue: Queue, run_worker: threading.Thread, poll_until: PollUntil
    ) -> None:
        outcomes: list[str] = []

        @queue.subscriber("orders", name="flaky", max_retries=0)
        def flaky(order_id: int) -> None:
            raise RuntimeError("boom")

        @queue.subscriber("orders", name="steady")
        def steady(order_id: int) -> None:
            outcomes.append("steady")

        queue.declare_subscriptions()
        deliveries = queue.publish("orders", 7)
        assert len(deliveries) == 2

        poll_until(lambda: outcomes == ["steady"], message="healthy subscriber should complete")
        jobs = [queue.get_job(r.id) for r in deliveries]
        by_subscription = {
            job.notes["subscription"]: delivery
            for job, delivery in zip(jobs, deliveries, strict=True)
            if job is not None and job.notes is not None
        }
        assert set(by_subscription) == {"flaky", "steady"}

        def status_of(subscription: str) -> str:
            delivery = by_subscription[subscription]
            delivery.refresh()
            return delivery.status

        poll_until(
            lambda: status_of("steady") == "complete",
            message="steady delivery should complete",
        )
        flaky_id = by_subscription["flaky"].id
        poll_until(
            lambda: any(dead["original_job_id"] == flaky_id for dead in queue.dead_letters()),
            message="flaky delivery should dead-letter independently",
        )

    def test_idempotency_key_dedupes_per_subscriber(self, queue: Queue) -> None:
        """Regression: the jobs unique index is global — an unsalted key
        would silently drop delivery to all but one subscriber."""

        @queue.subscriber("orders", name="email")
        def send_email(order_id: int) -> None: ...

        @queue.subscriber("orders", name="analytics")
        def track(order_id: int) -> None: ...

        queue.declare_subscriptions()
        first = queue.publish("orders", 1, idempotency_key="evt-9")
        assert len(first) == 2

        second = queue.publish("orders", 1, idempotency_key="evt-9")
        assert sorted(r.id for r in second) == sorted(r.id for r in first)

    def test_deliveries_carry_topic_and_subscription_notes(self, queue: Queue) -> None:
        @queue.subscriber("orders", name="email")
        def send_email(order_id: int) -> None: ...

        queue.declare_subscriptions()
        (result,) = queue.publish("orders", 5, notes={"tenant": "acme"})

        job = queue.get_job(result.id)
        assert job is not None and job.notes is not None
        assert job.notes["topic"] == "orders"
        assert job.notes["subscription"] == "email"
        assert job.notes["tenant"] == "acme"

    def test_late_join_gets_only_later_publishes(self, queue: Queue) -> None:
        @queue.subscriber("orders", name="email")
        def send_email(order_id: int) -> None: ...

        queue.declare_subscriptions()
        assert len(queue.publish("orders", 1)) == 1

        queue._inner.register_subscription(
            topic="orders", subscription_name="late", task_name=send_email.name
        )
        deliveries = queue.publish("orders", 2)
        assert len(deliveries) == 2


class TestCrossProcessDeliverySettings:
    def test_producer_without_task_applies_persisted_settings(
        self, tmp_path: Any, queue: Queue, run_worker: threading.Thread, poll_until: PollUntil
    ) -> None:
        """A subscriber's own @task settings must reach deliveries even when the
        publisher never registered the task — proving the settings persist on the
        subscription row, not just in the publishing process's registry."""

        @queue.subscriber("orders", name="picky", max_retries=7, priority=4, timeout=42)
        def handle(order_id: int) -> None: ...

        queue.declare_subscriptions()

        # A separate Queue on the same DB stands in for a producer-only process:
        # it has no task registry at all.
        producer = Queue(db_path=str(tmp_path / "test.db"))
        (delivery,) = producer.publish("orders", 1)

        job = queue.get_job(delivery.id)
        assert job is not None
        assert job._py_job.max_retries == 7
        assert job._py_job.priority == 4

    def test_publish_override_beats_persisted_setting(self, queue: Queue) -> None:
        @queue.subscriber("orders", name="picky", max_retries=7)
        def handle(order_id: int) -> None: ...

        queue.declare_subscriptions()
        (delivery,) = queue.publish("orders", 1, max_retries=1)
        job = queue.get_job(delivery.id)
        assert job is not None and job._py_job.max_retries == 1


class TestTopicStats:
    def test_backlog_counts_and_zero_rows(self, queue: Queue) -> None:
        @queue.subscriber("orders", name="email")
        def email(order_id: int) -> None: ...

        @queue.subscriber("orders", name="analytics")
        def track(order_id: int) -> None: ...

        queue.declare_subscriptions()
        queue.publish("orders", 1)
        queue.publish("orders", 2)

        stats = {s["subscription"]: s for s in queue.topic_stats("orders")}
        # Both registered subscriptions appear, each with 2 pending deliveries.
        assert set(stats) == {"email", "analytics"}
        assert stats["email"]["pending"] == 2
        assert stats["analytics"]["pending"] == 2
        assert stats["email"]["dead"] == 0
        assert stats["email"]["oldest_pending_age_ms"] is not None
        assert stats["email"]["oldest_pending_age_ms"] >= 0

    def test_idle_subscription_reports_zeros(self, queue: Queue) -> None:
        @queue.subscriber("orders", name="email")
        def email(order_id: int) -> None: ...

        queue.declare_subscriptions()
        (stat,) = queue.topic_stats("orders")
        assert stat["pending"] == stat["running"] == stat["dead"] == 0
        assert stat["oldest_pending_age_ms"] is None

    def test_failed_delivery_counts_as_dead(
        self, queue: Queue, run_worker: threading.Thread, poll_until: PollUntil
    ) -> None:
        @queue.subscriber("orders", name="flaky", max_retries=0)
        def flaky(order_id: int) -> None:
            raise RuntimeError("boom")

        queue.declare_subscriptions()
        queue.publish("orders", 1)

        def dead_counted() -> bool:
            stats = queue.topic_stats("orders")
            return bool(stats) and stats[0]["dead"] == 1 and stats[0]["pending"] == 0

        poll_until(dead_counted, message="failed delivery should count as dead")

    def test_non_pubsub_jobs_are_invisible(self, queue: Queue) -> None:
        @queue.task()
        def plain() -> None: ...

        plain.delay()
        assert queue.topic_stats() == []


class TestEphemeralSubscriptions:
    def test_ephemeral_registers_only_with_worker(self, queue: Queue) -> None:
        @queue.subscriber("orders", name="debug-tail", durable=False)
        def tail(order_id: int) -> None: ...

        queue.declare_subscriptions()
        assert queue.list_subscriptions("orders") == []

    def test_fresh_ephemeral_rows_survive_reap_grace_window(self, queue: Queue) -> None:
        """A just-registered ephemeral row must survive the reaper even with a
        dead owner: workers insert subscriptions before their first heartbeat
        lands, and another worker's reap tick must not race that gap. Aged-row
        reaping is covered by the Rust storage tests (created_at is not
        settable through the public API)."""
        queue._inner.register_subscription(
            topic="orders",
            subscription_name="ghost",
            task_name="some.task",
            durable=False,
            owner_worker_id="worker-that-never-heartbeats",
        )
        queue._inner.register_subscription(
            topic="orders", subscription_name="durable", task_name="some.task"
        )

        assert queue._inner.reap_ephemeral_subscriptions() == 0
        names = {s["name"] for s in queue.list_subscriptions("orders")}
        assert names == {"ghost", "durable"}

    def test_ephemeral_registration_requires_owner(self, queue: Queue) -> None:
        with pytest.raises(ValueError, match="owner_worker_id"):
            queue._inner.register_subscription(
                topic="orders",
                subscription_name="unowned",
                task_name="some.task",
                durable=False,
            )

    def test_subscriber_rejects_codecs(self, queue: Queue) -> None:
        with pytest.raises(ValueError, match="codecs"):

            @queue.subscriber("orders", name="bad", codecs=["gzip"])
            def handler(order_id: int) -> None: ...

    def test_redeclare_after_pause_stays_paused(self, queue: Queue) -> None:
        @queue.subscriber("orders", name="email")
        def send_email(order_id: int) -> None: ...

        queue.declare_subscriptions()
        assert queue.pause_subscription("orders", "email") is True
        queue.declare_subscriptions()
        assert queue.publish("orders", 1) == []

    def test_unsubscribe_drops_local_declaration(self, queue: Queue) -> None:
        @queue.subscriber("orders", name="email")
        def send_email(order_id: int) -> None: ...

        queue.declare_subscriptions()
        assert queue.unsubscribe("orders", "email") is True
        queue.declare_subscriptions()
        assert queue.list_subscriptions("orders") == []
