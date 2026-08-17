"""Tests for customizability configuration options."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

from flexiq.app import Queue
from flexiq.events import EventType
from flexiq.middleware import TaskMiddleware
from flexiq.webhooks import WebhookManager

# ── Middleware Hooks ──────────────────────────────────────────────────


class RecordingMiddleware(TaskMiddleware):
    """Middleware that records all hook calls for testing."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []

    def before(self, ctx: Any) -> None:
        self.calls.append(("before", ctx.task_name))

    def after(self, ctx: Any, result: Any, error: Any) -> None:
        self.calls.append(("after", ctx.task_name))

    def on_enqueue(self, task_name: str, args: tuple, kwargs: dict, options: dict) -> None:
        self.calls.append(("on_enqueue", task_name))

    def on_dead_letter(self, ctx: Any, error: Exception) -> None:
        self.calls.append(("on_dead_letter", ctx.task_name))

    def on_timeout(self, ctx: Any) -> None:
        self.calls.append(("on_timeout", ctx.task_name))

    def on_cancel(self, ctx: Any) -> None:
        self.calls.append(("on_cancel", ctx.task_name))


class TestMiddlewareHooks:
    def test_on_enqueue_called(self, tmp_path: Any) -> None:
        mw = RecordingMiddleware()
        q = Queue(db_path=str(tmp_path / "test.db"), middleware=[mw])

        @q.task()
        def my_task() -> None:
            pass

        my_task.delay()
        assert ("on_enqueue", my_task.name) in mw.calls

    def test_on_enqueue_can_mutate_options(self, tmp_path: Any) -> None:
        """on_enqueue can modify the options dict to change enqueue params."""

        class PriorityBoostMiddleware(TaskMiddleware):
            def on_enqueue(self, task_name: str, args: tuple, kwargs: dict, options: dict) -> None:
                options["priority"] = 99

        mw = PriorityBoostMiddleware()
        q = Queue(db_path=str(tmp_path / "test.db"), middleware=[mw])

        @q.task()
        def my_task() -> None:
            pass

        result = my_task.delay()
        job = q.get_job(result.id)
        assert job is not None
        assert job.to_dict()["priority"] == 99

    def test_default_hooks_are_noop(self) -> None:
        """Base TaskMiddleware hooks should not raise."""
        mw = TaskMiddleware()
        mw.on_enqueue("test", (), {}, {})
        mw.on_dead_letter(MagicMock(), Exception("test"))
        mw.on_timeout(MagicMock())
        mw.on_cancel(MagicMock())


# ── Event System ─────────────────────────────────────────────────────


class TestEventSystem:
    def test_new_event_types_exist(self) -> None:
        assert EventType.WORKER_STARTED.value == "worker.started"
        assert EventType.WORKER_STOPPED.value == "worker.stopped"
        assert EventType.QUEUE_PAUSED.value == "queue.paused"
        assert EventType.QUEUE_RESUMED.value == "queue.resumed"

    def test_event_workers_param(self, tmp_path: Any) -> None:
        q = Queue(db_path=str(tmp_path / "test.db"), event_workers=2)
        assert q._event_bus._executor._max_workers == 2

    def test_on_event_public_api(self, tmp_path: Any, poll_until: Any) -> None:
        q = Queue(db_path=str(tmp_path / "test.db"))
        received: list[Any] = []

        def callback(event_type: EventType, payload: dict) -> None:
            received.append((event_type, payload))

        q.on_event(EventType.JOB_ENQUEUED, callback)

        @q.task()
        def my_task() -> None:
            pass

        my_task.delay()
        poll_until(lambda: len(received) >= 1, message="JOB_ENQUEUED event not delivered")
        assert len(received) == 1
        assert received[0][0] == EventType.JOB_ENQUEUED


# ── Webhook Configuration ────────────────────────────────────────────


class TestWebhookConfig:
    def test_add_webhook_with_retry_params(self) -> None:
        mgr = WebhookManager()
        mgr.add_webhook(
            "https://example.com/hook",
            max_retries=5,
            timeout=30.0,
            retry_backoff=3.0,
        )
        wh = mgr._webhooks[0]
        assert wh["max_retries"] == 5
        assert wh["timeout"] == 30.0
        assert wh["retry_backoff"] == 3.0

    def test_add_webhook_defaults(self) -> None:
        mgr = WebhookManager()
        mgr.add_webhook("https://example.com/hook")
        wh = mgr._webhooks[0]
        assert wh["max_retries"] == 3
        assert wh["timeout"] == 10.0
        assert wh["retry_backoff"] == 2.0

    def test_queue_add_webhook_passes_params(self, tmp_path: Any) -> None:
        q = Queue(db_path=str(tmp_path / "test.db"))
        q.add_webhook(
            "https://example.com/hook",
            max_retries=1,
            timeout=5.0,
            retry_backoff=1.5,
        )
        wh = q._webhook_manager._webhooks[0]
        assert wh["max_retries"] == 1
        assert wh["timeout"] == 5.0


# ── Queue Configuration ──────────────────────────────────────────────


class TestQueueConfig:
    def test_scheduler_timing_params(self, tmp_path: Any) -> None:
        q = Queue(
            db_path=str(tmp_path / "test.db"),
            scheduler_poll_interval_ms=100,
            scheduler_reap_interval=50,
            scheduler_cleanup_interval=600,
        )
        # These are passed to the Rust side — just verify they don't error
        assert q._inner is not None

    def test_scheduler_timing_defaults(self, tmp_path: Any) -> None:
        q = Queue(db_path=str(tmp_path / "test.db"))
        # Defaults should work fine
        assert q._inner is not None


# ── Per-Task Configuration ───────────────────────────────────────────


class TestPerTaskConfig:
    def test_max_retry_delay_param(self, tmp_path: Any) -> None:
        q = Queue(db_path=str(tmp_path / "test.db"))

        @q.task(max_retry_delay=60)
        def my_task() -> None:
            pass

        config = q._task_configs[-1]
        assert config.max_retry_delay == 60

    def test_max_retry_delay_default(self, tmp_path: Any) -> None:
        q = Queue(db_path=str(tmp_path / "test.db"))

        @q.task()
        def my_task() -> None:
            pass

        config = q._task_configs[-1]
        assert config.max_retry_delay is None

    def test_max_concurrent_param(self, tmp_path: Any) -> None:
        q = Queue(db_path=str(tmp_path / "test.db"))

        @q.task(max_concurrent=5)
        def my_task() -> None:
            pass

        config = q._task_configs[-1]
        assert config.max_concurrent == 5

    def test_max_concurrent_default(self, tmp_path: Any) -> None:
        q = Queue(db_path=str(tmp_path / "test.db"))

        @q.task()
        def my_task() -> None:
            pass

        config = q._task_configs[-1]
        assert config.max_concurrent is None


# ── Per-Task Serializer ──────────────────────────────────────────────


class TestPerTaskSerializer:
    def test_task_level_serializer_used_for_enqueue(self, tmp_path: Any) -> None:
        """Per-task serializer is used instead of queue-level serializer."""
        mock_serializer = MagicMock()
        mock_serializer.dumps.return_value = b"\x80\x04\x95"

        q = Queue(db_path=str(tmp_path / "test.db"))

        @q.task(serializer=mock_serializer)
        def my_task(x: int) -> None:
            pass

        my_task.delay(42)
        mock_serializer.dumps.assert_called_once()

    def test_queue_serializer_used_when_no_task_serializer(self, tmp_path: Any) -> None:
        q = Queue(db_path=str(tmp_path / "test.db"))

        @q.task()
        def my_task() -> None:
            pass

        # Should use the default CloudpickleSerializer without error
        my_task.delay()
        assert my_task.name not in q._task_serializers


# ── Flask CLI ─────────────────────────────────────────────────────────


# ── Queue-Level Limits ────────────────────────────────────────────────


class TestQueueLevelLimits:
    def test_set_queue_rate_limit(self, tmp_path: Any) -> None:
        q = Queue(db_path=str(tmp_path / "test.db"))
        q.set_queue_rate_limit("default", "100/m")
        assert q._queue_configs["default"]["rate_limit"] == "100/m"

    def test_set_queue_concurrency(self, tmp_path: Any) -> None:
        q = Queue(db_path=str(tmp_path / "test.db"))
        q.set_queue_concurrency("default", 10)
        assert q._queue_configs["default"]["max_concurrent"] == 10

    def test_set_both_on_same_queue(self, tmp_path: Any) -> None:
        q = Queue(db_path=str(tmp_path / "test.db"))
        q.set_queue_rate_limit("emails", "50/m")
        q.set_queue_concurrency("emails", 5)
        assert q._queue_configs["emails"]["rate_limit"] == "50/m"
        assert q._queue_configs["emails"]["max_concurrent"] == 5

    def test_queue_configs_serialized_to_json(self, tmp_path: Any) -> None:
        import json

        q = Queue(db_path=str(tmp_path / "test.db"))
        q.set_queue_rate_limit("default", "10/s")
        q.set_queue_concurrency("default", 3)
        serialized = json.dumps(q._queue_configs)
        parsed = json.loads(serialized)
        assert parsed["default"]["rate_limit"] == "10/s"
        assert parsed["default"]["max_concurrent"] == 3


# ── Flask CLI ─────────────────────────────────────────────────────────


class TestFlaskConfig:
    def test_cli_group_param(self) -> None:
        from flexiq.contrib.flask import FlexiQ

        ext = FlexiQ(cli_group="jobs")
        assert ext._cli_group == "jobs"

    def test_cli_group_default(self) -> None:
        from flexiq.contrib.flask import FlexiQ

        ext = FlexiQ()
        assert ext._cli_group == "flexiq"
