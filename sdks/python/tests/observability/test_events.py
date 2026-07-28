"""Tests for EventBus event dispatch."""

import time
from typing import Any

from taskito.events import EventBus, EventType

PollUntil = Any  # the conftest fixture's runtime type


def test_callback_receives_event(poll_until: PollUntil) -> None:
    """Registered callbacks receive emitted events."""
    received: list[tuple[EventType, dict[str, Any]]] = []
    bus = EventBus()
    bus.on(EventType.JOB_COMPLETED, lambda et, p: received.append((et, p)))

    bus.emit(EventType.JOB_COMPLETED, {"job_id": "123"})
    poll_until(lambda: len(received) >= 1, message="event was not delivered")

    assert len(received) == 1
    assert received[0][0] == EventType.JOB_COMPLETED
    assert received[0][1]["job_id"] == "123"


def test_multiple_callbacks(poll_until: PollUntil) -> None:
    """Multiple callbacks for the same event type all fire."""
    counts = {"a": 0, "b": 0}
    bus = EventBus()
    bus.on(EventType.JOB_FAILED, lambda et, p: counts.__setitem__("a", counts["a"] + 1))
    bus.on(EventType.JOB_FAILED, lambda et, p: counts.__setitem__("b", counts["b"] + 1))

    bus.emit(EventType.JOB_FAILED, {"error": "boom"})
    poll_until(
        lambda: counts["a"] == 1 and counts["b"] == 1,
        message="not all callbacks fired",
    )

    assert counts["a"] == 1
    assert counts["b"] == 1


def test_event_filtering() -> None:
    """Callbacks only fire for their registered event type."""
    received: list[str] = []
    bus = EventBus()
    bus.on(EventType.JOB_COMPLETED, lambda et, p: received.append("completed"))

    bus.emit(EventType.JOB_FAILED, {"error": "boom"})
    # Brief settle so a (would-be incorrect) cross-type dispatch could land.
    time.sleep(0.2)

    assert received == []


def test_exception_in_callback_does_not_crash(poll_until: PollUntil) -> None:
    """A raising callback doesn't prevent other events from processing."""
    results: list[str] = []
    bus = EventBus()

    def bad_callback(et: EventType, p: dict[str, Any]) -> None:
        raise RuntimeError("callback error")

    def good_callback(et: EventType, p: dict[str, Any]) -> None:
        results.append("ok")

    bus.on(EventType.JOB_ENQUEUED, bad_callback)
    bus.on(EventType.JOB_ENQUEUED, good_callback)

    bus.emit(EventType.JOB_ENQUEUED, {})
    poll_until(lambda: results == ["ok"], message="good_callback did not run")

    assert results == ["ok"]


def test_emit_with_no_listeners() -> None:
    """Emitting an event with no listeners doesn't raise."""
    bus = EventBus()
    bus.emit(EventType.JOB_DEAD, {"job_id": "456"})


def test_all_event_types_exist() -> None:
    """All expected event types are defined."""
    expected = {
        "job.enqueued",
        "job.completed",
        "job.failed",
        "job.retrying",
        "job.dead",
        "job.cancelled",
        "worker.started",
        "worker.stopped",
        "worker.online",
        "worker.offline",
        "worker.unhealthy",
        "queue.paused",
        "queue.resumed",
        "workflow.submitted",
        "workflow.completed",
        "workflow.completed_with_failures",
        "workflow.failed",
        "workflow.cancelled",
        "workflow.gate_reached",
        # Saga events.
        "workflow.compensating",
        "workflow.compensated",
        "workflow.compensation_failed",
        "workflow.node_compensating",
        "workflow.node_compensated",
        "workflow.node_compensation_failed",
        "predicate.deferred",
        "predicate.cancelled",
        "predicate.rejected",
        "predicate.skipped",
    }
    assert {e.value for e in EventType} == expected
