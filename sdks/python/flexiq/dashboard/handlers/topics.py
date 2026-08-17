"""Topic pub/sub route handlers."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from flexiq.app import Queue


def _handle_topics(queue: Queue, qs: dict) -> list[dict[str, Any]]:
    """Aggregate ``topic_stats()`` into one row per topic.

    Backlog folds pending + running across every subscription so operators
    see a topic's total in-flight work at a glance; ``dead`` sums the DLQ
    depth across subscribers.
    """
    topics: dict[str, dict[str, Any]] = {}
    for row in queue.topic_stats():
        entry = topics.setdefault(
            row["topic"],
            {"topic": row["topic"], "subscription_count": 0, "backlog": 0, "dead": 0},
        )
        entry["subscription_count"] += 1
        entry["backlog"] += row["pending"] + row["running"]
        entry["dead"] += row["dead"]
    return list(topics.values())


def _handle_topic_detail(queue: Queue, qs: dict, topic: str) -> list[dict[str, Any]]:
    """Per-subscription backlog rows for a single topic."""
    return queue.topic_stats(topic=topic)


def _handle_pause_subscription(queue: Queue, topic_and_name: tuple[str, str]) -> dict[str, bool]:
    topic, name = topic_and_name
    return {"paused": queue.pause_subscription(topic, name)}


def _handle_resume_subscription(queue: Queue, topic_and_name: tuple[str, str]) -> dict[str, bool]:
    topic, name = topic_and_name
    return {"active": queue.resume_subscription(topic, name)}


def _handle_unsubscribe(queue: Queue, topic_and_name: tuple[str, str]) -> dict[str, bool]:
    topic, name = topic_and_name
    return {"unsubscribed": queue.unsubscribe(topic, name)}
