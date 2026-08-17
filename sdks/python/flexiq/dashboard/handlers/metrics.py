"""Metrics route handlers."""

from __future__ import annotations

from typing import TYPE_CHECKING

from flexiq.dashboard.handlers._qs import _parse_int_qs

if TYPE_CHECKING:
    from flexiq.app import Queue


def _handle_metrics(queue: Queue, qs: dict) -> dict:
    task = qs.get("task", [None])[0]
    since = _parse_int_qs(qs, "since", 3600)
    return queue.metrics(task_name=task, since=since)


def _handle_metrics_timeseries(queue: Queue, qs: dict) -> list:
    task = qs.get("task", [None])[0]
    since = _parse_int_qs(qs, "since", 3600)
    bucket = _parse_int_qs(qs, "bucket", 60)
    return queue.metrics_timeseries(task_name=task, since=since, bucket=bucket)
