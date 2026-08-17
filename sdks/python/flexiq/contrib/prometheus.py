"""Prometheus metrics integration for flexiq.

Requires the ``prometheus`` extra::

    pip install flexiq[prometheus]

Usage::

    from flexiq.contrib.prometheus import PrometheusMiddleware, start_metrics_server

    queue = Queue(middleware=[PrometheusMiddleware()])
    start_metrics_server(port=9090)  # optional standalone metrics endpoint
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from flexiq.middleware import TaskMiddleware, legacy_task_filter_to_predicate

if TYPE_CHECKING:
    from flexiq.app import Queue
    from flexiq.context import JobContext
    from flexiq.predicates import Predicate

logger = logging.getLogger("flexiq.prometheus")

try:
    from prometheus_client import Counter, Gauge, Histogram, start_http_server
except ImportError:
    Counter = None
    Gauge = None
    Histogram = None
    start_http_server = None

# ── Metric categories ─────────────────────────────────────
# Used by `disabled_metrics` to skip groups of metrics.
_METRIC_GROUPS: dict[str, list[str]] = {
    "jobs": ["jobs_total", "job_duration_seconds", "active_workers", "retries_total"],
    "queue": ["queue_depth", "dlq_size", "worker_utilization"],
    "resource": [
        "resource_health_status",
        "resource_recreation_total",
        "resource_init_duration_seconds",
        "resource_pool_size",
        "resource_pool_active",
        "resource_pool_idle",
        "resource_pool_timeout_total",
    ],
    "proxy": [
        "proxy_reconstruct_duration_seconds",
        "proxy_reconstruct_total",
        "proxy_reconstruct_errors_total",
    ],
    "intercept": ["intercept_duration_seconds", "intercept_strategy_total"],
}


# ── Per-namespace metric store ────────────────────────────

_store_lock = threading.Lock()
_metric_stores: dict[str, dict[str, Any]] = {}


def _get_or_create_metrics(
    namespace: str, disabled_metrics: set[str] | None = None
) -> dict[str, Any]:
    """Return a metric store for the given namespace, creating if needed."""
    if namespace in _metric_stores:
        return _metric_stores[namespace]

    with _store_lock:
        if namespace in _metric_stores:
            return _metric_stores[namespace]

        disabled_names: set[str] = set()
        if disabled_metrics:
            for group in disabled_metrics:
                disabled_names.update(_METRIC_GROUPS.get(group, [group]))

        store: dict[str, Any] = {}

        def _make(cls: Any, suffix: str, description: str, labels: list[str] | None = None) -> Any:
            if suffix in disabled_names:
                return None
            name = f"{namespace}_{suffix}"
            if labels:
                return cls(name, description, labels)
            return cls(name, description)

        store["jobs_total"] = _make(
            Counter, "jobs_total", "Total number of jobs processed", ["task", "status"]
        )
        store["job_duration"] = _make(
            Histogram, "job_duration_seconds", "Job execution duration in seconds", ["task"]
        )
        store["active_workers"] = _make(
            Gauge, "active_workers", "Number of currently active workers"
        )
        store["retries_total"] = _make(
            Counter, "retries_total", "Total number of job retries", ["task"]
        )
        store["queue_depth"] = _make(
            Gauge, "queue_depth", "Number of pending jobs per queue", ["queue"]
        )
        store["dlq_size"] = _make(Gauge, "dlq_size", "Number of dead-letter jobs")
        store["worker_utilization"] = _make(
            Gauge, "worker_utilization", "Worker utilization ratio (0.0-1.0)", ["queue"]
        )
        store["resource_health"] = _make(
            Gauge,
            "resource_health_status",
            "Resource health (1=healthy, 0=unhealthy)",
            ["resource"],
        )
        store["resource_recreations"] = _make(
            Gauge, "resource_recreation_total", "Total recreations per resource", ["resource"]
        )
        store["resource_init_duration"] = _make(
            Gauge,
            "resource_init_duration_seconds",
            "Time to initialize each resource",
            ["resource"],
        )
        store["proxy_reconstruct_duration"] = _make(
            Histogram,
            "proxy_reconstruct_duration_seconds",
            "Proxy reconstruction duration",
            ["handler"],
        )
        store["proxy_reconstruct_total"] = _make(
            Counter, "proxy_reconstruct_total", "Total proxy reconstructions", ["handler"]
        )
        store["proxy_reconstruct_errors"] = _make(
            Counter,
            "proxy_reconstruct_errors_total",
            "Total proxy reconstruction errors",
            ["handler"],
        )
        store["intercept_duration"] = _make(
            Histogram, "intercept_duration_seconds", "Argument interception duration"
        )
        store["intercept_strategy_total"] = _make(
            Counter, "intercept_strategy_total", "Interception strategy counts", ["strategy"]
        )
        store["pool_size"] = _make(
            Gauge, "resource_pool_size", "Resource pool max size", ["resource"]
        )
        store["pool_active"] = _make(
            Gauge, "resource_pool_active", "Active pool instances", ["resource"]
        )
        store["pool_idle"] = _make(
            Gauge, "resource_pool_idle", "Idle pool instances", ["resource"]
        )
        store["pool_timeouts"] = _make(
            Counter, "resource_pool_timeout_total", "Pool acquisition timeouts", ["resource"]
        )

        _metric_stores[namespace] = store
        return store


class PrometheusMiddleware(TaskMiddleware):
    """Middleware that exports Prometheus metrics for task execution.

    Args:
        namespace: Prefix for all metric names (default ``"flexiq"``).
        extra_labels_fn: Callable that returns extra labels to add to job
            metrics. Receives a :class:`~flexiq.context.JobContext` and
            returns a dict of label key-value pairs.
        disabled_metrics: Metric groups or individual metric names to skip.
            Groups: ``"jobs"``, ``"queue"``, ``"resource"``, ``"proxy"``,
            ``"intercept"``.
        task_filter: Predicate that receives a task name and returns ``True``
            to export metrics for the task, ``False`` to ignore it. Defaults
            to exporting all tasks. Parity with ``OTelMiddleware`` and
            ``SentryMiddleware``.
    """

    def __init__(
        self,
        *,
        namespace: str = "flexiq",
        extra_labels_fn: Callable[[JobContext], dict[str, str]] | None = None,
        disabled_metrics: set[str] | None = None,
        task_filter: Callable[[str], bool] | None = None,
        predicate: Predicate | Callable[..., Any] | None = None,
    ) -> None:
        if Counter is None:
            raise ImportError(
                "prometheus-client is required for PrometheusMiddleware. "
                "Install it with: pip install flexiq[prometheus]"
            )
        super().__init__(predicate=legacy_task_filter_to_predicate(task_filter, predicate))
        self._metrics = _get_or_create_metrics(namespace, disabled_metrics)
        self._extra_labels_fn = extra_labels_fn
        self._start_times: dict[str, float] = {}
        self._lock = threading.Lock()

    def before(self, ctx: JobContext) -> None:
        with self._lock:
            self._start_times[ctx.id] = time.monotonic()
        m = self._metrics["active_workers"]
        if m is not None:
            m.inc()

    def after(self, ctx: JobContext, result: Any, error: Exception | None) -> None:
        m = self._metrics["active_workers"]
        if m is not None:
            m.dec()

        status = "failed" if error is not None else "completed"
        m = self._metrics["jobs_total"]
        if m is not None:
            m.labels(task=ctx.task_name, status=status).inc()

        with self._lock:
            start = self._start_times.pop(ctx.id, None)
        if start is not None:
            duration = time.monotonic() - start
            m = self._metrics["job_duration"]
            if m is not None:
                m.labels(task=ctx.task_name).observe(duration)

    def on_retry(self, ctx: JobContext, error: Exception, retry_count: int) -> None:
        m = self._metrics["retries_total"]
        if m is not None:
            m.labels(task=ctx.task_name).inc()


class PrometheusStatsCollector:
    """Daemon thread that polls queue stats and updates Prometheus gauges.

    Args:
        queue: The Queue instance to poll.
        interval: Seconds between polls (default 10.0).
        namespace: Prefix for metric names (default ``"flexiq"``).
        disabled_metrics: Metric groups or names to skip.

    Usage::

        collector = PrometheusStatsCollector(queue, interval=10)
        collector.start()
    """

    def __init__(
        self,
        queue: Queue,
        interval: float = 10.0,
        *,
        namespace: str = "flexiq",
        disabled_metrics: set[str] | None = None,
    ):
        if Counter is None:
            raise ImportError(
                "prometheus-client is required for PrometheusStatsCollector. "
                "Install it with: pip install flexiq[prometheus]"
            )
        self._metrics = _get_or_create_metrics(namespace, disabled_metrics)
        self._queue = queue
        self._interval = interval
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()

    def start(self) -> None:
        self._thread = threading.Thread(target=self._poll, daemon=True, name="flexiq-prom-stats")
        self._thread.start()

    def stop(self) -> None:
        """Signal the collector to stop and wait for the thread to finish."""
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=5)

    def _poll(self) -> None:
        m = self._metrics
        while not self._stop_event.is_set():
            try:
                stats = self._queue.stats()
                if m["dlq_size"] is not None:
                    m["dlq_size"].set(stats.get("dead", 0))

                running = stats.get("running", 0)
                total_workers = self._queue._workers

                # Per-queue depth and utilization
                try:
                    all_q = self._queue.stats_all_queues()
                    for q_name, q_stats in all_q.items():
                        if m["queue_depth"] is not None:
                            m["queue_depth"].labels(queue=q_name).set(q_stats.get("pending", 0))
                        if m["worker_utilization"] is not None and total_workers > 0:
                            q_running = q_stats.get("running", 0)
                            m["worker_utilization"].labels(queue=q_name).set(
                                q_running / total_workers
                            )
                except Exception:
                    if m["queue_depth"] is not None:
                        m["queue_depth"].labels(queue="default").set(stats.get("pending", 0))
                    if m["worker_utilization"] is not None and total_workers > 0:
                        m["worker_utilization"].labels(queue="default").set(
                            running / total_workers
                        )
            except Exception:
                logger.debug("Stats collection failed", exc_info=True)

            # Resource metrics (including pool stats)
            try:
                for res in self._queue.resource_status():
                    name = res["name"]
                    if m["resource_health"] is not None:
                        m["resource_health"].labels(resource=name).set(
                            1.0 if res["health"] == "healthy" else 0.0
                        )
                    if m["resource_recreations"] is not None:
                        m["resource_recreations"].labels(resource=name).set(res["recreations"])
                    if m["resource_init_duration"] is not None:
                        m["resource_init_duration"].labels(resource=name).set(
                            res["init_duration_ms"] / 1000.0
                        )
                    pool = res.get("pool")
                    if pool:
                        if m["pool_size"] is not None:
                            m["pool_size"].labels(resource=name).set(pool["size"])
                        if m["pool_active"] is not None:
                            m["pool_active"].labels(resource=name).set(pool["active"])
                        if m["pool_idle"] is not None:
                            m["pool_idle"].labels(resource=name).set(pool["idle"])
                        if m["pool_timeouts"] is not None:
                            m["pool_timeouts"].labels(resource=name).set(pool["total_timeouts"])
            except Exception:
                logger.debug("Resource metrics collection failed", exc_info=True)

            # Proxy reconstruction metrics
            try:
                for pstat in self._queue.proxy_stats():
                    handler = pstat["handler"]
                    if m["proxy_reconstruct_total"] is not None:
                        m["proxy_reconstruct_total"].labels(handler=handler)._value.set(
                            pstat["total_reconstructions"]
                        )
                    if m["proxy_reconstruct_errors"] is not None:
                        m["proxy_reconstruct_errors"].labels(handler=handler)._value.set(
                            pstat["total_errors"]
                        )
            except Exception:
                logger.debug("Proxy metrics collection failed", exc_info=True)

            # Interception metrics
            try:
                istats = self._queue.interception_stats()
                if istats and m["intercept_strategy_total"] is not None:
                    for strategy, count in istats.get("strategy_counts", {}).items():
                        m["intercept_strategy_total"].labels(strategy=strategy)._value.set(count)
            except Exception:
                logger.debug("Interception metrics collection failed", exc_info=True)

            self._stop_event.wait(self._interval)


def start_metrics_server(port: int = 9090) -> None:
    """Start a standalone Prometheus metrics HTTP server.

    Args:
        port: Port to bind to (default: 9090).
    """
    if start_http_server is None:
        raise ImportError(
            "prometheus-client is required. Install with: pip install flexiq[prometheus]"
        )
    start_http_server(port)
