"""OpenTelemetry integration for flexiq.

Requires the ``otel`` extra::

    pip install flexiq[otel]

Usage::

    from flexiq.contrib.otel import OpenTelemetryMiddleware

    queue = Queue(middleware=[OpenTelemetryMiddleware()])
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from flexiq.middleware import TaskMiddleware, legacy_task_filter_to_predicate

if TYPE_CHECKING:
    from flexiq.context import JobContext
    from flexiq.predicates import Predicate

try:
    from opentelemetry import trace
    from opentelemetry.trace import StatusCode
except ImportError:
    trace = None
    StatusCode = None

_TRACER_NAME = "flexiq"


class OpenTelemetryMiddleware(TaskMiddleware):
    """Middleware that creates OpenTelemetry spans for task execution.

    Each task execution produces a span with:
    - Span name: ``flexiq.execute.<task_name>`` (customizable via ``span_name_fn``)
    - Attributes: ``flexiq.job_id``, ``flexiq.task_name``,
      ``flexiq.queue``, ``flexiq.retry_count`` (prefix customizable via
      ``attribute_prefix``)
    - Status: OK on success, ERROR on failure with exception recorded

    Args:
        tracer_name: OpenTelemetry tracer name.
        span_name_fn: Custom span name builder. Receives a
            :class:`~flexiq.context.JobContext` and returns a string.
        attribute_prefix: Prefix for span attribute keys (default ``"flexiq"``).
        extra_attributes_fn: Callable that returns extra attributes to add to
            each span. Receives a :class:`~flexiq.context.JobContext`.
        task_filter: Legacy ``Callable[[task_name], bool]`` filter. Kept for
            back-compat — prefer ``predicate=`` which accepts richer
            :class:`~flexiq.predicates.Predicate` objects.
        predicate: Optional :class:`~flexiq.predicates.Predicate` (or
            callable taking a :class:`~flexiq.predicates.PredicateContext`)
            controlling which tasks this middleware applies to.
    """

    def __init__(
        self,
        tracer_name: str = _TRACER_NAME,
        *,
        span_name_fn: Callable[[JobContext], str] | None = None,
        attribute_prefix: str = "flexiq",
        extra_attributes_fn: Callable[[JobContext], dict[str, Any]] | None = None,
        task_filter: Callable[[str], bool] | None = None,
        predicate: Predicate | Callable[..., Any] | None = None,
    ):
        if trace is None:
            raise ImportError(
                "opentelemetry-api is required for OpenTelemetryMiddleware. "
                "Install it with: pip install flexiq[otel]"
            )
        super().__init__(predicate=legacy_task_filter_to_predicate(task_filter, predicate))
        self._tracer = trace.get_tracer(tracer_name)
        self._span_name_fn = span_name_fn
        self._attr_prefix = attribute_prefix
        self._extra_attributes_fn = extra_attributes_fn
        self._spans: dict[str, Any] = {}
        self._lock = threading.Lock()

    def _span_name(self, ctx: JobContext) -> str:
        if self._span_name_fn is not None:
            return self._span_name_fn(ctx)
        return f"{self._attr_prefix}.execute.{ctx.task_name}"

    def before(self, ctx: JobContext) -> None:
        prefix = self._attr_prefix
        attributes: dict[str, Any] = {
            f"{prefix}.job_id": ctx.id,
            f"{prefix}.task_name": ctx.task_name,
            f"{prefix}.queue": ctx.queue_name,
            f"{prefix}.retry_count": ctx.retry_count,
        }
        if self._extra_attributes_fn is not None:
            attributes.update(self._extra_attributes_fn(ctx))

        span = self._tracer.start_span(self._span_name(ctx), attributes=attributes)
        with self._lock:
            self._spans[ctx.id] = span

    def after(self, ctx: JobContext, result: Any, error: Exception | None) -> None:
        with self._lock:
            span = self._spans.pop(ctx.id, None)
        if span is None:
            return  # before() didn't emit a span (predicate filtered, or error)

        try:
            if error is not None:
                span.set_status(StatusCode.ERROR, str(error))
                span.record_exception(error)
            else:
                span.set_status(StatusCode.OK)
        finally:
            span.end()

    def on_sleep(self, ctx: JobContext, wake_at: int) -> None:
        """End the span for an attempt that slept, without calling it a result.

        The span has to end — the attempt is over and the worker slot is gone —
        but its status stays unset: the task neither succeeded nor failed, and
        marking it OK would make a job that sleeps three times look like three
        successful executions.
        """
        with self._lock:
            span = self._spans.pop(ctx.id, None)
        if span is None:
            return

        prefix = self._attr_prefix
        try:
            span.set_attribute(f"{prefix}.slept", True)
            span.add_event("sleep", attributes={f"{prefix}.wake_at": wake_at})
        finally:
            span.end()

    def on_retry(self, ctx: JobContext, error: Exception, retry_count: int) -> None:
        with self._lock:
            span = self._spans.get(ctx.id)
        if span is not None:
            prefix = self._attr_prefix
            span.add_event(
                "retry",
                attributes={
                    f"{prefix}.retry_count": retry_count,
                    f"{prefix}.error": str(error),
                },
            )
