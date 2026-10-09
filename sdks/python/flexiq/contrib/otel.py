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

from flexiq.context import JobContext
from flexiq.middleware import TaskMiddleware, legacy_task_filter_to_predicate
from flexiq.trace_context import merge_trace_carrier, trace_carrier

if TYPE_CHECKING:
    from flexiq.predicates import Predicate

# Kept rather than replaced with ``None`` stand-ins, so the names stay typed as
# what they are when the extra is installed.
_otel_missing: ImportError | None = None
try:
    from opentelemetry import context as otel_context
    from opentelemetry import propagate, trace
    from opentelemetry.trace import SpanKind, StatusCode
except ImportError as error:
    _otel_missing = error

_TRACER_NAME = "flexiq"


class OpenTelemetryMiddleware(TaskMiddleware):
    """Middleware that creates OpenTelemetry spans for task execution.

    Each task execution produces a span with:
    - Span name: ``flexiq.execute.<task_name>`` (customizable via ``span_name_fn``)
    - Attributes: ``flexiq.job_id``, ``flexiq.task_name``,
      ``flexiq.queue``, ``flexiq.retry_count`` (prefix customizable via
      ``attribute_prefix``)
    - Status: OK on success, ERROR on failure with exception recorded

    Trace context crosses the queue: an enqueue injects the caller's context into
    the job's metadata (``traceparent``/``tracestate``, merged, never replacing
    the caller's keys), and the execute span is a child of it — whichever SDK
    enqueued the job. An enqueue made inside a task with no span of its own
    continues that task's execute span. A workflow submit stores the same
    context on the run, so every step's execute span is a child of the submit.

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
        tracer_provider: Provider to take the tracer from. Defaults to the
            globally registered one.
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
        tracer_provider: Any | None = None,
    ):
        if _otel_missing is not None:
            raise ImportError(
                "opentelemetry-api is required for OpenTelemetryMiddleware. "
                "Install it with: pip install flexiq[otel]"
            ) from _otel_missing
        super().__init__(predicate=legacy_task_filter_to_predicate(task_filter, predicate))
        self._tracer = trace.get_tracer(tracer_name, tracer_provider=tracer_provider)
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

        parent = propagate.extract(trace_carrier(ctx.metadata))
        span = self._tracer.start_span(
            self._span_name(ctx),
            context=parent,
            kind=SpanKind.CONSUMER,
            attributes=attributes,
        )
        with self._lock:
            self._spans[ctx.id] = span

    def on_enqueue(self, task_name: str, args: tuple, kwargs: dict, options: dict) -> None:
        carrier: dict[str, str] = {}
        propagate.inject(carrier, context=self._enqueue_context())
        options["metadata"] = merge_trace_carrier(options.get("metadata"), carrier)

    def on_workflow_submit(self, workflow_name: str, options: dict) -> None:
        # A carrier the caller passed explicitly wins, as metadata keys do.
        if options.get("trace_context"):
            return
        carrier: dict[str, str] = {}
        propagate.inject(carrier, context=self._enqueue_context())
        options["trace_context"] = carrier or None

    def _enqueue_context(self) -> Any:
        """The context an enqueue propagates: the caller's own span if it has
        one, else the execute span of the task it runs inside, if any."""
        current = otel_context.get_current()
        if trace.get_current_span(current).get_span_context().is_valid:
            return current
        job = JobContext._active_context()
        if job is None:
            return current
        with self._lock:
            span = self._spans.get(job.job_id)
        return current if span is None else trace.set_span_in_context(span, current)

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
