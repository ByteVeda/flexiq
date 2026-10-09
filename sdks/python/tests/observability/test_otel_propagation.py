"""Trace context crosses the queue: enqueue injects, execution continues.

Real OpenTelemetry SDK spans, not mocks — parentage is the property under test,
and a mock tracer cannot get it wrong.
"""

from __future__ import annotations

import json
import threading
from collections.abc import Callable, Generator
from pathlib import Path
from typing import Any

import pytest
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind

from conftest import join_worker
from flexiq import Queue
from flexiq.contrib.otel import OpenTelemetryMiddleware

PollUntil = Any  # the conftest fixture's runtime type


@pytest.fixture
def exporter() -> InMemorySpanExporter:
    return InMemorySpanExporter()


@pytest.fixture
def provider(exporter: InMemorySpanExporter) -> TracerProvider:
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return provider


@pytest.fixture
def traced(tmp_path: Path, provider: TracerProvider) -> Queue:
    return Queue(
        db_path=str(tmp_path / "traced.db"),
        workers=2,
        middleware=[OpenTelemetryMiddleware(tracer_provider=provider)],
    )


@pytest.fixture
def start_worker(traced: Queue) -> Generator[Callable[[], None]]:
    """Start the worker once a test has enqueued what it needs: a job enqueued
    while the worker runs could be claimed before the test reads it back."""
    threads: list[threading.Thread] = []

    def start() -> None:
        thread = threading.Thread(target=traced.run_worker, daemon=True)
        thread.start()
        threads.append(thread)

    yield start
    if threads:
        traced.shutdown()
        join_worker(threads[0])


def _executions(exporter: InMemorySpanExporter, task: str) -> list[ReadableSpan]:
    return [
        span
        for span in exporter.get_finished_spans()
        if span.name.startswith("flexiq.execute.") and span.name.endswith(task)
    ]


def _traceparent(metadata: str | None) -> str:
    assert metadata is not None, "the job carries no metadata"
    value = json.loads(metadata)["traceparent"]
    assert isinstance(value, str)
    return value


def test_an_enqueue_carries_the_callers_span(traced: Queue, provider: TracerProvider) -> None:
    @traced.task()
    def noop() -> None:
        return None

    with provider.get_tracer("test").start_as_current_span("request") as request:
        job = noop.delay()

    stored = traced.get_job(job.id)
    assert stored is not None
    context = request.get_span_context()
    assert _traceparent(stored.metadata) == (
        f"00-{context.trace_id:032x}-{context.span_id:016x}-{context.trace_flags:02x}"
    )


def test_an_enqueue_outside_any_span_leaves_metadata_alone(traced: Queue) -> None:
    @traced.task()
    def noop() -> None:
        return None

    plain = noop.delay()
    kept = noop.apply_async(metadata="not json")

    assert (stored := traced.get_job(plain.id)) is not None
    assert stored.metadata is None
    assert (stored := traced.get_job(kept.id)) is not None
    assert stored.metadata == "not json"


def test_user_metadata_keeps_its_keys(traced: Queue, provider: TracerProvider) -> None:
    @traced.task()
    def noop() -> None:
        return None

    with provider.get_tracer("test").start_as_current_span("request"):
        job = noop.apply_async(metadata='{"tenant":"acme"}')

    stored = traced.get_job(job.id)
    assert stored is not None and stored.metadata is not None
    metadata = json.loads(stored.metadata)
    assert metadata["tenant"] == "acme"
    assert "traceparent" in metadata


def test_the_execute_span_is_a_child_of_the_enqueuer(
    traced: Queue,
    provider: TracerProvider,
    start_worker: Callable[[], None],
    exporter: InMemorySpanExporter,
    poll_until: PollUntil,
) -> None:
    @traced.task()
    def add(a: int, b: int) -> int:
        return a + b

    with provider.get_tracer("test").start_as_current_span("request") as request:
        job = add.delay(1, 2)
    start_worker()

    assert job.result(timeout=10) == 3
    poll_until(lambda: _executions(exporter, "add"), message="no execute span")
    (execute,) = _executions(exporter, "add")
    assert execute.kind is SpanKind.CONSUMER
    assert execute.parent is not None
    assert execute.parent.span_id == request.get_span_context().span_id
    assert execute.context.trace_id == request.get_span_context().trace_id


def test_an_async_task_continues_the_trace_too(
    traced: Queue,
    provider: TracerProvider,
    start_worker: Callable[[], None],
    exporter: InMemorySpanExporter,
    poll_until: PollUntil,
) -> None:
    @traced.task()
    async def double(n: int) -> int:
        return n * 2

    with provider.get_tracer("test").start_as_current_span("request") as request:
        job = double.delay(4)
    start_worker()

    assert job.result(timeout=10) == 8
    poll_until(lambda: _executions(exporter, "double"), message="no execute span")
    (execute,) = _executions(exporter, "double")
    assert execute.parent is not None
    assert execute.parent.span_id == request.get_span_context().span_id


def test_a_job_enqueued_inside_a_task_continues_its_trace(
    traced: Queue,
    provider: TracerProvider,
    start_worker: Callable[[], None],
    exporter: InMemorySpanExporter,
    poll_until: PollUntil,
) -> None:
    @traced.task()
    def child() -> str:
        return "done"

    @traced.task()
    def parent() -> str:
        return child.delay().id

    with provider.get_tracer("test").start_as_current_span("request") as request:
        job = parent.delay()
    start_worker()

    child_id = job.result(timeout=10)
    assert traced.get_job(child_id) is not None
    poll_until(lambda: _executions(exporter, ".child"), message="no child span")
    (outer,) = _executions(exporter, ".parent")
    (inner,) = _executions(exporter, ".child")
    assert inner.parent is not None
    assert inner.parent.span_id == outer.context.span_id
    assert inner.context.trace_id == request.get_span_context().trace_id


def test_every_attempt_of_a_retried_job_shares_the_parent(
    traced: Queue,
    provider: TracerProvider,
    start_worker: Callable[[], None],
    exporter: InMemorySpanExporter,
    poll_until: PollUntil,
) -> None:
    attempts = 0

    @traced.task(max_retries=1, retry_backoff=0.01)
    def flaky() -> str:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("first attempt fails")
        return "ok"

    with provider.get_tracer("test").start_as_current_span("request") as request:
        job = flaky.delay()
    start_worker()

    assert job.result(timeout=10) == "ok"
    poll_until(lambda: len(_executions(exporter, "flaky")) == 2, message="two attempts")
    parents = {span.parent.span_id for span in _executions(exporter, "flaky") if span.parent}
    assert parents == {request.get_span_context().span_id}


def test_a_replayed_dead_letter_keeps_the_trace(
    traced: Queue,
    provider: TracerProvider,
    start_worker: Callable[[], None],
    poll_until: PollUntil,
) -> None:
    @traced.task(max_retries=0)
    def doomed() -> None:
        raise RuntimeError("always")

    with provider.get_tracer("test").start_as_current_span("request"):
        job = doomed.delay()
    original = traced.get_job(job.id)
    assert original is not None
    start_worker()

    poll_until(lambda: traced.dead_letters(), timeout=10, message="no dead letter")
    replayed = traced.get_job(traced.retry_dead(traced.dead_letters()[0]["id"]))
    assert replayed is not None
    assert _traceparent(replayed.metadata) == _traceparent(original.metadata)
