"""A workflow's steps join the trace of whoever submitted it.

The submit's context is stored on the run, and every node job — pre-enqueued,
released later by the tracker, fanned out, or compensating — carries it, so each
step's execute span is a child of the submit span (flat, not chained).
"""

from __future__ import annotations

import json
import threading
from collections.abc import Callable, Generator
from contextlib import AbstractContextManager, contextmanager
from pathlib import Path
from typing import Any

import pytest
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import Span

from conftest import join_worker
from flexiq import Queue
from flexiq.contrib.otel import OpenTelemetryMiddleware
from flexiq.workflows import Workflow, WorkflowState

PollUntil = Any  # the conftest fixture's runtime type
WorkerFactory = Callable[[], AbstractContextManager[threading.Thread]]

TRACEPARENT = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"


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
def worker(traced: Queue) -> WorkerFactory:
    @contextmanager
    def run() -> Generator[threading.Thread]:
        thread = threading.Thread(target=traced.run_worker, daemon=True)
        thread.start()
        try:
            yield thread
        finally:
            traced.shutdown()
            join_worker(thread)

    return run


def _executions(exporter: InMemorySpanExporter, task: str) -> list[ReadableSpan]:
    return [
        span
        for span in exporter.get_finished_spans()
        if span.name.startswith("flexiq.execute.") and span.name.endswith(task)
    ]


def _assert_children_of(spans: list[ReadableSpan], submit: Span) -> None:
    context = submit.get_span_context()
    for span in spans:
        assert span.context.trace_id == context.trace_id, span.name
        assert span.parent is not None and span.parent.span_id == context.span_id, span.name


def _node_metadata(queue: Queue, run_id: str) -> dict[str, dict[str, Any]]:
    statuses = queue._inner.get_workflow_run_status(run_id).node_statuses()
    metadata: dict[str, dict[str, Any]] = {}
    for name, info in statuses.items():
        job = queue.get_job(info["job_id"])
        assert job is not None and job.metadata is not None
        metadata[name] = json.loads(job.metadata)
    return metadata


def test_every_step_is_a_child_of_the_submit_span(
    traced: Queue,
    provider: TracerProvider,
    worker: WorkerFactory,
    exporter: InMemorySpanExporter,
    poll_until: PollUntil,
) -> None:
    @traced.task()
    def first() -> int:
        return 1

    @traced.task()
    def second() -> int:
        return 2

    wf = Workflow(name="traced_linear")
    wf.step("a", first)
    wf.step("b", second, after="a")

    with worker():
        with provider.get_tracer("test").start_as_current_span("submit") as submit:
            run = traced.submit_workflow(wf)
        assert run.wait(timeout=20).state == WorkflowState.COMPLETED
        poll_until(
            lambda: (
                len(_executions(exporter, "first")) == 1
                and len(_executions(exporter, "second")) == 1
            ),
            message="missing linear execute span",
        )

    assert len(_executions(exporter, "first")) == 1
    assert len(_executions(exporter, "second")) == 1
    _assert_children_of(_executions(exporter, "first") + _executions(exporter, "second"), submit)


def test_a_fan_out_child_and_a_released_successor_join_the_trace(
    traced: Queue,
    provider: TracerProvider,
    worker: WorkerFactory,
    exporter: InMemorySpanExporter,
    poll_until: PollUntil,
) -> None:
    @traced.task()
    def source() -> list[int]:
        return [1, 2]

    @traced.task()
    def double(x: int) -> int:
        return x * 2

    @traced.task()
    def collect(results: list[int]) -> int:
        return sum(results)

    wf = Workflow(name="traced_fan_out")
    wf.step("fetch", source)
    wf.step("process", double, after="fetch", fan_out="each")
    wf.step("gather", collect, after="process", fan_in="all")

    with worker():
        with provider.get_tracer("test").start_as_current_span("submit") as submit:
            run = traced.submit_workflow(wf)
        assert run.wait(timeout=20).state == WorkflowState.COMPLETED
        poll_until(lambda: _executions(exporter, "collect"), message="no execute span")

    children = _executions(exporter, "double")
    assert len(children) == 2
    _assert_children_of(children + _executions(exporter, "collect"), submit)


def test_a_compensation_joins_the_submitters_trace(
    traced: Queue,
    provider: TracerProvider,
    worker: WorkerFactory,
    exporter: InMemorySpanExporter,
    poll_until: PollUntil,
) -> None:
    @traced.task(max_retries=0)
    def undo(args: tuple, kwargs: dict, result: object) -> None:
        return None

    @traced.task(max_retries=0, compensates=undo)
    def charge() -> None:
        return None

    @traced.task(max_retries=0)
    def ship() -> None:
        raise RuntimeError("boom")

    wf = Workflow(name="traced_saga")
    wf.step("charge", charge)
    wf.step("ship", ship, after="charge")

    with worker():
        with provider.get_tracer("test").start_as_current_span("submit") as submit:
            run = traced.submit_workflow(wf)
        assert run.wait(timeout=30).state == WorkflowState.COMPENSATED
        poll_until(lambda: _executions(exporter, "undo"), message="no compensation span")

    _assert_children_of(_executions(exporter, "undo"), submit)


def test_an_explicit_trace_context_reaches_every_node_job(queue: Queue) -> None:
    @queue.task()
    def noop() -> None:
        return None

    wf = Workflow(name="explicit_trace")
    wf.step("a", noop)
    wf.step("b", noop, after="a")

    run = queue.submit_workflow(
        wf, trace_context={"traceparent": TRACEPARENT, "tracestate": "vendor=a"}
    )

    for name, metadata in _node_metadata(queue, run.id).items():
        assert metadata["workflow_run_id"] == run.id
        assert metadata["workflow_node_name"] == name
        assert metadata["traceparent"] == TRACEPARENT
        assert metadata["tracestate"] == "vendor=a"


def test_an_explicit_trace_context_wins_over_the_middleware(
    traced: Queue, provider: TracerProvider
) -> None:
    @traced.task()
    def noop() -> None:
        return None

    wf = Workflow(name="explicit_wins")
    wf.step("a", noop)

    with provider.get_tracer("test").start_as_current_span("submit"):
        run = traced.submit_workflow(wf, trace_context={"traceparent": TRACEPARENT})

    assert _node_metadata(traced, run.id)["a"]["traceparent"] == TRACEPARENT


def test_a_submit_outside_any_span_carries_routing_keys_only(traced: Queue) -> None:
    @traced.task()
    def noop() -> None:
        return None

    wf = Workflow(name="untraced")
    wf.step("a", noop)

    run = traced.submit_workflow(wf)

    assert set(_node_metadata(traced, run.id)["a"]) == {
        "workflow_run_id",
        "workflow_node_name",
    }
