"""Tests for task dependency feature."""

import threading

import pytest

from flexiq import Queue


def test_enqueue_with_depends_on(queue: Queue) -> None:
    """Jobs can declare dependencies on other jobs."""

    @queue.task()
    def step(x: int) -> int:
        return x

    job_a = step.delay(1)
    job_b = step.apply_async(args=(2,), depends_on=job_a.id)

    assert job_b.dependencies == [job_a.id]
    assert job_a.dependents == [job_b.id]


def test_enqueue_with_multiple_deps(queue: Queue) -> None:
    """Jobs can depend on multiple other jobs."""

    @queue.task()
    def step(x: int) -> int:
        return x

    job_a = step.delay(1)
    job_b = step.delay(2)
    job_c = step.apply_async(args=(3,), depends_on=[job_a.id, job_b.id])

    deps = set(job_c.dependencies)
    assert deps == {job_a.id, job_b.id}


def test_depends_on_string_coercion(queue: Queue) -> None:
    """depends_on accepts a single string ID."""

    @queue.task()
    def step(x: int) -> int:
        return x

    job_a = step.delay(1)
    # Pass as single string, not list
    job_b = queue.enqueue(
        task_name=step.name,
        args=(2,),
        depends_on=job_a.id,
    )

    assert job_b.dependencies == [job_a.id]


def test_dependency_blocks_execution(queue: Queue) -> None:
    """Dependent job waits until dependency completes."""

    @queue.task()
    def step(x: int) -> int:
        return x * 10

    job_a = step.delay(1)
    job_b = step.apply_async(args=(2,), depends_on=job_a.id)

    worker_thread = threading.Thread(target=queue.run_worker, daemon=True)
    worker_thread.start()

    # Both should complete — job_a first, then job_b
    result_a = job_a.result(timeout=10)
    result_b = job_b.result(timeout=10)

    assert result_a == 10
    assert result_b == 20


def test_cascade_cancel_on_job_cancel(queue: Queue) -> None:
    """Cancelling a job cascades to its dependents."""

    @queue.task()
    def step(x: int) -> int:
        return x

    job_a = step.delay(1)
    job_b = step.apply_async(args=(2,), depends_on=job_a.id)
    job_c = step.apply_async(args=(3,), depends_on=job_b.id)

    queue.cancel_job(job_a.id)

    job_b.refresh()
    job_c.refresh()
    assert job_b.status == "cancelled"
    assert job_c.status == "cancelled"


def test_no_dependencies_property_when_none(queue: Queue) -> None:
    """Jobs without dependencies return empty list."""

    @queue.task()
    def step(x: int) -> int:
        return x

    job = step.delay(1)
    assert job.dependencies == []
    assert job.dependents == []


def test_enqueue_rejects_missing_dependency(queue: Queue) -> None:
    """Enqueuing with a nonexistent dependency raises an error."""

    @queue.task()
    def step(x: int) -> int:
        return x

    with pytest.raises(RuntimeError):
        step.apply_async(args=(1,), depends_on="nonexistent-id")
