"""Basic tests for flexiq — enqueue, dequeue, result retrieval."""

import threading
from typing import Any

import pytest

from flexiq import Queue


def test_task_registration(queue: Queue) -> None:
    """Tasks can be registered with the decorator."""

    @queue.task()
    def add(a: int, b: int) -> int:
        return a + b

    assert add.name.endswith("add")
    assert add.name in queue._task_registry


def test_enqueue_returns_job_result(queue: Queue) -> None:
    """Enqueueing a task returns a JobResult handle."""

    @queue.task()
    def noop() -> None:
        pass

    job = noop.delay()
    assert job.id is not None
    assert len(job.id) > 0


def test_task_direct_call(queue: Queue) -> None:
    """Decorated tasks can still be called directly."""

    @queue.task()
    def multiply(a: int, b: int) -> int:
        return a * b

    assert multiply(3, 4) == 12


def test_apply_async_with_delay(queue: Queue) -> None:
    """apply_async accepts a delay parameter."""

    @queue.task()
    def slow_task() -> None:
        pass

    job = slow_task.apply_async(delay=60)
    assert job.id is not None


def test_apply_async_with_overrides(queue: Queue) -> None:
    """apply_async can override default task settings."""

    @queue.task(priority=1, queue="default")
    def configurable_task(x: int) -> int:
        return x

    job = configurable_task.apply_async(
        args=(42,),
        priority=10,
        queue="urgent",
        max_retries=5,
        timeout=600,
    )
    assert job.id is not None


def test_queue_stats(queue: Queue) -> None:
    """stats() returns counts by status."""

    @queue.task()
    def sample_task() -> None:
        pass

    sample_task.delay()
    sample_task.delay()

    stats = queue.stats()
    assert stats["pending"] == 2
    assert stats["running"] == 0


def test_worker_executes_task(queue: Queue) -> None:
    """Worker processes tasks and stores results."""

    @queue.task()
    def add(a: int, b: int) -> int:
        return a + b

    job = add.delay(2, 3)

    # Run worker in a background thread
    worker_thread = threading.Thread(
        target=queue.run_worker,
        daemon=True,
    )
    worker_thread.start()

    # Wait for result
    result = job.result(timeout=10)
    assert result == 5


def test_worker_handles_kwargs(queue: Queue) -> None:
    """Worker correctly passes keyword arguments."""

    @queue.task()
    def greet(name: str, greeting: str = "Hello") -> str:
        return f"{greeting}, {name}!"

    job = greet.delay("World", greeting="Hi")

    worker_thread = threading.Thread(
        target=queue.run_worker,
        daemon=True,
    )
    worker_thread.start()

    result = job.result(timeout=10)
    assert result == "Hi, World!"


def test_worker_none_result(queue: Queue) -> None:
    """Tasks returning None work correctly."""

    @queue.task()
    def void_task() -> None:
        pass

    job = void_task.delay()

    worker_thread = threading.Thread(
        target=queue.run_worker,
        daemon=True,
    )
    worker_thread.start()

    result = job.result(timeout=10)
    assert result is None


def test_re_registering_a_name_drops_options_the_new_task_omits(queue: Queue) -> None:
    """Re-registering a name replaces the task, its options included.

    Options whose absence means "default" live in per-task maps keyed by name.
    Leaving them in place would apply the previous task's configuration to the
    new function, which is neither declaration.
    """

    @queue.task(name="reused", idempotent=True, soft_timeout=5.0)
    def first(n: int) -> int:
        return n

    @queue.task(name="reused")
    def second(n: int) -> int:
        return n

    assert second.delay(1).id != second.delay(1).id
    assert "reused" not in queue._task_soft_timeouts
    assert "reused" not in queue._task_idempotent


def test_re_registering_a_name_leaves_one_task_config(queue: Queue) -> None:
    """Two configs for one name would list the task twice in the override APIs."""

    @queue.task(name="reused")
    def first(n: int) -> int:
        return n

    @queue.task(name="reused", max_retries=9)
    def second(n: int) -> int:
        return n

    configs = [c for c in queue._task_configs if c.name == "reused"]
    assert len(configs) == 1
    assert configs[0].max_retries == 9


def test_a_rejected_re_registration_leaves_the_previous_task_intact(queue: Queue) -> None:
    """A declaration that fails validation must not half-replace the live task.

    Registering resets the name's state before recording the new options, so
    anything that can be rejected has to be rejected first — otherwise the
    error leaves the old function registered with none of its configuration.
    """

    @queue.task(name="fragile", idempotent=True, max_retries=7)
    def original(n: int) -> int:
        return n

    # Deliberately mistyped, one per rejection site: the compensator check,
    # predicate coercion, and PyO3's argument conversion for PyTaskConfig.
    rejected: list[dict[str, Any]] = [
        {"compensates": 123},
        {"predicate": 123},
        {"rate_limit": 123},
    ]
    for bad in rejected:
        with pytest.raises(TypeError):
            queue.task(name="fragile", **bad)(original)

        assert queue._task_idempotent.get("fragile") is True
        configs = [c for c in queue._task_configs if c.name == "fragile"]
        assert len(configs) == 1
        assert configs[0].max_retries == 7
