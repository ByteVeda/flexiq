"""Test mode for flexiq — run tasks synchronously without a worker.

Usage::

    with queue.test_mode() as results:
        my_task.delay(42)
        assert len(results) == 1
        assert results[0].return_value == expected

    # Or use the context manager directly:
    from flexiq.testing import TestMode

    with TestMode(queue) as results:
        my_task.delay("hello")
        assert results[0].task_name == "mymodule.my_task"
"""

from __future__ import annotations

import traceback
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any
from unittest.mock import patch

from flexiq.context import _clear_context, _set_context
from flexiq.resources.runtime import ResourceRuntime

if TYPE_CHECKING:
    from flexiq.app import Queue


@dataclass
class TestResult:
    """Result of a task executed in test mode."""

    job_id: str
    task_name: str
    args: tuple
    kwargs: dict
    return_value: Any = None
    error: Exception | None = None
    traceback: str | None = None

    @property
    def succeeded(self) -> bool:
        return self.error is None

    @property
    def failed(self) -> bool:
        return self.error is not None


class TestResults(list):
    """List of TestResult with convenience query methods."""

    def filter(self, task_name: str | None = None, succeeded: bool | None = None) -> TestResults:
        out = TestResults()
        for r in self:
            if task_name is not None and r.task_name != task_name:
                continue
            if succeeded is not None and r.succeeded != succeeded:
                continue
            out.append(r)
        return out

    @property
    def succeeded(self) -> TestResults:
        return self.filter(succeeded=True)

    @property
    def failed(self) -> TestResults:
        return self.filter(succeeded=False)


class MockResource:
    """Test double for a worker resource with optional call tracking.

    Usage::

        mock_redis = MockResource("redis", return_value=FakeRedis())
        spy_db = MockResource("db", wraps=real_db, track_calls=True)

        with queue.test_mode(resources={"redis": mock_redis, "db": spy_db}):
            my_task.delay(data)
            assert spy_db.call_count == 1
    """

    def __init__(
        self,
        name: str,
        return_value: Any = None,
        wraps: Any = None,
        track_calls: bool = False,
    ) -> None:
        self.name = name
        self._return_value = return_value
        self._wraps = wraps
        self._track_calls = track_calls
        self.call_count = 0
        self.calls: list[tuple[str, ...]] = []

    def get(self) -> Any:
        """Return the mock resource value."""
        if self._track_calls:
            self.call_count += 1
        if self._wraps is not None:
            return self._wraps
        return self._return_value


class TestMode:
    """Context manager that intercepts enqueue() to run tasks synchronously.

    Tasks execute eagerly in the calling thread — no worker, no Rust, no SQLite.
    Chain, group, and chord work correctly because they call enqueue() internally.
    """

    def __init__(
        self,
        queue: Queue,
        propagate_errors: bool = False,
        resources: dict[str, Any] | None = None,
    ):
        """
        Args:
            queue: The Queue instance to put into test mode.
            propagate_errors: If True, re-raise task exceptions immediately
                instead of capturing them in TestResult.error.
            resources: Dict of resource name → mock instance or MockResource
                for injection.
        """
        self._queue = queue
        self._propagate = propagate_errors
        self._resources = resources
        self._results = TestResults()
        self._patches: list[Any] = []
        self._job_counter = 0
        self._prev_runtime: Any = None
        self._mock_resources: dict[str, MockResource] = {}

    def __enter__(self) -> TestResults:
        # Set test mode flag
        self._queue._test_mode_active = True

        # Set up test resource runtime if resources provided
        if self._resources is not None:
            self._prev_runtime = self._queue._resource_runtime
            resolved: dict[str, Any] = {}
            for name, value in self._resources.items():
                if isinstance(value, MockResource):
                    resolved[name] = value.get()
                    self._mock_resources[name] = value
                else:
                    resolved[name] = value
            self._queue._resource_runtime = ResourceRuntime.from_test_overrides(resolved)

        def test_enqueue(
            task_name: str,
            args: tuple = (),
            kwargs: dict | None = None,
            **enqueue_kwargs: Any,
        ) -> Any:
            return self._execute_task(task_name, args, kwargs or {}, enqueue_kwargs)

        p = patch.object(self._queue, "enqueue", side_effect=test_enqueue)
        self._patches.append(p)
        p.start()
        return self._results

    def __exit__(self, *exc_info: Any) -> None:
        for p in self._patches:
            p.stop()
        self._patches.clear()

        # Clear test mode flag
        self._queue._test_mode_active = False

        # Restore previous resource runtime
        if self._resources is not None:
            self._queue._resource_runtime = self._prev_runtime
            self._prev_runtime = None
            self._mock_resources.clear()

    def _execute_task(
        self,
        task_name: str,
        args: tuple,
        kwargs: dict,
        enqueue_kwargs: dict,
    ) -> Any:
        self._job_counter += 1
        job_id = f"test-{self._job_counter:06d}"

        fn = self._queue._task_registry.get(task_name)
        if fn is None:
            raise KeyError(f"Task '{task_name}' not found in registry")

        # Set up context so current_job works inside tasks
        queue_name = enqueue_kwargs.get("queue", "default")
        _set_context(
            job_id=job_id,
            task_name=task_name,
            retry_count=0,
            queue_name=queue_name if queue_name else "default",
        )

        result = TestResult(
            job_id=job_id,
            task_name=task_name,
            args=args,
            kwargs=kwargs,
        )

        try:
            ret = fn(*args, **kwargs)
            result.return_value = ret
        except Exception as exc:
            result.error = exc
            result.traceback = traceback.format_exc()
            if self._propagate:
                raise
        finally:
            _clear_context()

        self._results.append(result)

        # Return a fake JobResult-like object so chains/groups work
        return _FakeJobResult(result)


class _FakeJobResult:
    """Minimal stand-in for JobResult during test mode."""

    def __init__(self, test_result: TestResult):
        self._tr = test_result

    @property
    def id(self) -> str:
        return self._tr.job_id

    @property
    def status(self) -> str:
        return "complete" if self._tr.succeeded else "failed"

    @property
    def error(self) -> str | None:
        return str(self._tr.error) if self._tr.error else None

    @property
    def progress(self) -> int | None:
        return 100 if self._tr.succeeded else None

    def result(
        self,
        timeout: float = 30.0,
        poll_interval: float = 0.05,
        max_poll_interval: float = 0.5,
    ) -> Any:
        if self._tr.error:
            raise RuntimeError(f"Job {self._tr.job_id} failed: {self._tr.error}")
        return self._tr.return_value

    async def aresult(
        self,
        timeout: float = 30.0,
        poll_interval: float = 0.05,
        max_poll_interval: float = 0.5,
    ) -> Any:
        return self.result(timeout=timeout)

    def refresh(self) -> None:
        pass

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self._tr.job_id,
            "task_name": self._tr.task_name,
            "status": self.status,
            "error": self.error,
        }

    def __repr__(self) -> str:
        return f"<_FakeJobResult id={self._tr.job_id} status={self.status}>"
