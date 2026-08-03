"""Tests for the Celery-style ``Task X[id] received|succeeded|raised`` logs."""

from __future__ import annotations

import logging
import re

import pytest

from taskito import Queue
from taskito.exceptions import TaskCancelledError
from taskito.task_lifecycle import _MAX_RESULT_REPR, _safe_result_repr

_RECEIVED = re.compile(r"^Task (\S+)\[(\S+)\] received$")
_SUCCEEDED = re.compile(r"^Task (\S+)\[(\S+)\] succeeded in \d+\.\d{3}s: (.+)$")
_RAISED = re.compile(r"^Task (\S+)\[(\S+)\] raised (\w+) in \d+\.\d{3}s: (.+)$")
_CANCELLED = re.compile(r"^Task (\S+)\[(\S+)\] cancelled in \d+\.\d{3}s: (.+)$")


def _taskito_records(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    return [r for r in caplog.records if r.name == "taskito"]


def _matching(
    caplog: pytest.LogCaptureFixture,
    pattern: re.Pattern[str],
    task_name: str,
    level: int,
) -> list[str]:
    """Lifecycle messages this test's own task produced, at `level`.

    Scoped by task name because ``caplog`` captures the whole process: any worker
    another test left running logs onto the same ``taskito`` logger, and an
    unscoped count then depends on test order and timing.
    """
    return [
        message
        for record in _taskito_records(caplog)
        if record.levelno == level
        for message in [record.getMessage()]
        if (match := pattern.match(message)) is not None and match.group(1) == task_name
    ]


def test_safe_result_repr_truncates_long_values() -> None:
    long = "x" * (_MAX_RESULT_REPR * 2)
    rendered = _safe_result_repr(long)
    assert len(rendered) == _MAX_RESULT_REPR
    assert rendered.endswith("…")


def test_safe_result_repr_handles_none() -> None:
    assert _safe_result_repr(None) == "None"


def test_safe_result_repr_survives_broken_repr() -> None:
    class Boom:
        def __repr__(self) -> str:
            raise RuntimeError("nope")

    assert _safe_result_repr(Boom()) == "<unrepresentable>"


def _invoke_registered(queue: Queue, task_name: str, *args: object, **kwargs: object) -> object:
    """Drive the wrapped task through the same path the Rust worker uses.

    Sets up a job context, calls the registered wrapper, then tears the
    context down — exercising the full ``_wrap_task`` lifecycle (hooks,
    middleware, lifecycle logs) without booting an actual worker.
    """
    from taskito.context import _clear_context, _set_context

    _set_context("job-" + task_name, task_name, 0, "default")
    try:
        return queue._task_registry[task_name](*args, **kwargs)
    finally:
        _clear_context()


def test_received_and_succeeded_logged_for_successful_task(
    queue: Queue, caplog: pytest.LogCaptureFixture
) -> None:
    @queue.task()
    def add(a: int, b: int) -> int:
        return a + b

    caplog.set_level(logging.INFO, logger="taskito")
    assert _invoke_registered(queue, add.name, 2, 3) == 5

    received = _matching(caplog, _RECEIVED, add.name, logging.INFO)
    succeeded = _matching(caplog, _SUCCEEDED, add.name, logging.INFO)

    assert len(received) == 1, received
    assert len(succeeded) == 1, succeeded
    m = _SUCCEEDED.match(succeeded[0])
    assert m is not None
    assert m.group(3) == "5"


def test_raised_logged_at_error_for_failing_task(
    queue: Queue, caplog: pytest.LogCaptureFixture
) -> None:
    @queue.task()
    def boom() -> None:
        raise ValueError("bad input")

    caplog.set_level(logging.INFO, logger="taskito")
    with pytest.raises(ValueError):
        _invoke_registered(queue, boom.name)

    raised = _matching(caplog, _RAISED, boom.name, logging.ERROR)
    assert len(raised) == 1, raised
    m = _RAISED.match(raised[0])
    assert m is not None
    assert m.group(3) == "ValueError"
    assert "bad input" in m.group(4)


def test_cancelled_logged_at_info_for_task_cancelled(
    queue: Queue, caplog: pytest.LogCaptureFixture
) -> None:
    @queue.task()
    def stoppable() -> None:
        raise TaskCancelledError("user pulled the plug")

    caplog.set_level(logging.INFO, logger="taskito")
    with pytest.raises(TaskCancelledError):
        _invoke_registered(queue, stoppable.name)

    cancelled = _matching(caplog, _CANCELLED, stoppable.name, logging.INFO)
    assert len(cancelled) == 1, cancelled

    # A cancel must not also log an error for this task — scoped for the same
    # reason as above, so a stray worker's failure elsewhere cannot fail it.
    errors = [
        record.getMessage()
        for record in _taskito_records(caplog)
        if record.levelno == logging.ERROR and stoppable.name in record.getMessage()
    ]
    assert errors == []
