"""Tests for the central log configuration module."""

from __future__ import annotations

import io
import logging
from collections.abc import Iterator

import pytest

from flexiq import log_config
from flexiq.log_config import (
    _PIPE_CLOSED_FILTER,
    DEFAULT_DATEFMT,
    DEFAULT_FORMAT,
    configure,
    restore_asyncio_pipe_noise,
    silence_asyncio_pipe_noise,
)

_MANAGED_LOGGERS = (
    "flexiq",
    "flexiq_core",
    "flexiq_python",
    "flexiq_async",
    "flexiq_workflows",
)


@pytest.fixture(autouse=True)
def _reset_flexiq_loggers() -> Iterator[None]:
    """Strip any handlers/state on the flexiq loggers between tests.

    The module keeps a process-wide cache so tests must isolate themselves
    from each other (and from anything an earlier import may have set up).
    """
    log_config._LAST_CONFIG = None
    for name in _MANAGED_LOGGERS:
        logger = logging.getLogger(name)
        logger.handlers.clear()
        logger.setLevel(logging.NOTSET)
        logger.propagate = True
    yield
    log_config._LAST_CONFIG = None
    for name in _MANAGED_LOGGERS:
        logger = logging.getLogger(name)
        logger.handlers.clear()
        logger.setLevel(logging.NOTSET)
        logger.propagate = True


def test_configure_attaches_handler_to_all_known_loggers() -> None:
    stream = io.StringIO()
    configure(level="DEBUG", stream=stream)

    for name in _MANAGED_LOGGERS:
        logger = logging.getLogger(name)
        managed = [h for h in logger.handlers if getattr(h, "_flexiq_managed", False)]
        assert len(managed) == 1, f"{name} should have exactly one managed handler"
        assert logger.level == logging.DEBUG


def test_configure_is_idempotent_for_identical_calls() -> None:
    stream = io.StringIO()
    configure(level="INFO", stream=stream)
    handler_before = logging.getLogger("flexiq").handlers[0]

    configure(level="INFO", stream=stream)
    handler_after = logging.getLogger("flexiq").handlers[0]

    assert handler_before is handler_after, "no-op call must not rebuild the handler"
    assert len(logging.getLogger("flexiq").handlers) == 1


def test_configure_swaps_handler_when_level_changes() -> None:
    stream = io.StringIO()
    configure(level="INFO", stream=stream)
    first = logging.getLogger("flexiq").handlers[0]

    configure(level="DEBUG", stream=stream)
    second = logging.getLogger("flexiq").handlers[0]

    assert first is not second
    assert logging.getLogger("flexiq").level == logging.DEBUG
    # Still exactly one managed handler — the old one was detached.
    assert len(logging.getLogger("flexiq").handlers) == 1


def test_configure_leaves_caller_handlers_alone() -> None:
    stream = io.StringIO()
    foreign = logging.StreamHandler(io.StringIO())
    logging.getLogger("flexiq").addHandler(foreign)

    configure(level="INFO", stream=stream)
    handlers = logging.getLogger("flexiq").handlers
    assert foreign in handlers, "caller-installed handlers must be preserved"
    managed = [h for h in handlers if getattr(h, "_flexiq_managed", False)]
    assert len(managed) == 1


def test_level_resolution_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FLEXIQ_LOG_LEVEL", "WARNING")
    configure()
    assert logging.getLogger("flexiq").level == logging.WARNING


def test_level_resolution_falls_back_to_info(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FLEXIQ_LOG_LEVEL", raising=False)
    configure()
    assert logging.getLogger("flexiq").level == logging.INFO


def test_default_format_emits_am_pm_timestamp() -> None:
    stream = io.StringIO()
    configure(level="INFO", stream=stream)

    logging.getLogger("flexiq").info("hello")
    output = stream.getvalue()

    assert "INFO" in output
    assert "hello" in output
    assert ("AM" in output) or ("PM" in output), "default datefmt must include AM/PM marker"


def test_format_constants_are_stable() -> None:
    # Catch accidental drift in the default format strings — these are part of
    # the public surface (downstream tools that grep logs may rely on them).
    assert DEFAULT_FORMAT == "[%(asctime)s] %(levelname)s %(message)s"
    assert "%p" in DEFAULT_DATEFMT


# --- asyncio pipe-noise filter ------------------------------------------------


@pytest.fixture
def _cleanup_pipe_filter() -> Iterator[None]:
    """Ensure each filter test starts and ends with the filter detached."""
    restore_asyncio_pipe_noise()
    yield
    restore_asyncio_pipe_noise()


def _asyncio_record(message: str, level: int = logging.WARNING) -> logging.LogRecord:
    return logging.LogRecord(
        name="asyncio",
        level=level,
        pathname=__file__,
        lineno=0,
        msg=message,
        args=None,
        exc_info=None,
    )


def test_pipe_filter_demotes_spurious_warning_to_debug() -> None:
    record = _asyncio_record("pipe closed by peer or os.write(pipe, data) raised exception.")

    assert _PIPE_CLOSED_FILTER.filter(record) is True
    assert record.levelno == logging.DEBUG
    assert record.levelname == "DEBUG"


def test_pipe_filter_passes_other_asyncio_warnings_unchanged() -> None:
    record = _asyncio_record("socket.send() raised exception.")

    assert _PIPE_CLOSED_FILTER.filter(record) is True
    assert record.levelno == logging.WARNING
    assert record.levelname == "WARNING"


def test_pipe_filter_does_not_touch_non_warning_records() -> None:
    record = _asyncio_record(
        "pipe closed by peer or os.write(pipe, data) raised exception.",
        level=logging.ERROR,
    )

    assert _PIPE_CLOSED_FILTER.filter(record) is True
    assert record.levelno == logging.ERROR


def test_silence_and_restore_are_idempotent(_cleanup_pipe_filter: None) -> None:
    asyncio_logger = logging.getLogger("asyncio")

    silence_asyncio_pipe_noise()
    silence_asyncio_pipe_noise()
    assert asyncio_logger.filters.count(_PIPE_CLOSED_FILTER) == 1

    restore_asyncio_pipe_noise()
    restore_asyncio_pipe_noise()
    assert _PIPE_CLOSED_FILTER not in asyncio_logger.filters


def test_silence_suppresses_warning_end_to_end(
    caplog: pytest.LogCaptureFixture,
    _cleanup_pipe_filter: None,
) -> None:
    asyncio_logger = logging.getLogger("asyncio")
    silence_asyncio_pipe_noise()

    with caplog.at_level(logging.WARNING, logger="asyncio"):
        asyncio_logger.warning("pipe closed by peer or os.write(pipe, data) raised exception.")
        asyncio_logger.warning("a different asyncio warning")

    warning_messages = [r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING]
    assert warning_messages == ["a different asyncio warning"]
