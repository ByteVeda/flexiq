"""Central log configuration for flexiq.

A single entry point — :func:`configure` — owns the ``flexiq`` Python logger
and the parallel logger names that PyO3-log uses for Rust crates
(``flexiq_core``, ``flexiq_python``, ``flexiq_async``, ``flexiq_workflows``).
Calling it more than once is a no-op when the resolved settings haven't
changed; passing different arguments swaps the managed handler in place
rather than stacking handlers.

The Rust side is bridged into Python's :mod:`logging` by ``pyo3_log::try_init``
inside the ``_flexiq`` extension module init, so a single configure call
captures Rust ``log::*`` output too.
"""

from __future__ import annotations

import logging
import os
import sys
import threading
from typing import TextIO

from flexiq._flexiq import _init_rust_logging

__all__ = [
    "DEFAULT_DATEFMT",
    "DEFAULT_FORMAT",
    "configure",
    "restore_asyncio_pipe_noise",
    "silence_asyncio_pipe_noise",
]

DEFAULT_FORMAT = "[%(asctime)s] %(levelname)s %(message)s"
DEFAULT_DATEFMT = "%Y-%m-%d %I:%M:%S %p"

# Logger names that flexiq emits under. The Python logger is well-known;
# the Rust ones are derived from cargo crate names (``_`` form) by pyo3-log.
_PY_LOGGER = "flexiq"
_RUST_LOGGERS = (
    "flexiq_core",
    "flexiq_python",
    "flexiq_async",
    "flexiq_workflows",
)
_ALL_LOGGERS = (_PY_LOGGER, *_RUST_LOGGERS)

# Sentinel attribute set on a handler we manage, so we never double-attach
# and we leave caller-installed handlers untouched.
_OWN_HANDLER_ATTR = "_flexiq_managed"

# Guards concurrent calls to ``configure`` from multiple threads (CLI startup
# vs. ``Queue.run_worker`` background thread).
_CONFIG_LOCK = threading.Lock()

# Cached signature of the last successful configure() call, so identical
# repeat calls short-circuit instead of churning handlers.
_LAST_CONFIG: tuple[int, int] | None = None


def _resolve_level(level: int | str | None) -> int:
    """Resolve a level argument or ``FLEXIQ_LOG_LEVEL`` env var to a logging int."""
    if level is None:
        level = os.environ.get("FLEXIQ_LOG_LEVEL", "INFO")
    if isinstance(level, str):
        try:
            return int(level)
        except ValueError:
            pass
        resolved = logging.getLevelName(level.upper())
        if isinstance(resolved, int):
            return resolved
        return logging.INFO
    return level


def _build_handler(stream: TextIO) -> logging.Handler:
    handler = logging.StreamHandler(stream)
    handler.setFormatter(logging.Formatter(DEFAULT_FORMAT, datefmt=DEFAULT_DATEFMT))
    setattr(handler, _OWN_HANDLER_ATTR, True)
    return handler


def _detach_managed(logger: logging.Logger) -> None:
    for existing in list(logger.handlers):
        if getattr(existing, _OWN_HANDLER_ATTR, False):
            logger.removeHandler(existing)


def configure(
    level: int | str | None = None,
    *,
    stream: TextIO | None = None,
) -> logging.Logger:
    """Configure the central ``flexiq`` logger and its Rust counterparts.

    Idempotent: identical repeat calls are no-ops; argument changes swap the
    managed handler in place rather than stacking handlers. Caller-installed
    handlers on the same loggers are left intact.

    Args:
        level: Logging level as an int (e.g. ``logging.DEBUG``) or string
            (``"INFO"``). Falls back to ``FLEXIQ_LOG_LEVEL``, then ``INFO``.
        stream: Where to write log lines. Defaults to ``sys.stderr``.

    Returns:
        The configured ``flexiq`` Python logger.
    """
    global _LAST_CONFIG

    resolved_level = _resolve_level(level)
    target_stream = stream if stream is not None else sys.stderr
    signature = (resolved_level, id(target_stream))

    with _CONFIG_LOCK:
        if signature == _LAST_CONFIG:
            return logging.getLogger(_PY_LOGGER)

        # Activate the Rust → Python log bridge once. Doing this here (rather
        # than from `_flexiq` module init) avoids a deadlock during cold
        # imports where the GIL is held by a blocking connection-pool retry.
        _init_rust_logging()

        handler = _build_handler(target_stream)
        for name in _ALL_LOGGERS:
            logger = logging.getLogger(name)
            _detach_managed(logger)
            logger.addHandler(handler)
            logger.setLevel(resolved_level)
            # Leave `propagate` alone. Embedded hosts (Django, FastAPI) that
            # already have a root handler can set `propagate = False` on the
            # `flexiq` logger themselves to avoid duplicate lines; flipping
            # it here would silently break pytest `caplog` and any other
            # consumer that listens at the root.

        _LAST_CONFIG = signature
        return logging.getLogger(_PY_LOGGER)


# --- asyncio shutdown noise suppression ---------------------------------------
#
# When a flexiq worker receives SIGINT (Ctrl+C in a terminal), the kernel
# broadcasts the signal to every process in the foreground process group,
# including subprocesses the user task spawned (Playwright browsers, ffmpeg,
# any asyncio.create_subprocess_exec child). Those subprocesses exit and close
# their stdin pipes. Asyncio's _UnixWritePipeTransport.write() silently
# tolerates the first 5 failed writes, then emits a WARNING on every write
# thereafter — flooding the drain window with messages whose underlying error
# was already lost. The filter demotes that specific record to DEBUG so it
# stops drowning out the legitimate "Warm shutdown" / "Worker stopped" lines,
# while still being recoverable by anyone running the asyncio logger at DEBUG.


_ASYNCIO_LOGGER_NAME = "asyncio"


class _AsyncioPipeClosedFilter(logging.Filter):
    """Demote asyncio's spurious 'pipe closed by peer' WARNING to DEBUG."""

    _NEEDLE = "pipe closed by peer or os.write"

    def filter(self, record: logging.LogRecord) -> bool:
        if record.levelno == logging.WARNING and self._NEEDLE in record.getMessage():
            record.levelno = logging.DEBUG
            record.levelname = "DEBUG"
        return True


_PIPE_CLOSED_FILTER = _AsyncioPipeClosedFilter()


def silence_asyncio_pipe_noise() -> None:
    """Demote the spurious asyncio 'pipe closed by peer' warning to DEBUG.

    Idempotent — attaches the filter only once per process. Pair with
    :func:`restore_asyncio_pipe_noise` when the noisy window ends so unrelated
    asyncio activity is unaffected.
    """
    logger = logging.getLogger(_ASYNCIO_LOGGER_NAME)
    if _PIPE_CLOSED_FILTER not in logger.filters:
        logger.addFilter(_PIPE_CLOSED_FILTER)


def restore_asyncio_pipe_noise() -> None:
    """Detach the filter installed by :func:`silence_asyncio_pipe_noise`.

    Safe to call when the filter is not attached.
    """
    logging.getLogger(_ASYNCIO_LOGGER_NAME).removeFilter(_PIPE_CLOSED_FILTER)
