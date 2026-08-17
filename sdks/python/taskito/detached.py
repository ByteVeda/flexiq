"""Running a ``Queue`` with no storage behind it.

An attached executor exists so the app image needs no database credentials:
the scheduler holds the connection and dispatches over a socket, and everything
a task needs to run arrives on the wire. But an executor still imports the
user's app module to find its handlers, and that module builds a ``Queue`` —
which would otherwise open storage the moment it is constructed, putting the
credentials right back in the app image.

So in an executor the native queue is replaced by :class:`DetachedNative`. Task
execution never touches storage, so nothing on the hot path notices. The
operations that genuinely need a database fail loudly rather than silently
doing nothing, because an enqueue that quietly vanished would be worse than one
that raised. Reads are the exception: ``None`` is what a queue with no such row
returns anyway, and ``Queue.__init__`` performs some.

The same split shows up in the job a handler receives. A dispatch frame carries
what running the task needs, so ``created_at``, ``scheduled_at``, ``priority``,
``metadata``, ``unique_key`` and ``notes`` arrive as zeros and ``None`` on an
executor where an in-process worker would show the stored values. A task that
needs them wants a worker, not an executor.

Set by ``taskito executor`` before it imports the app, and inherited by the
prefork children it spawns. Internal: applications should not set it.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Sequence
from typing import NoReturn, Protocol

logger = logging.getLogger("taskito.executor")

__all__ = [
    "DETACHED_ENV",
    "DetachedNative",
    "DetachedStorageError",
    "ExecutorSink",
    "clear_sink",
    "disabled_middleware",
    "install_sink",
    "is_detached",
    "set_disabled_middleware",
]

#: Marks this process as an executor, so a ``Queue`` built here opens no storage.
DETACHED_ENV = "FLEXIQ_DETACHED_EXECUTOR"


class ExecutorSink(Protocol):
    """Where an executor's storage-shaped writes go instead of a database.

    Implemented by the prefork child, which frames them to its parent; the
    parent relays them to the scheduler, which owns the connection. Both
    methods are fire-and-forget: a task reporting progress must not be able to
    fail, or block, because of what is happening at the far end.
    """

    def update_progress(self, job_id: str, progress: int) -> None: ...

    def write_task_log(
        self,
        job_id: str,
        task_name: str,
        level: str,
        message: str,
        extra: str | None,
    ) -> None: ...


#: Installed by the prefork child once it is running under an executor.
_sink: ExecutorSink | None = None

#: Middleware disabled for the job this process is running, as resolved by the
#: scheduler and carried on the dispatch frame. A prefork child runs one job at
#: a time, so a single value is the whole story.
_disabled_middleware: tuple[str, ...] = ()


def install_sink(sink: ExecutorSink) -> None:
    """Route this process's progress and task logs through ``sink``."""
    global _sink
    _sink = sink


def clear_sink() -> None:
    """Stop routing writes, so they degrade to a warning again."""
    global _sink
    _sink = None


def set_disabled_middleware(disabled: Sequence[str]) -> None:
    """Record the toggle list the scheduler attached to the current dispatch."""
    global _disabled_middleware
    _disabled_middleware = tuple(disabled)


def disabled_middleware() -> tuple[str, ...]:
    """Middleware disabled for the job running in this process."""
    return _disabled_middleware


class DetachedStorageError(RuntimeError, AttributeError):
    """An executor was asked for something only a database could answer.

    Deliberately both: ``RuntimeError`` so it reads as the operational fault it
    is, and ``AttributeError`` so a capability probe (``hasattr(queue._inner,
    "submit_workflow")``) answers "no" instead of exploding. A detached queue
    genuinely does not have those capabilities.
    """


def is_detached() -> bool:
    """Whether this process runs task bodies without any storage of its own."""
    return os.environ.get(DETACHED_ENV) == "1"


class DetachedNative:
    """Stands in for the native queue in an executor.

    Degrades the three job-scoped conveniences that only observability depends
    on, and refuses everything else. A task calling ``update_progress`` must not
    fail merely because it happens to be running detached, but a task calling
    ``enqueue`` must not appear to succeed.

    Progress and task logs are forwarded to the installed :class:`ExecutorSink`
    rather than dropped: the executor has no storage, but the scheduler does,
    and it applies them on this process's behalf. Without a sink — an app
    importing itself outside ``taskito executor``, or a scheduler that
    advertised no side-channel — they degrade to one warning, as they did
    before the side-channel existed.
    """

    __slots__ = ("_warned",)

    def __init__(self) -> None:
        # One warning per process, not per call: a progress-reporting loop would
        # otherwise bury the log it is trying to be useful in.
        self._warned: set[str] = set()

    def _warn_once(self, what: str) -> None:
        if what not in self._warned:
            self._warned.add(what)
            logger.warning(
                "%s is unavailable on an attached executor with no side-channel to the "
                "scheduler; ignoring. Run an in-process worker if you need it.",
                what,
            )

    def update_progress(self, job_id: str, progress: int) -> None:
        """Reported to the scheduler, which owns the storage this lives in."""
        if _sink is None:
            self._warn_once("update_progress")
            return
        _sink.update_progress(job_id, progress)

    def write_task_log(
        self,
        job_id: str,
        task_name: str,
        level: str,
        message: str,
        extra: str | None = None,
    ) -> None:
        """Reported to the scheduler. A published partial arrives as ``result``."""
        if _sink is None:
            self._warn_once("current_job.log/publish")
            return
        _sink.write_task_log(job_id, task_name, level, message, extra)

    def is_cancel_requested(self, job_id: str) -> bool:
        """Always false: a cancel reaches an executor as a protocol frame.

        ``check_cancelled`` consults the local signal the prefork child installs
        before it ever gets here, so answering false loses nothing.
        """
        return False

    def is_migrated(self) -> bool:
        """An executor has no storage, so nothing about its schema is pending.

        Answers rather than raising for the same reason the reads below do:
        ``Queue.__init__`` consults it, and an executor must still be able to
        build a Queue.
        """
        return True

    def get_setting(self, key: str) -> None:
        """No settings without storage.

        Reads degrade, writes do not: ``None`` is exactly what a queue with no
        such setting returns, so callers already handle it. ``Queue.__init__``
        reads settings — webhook subscriptions, dashboard overrides — so this
        has to answer rather than raise, or an executor could not build a Queue
        at all.
        """
        return None

    def __getattr__(self, name: str) -> NoReturn:
        raise DetachedStorageError(
            f"'{name}' needs a database, and an attached executor has none. "
            "Only running tasks is supported here — the scheduler owns storage. "
            "Use an in-process worker (`taskito worker`) if this app needs to "
            "reach the queue itself."
        )

    def __repr__(self) -> str:
        return "<DetachedNative: an executor's storage-free queue>"
