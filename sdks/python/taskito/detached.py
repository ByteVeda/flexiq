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
from typing import NoReturn

logger = logging.getLogger("taskito.executor")

__all__ = ["DETACHED_ENV", "DetachedNative", "DetachedStorageError", "is_detached"]

#: Marks this process as an executor, so a ``Queue`` built here opens no storage.
DETACHED_ENV = "TASKITO_DETACHED_EXECUTOR"


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
                "%s is unavailable on an attached executor, which has no storage; "
                "ignoring. Run an in-process worker if you need it.",
                what,
            )

    def update_progress(self, job_id: str, progress: int) -> None:
        """Ignored: progress lives in storage, and there is none here."""
        self._warn_once("update_progress")

    def write_task_log(
        self,
        job_id: str,
        task_name: str,
        level: str,
        message: str,
        extra: str | None = None,
    ) -> None:
        """Ignored: task logs and published partials live in storage."""
        self._warn_once("current_job.log/publish")

    def is_cancel_requested(self, job_id: str) -> bool:
        """Always false: a cancel reaches an executor as a protocol frame.

        ``check_cancelled`` consults the local signal the prefork child installs
        before it ever gets here, so answering false loses nothing.
        """
        return False

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
