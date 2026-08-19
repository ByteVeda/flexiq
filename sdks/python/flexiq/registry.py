"""Module-global pending registry behind ``@flexiq.task``.

``@queue.task()`` binds a task to a ``Queue`` instance, so a task module has to
import the module that constructs the queue — and the queue module wants to
import the task modules back. ``@flexiq.task`` breaks that cycle: it records
the function and its options here, and a ``Queue`` claims them later.

This module deliberately imports neither ``flexiq.app`` nor ``flexiq.mixins``.
Both sides reach the registry, so the registry must not reach back.
"""

from __future__ import annotations

import functools
import inspect
import os
import sys
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, NamedTuple

from flexiq.exceptions import DuplicateTaskError
from flexiq.task import DeferredTask, TaskWrapper

# The options ``@task(...)`` accepts and the values they default to, taken
# from the real ``Queue.task`` signature by ``flexiq.mixins.decorators`` at
# import time. Reading them off the live signature rather than restating them
# here keeps the deferred decorator from drifting as options are added — and
# importing ``Queue`` from this module would be the cycle this module exists
# to avoid. Importing ``flexiq.registry`` always initializes the ``flexiq``
# package first, and that imports ``flexiq.app``, so this is populated before
# any caller can reach ``task()``.
_TASK_OPTIONS: dict[str, Any] = {}


def set_task_options(signature: inspect.Signature) -> None:
    """Record ``Queue.task``'s options and defaults (called once, at import)."""
    _TASK_OPTIONS.clear()
    _TASK_OPTIONS.update(
        {
            name: (None if p.default is inspect.Parameter.empty else p.default)
            for name, p in signature.parameters.items()
            if name != "self"
        }
    )


def _resolve_module_name(module_name: str) -> str:
    """Resolve __main__ to the actual module name."""
    if module_name != "__main__":
        return module_name

    main = sys.modules.get("__main__")
    if main is not None:
        spec = getattr(main, "__spec__", None)
        if spec and spec.name:
            return str(spec.name)
        f = getattr(main, "__file__", None)
        if f:
            return str(os.path.splitext(os.path.basename(f))[0])
    return module_name


def resolve_task_name(fn: Callable, name: str | None) -> str:
    """The name a task registers under — the same rule ``Queue.task`` applies."""
    return name or f"{_resolve_module_name(fn.__module__)}.{fn.__qualname__}"


@dataclass
class PendingTask:
    """One ``@flexiq.task`` declaration awaiting a queue."""

    name: str
    fn: Callable
    handle: DeferredTask = field(repr=False)
    options: dict[str, Any] = field(default_factory=dict)

    @property
    def origin(self) -> str:
        """Where the task was declared — ``module.qualname``.

        Two declarations sharing a name are a conflict only when they come
        from different origins. The same origin means the module was re-run
        (``importlib.reload``, a re-executed notebook cell), which replaces.
        """
        return f"{self.fn.__module__}.{self.fn.__qualname__}"


class ClaimedTask(NamedTuple):
    """A pending entry a queue has claimed, and the wrapper it built for it."""

    entry: PendingTask
    wrapper: TaskWrapper


_PENDING: dict[str, PendingTask] = {}


def task(*args: Any, **options: Any) -> Callable[[Callable[..., Any]], DeferredTask]:
    """Register a task without a ``Queue``.

    Takes every option :meth:`flexiq.Queue.task` takes; they are replayed
    verbatim when a queue drains the registry, so live objects (``predicate``,
    ``middleware`` instances, a per-task ``serializer``) survive the deferral.

    Usage::

        from flexiq import task

        @task(rate_limit="10/s")
        def send_invoice(user_id): ...

    A ``Queue`` claims the task at construction, at
    :meth:`~flexiq.Queue.autodiscover`, or at worker start. Submitting before
    then raises :class:`~flexiq.TaskNotBoundError`.

    Raises:
        TypeError: The decorator was used without parentheses, or an option
            name is not one ``Queue.task`` accepts.
        DuplicateTaskError: Another module already registered this name.
    """
    if args:
        raise TypeError("@task requires parentheses — use @task() or @task(name=..., ...)")

    unknown = sorted(set(options) - set(_TASK_OPTIONS))
    if unknown and _TASK_OPTIONS:
        raise TypeError(
            f"@task() got unexpected keyword argument(s) {', '.join(unknown)!r} — "
            "it accepts the same options as Queue.task()"
        )

    def decorator(fn: Callable[..., Any]) -> DeferredTask:
        name = resolve_task_name(fn, options.get("name"))
        # Until a queue resolves them for real, the handle reports the defaults
        # the declaration asked for — read off ``Queue.task``'s own signature,
        # so an unbound task never claims a value the bound one would not.
        handle = DeferredTask(fn=fn, name=name, defaults={**_TASK_OPTIONS, **options})
        functools.update_wrapper(handle, fn)
        entry = PendingTask(name=name, fn=fn, handle=handle, options=dict(options))

        existing = _PENDING.get(name)
        if existing is not None and existing.origin != entry.origin:
            raise DuplicateTaskError(
                f"task name {name!r} is already registered by "
                f"{existing.origin} — pass an explicit name= to one of them"
            )

        _PENDING[name] = entry
        return handle

    return decorator


def pending_tasks() -> list[PendingTask]:
    """Every task awaiting (or already claimed by) a queue, in declaration order.

    The registry is never drained destructively: a second ``Queue`` in the same
    process gets the same tasks, and re-running ``autodiscover`` is a no-op
    rather than a conflict.
    """
    return list(_PENDING.values())


def clear_pending() -> None:
    """Drop every pending registration. Intended for tests."""
    _PENDING.clear()
