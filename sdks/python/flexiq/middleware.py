"""Per-task middleware system for flexiq."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from flexiq.predicates.context import PredicateContext
from flexiq.predicates.core import Predicate, coerce_predicate
from flexiq.predicates.evaluate import evaluate_predicate

if TYPE_CHECKING:
    from flexiq.context import JobContext


def legacy_task_filter_to_predicate(
    task_filter: Callable[[str], bool] | None,
    predicate: Predicate | Callable[..., Any] | None,
) -> Predicate | Callable[..., Any] | None:
    """Translate the contrib-legacy ``task_filter`` kwarg into a predicate.

    Contrib middlewares historically accepted ``task_filter=Callable[[str], bool]``.
    The modern equivalent is ``predicate=`` taking a Predicate or a
    callable receiving a PredicateContext. This helper preserves
    back-compat: ``predicate`` wins if both are supplied; otherwise the
    legacy callable is wrapped into a Predicate that ignores everything
    except ``ctx.task_name``.
    """
    if predicate is not None:
        return predicate
    if task_filter is None:
        return None
    return coerce_predicate(task_filter, str_callable=True)


class TaskMiddleware:
    """Base class for task middleware.

    Subclass and override any of the hooks. Register globally via
    ``Queue(middleware=[...])`` or per-task via ``@queue.task(middleware=[...])``.

    A ``predicate`` may be passed to gate which tasks the middleware
    applies to: only when the predicate returns ``True`` will the hooks
    fire. Plain ``Callable[[task_name], bool]`` callables are accepted for
    backwards compatibility with contrib middleware ``task_filter`` kwargs.

    Example::

        class LoggingMiddleware(TaskMiddleware):
            def before(self, ctx):
                print(f"Starting {ctx.task_name}")

            def after(self, ctx, result, error):
                status = "OK" if error is None else f"FAILED: {error}"
                print(f"Finished {ctx.task_name}: {status}")
    """

    #: Stable identifier used to refer to this middleware from the dashboard
    #: when toggling it on/off per task. Defaults to the class' fully-qualified
    #: name so it survives restarts. Override on a subclass to pin a
    #: shorter / more user-facing name.
    name: str = ""

    def __init__(
        self,
        *,
        predicate: Predicate | Callable[..., Any] | None = None,
    ) -> None:
        self._predicate = coerce_predicate(predicate)
        if not type(self).name:
            type(self).name = f"{type(self).__module__}.{type(self).__qualname__}"

    def _should_apply(self, ctx: JobContext | None, task_name: str = "") -> bool:
        """Decide whether this middleware's hooks should fire for ``ctx``.

        Returns ``True`` when no predicate is configured. When a predicate
        is configured, builds a lightweight :class:`PredicateContext` from
        the available data and evaluates it. ``Defer`` and ``Cancel`` from
        a middleware predicate are interpreted as "skip this middleware
        only" — they do not influence job dispatch.

        Subclasses that skip ``super().__init__()`` fall through to the
        default ``True`` behaviour (no predicate).
        """
        predicate = getattr(self, "_predicate", None)
        if predicate is None:
            return True
        if ctx is not None:
            pctx = PredicateContext(
                task_name=ctx.task_name,
                queue=ctx.queue_name,
                retry_count=ctx.retry_count,
                job_id=ctx.id,
            )
        else:
            pctx = PredicateContext(task_name=task_name, queue="default")
        outcome = evaluate_predicate(predicate, pctx)
        return outcome is True

    def before(self, ctx: JobContext) -> None:
        """Called before task execution."""

    def after(self, ctx: JobContext, result: Any, error: Exception | None) -> None:
        """Called after task execution (success or failure)."""

    def on_retry(self, ctx: JobContext, error: Exception, retry_count: int) -> None:
        """Called when a task is about to be retried."""

    def on_enqueue(self, task_name: str, args: tuple, kwargs: dict, options: dict) -> None:
        """Called when a job is about to be enqueued.

        The ``options`` dict contains enqueue parameters (priority, delay,
        queue, etc.) and may be mutated to modify the enqueue call.
        """

    def on_dead_letter(self, ctx: JobContext, error: Exception) -> None:
        """Called when a job exhausts retries and moves to the dead-letter queue."""

    def on_timeout(self, ctx: JobContext) -> None:
        """Called when a job hits its timeout limit."""

    def on_sleep(self, ctx: JobContext, wake_at: int) -> None:
        """Called when an attempt ends in a ``step.sleep``. Pairs with ``before``.

        Every ``before`` is matched by exactly one of ``after`` or ``on_sleep``.
        A sleep is not a result: ``after(ctx, None, None)`` is indistinguishable
        from "the task returned ``None``", which would close a tracing span as a
        success, increment a success counter and clear an error scope — all
        wrong for an attempt that has not finished.

        Args:
            ctx: The sleeping job's context.
            wake_at: Deadline the job was rescheduled to, in Unix milliseconds.
        """

    def on_cancel(self, ctx: JobContext) -> None:
        """Called when a job is cancelled during execution."""


def middleware_class_path(mw: TaskMiddleware) -> str:
    """Fully-qualified class path of a middleware instance."""
    return f"{type(mw).__module__}.{type(mw).__qualname__}"


def middleware_key(mw: TaskMiddleware) -> str:
    """Stable identifier a middleware is keyed by in the dashboard disable
    list: its ``name`` if set, otherwise its class path. This is the single
    source of truth shared by chain filtering and admin discovery."""
    return getattr(mw, "name", "") or middleware_class_path(mw)
