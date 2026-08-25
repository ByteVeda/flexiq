"""Sentry integration for flexiq.

Requires the ``sentry`` extra::

    pip install flexiq[sentry]

Usage::

    from flexiq.contrib.sentry import SentryMiddleware

    queue = Queue(middleware=[SentryMiddleware()])
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from flexiq.middleware import TaskMiddleware, legacy_task_filter_to_predicate

if TYPE_CHECKING:
    from flexiq.context import JobContext
    from flexiq.predicates import Predicate

try:
    import sentry_sdk
except ImportError:
    sentry_sdk = None


class SentryMiddleware(TaskMiddleware):
    """Middleware that reports task errors to Sentry and sets transaction context.

    Each task execution gets:
    - A Sentry scope with tags (prefix customizable via ``tag_prefix``)
    - Exceptions automatically captured via ``capture_exception``
    - Retries recorded as breadcrumbs

    Args:
        tag_prefix: Prefix for Sentry tag keys (default ``"flexiq"``).
        transaction_name_fn: Custom transaction name builder. Receives a
            :class:`~flexiq.context.JobContext` and returns a string.
        task_filter: Predicate that receives a task name and returns ``True``
            to report the task. ``None`` reports all tasks.
        extra_tags_fn: Callable that returns extra tags to set on the scope.
            Receives a :class:`~flexiq.context.JobContext`.
    """

    def __init__(
        self,
        *,
        tag_prefix: str = "flexiq",
        transaction_name_fn: Callable[[JobContext], str] | None = None,
        task_filter: Callable[[str], bool] | None = None,
        extra_tags_fn: Callable[[JobContext], dict[str, str]] | None = None,
        predicate: Predicate | Callable[..., Any] | None = None,
    ) -> None:
        if sentry_sdk is None:
            raise ImportError(
                "sentry-sdk is required for SentryMiddleware. "
                "Install it with: pip install flexiq[sentry]"
            )
        super().__init__(predicate=legacy_task_filter_to_predicate(task_filter, predicate))
        self._tag_prefix = tag_prefix
        self._transaction_name_fn = transaction_name_fn
        self._extra_tags_fn = extra_tags_fn

    def before(self, ctx: JobContext) -> None:
        sentry_sdk.push_scope()
        try:
            scope = sentry_sdk.get_current_scope()
            prefix = self._tag_prefix
            scope.set_tag(f"{prefix}.task_name", ctx.task_name)
            scope.set_tag(f"{prefix}.job_id", ctx.id)
            scope.set_tag(f"{prefix}.queue", ctx.queue_name)
            scope.set_tag(f"{prefix}.retry_count", str(ctx.retry_count))
            if self._extra_tags_fn is not None:
                for key, value in self._extra_tags_fn(ctx).items():
                    scope.set_tag(key, value)
            if self._transaction_name_fn is not None:
                scope.set_transaction_name(self._transaction_name_fn(ctx))
            else:
                scope.set_transaction_name(f"{prefix}:{ctx.task_name}")
        except Exception:
            sentry_sdk.pop_scope_unsafe()
            raise

    def after(self, ctx: JobContext, result: Any, error: Exception | None) -> None:
        if error is not None:
            sentry_sdk.capture_exception(error)
        sentry_sdk.pop_scope_unsafe()

    def on_sleep(self, ctx: JobContext, wake_at: int) -> None:
        """Pop the scope ``before`` pushed, without reporting anything.

        Nothing failed, so there is no exception to capture — but the scope
        must come off the stack, or every later job on this thread inherits
        this job's tags.
        """
        sentry_sdk.add_breadcrumb(
            category=self._tag_prefix,
            message=f"{ctx.task_name} sleeping until {wake_at}",
            level="info",
        )
        sentry_sdk.pop_scope_unsafe()

    def on_retry(self, ctx: JobContext, error: Exception, retry_count: int) -> None:
        sentry_sdk.add_breadcrumb(
            category=self._tag_prefix,
            message=f"Retrying {ctx.task_name} (attempt {retry_count}): {error}",
            level="warning",
        )
