"""Predicate registration, inspection, and gating (enqueue + dispatch).

All predicate state and behaviour lives here. The Queue composes this
mixin alongside :class:`~flexiq.mixins.decorators.QueueDecoratorMixin`,
which handles wiring predicates into ``@queue.task`` and triggering the
worker-side dispatch gate during ``_wrap_task``.

The two main flows are:

* **Enqueue gate** — :meth:`_apply_enqueue_predicate` runs before the
  Rust ``enqueue`` call (from ``Queue.enqueue`` / ``Queue.enqueue_many``).
  It returns a (possibly bumped) ``delay`` and raises
  :class:`~flexiq.exceptions.PredicateRejectedError` when the predicate
  cancels the enqueue.
* **Dispatch gate** — :meth:`_apply_dispatch_predicate` runs on the
  worker, called from ``QueueDecoratorMixin._wrap_task`` before the
  task body executes. ``Defer`` re-enqueues a fresh job via
  :meth:`_reenqueue_after_defer` and raises
  :class:`~flexiq.exceptions.TaskCancelledError`. ``Cancel`` raises
  the same exception without re-enqueueing.

Both gates emit the appropriate ``PREDICATE_*`` events through
``self._emit_event`` and record outcomes on ``self._predicate_metrics``.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from flexiq.events import EventType
from flexiq.exceptions import PredicateRejectedError, TaskCancelledError
from flexiq.predicates.context import PredicateContext
from flexiq.predicates.core import Predicate as _Predicate
from flexiq.predicates.evaluate import evaluate_predicate
from flexiq.predicates.metrics import PredicateMetrics
from flexiq.predicates.outcomes import Cancel, Defer, PredicateAction
from flexiq.predicates.registry import PredicateValidationError
from flexiq.predicates.registry import (
    register_predicate as _register_predicate,
)

if TYPE_CHECKING:
    from flexiq._flexiq import PyQueue
    from flexiq.serializers import Serializer


class QueuePredicateMixin:
    """Public predicate API + internal enqueue/dispatch gating."""

    _inner: PyQueue
    _task_predicates: dict[str, _Predicate]
    _task_predicate_on_false: dict[str, PredicateAction]
    _task_predicate_extras: dict[str, dict[str, Any]]
    _task_default_defer: dict[str, float]
    _task_predicate_serialized: dict[str, dict[str, Any] | None]
    _predicate_metrics: PredicateMetrics

    # Supplied by other mixins on the composed Queue.
    _emit_event: Callable[..., None]
    _get_serializer: Callable[[str], Serializer]
    _encode_payload: Callable[..., bytes]

    def _init_predicate_state(self) -> None:
        """Initialise predicate-related instance state.

        Invoked once from ``Queue.__init__``. Kept on the mixin so the
        full set of predicate fields stays in one place.
        """
        self._task_predicates = {}
        self._task_predicate_on_false = {}
        self._task_predicate_extras = {}
        self._task_default_defer = {}
        self._task_predicate_serialized = {}
        self._predicate_metrics = PredicateMetrics()

    # -- Inspection / registration ---------------------------------------

    def list_predicates(self) -> dict[str, dict[str, Any] | None]:
        """Return the serialized predicate (or ``None`` for bare callables)
        registered for every task that has one.

        The values are JSON-safe dicts produced by
        :meth:`Predicate.to_dict`. Consumers (dashboard, audit logs) can
        feed each value back through :meth:`Predicate.from_dict` to
        rebuild the AST.
        """
        return dict(self._task_predicate_serialized)

    def predicate_for(self, task_name: str) -> dict[str, Any] | None:
        """Return the serialized predicate for ``task_name`` or ``None``."""
        return self._task_predicate_serialized.get(task_name)

    def predicate_stats(self) -> dict[str, int]:
        """Return predicate outcome counters for this queue instance.

        Keys: ``allowed``, ``denied``, ``deferred``, ``cancelled``, ``errors``.
        """
        return self._predicate_metrics.snapshot()

    def register_predicate(self, op: str, *, replace: bool = False) -> Callable[[type], type]:
        """Class decorator: register a custom :class:`Predicate` subclass.

        Example::

            from flexiq.predicates import Predicate

            @queue.register_predicate("tenant_quota_under")
            class TenantQuotaUnder(Predicate):
                OP = "tenant_quota_under"
                ...

        The ``OP`` set on the class must match ``op``. Once registered,
        the predicate participates in JSON serialization and the string
        DSL just like a built-in recipe.
        """

        def decorator(cls: type) -> type:
            if not isinstance(cls, type) or not issubclass(cls, _Predicate):
                raise PredicateValidationError(
                    f"register_predicate target must subclass Predicate; got {cls!r}"
                )
            declared = cls.__dict__.get("OP")
            if declared and declared != op:
                raise PredicateValidationError(
                    f"OP mismatch: decorator says {op!r}, class declares {declared!r}"
                )
            cls.OP = op
            _register_predicate(op, cls, replace=replace)
            return cls

        return decorator

    # -- Dispatch-time gate ----------------------------------------------

    def _apply_dispatch_predicate(
        self,
        *,
        task_name: str,
        args: tuple,
        kwargs: dict,
        job_id: str,
        queue_name: str,
        priority: int = 0,
        retry_count: int = 0,
    ) -> None:
        """Evaluate the worker-dispatch predicate; defer or cancel as needed.

        ``Defer`` (or ``False`` with ``on_false="defer"``) re-enqueues a
        fresh job with the same payload and a delay, then raises
        :class:`TaskCancelledError` so the current execution is marked
        cancelled by the Rust runner. ``Cancel`` (or ``False`` with
        ``on_false="cancel"``) raises :class:`TaskCancelledError` directly
        without re-enqueueing.

        Returns silently when the predicate allows.
        """
        predicate = self._task_predicates.get(task_name)
        if predicate is None:
            return

        ctx = PredicateContext.for_dispatch(
            task_name=task_name,
            queue=queue_name,
            priority=priority,
            retry_count=retry_count,
            args=tuple(args),
            kwargs=dict(kwargs),
            job_id=job_id,
            payload_size=0,
            extras=self._task_predicate_extras.get(task_name),
            queue_ref=self,  # type: ignore[arg-type]
        )
        outcome = evaluate_predicate(predicate, ctx, metrics=self._predicate_metrics)

        if outcome is True:
            return

        if isinstance(outcome, Cancel):
            self._emit_dispatch_cancel(task_name, job_id, queue_name, outcome.reason)
            raise TaskCancelledError(_dispatch_cancel_message(job_id, outcome.reason))

        if isinstance(outcome, Defer):
            self._reenqueue_after_defer(
                task_name=task_name,
                args=args,
                kwargs=kwargs,
                queue_name=queue_name,
                delay_seconds=outcome.seconds,
            )
            self._emit_dispatch_defer(task_name, job_id, queue_name, outcome.seconds)
            raise TaskCancelledError(f"predicate deferred job {job_id} by {outcome.seconds:.1f}s")

        # Plain False — branch on the task's on_false setting.
        action = self._task_predicate_on_false.get(task_name, PredicateAction.DEFER)
        if action is PredicateAction.CANCEL:
            self._emit_dispatch_cancel(task_name, job_id, queue_name, None)
            raise TaskCancelledError(f"predicate rejected job {job_id}")

        defer_seconds = self._task_default_defer.get(task_name, 60.0)
        self._reenqueue_after_defer(
            task_name=task_name,
            args=args,
            kwargs=kwargs,
            queue_name=queue_name,
            delay_seconds=defer_seconds,
        )
        self._emit_dispatch_defer(task_name, job_id, queue_name, defer_seconds)
        raise TaskCancelledError(f"predicate deferred job {job_id}")

    def _reenqueue_after_defer(
        self,
        *,
        task_name: str,
        args: tuple,
        kwargs: dict,
        queue_name: str,
        delay_seconds: float,
    ) -> None:
        """Re-enqueue a job with a delay, bypassing predicate re-evaluation.

        Args/kwargs are serialized fresh via the task's serializer. We go
        straight to the Rust queue to avoid running enqueue-time
        middleware or re-evaluating the predicate (which would create an
        infinite ping-pong).
        """
        payload = self._encode_payload(task_name, tuple(args), dict(kwargs))
        self._inner.enqueue(
            task_name=task_name,
            payload=payload,
            queue=queue_name,
            delay_seconds=delay_seconds,
        )

    # -- Enqueue-time gate -----------------------------------------------

    def _apply_enqueue_predicate(
        self,
        *,
        predicate: Any,
        task_name: str,
        queue_name: str,
        priority: int | None,
        args: tuple,
        kwargs: dict,
        payload_size: int,
        delay: float | None,
    ) -> float | None:
        """Evaluate an enqueue-time predicate; return adjusted ``delay``.

        Raises :class:`~flexiq.exceptions.PredicateRejectedError` when the
        outcome is a :class:`~flexiq.predicates.Cancel`, or a plain
        ``False`` paired with ``on_false="cancel"``. Returns the (possibly
        bumped) ``delay`` for the caller to pass through to the Rust
        ``enqueue``.
        """
        ctx = PredicateContext.for_enqueue(
            task_name=task_name,
            queue=queue_name,
            priority=priority,
            args=tuple(args),
            kwargs=dict(kwargs),
            payload_size=payload_size,
            delay_seconds=delay,
            extras=self._task_predicate_extras.get(task_name),
            queue_ref=self,  # type: ignore[arg-type]
        )
        outcome = evaluate_predicate(predicate, ctx, metrics=self._predicate_metrics)

        if outcome is True:
            return delay
        if isinstance(outcome, Defer):
            self._emit_enqueue_defer(task_name, queue_name, outcome.seconds)
            return (delay or 0.0) + outcome.seconds
        if isinstance(outcome, Cancel):
            self._emit_enqueue_reject(task_name, queue_name, outcome.reason)
            raise PredicateRejectedError(task_name, outcome.reason)

        # Plain False — branch on the task's on_false setting.
        action = self._task_predicate_on_false.get(task_name, PredicateAction.DEFER)
        if action is PredicateAction.CANCEL:
            self._emit_enqueue_reject(task_name, queue_name, None)
            raise PredicateRejectedError(task_name)

        defer_seconds = self._task_default_defer.get(task_name, 60.0)
        self._emit_enqueue_defer(task_name, queue_name, defer_seconds)
        return (delay or 0.0) + defer_seconds

    # -- Event emission helpers ------------------------------------------

    def _emit_dispatch_cancel(
        self, task_name: str, job_id: str, queue_name: str, reason: str | None
    ) -> None:
        payload: dict[str, Any] = {
            "task_name": task_name,
            "job_id": job_id,
            "queue": queue_name,
            "phase": "dispatch",
        }
        if reason:
            payload["reason"] = reason
        self._emit_event(EventType.PREDICATE_CANCELLED, payload)

    def _emit_dispatch_defer(
        self, task_name: str, job_id: str, queue_name: str, defer_seconds: float
    ) -> None:
        self._emit_event(
            EventType.PREDICATE_DEFERRED,
            {
                "task_name": task_name,
                "job_id": job_id,
                "queue": queue_name,
                "defer_seconds": defer_seconds,
                "phase": "dispatch",
            },
        )

    def _emit_enqueue_defer(self, task_name: str, queue_name: str, defer_seconds: float) -> None:
        self._emit_event(
            EventType.PREDICATE_DEFERRED,
            {
                "task_name": task_name,
                "queue": queue_name,
                "defer_seconds": defer_seconds,
                "phase": "enqueue",
            },
        )

    def _emit_enqueue_reject(self, task_name: str, queue_name: str, reason: str | None) -> None:
        payload: dict[str, Any] = {
            "task_name": task_name,
            "queue": queue_name,
            "phase": "enqueue",
        }
        if reason:
            payload["reason"] = reason
        self._emit_event(EventType.PREDICATE_REJECTED, payload)


def _dispatch_cancel_message(job_id: str, reason: str) -> str:
    if reason:
        return f"predicate cancelled job {job_id}: {reason}"
    return f"predicate cancelled job {job_id}"
