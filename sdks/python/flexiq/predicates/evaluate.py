"""Synchronous predicate evaluation runner.

Wraps a predicate's ``evaluate`` call with fail-closed error handling and
normalises the return type. Async evaluation lives in
:mod:`flexiq.async_support.predicates` to keep all ``asyncio`` use inside
the async package.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from flexiq.async_support.helpers import run_maybe_async
from flexiq.predicates.outcomes import Cancel, Defer

if TYPE_CHECKING:
    from flexiq.predicates.context import PredicateContext
    from flexiq.predicates.core import Predicate
    from flexiq.predicates.metrics import PredicateMetrics

logger = logging.getLogger("flexiq.predicates")


def evaluate_predicate(
    predicate: Predicate,
    ctx: PredicateContext,
    *,
    metrics: PredicateMetrics | None = None,
) -> bool | Defer | Cancel:
    """Evaluate ``predicate`` fail-closed.

    On exception, returns ``False`` and (if provided) records an error on
    ``metrics``. Coroutine returns are resolved synchronously via
    :func:`run_maybe_async` so this is safe to call from worker threads.
    """
    outcome = _resolve_outcome(predicate, ctx, metrics=metrics)
    if metrics is not None:
        if isinstance(outcome, Defer):
            metrics.record_deferred()
        elif isinstance(outcome, Cancel):
            metrics.record_cancelled()
        elif outcome is True:
            metrics.record_allowed()
        else:
            metrics.record_denied()
    return outcome


def _resolve_outcome(
    predicate: Predicate,
    ctx: PredicateContext,
    *,
    metrics: PredicateMetrics | None = None,
) -> bool | Defer | Cancel:
    """Run ``evaluate`` with fail-closed semantics and normalise the return.

    Used internally by composition operators (And/Or/Not) — they do NOT
    record outcome metrics themselves to avoid double-counting; only the
    top-level call from :func:`evaluate_predicate` records.
    """
    try:
        raw = predicate.evaluate(ctx)
        resolved = run_maybe_async(raw)
    except Exception:
        logger.exception("Predicate %r raised; treating as False (fail-closed)", predicate)
        if metrics is not None:
            metrics.record_error()
        return False

    if isinstance(resolved, (Defer, Cancel)):
        return resolved
    return bool(resolved)
