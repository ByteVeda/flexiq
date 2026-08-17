"""Composable, fail-closed predicates for gating tasks.

A predicate is a serializable AST node — a subclass of
:class:`Predicate` whose :meth:`Predicate.evaluate` returns ``True``
(allow), ``False`` (deny), :class:`Defer` (skip now, retry later), or
:class:`Cancel` (skip permanently). Predicates compose with ``&`` /
``|`` / ``~``; every resulting tree serializes through
:meth:`Predicate.to_dict` and :func:`parse` / :meth:`Predicate.format`.

Built-in recipes are imported from :mod:`flexiq.predicates.recipes`.
"""

from __future__ import annotations

from flexiq.predicates.context import PredicateContext
from flexiq.predicates.core import (
    AndPredicate,
    NotPredicate,
    OrPredicate,
    Predicate,
    coerce_predicate,
)
from flexiq.predicates.evaluate import evaluate_predicate
from flexiq.predicates.metrics import PredicateMetrics
from flexiq.predicates.outcomes import Cancel, Defer, PredicateAction, PredicateOutcome
from flexiq.predicates.parser import format_predicate, parse
from flexiq.predicates.providers import FeatureFlagProvider, env_feature_flag_provider
from flexiq.predicates.recipes import (
    after,
    before,
    env_var_truthy,
    feature_flag,
    in_time_window,
    in_timezone,
    is_business_hours,
    is_weekend,
    payload_matches,
    queue_paused,
    register_feature_flag_provider,
)
from flexiq.predicates.registry import (
    PredicateRegistry,
    PredicateValidationError,
    default_registry,
    register_predicate,
)

__all__ = [
    "AndPredicate",
    "Cancel",
    "Defer",
    "FeatureFlagProvider",
    "NotPredicate",
    "OrPredicate",
    "Predicate",
    "PredicateAction",
    "PredicateContext",
    "PredicateMetrics",
    "PredicateOutcome",
    "PredicateRegistry",
    "PredicateValidationError",
    "after",
    "before",
    "coerce_predicate",
    "default_registry",
    "env_feature_flag_provider",
    "env_var_truthy",
    "evaluate_predicate",
    "feature_flag",
    "format_predicate",
    "in_time_window",
    "in_timezone",
    "is_business_hours",
    "is_weekend",
    "parse",
    "payload_matches",
    "queue_paused",
    "register_feature_flag_provider",
    "register_predicate",
]
