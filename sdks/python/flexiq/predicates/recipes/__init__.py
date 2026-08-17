"""Built-in predicate recipes.

Each recipe is a factory function returning a
:class:`~flexiq.predicates.Predicate`. Every recipe is a registered AST
node (`OP` declared) so it round-trips through JSON and the string DSL.

Recipes that duplicated Rust-side enforcement (``by_queue``,
``by_task``, ``queue_size_under``, ``error_rate_under``,
``retry_count_under``, ``by_priority_at_least``) were intentionally
removed in v2 — use the corresponding ``@queue.task`` / ``Queue.set_*``
options instead.
"""

from __future__ import annotations

from flexiq.predicates.recipes.attributes import payload_matches
from flexiq.predicates.recipes.config import (
    env_var_truthy,
    feature_flag,
    register_feature_flag_provider,
)
from flexiq.predicates.recipes.system import queue_paused
from flexiq.predicates.recipes.time import (
    after,
    before,
    in_time_window,
    in_timezone,
    is_business_hours,
    is_weekend,
)

__all__ = [
    "after",
    "before",
    "env_var_truthy",
    "feature_flag",
    "in_time_window",
    "in_timezone",
    "is_business_hours",
    "is_weekend",
    "payload_matches",
    "queue_paused",
    "register_feature_flag_provider",
]
