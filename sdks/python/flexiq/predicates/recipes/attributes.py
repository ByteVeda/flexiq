"""Predicates that read job payload fields.

The other "attribute"-flavoured recipes (``by_queue``, ``by_task``,
``by_priority_at_least``, ``retry_count_under``) were intentionally
removed in v2: each duplicated a gate the Rust scheduler already
enforces (per-task queue routing, ``priority`` ordering,
``max_concurrent``/``rate_limit``, ``max_retries``). Restating those
inside a Python predicate produces weaker, non-atomic gates that race
with the authoritative enforcement path.

What remains here is :func:`payload_matches` — a genuinely new
capability that lets predicates branch on the deserialized argument
graph at runtime.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, ClassVar

from flexiq.predicates.core import Predicate
from flexiq.predicates.registry import PredicateValidationError

if TYPE_CHECKING:
    from flexiq.predicates.context import PredicateContext


_MISSING = object()


def _safe_lookup(node: Any, key: str) -> Any:
    if isinstance(node, dict):
        return node.get(key, _MISSING)
    if isinstance(node, (list, tuple)):
        try:
            return node[int(key)]
        except (ValueError, IndexError):
            return _MISSING
    return getattr(node, key, _MISSING)


@dataclass(frozen=True)
class PayloadMatches(Predicate):
    """Match a value in ``args``/``kwargs`` by dotted path."""

    OP: ClassVar[str | None] = "payload_matches"

    path: str = ""
    expected: Any = None
    _segments: tuple[str, ...] = field(default=(), init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        if not self.path:
            raise PredicateValidationError("payload_matches: path must be non-empty")
        object.__setattr__(self, "_segments", tuple(self.path.split(".")))

    def evaluate(self, ctx: PredicateContext) -> bool:
        node: Any = {"args": ctx.args, "kwargs": ctx.kwargs}
        for segment in self._segments:
            node = _safe_lookup(node, segment)
            if node is _MISSING:
                return False
        return bool(node == self.expected)


def payload_matches(path: str, expected: Any) -> Predicate:
    """Match a value in args/kwargs by dotted path.

    ``path`` is a dotted string addressing a value within
    ``{"args": (...), "kwargs": {...}}``. Examples:

    * ``"kwargs.tenant_id"`` → ``ctx.kwargs["tenant_id"]``
    * ``"args.0"`` → ``ctx.args[0]``
    * ``"kwargs.config.region"`` → ``ctx.kwargs["config"]["region"]``
    """
    return PayloadMatches(path=path, expected=expected)
