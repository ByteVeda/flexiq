"""Registry of named predicate ops.

Every node in the predicate AST has a stable string identifier (the
``OP`` class var). The registry maps ``OP`` → class, enabling JSON
deserialization, string parsing, and dashboard lookup. Built-in ops
self-register via ``__init_subclass__`` on :class:`~flexiq.predicates.core.Predicate`.

User-defined predicates register through :func:`register_predicate` (or
the ``Queue.register_predicate`` decorator that calls into it).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from flexiq.predicates.core import Predicate


class PredicateValidationError(ValueError):
    """Raised when a predicate description (JSON / string) cannot be resolved.

    Common causes: unknown ``op`` name, missing required field, type
    mismatch in declared field, or schema drift between writer and
    reader.
    """


class PredicateRegistry:
    """Closed set of named predicate op classes."""

    __slots__ = ("_ops",)

    def __init__(self) -> None:
        self._ops: dict[str, type[Predicate]] = {}

    def register(self, op: str, cls: type[Predicate], *, replace: bool = False) -> None:
        if not op:
            raise PredicateValidationError("op name must be non-empty")
        if not replace and op in self._ops and self._ops[op] is not cls:
            existing = self._ops[op]
            raise PredicateValidationError(
                f"op {op!r} already registered to "
                f"{existing.__module__}.{existing.__qualname__}; "
                f"refusing to overwrite without replace=True"
            )
        self._ops[op] = cls

    def lookup(self, op: str) -> type[Predicate]:
        try:
            return self._ops[op]
        except KeyError:
            known = ", ".join(sorted(self._ops)) or "<empty>"
            raise PredicateValidationError(
                f"unknown predicate op: {op!r} (known ops: {known})"
            ) from None

    def names(self) -> list[str]:
        return sorted(self._ops)

    def __contains__(self, op: str) -> bool:
        return op in self._ops


_DEFAULT_REGISTRY = PredicateRegistry()


def default_registry() -> PredicateRegistry:
    """Return the process-wide default registry used by built-in ops."""
    return _DEFAULT_REGISTRY


def register_predicate(op: str, cls: type[Predicate], *, replace: bool = False) -> None:
    """Register ``cls`` under ``op`` in the default registry."""
    _DEFAULT_REGISTRY.register(op, cls, replace=replace)
