"""Predicate AST root + boolean combinator nodes.

The :class:`Predicate` base is a serializable AST node. Every concrete
subclass declares a stable ``OP`` name that registers it with the
default :class:`~flexiq.predicates.registry.PredicateRegistry`.

Authors compose predicates with the standard Python operators (``&``,
``|``, ``~``); each operator produces a typed AST node so the resulting
tree round-trips through JSON via :meth:`Predicate.to_dict` and
:meth:`Predicate.from_dict`. A human-readable string surface is
available through :meth:`format` and the matching
:func:`~flexiq.predicates.parser.parse` function.
"""

from __future__ import annotations

import dataclasses
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any, ClassVar

from flexiq.predicates.evaluate import _resolve_outcome
from flexiq.predicates.outcomes import Cancel, Defer
from flexiq.predicates.registry import (
    PredicateValidationError,
    default_registry,
)

if TYPE_CHECKING:
    from flexiq.predicates.context import PredicateContext


PredicateReturn = bool | Defer | Cancel | Awaitable[Any]
"""Anything a :meth:`Predicate.evaluate` is allowed to return.

Synchronous predicates return :class:`bool`, :class:`Defer`, or
:class:`Cancel`. Async predicates may also return an awaitable that
resolves to one of those values.
"""


# Operator precedence used by :meth:`Predicate.format` so children of a
# higher-precedence node are wrapped in parentheses only when needed.
_PREC_OR = 1
_PREC_AND = 2
_PREC_NOT = 3
_PREC_ATOM = 4


class Predicate(ABC):
    """Base class for every node in the predicate AST.

    Concrete subclasses set ``OP`` to a stable, registry-unique string.
    Built-in subclasses register themselves automatically through
    ``__init_subclass__``. Custom op classes either inherit and set
    ``OP``, or register manually via
    :func:`~flexiq.predicates.registry.register_predicate`.

    Serialization:

    * :meth:`to_dict` — emit ``{"op": OP, **fields}``.
    * :meth:`from_dict` — dispatch via the registry, recurse for
      composite nodes.
    * :meth:`format` — stable, parser-compatible string surface.
    """

    OP: ClassVar[str | None] = None
    PREC: ClassVar[int] = _PREC_ATOM

    def __init_subclass__(cls, **kw: Any) -> None:
        super().__init_subclass__(**kw)
        op = cls.__dict__.get("OP")
        if op:
            default_registry().register(op, cls)

    # -- Evaluation -------------------------------------------------------

    @abstractmethod
    def evaluate(self, ctx: PredicateContext) -> PredicateReturn:
        """Return ``True`` to allow, ``False`` / Defer / Cancel to gate."""

    # -- Composition ------------------------------------------------------

    def __and__(self, other: Predicate) -> Predicate:
        return AndPredicate(self, other)

    def __or__(self, other: Predicate) -> Predicate:
        return OrPredicate(self, other)

    def __invert__(self) -> Predicate:
        return NotPredicate(self)

    # -- Serialization ----------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        """Serialize the node to a JSON-safe dict.

        Default implementation works for any frozen dataclass: every
        dataclass field is emitted as a key. Non-dataclass nodes
        (``and``/``or``/``not``, custom classes) override this.
        """
        if not self.OP:
            raise PredicateValidationError(
                f"{type(self).__name__} has no OP name; cannot serialize"
            )
        if dataclasses.is_dataclass(self):
            fields = {
                f.name: getattr(self, f.name)
                for f in dataclasses.fields(self)
                if not f.name.startswith("_")
            }
            return {"op": self.OP, **fields}
        return {"op": self.OP}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Predicate:
        """Build a predicate from a JSON-safe dict.

        Dispatches to the registry on ``data["op"]``. Unknown ops raise
        :class:`PredicateValidationError`.
        """
        if not isinstance(data, dict):
            raise PredicateValidationError(f"expected dict, got {type(data).__name__}")
        op = data.get("op")
        if not isinstance(op, str) or not op:
            raise PredicateValidationError(f"missing or invalid 'op' field: {data!r}")
        target = default_registry().lookup(op)
        kwargs = {k: v for k, v in data.items() if k != "op"}
        try:
            return target._from_kwargs(kwargs)
        except (TypeError, ValueError) as exc:
            raise PredicateValidationError(
                f"cannot construct {op!r} from {kwargs!r}: {exc}"
            ) from exc

    @classmethod
    def _from_kwargs(cls, kwargs: dict[str, Any]) -> Predicate:
        """Build an instance from deserialized kwargs.

        Default works for dataclasses with primitive fields. Composite
        nodes (and/or/not) override to recurse.
        """
        return cls(**kwargs)

    # -- String surface ---------------------------------------------------

    def format(self) -> str:
        """Render this node in the parser-compatible string DSL."""
        return self._format_atom()

    def _format_atom(self) -> str:
        """Default leaf rendering: ``op_name(field=value, ...)``."""
        if not self.OP:
            # Custom Predicate subclasses without an OP can still appear
            # in debug output of composed trees; emit a stable opaque
            # marker rather than raising.
            return f"<{type(self).__name__}>"
        if dataclasses.is_dataclass(self):
            parts: list[str] = []
            for f in dataclasses.fields(self):
                if f.name.startswith("_"):
                    continue
                value = getattr(self, f.name)
                default = (
                    f.default
                    if f.default is not dataclasses.MISSING
                    else (
                        f.default_factory()
                        if f.default_factory is not dataclasses.MISSING
                        else _UNSET
                    )
                )
                if default is not _UNSET and value == default:
                    continue
                parts.append(f"{f.name}={_format_literal(value)}")
            args = ", ".join(parts)
            return f"{self.OP}({args})"
        return f"{self.OP}()"

    def _format_child(self, child: Predicate) -> str:
        text = child.format()
        return f"({text})" if child.PREC < self.PREC else text

    # -- Repr passthrough -------------------------------------------------

    def __repr__(self) -> str:  # pragma: no cover - human aid
        try:
            return self.format()
        except Exception:
            return f"{type(self).__name__}()"


# -- Boolean combinator nodes --------------------------------------------


class AndPredicate(Predicate):
    """Logical AND with short-circuit evaluation."""

    __slots__ = ("_left", "_right")
    OP: ClassVar[str | None] = "and"
    PREC = _PREC_AND

    def __init__(self, left: Predicate, right: Predicate) -> None:
        self._left = left
        self._right = right

    def evaluate(self, ctx: PredicateContext) -> PredicateReturn:
        left = _resolve_outcome(self._left, ctx)
        if isinstance(left, (Defer, Cancel)) or left is False:
            return left
        return _resolve_outcome(self._right, ctx)

    def to_dict(self) -> dict[str, Any]:
        return {"op": "and", "args": [self._left.to_dict(), self._right.to_dict()]}

    @classmethod
    def _from_kwargs(cls, kwargs: dict[str, Any]) -> Predicate:
        args = kwargs.get("args")
        if not isinstance(args, list) or len(args) != 2:
            raise PredicateValidationError(f"'and' expects exactly 2 args in a list, got {args!r}")
        return cls(Predicate.from_dict(args[0]), Predicate.from_dict(args[1]))

    def format(self) -> str:
        return f"{self._format_child(self._left)} & {self._format_child(self._right)}"


class OrPredicate(Predicate):
    """Logical OR with short-circuit evaluation."""

    __slots__ = ("_left", "_right")
    OP: ClassVar[str | None] = "or"
    PREC = _PREC_OR

    def __init__(self, left: Predicate, right: Predicate) -> None:
        self._left = left
        self._right = right

    def evaluate(self, ctx: PredicateContext) -> PredicateReturn:
        left = _resolve_outcome(self._left, ctx)
        if left is True:
            return True
        right = _resolve_outcome(self._right, ctx)
        if right is True:
            return True
        # Both deny — surface the most informative outcome.
        if isinstance(left, Cancel) or isinstance(right, Cancel):
            return left if isinstance(left, Cancel) else right
        if isinstance(left, Defer) or isinstance(right, Defer):
            return left if isinstance(left, Defer) else right
        return False

    def to_dict(self) -> dict[str, Any]:
        return {"op": "or", "args": [self._left.to_dict(), self._right.to_dict()]}

    @classmethod
    def _from_kwargs(cls, kwargs: dict[str, Any]) -> Predicate:
        args = kwargs.get("args")
        if not isinstance(args, list) or len(args) != 2:
            raise PredicateValidationError(f"'or' expects exactly 2 args in a list, got {args!r}")
        return cls(Predicate.from_dict(args[0]), Predicate.from_dict(args[1]))

    def format(self) -> str:
        return f"{self._format_child(self._left)} | {self._format_child(self._right)}"


class NotPredicate(Predicate):
    """Logical NOT.

    Inverts ``True``/``False``. ``Defer`` and ``Cancel`` outcomes pass
    through unchanged — they are terminal, not booleans.
    """

    __slots__ = ("_inner",)
    OP: ClassVar[str | None] = "not"
    PREC = _PREC_NOT

    def __init__(self, inner: Predicate) -> None:
        self._inner = inner

    def evaluate(self, ctx: PredicateContext) -> PredicateReturn:
        outcome = _resolve_outcome(self._inner, ctx)
        if isinstance(outcome, (Defer, Cancel)):
            return outcome
        return not outcome

    def to_dict(self) -> dict[str, Any]:
        return {"op": "not", "arg": self._inner.to_dict()}

    @classmethod
    def _from_kwargs(cls, kwargs: dict[str, Any]) -> Predicate:
        arg = kwargs.get("arg")
        if not isinstance(arg, dict):
            raise PredicateValidationError(f"'not' expects an 'arg' dict, got {arg!r}")
        return cls(Predicate.from_dict(arg))

    def format(self) -> str:
        return f"!{self._format_child(self._inner)}"


# -- Callable adapter (non-serializable) ---------------------------------


class _CallablePredicate(Predicate):
    """Adapter that wraps a plain callable as a :class:`Predicate`.

    Used for back-compat with the contrib middleware ``task_filter``
    kwarg and for end-users who pass a bare callable to ``@queue.task``.
    Not registered; ``to_dict`` / ``format`` raise because a callable
    has no stable schema.
    """

    __slots__ = ("_fn", "_takes_str")
    OP: ClassVar[str | None] = None

    def __init__(self, fn: Callable[..., Any], *, takes_str: bool = False) -> None:
        self._fn = fn
        self._takes_str = takes_str

    def evaluate(self, ctx: PredicateContext) -> PredicateReturn:
        if self._takes_str:
            return bool(self._fn(ctx.task_name))
        return self._fn(ctx)  # type: ignore[no-any-return]

    def to_dict(self) -> dict[str, Any]:
        raise PredicateValidationError(
            "bare callables cannot be serialized; wrap your logic in a "
            "Predicate subclass with a stable OP name"
        )

    def format(self) -> str:
        return f"<callable:{getattr(self._fn, '__qualname__', repr(self._fn))}>"


# -- Public coercion helper ----------------------------------------------


def coerce_predicate(
    value: Predicate | Callable[..., Any] | None,
    *,
    str_callable: bool = False,
) -> Predicate | None:
    """Wrap a callable as a :class:`Predicate`. ``None`` passes through.

    Use ``str_callable=True`` to accept the legacy
    ``Callable[[str], bool]`` contrib ``task_filter`` shape.
    """
    if value is None:
        return None
    if isinstance(value, Predicate):
        return value
    if callable(value):
        return _CallablePredicate(value, takes_str=str_callable)
    raise TypeError(
        f"predicate must be a Predicate, callable, or None; got {type(value).__name__}"
    )


# -- Internal helpers ----------------------------------------------------


_UNSET = object()


def _format_literal(value: Any) -> str:
    """Render ``value`` as a parser-compatible literal."""
    if isinstance(value, str):
        # Use double quotes; escape inner doubles.
        return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return "null"
    if isinstance(value, (int, float)):
        return repr(value)
    if isinstance(value, (list, tuple)):
        return "[" + ", ".join(_format_literal(v) for v in value) + "]"
    # Fallback: dataclasses / objects we don't deeply support yet — let
    # repr handle it. Round-trip via the string parser isn't guaranteed
    # for non-primitive values, but JSON round-trip still works.
    return repr(value)
