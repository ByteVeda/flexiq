"""Recursive argument walker for deep interception."""

from __future__ import annotations

import dataclasses
import logging
import uuid
from typing import TYPE_CHECKING, Any

from taskito.interception.converters import convert_dataclass, convert_named_tuple
from taskito.interception.errors import ArgumentFailure
from taskito.interception.registry import RegistryEntry, TypeRegistry
from taskito.interception.strategy import Strategy

if TYPE_CHECKING:
    from taskito.proxies.registry import ProxyRegistry

logger = logging.getLogger("taskito.interception")

# Types whose elements should be walked recursively
_CONTAINER_TYPES = (list, tuple, set, frozenset)


class WalkResult:
    """Accumulated results from walking an argument tree."""

    __slots__ = ("failures", "max_depth", "redirects", "strategy_counts")

    def __init__(self) -> None:
        self.failures: list[ArgumentFailure] = []
        self.redirects: dict[str, str] = {}  # path -> resource_name
        self.strategy_counts: dict[str, int] = {}
        self.max_depth: int = 0


class ArgumentWalker:
    """Depth-limited recursive walker that transforms arguments in-place.

    Walks the entire argument tree (positional + keyword args), applying
    the interception strategy from the type registry to each value.
    """

    def __init__(
        self,
        registry: TypeRegistry,
        max_depth: int = 10,
        proxy_registry: ProxyRegistry | None = None,
    ) -> None:
        self._registry = registry
        self._max_depth = max_depth
        self._proxy_registry = proxy_registry

    def walk(self, args: tuple, kwargs: dict) -> tuple[tuple, dict, WalkResult]:
        """Walk args and kwargs, returning transformed copies and walk results."""
        result = WalkResult()
        seen: set[int] = set()  # circular reference detection via id()
        proxy_identity: dict[int, str] = {}  # id(obj) -> UUID for proxy dedup

        new_args = tuple(
            self._walk_value(v, f"args[{i}]", 0, seen, result, proxy_identity)
            for i, v in enumerate(args)
        )
        new_kwargs = {
            k: self._walk_value(v, f"kwargs.{k}", 0, seen, result, proxy_identity)
            for k, v in kwargs.items()
        }
        return new_args, new_kwargs, result

    def _walk_value(
        self,
        obj: Any,
        path: str,
        depth: int,
        seen: set[int],
        result: WalkResult,
        proxy_identity: dict[int, str],
    ) -> Any:
        """Walk a single value, transforming it according to its strategy."""
        # None is always passthrough
        if obj is None:
            return obj

        # NoProxy wrapper — unwrap and skip interception
        if (
            hasattr(obj, "__class__")
            and obj.__class__.__name__ == "NoProxy"
            and hasattr(obj, "value")
        ):
            return obj.value

        # Track max depth
        result.max_depth = max(result.max_depth, depth)

        # Depth limit — pass through to serializer as-is
        if depth > self._max_depth:
            return obj

        # Circular reference guard
        obj_id = id(obj)
        if obj_id in seen:
            return obj
        # Only track mutable containers to avoid false positives on interned ints/strings
        is_container = isinstance(obj, (dict, list, set, frozenset, tuple))

        if is_container:
            seen.add(obj_id)

        try:
            return self._process(obj, path, depth, seen, result, proxy_identity)
        finally:
            if is_container:
                seen.discard(obj_id)

    def _process(
        self,
        obj: Any,
        path: str,
        depth: int,
        seen: set[int],
        result: WalkResult,
        proxy_identity: dict[int, str],
    ) -> Any:
        # Lambda detection — must check before registry (shares type with functions)
        if callable(obj) and hasattr(obj, "__name__") and obj.__name__ == "<lambda>":
            result.failures.append(
                ArgumentFailure(
                    path=path,
                    type_name="lambda",
                    reason="Lambda functions cannot be serialized.",
                    suggestions=[
                        "Extract the lambda into a named function",
                        "Pass the computed value instead of the lambda",
                    ],
                )
            )
            return obj

        # Tempfile detection — module-based check since types are private
        if getattr(type(obj), "__module__", None) == "tempfile":
            result.failures.append(
                ArgumentFailure(
                    path=path,
                    type_name=type(obj).__qualname__,
                    reason="Temporary file handles are OS-level and tied to the current process.",
                    suggestions=[
                        "Write to a permanent file and pass the path instead",
                        "Read the contents and pass the data directly",
                    ],
                )
            )
            return obj

        # NamedTuple detection — must check before registry (tuples are PASS)
        if isinstance(obj, tuple) and hasattr(obj, "_fields") and hasattr(obj, "_asdict"):
            return convert_named_tuple(obj)

        # Check dataclass (can't use isinstance, need is_dataclass)
        if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
            entry = self._registry.resolve(obj)
            # If there's a specific registered entry for this dataclass type, use it
            if entry is not None and entry.type_key == "dataclass":
                return self._apply_strategy(obj, path, entry, result, proxy_identity)
            if entry is not None:
                return self._apply_strategy(obj, path, entry, result, proxy_identity)
            # Fallback: auto-convert as dataclass
            return convert_dataclass(obj)

        # Look up in registry
        entry = self._registry.resolve(obj)
        if entry is not None:
            return self._apply_strategy(obj, path, entry, result, proxy_identity)

        # Not in registry — walk containers recursively
        if isinstance(obj, dict):
            return {
                k: self._walk_value(v, f"{path}.{k}", depth + 1, seen, result, proxy_identity)
                for k, v in obj.items()
            }
        if isinstance(obj, _CONTAINER_TYPES):
            walked = [
                self._walk_value(v, f"{path}[{i}]", depth + 1, seen, result, proxy_identity)
                for i, v in enumerate(obj)
            ]
            return type(obj)(walked)

        # Unknown type — pass through (serializer will handle or fail)
        return obj

    def _apply_strategy(
        self,
        obj: Any,
        path: str,
        entry: RegistryEntry,
        result: WalkResult,
        proxy_identity: dict[int, str],
    ) -> Any:
        strategy = entry.strategy
        key = strategy.value
        result.strategy_counts[key] = result.strategy_counts.get(key, 0) + 1

        if strategy == Strategy.PASS:
            # For containers registered as PASS (list, dict, etc.),
            # we still need to walk their children — but the container itself passes.
            # However, primitives (int, str, etc.) are truly pass-through.
            return obj

        if strategy == Strategy.CONVERT:
            if entry.converter is None:
                logger.warning("CONVERT entry for %s has no converter, passing through", path)
                return obj
            return entry.converter(obj)

        if strategy == Strategy.REDIRECT:
            resource = entry.redirect_resource or "unknown"
            result.redirects[path] = resource
            return {"__flexiq_redirect__": True, "resource": resource}

        if strategy == Strategy.PROXY:
            return self._apply_proxy(obj, entry, proxy_identity)

        if strategy == Strategy.REJECT:
            result.failures.append(
                ArgumentFailure(
                    path=path,
                    type_name=type(obj).__qualname__,
                    reason=entry.reject_reason or "This type cannot be serialized.",
                    suggestions=entry.reject_suggestions,
                )
            )
            return obj

        return obj

    def _apply_proxy(
        self,
        obj: Any,
        entry: RegistryEntry,
        proxy_identity: dict[int, str],
    ) -> Any:
        """Deconstruct an object into a proxy marker via its handler."""
        handler_name = entry.proxy_handler
        if handler_name is None or self._proxy_registry is None:
            return obj

        handler = self._proxy_registry.get(handler_name)
        if handler is None or not handler.detect(obj):
            return obj

        # Identity tracking: same object in multiple positions → single recipe
        obj_id = id(obj)
        if obj_id in proxy_identity:
            return {"__flexiq_ref__": proxy_identity[obj_id]}

        identity = str(uuid.uuid4())
        proxy_identity[obj_id] = identity
        recipe = handler.deconstruct(obj)
        return {
            "__flexiq_proxy__": True,
            "handler": handler_name,
            "version": handler.version,
            "identity": identity,
            "recipe": recipe,
        }
