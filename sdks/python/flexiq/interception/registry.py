"""Type registry mapping Python types to interception strategies."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from flexiq.interception.strategy import Strategy

# Sentinel distinguishing "not cached" from "cached as no-match (None)".
_UNCACHED = object()


@dataclass
class RegistryEntry:
    """A single type → strategy mapping in the registry."""

    types: tuple[type, ...]
    strategy: Strategy
    priority: int = 0
    converter: Callable[[Any], Any] | None = None
    reconstructor: Callable[[Any], Any] | None = None
    type_key: str | None = None
    reject_reason: str | None = None
    reject_suggestions: list[str] = field(default_factory=list)
    redirect_resource: str | None = None
    proxy_handler: str | None = None


class TypeRegistry:
    """Priority-sorted registry of type → strategy mappings.

    More specific types (higher priority) are checked first.
    At resolve time, a linear scan with ``isinstance()`` finds the
    first matching entry — fast enough for typical registries (20-50 entries).
    """

    def __init__(self) -> None:
        self._entries: list[RegistryEntry] = []
        self._sorted = False
        # Memoize resolution per concrete object type — the result for a given
        # type is stable until the registry changes. Cleared on register().
        self._cache: dict[type, RegistryEntry | None] = {}

    def register(
        self,
        types: type | tuple[type, ...],
        strategy: Strategy,
        *,
        priority: int = 0,
        converter: Callable[[Any], Any] | None = None,
        reconstructor: Callable[[Any], Any] | None = None,
        type_key: str | None = None,
        reject_reason: str | None = None,
        reject_suggestions: list[str] | None = None,
        redirect_resource: str | None = None,
        proxy_handler: str | None = None,
    ) -> None:
        """Register a type (or tuple of types) with an interception strategy."""
        if isinstance(types, type):
            types = (types,)
        entry = RegistryEntry(
            types=types,
            strategy=strategy,
            priority=priority,
            converter=converter,
            reconstructor=reconstructor,
            type_key=type_key,
            reject_reason=reject_reason,
            reject_suggestions=reject_suggestions or [],
            redirect_resource=redirect_resource,
            proxy_handler=proxy_handler,
        )
        self._entries.append(entry)
        self._sorted = False
        self._cache.clear()

    def resolve(self, obj: Any) -> RegistryEntry | None:
        """Find the highest-priority entry matching ``obj``."""
        if not self._sorted:
            self._entries.sort(key=lambda e: e.priority, reverse=True)
            self._sorted = True

        obj_type = type(obj)
        cached = self._cache.get(obj_type, _UNCACHED)
        if cached is not _UNCACHED:
            return cached  # type: ignore[return-value]

        result: RegistryEntry | None = None
        for entry in self._entries:
            try:
                if isinstance(obj, entry.types):
                    result = entry
                    break
            except TypeError:
                # Entry contains a non-type — skip it
                continue
        self._cache[obj_type] = result
        return result

    def __len__(self) -> int:
        return len(self._entries)
