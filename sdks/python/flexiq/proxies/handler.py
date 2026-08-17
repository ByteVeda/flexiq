"""ProxyHandler protocol and ProxyRecipe dataclass."""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class ProxyHandler(Protocol):
    """Interface for proxy handlers that deconstruct/reconstruct objects."""

    name: str
    version: int
    handled_types: tuple[type, ...]

    def detect(self, obj: Any) -> bool:
        """Return True if this handler can proxy this specific instance."""
        ...

    def deconstruct(self, obj: Any) -> dict[str, Any]:
        """Extract a JSON-serializable recipe from the live object."""
        ...

    def reconstruct(self, recipe: dict[str, Any], version: int) -> Any:
        """Rebuild the object from a recipe dict."""
        ...

    def cleanup(self, obj: Any) -> None:
        """Clean up the reconstructed object (e.g. close file handles)."""
        ...
