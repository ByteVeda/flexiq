"""HttpxClientHandler — proxy for httpx.Client / httpx.AsyncClient (optional dep)."""

from __future__ import annotations

from typing import Any

try:
    import httpx  # type: ignore[import-not-found]

    _HAS_HTTPX = True
except ImportError:
    _HAS_HTTPX = False


class HttpxClientHandler:
    """Deconstructs/reconstructs httpx Client instances."""

    name = "httpx_client"
    version = 1
    handled_types: tuple[type, ...] = ()

    def __init__(self) -> None:
        if _HAS_HTTPX:
            self.handled_types = (httpx.Client, httpx.AsyncClient)

    def detect(self, obj: Any) -> bool:
        if not _HAS_HTTPX:
            return False
        return isinstance(obj, (httpx.Client, httpx.AsyncClient))

    def deconstruct(self, obj: Any) -> dict[str, Any]:
        recipe: dict[str, Any] = {
            "headers": dict(obj.headers),
            "cookies": dict(obj.cookies),
            "follow_redirects": obj.follow_redirects,
            "is_async": isinstance(obj, httpx.AsyncClient),
        }
        if obj.base_url:
            recipe["base_url"] = str(obj.base_url)
        return recipe

    def reconstruct(self, recipe: dict[str, Any], version: int) -> Any:
        kwargs: dict[str, Any] = {
            "headers": recipe.get("headers", {}),
            "cookies": recipe.get("cookies", {}),
            "follow_redirects": recipe.get("follow_redirects", True),
        }
        if recipe.get("base_url"):
            kwargs["base_url"] = recipe["base_url"]

        if recipe.get("is_async"):
            return httpx.AsyncClient(**kwargs)
        return httpx.Client(**kwargs)

    def cleanup(self, obj: Any) -> None:
        if isinstance(obj, httpx.AsyncClient):
            # AsyncClient provides a sync close() for cleanup scenarios
            if not obj.is_closed:
                obj.close()
        elif not obj.is_closed:
            obj.close()
