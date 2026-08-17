"""RequestsSessionHandler — proxy for requests.Session (optional dep)."""

from __future__ import annotations

from typing import Any

try:
    import requests

    _HAS_REQUESTS = True
except ImportError:
    _HAS_REQUESTS = False


class RequestsSessionHandler:
    """Deconstructs/reconstructs requests.Session instances."""

    name = "requests_session"
    version = 1
    handled_types: tuple[type, ...] = ()

    def __init__(self) -> None:
        if _HAS_REQUESTS:
            self.handled_types = (requests.Session,)

    def detect(self, obj: Any) -> bool:
        if not _HAS_REQUESTS:
            return False
        return isinstance(obj, requests.Session)

    def deconstruct(self, obj: Any) -> dict[str, Any]:
        return {
            "headers": dict(obj.headers),
            "cookies": dict(obj.cookies),
            "verify": obj.verify,
            "max_redirects": obj.max_redirects,
        }

    def reconstruct(self, recipe: dict[str, Any], version: int) -> Any:
        session = requests.Session()
        session.headers.update(recipe.get("headers", {}))
        for name, value in recipe.get("cookies", {}).items():
            session.cookies.set(name, value)
        session.verify = recipe.get("verify", True)
        session.max_redirects = recipe.get("max_redirects", 30)
        return session

    def cleanup(self, obj: Any) -> None:
        obj.close()
