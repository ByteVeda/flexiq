"""Static asset delivery for the dashboard SPA.

Resolves files bundled at ``sdks/python/flexiq/static/dashboard/`` (the Vite
build output) and serves them with the right Content-Type. Treat
``StaticAssets`` instances as immutable — the root is fixed at
construction.
"""

from __future__ import annotations

import os
import threading
from importlib import resources
from typing import Any

_CONTENT_TYPES: dict[str, str] = {
    ".html": "text/html; charset=utf-8",
    ".js": "application/javascript; charset=utf-8",
    ".mjs": "application/javascript; charset=utf-8",
    ".css": "text/css; charset=utf-8",
    ".json": "application/json; charset=utf-8",
    ".svg": "image/svg+xml",
    ".png": "image/png",
    ".ico": "image/x-icon",
    ".webmanifest": "application/manifest+json",
    ".woff2": "font/woff2",
    ".woff": "font/woff",
    ".ttf": "font/ttf",
    ".txt": "text/plain; charset=utf-8",
    ".map": "application/json; charset=utf-8",
}

_STATIC_ROOT_REL = "static/dashboard"
IMMUTABLE_PREFIX = "/assets/"

MISSING_ASSETS_HTML = (
    "<!doctype html><html><head><meta charset='utf-8'>"
    "<title>FlexiQ — dashboard assets missing</title></head>"
    "<body style='font-family:system-ui;padding:2rem;max-width:640px'>"
    "<h1>Dashboard assets not bundled</h1>"
    "<p>This flexiq install doesn't ship the compiled dashboard. "
    "If you're working from source, rebuild with:</p>"
    "<pre>pnpm --dir dashboard install &amp;&amp; pnpm --dir dashboard build</pre>"
    "<p>Then reinstall the package (<code>uv sync --reinstall-package flexiq</code> "
    "or <code>pip install -e .</code>).</p>"
    "</body></html>"
)


def _content_type_for(path: str) -> str:
    """Return the Content-Type for a request path by extension."""
    ext = os.path.splitext(path)[1].lower()
    return _CONTENT_TYPES.get(ext, "application/octet-stream")


def _resolve_static_node(base: Any, rel_path: str) -> Any | None:
    """Resolve a request path to a file node under ``base``.

    Rejects traversal attempts, null bytes, and backslash escapes. Returns
    ``None`` if the resolved node is not an existing regular file.

    ``base`` must support ``joinpath(name)``; the returned node must
    support ``is_file()`` and ``read_bytes()``. Works with both
    ``pathlib.Path`` and ``importlib.resources.abc.Traversable``.
    """
    clean = rel_path.lstrip("/")
    if not clean:
        return None
    parts = clean.split("/")
    for part in parts:
        if part in ("", ".", ".."):
            return None
        if "\x00" in part or "\\" in part:
            return None
    node = base
    for part in parts:
        node = node.joinpath(part)
    return node if node.is_file() else None


class StaticAssets:
    """Resolves dashboard SPA files under a single root.

    Treat instances as immutable — the root is fixed at construction.
    Pass one explicitly to :func:`serve_dashboard` or ``_make_handler``
    to override the default lookup; this is the seam tests use to swap in
    a tmp directory or force the missing-assets fallback without touching
    module-level state.
    """

    __slots__ = ("_root",)

    def __init__(self, root: Any | None) -> None:
        self._root = root

    @classmethod
    def from_package(cls) -> StaticAssets:
        """Locate assets bundled with the installed ``flexiq`` package.

        Returns an instance whose root points at ``static/dashboard/`` if
        ``index.html`` is present in the wheel, otherwise an instance with
        ``available is False`` so the handler can render the missing-
        assets fallback.
        """
        try:
            candidate = resources.files("flexiq").joinpath(_STATIC_ROOT_REL)
            if candidate.joinpath("index.html").is_file():
                return cls(candidate)
        except (ModuleNotFoundError, FileNotFoundError, AttributeError):
            pass
        return cls(None)

    @property
    def available(self) -> bool:
        return self._root is not None

    def resolve(self, rel_path: str) -> Any | None:
        """Return a file node for ``rel_path`` under the root, or ``None``."""
        if self._root is None:
            return None
        return _resolve_static_node(self._root, rel_path)

    def index(self) -> Any | None:
        """Return the ``index.html`` node, or ``None`` if assets aren't bundled."""
        if self._root is None:
            return None
        node = self._root.joinpath("index.html")
        return node if node.is_file() else None


_default_assets_lock = threading.Lock()
_default_assets: StaticAssets | None = None


def _get_default_assets() -> StaticAssets:
    """Lazily resolve and memoise the package-bundled ``StaticAssets``.

    Cheap to call repeatedly — the actual filesystem probe runs at most
    once per process. Tests don't touch this; they construct a
    ``StaticAssets`` directly and pass it to the handler.
    """
    global _default_assets
    if _default_assets is not None:
        return _default_assets
    with _default_assets_lock:
        if _default_assets is None:
            _default_assets = StaticAssets.from_package()
    return _default_assets
