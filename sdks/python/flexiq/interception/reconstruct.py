"""Worker-side reconstruction of intercepted arguments."""

from __future__ import annotations

from typing import Any

from flexiq.interception.converters import reconstruct_converted

# Sentinel keys used to identify interceptor markers in deserialized payloads
_CONVERT_KEY = "__flexiq_convert__"
_REDIRECT_KEY = "__flexiq_redirect__"
_PROXY_KEY = "__flexiq_proxy__"


def is_marker(obj: Any) -> bool:
    """Check if an object is a flexiq interception marker dict."""
    return isinstance(obj, dict) and (
        _CONVERT_KEY in obj or _REDIRECT_KEY in obj or _PROXY_KEY in obj
    )


def reconstruct_args(args: tuple, kwargs: dict) -> tuple[tuple, dict, dict[str, str]]:
    """Reconstruct intercepted arguments on the worker side.

    Scans (args, kwargs) for interception markers and reconstructs them:
    - CONVERT markers are reconstructed to their original types.
    - REDIRECT markers are collected (resource name) for later DI injection.
    - PROXY markers are left as-is for now (Phase 3).

    Returns:
        (reconstructed_args, reconstructed_kwargs, redirects)
        where redirects maps kwarg_name -> resource_name.
    """
    redirects: dict[str, str] = {}

    new_args = tuple(_reconstruct_value(v, f"args[{i}]", redirects) for i, v in enumerate(args))
    new_kwargs = {}
    skip_keys: set[str] = set()

    for k, v in kwargs.items():
        if isinstance(v, dict) and v.get(_REDIRECT_KEY):
            redirects[k] = v["resource"]
            skip_keys.add(k)
        else:
            new_kwargs[k] = _reconstruct_value(v, f"kwargs.{k}", redirects)

    return new_args, new_kwargs, redirects


def _reconstruct_value(obj: Any, path: str, redirects: dict[str, str]) -> Any:
    """Reconstruct a single value, walking containers recursively."""
    if obj is None:
        return obj

    if isinstance(obj, dict):
        if obj.get(_CONVERT_KEY):
            return reconstruct_converted(obj)
        if obj.get(_REDIRECT_KEY):
            return obj  # handled at top level in reconstruct_args
        if obj.get(_PROXY_KEY):
            return obj  # Phase 3
        # Regular dict — recurse
        return {k: _reconstruct_value(v, f"{path}.{k}", redirects) for k, v in obj.items()}

    if isinstance(obj, (list, tuple)):
        walked = [_reconstruct_value(v, f"{path}[{i}]", redirects) for i, v in enumerate(obj)]
        return type(obj)(walked)

    return obj
