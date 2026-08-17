"""Worker-side reconstruction and cleanup of proxy markers."""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from flexiq.exceptions import ProxyReconstructionError
from flexiq.proxies.handler import ProxyHandler
from flexiq.proxies.registry import ProxyRegistry
from flexiq.proxies.schema import validate_recipe
from flexiq.proxies.signing import verify_recipe

if TYPE_CHECKING:
    from flexiq.proxies.metrics import ProxyMetrics

logger = logging.getLogger("flexiq.proxies")

# Set once we've warned about reconstructing proxy recipes without a signing
# key, so the warning fires at most once per process instead of per recipe.
_UNSIGNED_WARNED = False


def _warn_unsigned_once() -> None:
    """Warn (once) that a proxy recipe was reconstructed without verification.

    A proxy recipe travels inside the untrusted payload. With no signing key
    its integrity is unchecked, so anyone able to write to the queue can forge
    one. Surfacing this at the moment it actually happens avoids spamming every
    Queue that merely has the built-in handlers registered.
    """
    global _UNSIGNED_WARNED
    if _UNSIGNED_WARNED:
        return
    _UNSIGNED_WARNED = True
    logger.warning(
        "Reconstructing proxy recipes without integrity verification; set "
        "recipe_signing_key= or FLEXIQ_RECIPE_SECRET to enable HMAC signing."
    )


_PROXY_KEY = "__flexiq_proxy__"
_REF_KEY = "__flexiq_ref__"


class _ReconstructionTimeout(Exception):
    """Internal signal that a reconstruction exceeded its wall-clock budget."""


def _run_with_timeout(fn: Callable[[], Any], timeout: float) -> Any:
    """Run ``fn`` with a wall-clock ``timeout`` and return its result.

    ``fn`` runs on a dedicated **daemon** thread so an overrunning reconstruction
    is abandoned without blocking either the worker or interpreter shutdown.
    This is the only safe in-process option:

    * A process pool could be terminated, but reconstructed resources (sessions,
      clients, file handles) are not picklable and so can't be returned across a
      process boundary.
    * A shared ``ThreadPoolExecutor`` force-joins its workers at interpreter exit
      (CPython issue 36780), so a hung task would hang shutdown.
    * Python cannot safely kill a running thread.

    The watchdog only bounds how long the *caller* waits; a hung reconstruction
    keeps running in the background until it returns. Hard runaway protection
    must come from the handler bounding its own I/O (connect/read timeouts).

    Raises :class:`_ReconstructionTimeout` if the budget is exceeded, and
    re-raises any exception ``fn`` itself raised.
    """
    result: list[Any] = []
    error: list[Exception] = []

    def runner() -> None:
        try:
            result.append(fn())
        except Exception as exc:  # surfaced to the caller below
            error.append(exc)

    worker = threading.Thread(target=runner, name="flexiq-proxy-reconstruct", daemon=True)
    worker.start()
    worker.join(timeout)
    if worker.is_alive():
        raise _ReconstructionTimeout
    if error:
        raise error[0]
    return result[0]


def reconstruct_proxies(
    args: tuple,
    kwargs: dict,
    registry: ProxyRegistry,
    *,
    signing_secret: str | None = None,
    max_timeout: int = 10,
    metrics: ProxyMetrics | None = None,
) -> tuple[tuple, dict, list[tuple[ProxyHandler, Any]]]:
    """Walk args/kwargs, reconstruct ``__flexiq_proxy__`` markers.

    Returns:
        (args, kwargs, cleanup_list) where cleanup_list contains
        (handler, reconstructed_object) pairs in reconstruction order.
    """
    identity_cache: dict[str, Any] = {}
    cleanup_list: list[tuple[ProxyHandler, Any]] = []

    new_args = tuple(
        _walk_proxy(
            v, registry, identity_cache, cleanup_list, signing_secret, max_timeout, metrics
        )
        for v in args
    )
    new_kwargs = {
        k: _walk_proxy(
            v, registry, identity_cache, cleanup_list, signing_secret, max_timeout, metrics
        )
        for k, v in kwargs.items()
    }
    return new_args, new_kwargs, cleanup_list


def cleanup_proxies(
    cleanup_list: list[tuple[ProxyHandler, Any]],
    metrics: ProxyMetrics | None = None,
) -> None:
    """Run cleanup on all reconstructed proxies in LIFO order."""
    for handler, obj in reversed(cleanup_list):
        try:
            handler.cleanup(obj)
        except Exception:
            logger.exception("Proxy cleanup failed for handler '%s'", handler.name)
            if metrics is not None:
                metrics.record_cleanup_error(handler.name)


def _walk_proxy(
    obj: Any,
    registry: ProxyRegistry,
    identity_cache: dict[str, Any],
    cleanup_list: list[tuple[ProxyHandler, Any]],
    signing_secret: str | None,
    max_timeout: int,
    metrics: ProxyMetrics | None,
) -> Any:
    """Recursively walk a value, reconstructing proxy markers."""
    if obj is None:
        return obj

    if isinstance(obj, dict):
        if obj.get(_PROXY_KEY):
            return _reconstruct_one(
                obj, registry, identity_cache, cleanup_list, signing_secret, max_timeout, metrics
            )
        if _REF_KEY in obj:
            identity = obj[_REF_KEY]
            if identity in identity_cache:
                return identity_cache[identity]
            raise ProxyReconstructionError(
                f"Proxy reference '{identity}' not found in identity cache"
            )
        # Regular dict — recurse
        return {
            k: _walk_proxy(
                v, registry, identity_cache, cleanup_list, signing_secret, max_timeout, metrics
            )
            for k, v in obj.items()
        }

    if isinstance(obj, (list, tuple)):
        walked = [
            _walk_proxy(
                v, registry, identity_cache, cleanup_list, signing_secret, max_timeout, metrics
            )
            for v in obj
        ]
        return type(obj)(walked)

    return obj


def _reconstruct_one(
    marker: dict[str, Any],
    registry: ProxyRegistry,
    identity_cache: dict[str, Any],
    cleanup_list: list[tuple[ProxyHandler, Any]],
    signing_secret: str | None,
    max_timeout: int,
    metrics: ProxyMetrics | None,
) -> Any:
    """Reconstruct a single __flexiq_proxy__ marker."""
    handler_name = marker["handler"]
    version = marker["version"]
    recipe = marker["recipe"]
    identity = marker.get("identity")

    # Check identity cache first (deduplication)
    if identity and identity in identity_cache:
        return identity_cache[identity]

    handler = registry.get(handler_name)
    if handler is None:
        raise ProxyReconstructionError(f"No proxy handler registered for '{handler_name}'")

    # Verify recipe signature if signing is enabled
    if signing_secret is not None:
        checksum = marker.get("checksum")
        if checksum is None:
            raise ProxyReconstructionError(
                f"Recipe for '{handler_name}' is missing checksum (signing is enabled)"
            )
        try:
            verify_recipe(handler_name, version, recipe, checksum, signing_secret)
        except ProxyReconstructionError:
            if metrics is not None:
                metrics.record_checksum_failure(handler_name)
            raise
    else:
        # No signing key: the recipe's integrity is unverified. Warn once so an
        # unsigned-reconstruction deployment is a deliberate choice, not silent.
        _warn_unsigned_once()

    # Schema validation
    handler_schema = getattr(handler, "schema", None)
    if handler_schema is not None:
        validate_recipe(handler_name, recipe, handler_schema)

    # Version migration
    handler_version = getattr(handler, "version", version)
    if version != handler_version and hasattr(handler, "migrate"):
        recipe = handler.migrate(recipe, version, handler_version)

    # Reconstruct with timeout
    start = time.monotonic()
    try:
        if max_timeout > 0:
            obj = _run_with_timeout(lambda: handler.reconstruct(recipe, version), max_timeout)
        else:
            obj = handler.reconstruct(recipe, version)
    except _ReconstructionTimeout:
        if metrics is not None:
            metrics.record_error(handler_name)
        raise ProxyReconstructionError(
            f"Reconstruction of '{handler_name}' timed out after {max_timeout}s"
        ) from None
    except ProxyReconstructionError:
        if metrics is not None:
            metrics.record_error(handler_name)
        raise
    except Exception as exc:
        if metrics is not None:
            metrics.record_error(handler_name)
        raise ProxyReconstructionError(
            f"Failed to reconstruct proxy '{handler_name}': {exc}"
        ) from exc
    finally:
        duration_ms = (time.monotonic() - start) * 1000
        if metrics is not None:
            metrics.record_reconstruction(handler_name, duration_ms)

    if identity:
        identity_cache[identity] = obj
    cleanup_list.append((handler, obj))
    return obj
