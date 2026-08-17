"""HMAC-SHA256 recipe signing for proxy markers."""

from __future__ import annotations

import hashlib
import hmac
import json
from typing import Any

from flexiq.exceptions import ProxyReconstructionError


def _key_bytes(secret: str | bytes) -> bytes:
    """Normalise the signing secret to bytes (UTF-8 for ``str``)."""
    return secret if isinstance(secret, bytes) else secret.encode("utf-8")


def sign_recipe(
    handler_name: str, version: int, recipe: dict[str, Any], secret: str | bytes
) -> str:
    """Compute HMAC-SHA256 over handler_name + version + canonical JSON.

    The signature covers the handler name, version, and recipe — but not the
    owning job, so a valid signed recipe can be replayed into another job.
    Treat the signing key as a shared secret and rotate it if leaked.
    """
    canonical = f"{handler_name}:{version}:" + json.dumps(
        recipe, sort_keys=True, separators=(",", ":")
    )
    return hmac.new(_key_bytes(secret), canonical.encode("utf-8"), hashlib.sha256).hexdigest()


def verify_recipe(
    handler_name: str,
    version: int,
    recipe: dict[str, Any],
    checksum: str,
    secret: str | bytes,
) -> bool:
    """Return True if checksum matches; raise ProxyReconstructionError if not."""
    expected = sign_recipe(handler_name, version, recipe, secret)
    if not hmac.compare_digest(expected, checksum):
        raise ProxyReconstructionError(
            f"Recipe integrity check failed for handler '{handler_name}': "
            "checksum mismatch (possible tampering)"
        )
    return True
