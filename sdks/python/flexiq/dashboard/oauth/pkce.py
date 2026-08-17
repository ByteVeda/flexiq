"""PKCE S256 code-challenge derivation."""

from __future__ import annotations

import base64
import hashlib


def s256_challenge(verifier: str) -> str:
    """Return the S256 code-challenge for ``verifier`` per RFC 7636.

    The challenge is ``base64url(sha256(verifier))`` with trailing ``=``
    padding stripped, matching every OAuth provider's PKCE implementation.
    """
    digest = hashlib.sha256(verifier.encode("ascii")).digest()
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")
