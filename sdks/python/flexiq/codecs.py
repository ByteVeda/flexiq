"""Payload codecs: reversible byte transforms layered over a serializer.

A :class:`PayloadCodec` transforms serialized bytes on the way out (producer)
and reverses the transform on the way in (worker) — compression, encryption,
signing. Codecs compose: a chain encodes in list order and decodes in reverse,
so ``[gzip, hmac]`` verifies integrity *before* decompressing.

Wire formats are part of the cross-SDK contract, so codec-framed payloads
decode from any FlexiQ SDK.

Enable globally via ``Queue(codec=...)`` (wraps the queue serializer, covers
payloads and results) or per task via ``Queue(codecs={"name": codec})`` +
``@queue.task(codecs=["name"])`` (payload only; results stay on the queue
serializer). Jobs persisted before a codec chain is enabled cannot be decoded
through it — drain the queue before turning codecs on.
"""

from __future__ import annotations

import gzip
import hashlib
import hmac
import os
import zlib
from collections.abc import Sequence
from typing import Any, Protocol, runtime_checkable

from .exceptions import CryptoError, SerializationError
from .serializers import Serializer

_GZIP_WBITS = 16 + zlib.MAX_WBITS  # gzip framing for zlib.decompressobj
_HMAC_DIGEST_SIZE = 32  # SHA-256 output length in bytes
_AES_NONCE_SIZE = 12  # GCM standard nonce length in bytes


@runtime_checkable
class PayloadCodec(Protocol):
    """Protocol for reversible payload byte transforms."""

    def encode(self, data: bytes) -> bytes:
        """Transform serialized bytes on the producer (compress, encrypt, sign)."""
        ...

    def decode(self, data: bytes) -> bytes:
        """Reverse the transform on the worker."""
        ...


class GzipCodec:
    """Gzip compression codec.

    Decompression is capped at ``max_decompressed_bytes`` (default 64 MiB) so a
    malicious or corrupt payload cannot expand into a zip bomb.
    """

    _DEFAULT_MAX_DECOMPRESSED_BYTES = 64 * 1024 * 1024

    def __init__(self, max_decompressed_bytes: int = _DEFAULT_MAX_DECOMPRESSED_BYTES):
        if max_decompressed_bytes <= 0:
            raise ValueError(
                f"max_decompressed_bytes must be positive, got {max_decompressed_bytes}"
            )
        self._max_decompressed_bytes = max_decompressed_bytes

    def encode(self, data: bytes) -> bytes:
        return gzip.compress(data)

    def decode(self, data: bytes) -> bytes:
        # stdlib gzip.decompress has no output bound; stream through a
        # decompressobj with max_length so the cap holds mid-stream.
        decompressor = zlib.decompressobj(wbits=_GZIP_WBITS)
        try:
            output = decompressor.decompress(data, self._max_decompressed_bytes)
            if decompressor.unconsumed_tail:
                raise SerializationError(
                    f"decompressed payload exceeds the {self._max_decompressed_bytes}-byte limit"
                )
            output += decompressor.flush()
        except zlib.error as exc:
            raise SerializationError(f"gzip decompression failed: {exc}") from exc
        if len(output) > self._max_decompressed_bytes:
            raise SerializationError(
                f"decompressed payload exceeds the {self._max_decompressed_bytes}-byte limit"
            )
        return output


class AesGcmCodec:
    """AES-GCM encryption codec.

    Wire format: ``[12-byte random nonce][ciphertext || 16-byte GCM tag]`` —
    the cross-SDK contract. Requires the ``encryption`` extra
    (``pip install flexiq[encryption]``). Key must be 16, 24, or 32 bytes
    (AES-128/192/256).
    """

    def __init__(self, key: bytes):
        if not isinstance(key, bytes):
            raise TypeError(f"key must be bytes, got {type(key).__name__}")
        if len(key) not in (16, 24, 32):
            raise ValueError(
                f"key must be 16, 24, or 32 bytes for AES-128/192/256, got {len(key)} bytes"
            )

        from cryptography.exceptions import InvalidTag
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM

        self._aesgcm = AESGCM(key)
        # Cache the exception class so ``decode`` doesn't re-import per call.
        self._invalid_tag = InvalidTag

    def encode(self, data: bytes) -> bytes:
        nonce = os.urandom(_AES_NONCE_SIZE)
        return bytes(nonce + self._aesgcm.encrypt(nonce, data, None))

    def decode(self, data: bytes) -> bytes:
        if len(data) < _AES_NONCE_SIZE:
            raise CryptoError("encrypted payload is too short")
        nonce, ciphertext = data[:_AES_NONCE_SIZE], data[_AES_NONCE_SIZE:]
        try:
            return bytes(self._aesgcm.decrypt(nonce, ciphertext, None))
        except self._invalid_tag as exc:
            raise CryptoError("decryption failed") from exc


class HmacCodec:
    """HMAC-SHA256 signing codec (authenticates, does not encrypt).

    Wire format: ``[32-byte mac][body]`` — the cross-SDK contract.
    """

    def __init__(self, key: bytes):
        if not isinstance(key, bytes):
            raise TypeError(f"key must be bytes, got {type(key).__name__}")
        if not key:
            raise ValueError("key must not be empty")
        self._key = key

    def encode(self, data: bytes) -> bytes:
        mac = hmac.new(self._key, data, hashlib.sha256).digest()
        return mac + data

    def decode(self, data: bytes) -> bytes:
        if len(data) < _HMAC_DIGEST_SIZE:
            raise CryptoError("signed payload is too short")
        mac, body = data[:_HMAC_DIGEST_SIZE], data[_HMAC_DIGEST_SIZE:]
        expected = hmac.new(self._key, body, hashlib.sha256).digest()
        if not hmac.compare_digest(mac, expected):
            raise CryptoError("signature mismatch")
        return body


class CodecSerializer:
    """Serializer decorator applying a codec chain around a delegate.

    ``dumps`` serializes with the delegate then encodes codecs in list order;
    ``loads`` decodes in reverse order then deserializes. Used internally when
    ``Queue(codec=...)`` is set — the chain then covers every payload and
    result flowing through the queue serializer.
    """

    def __init__(self, delegate: Serializer, codecs: Sequence[PayloadCodec]):
        self._delegate = delegate
        self._codecs = list(codecs)

    def dumps(self, obj: Any) -> bytes:
        data = self._delegate.dumps(obj)
        for codec in self._codecs:
            data = codec.encode(data)
        return data

    def loads(self, data: bytes) -> Any:
        for codec in reversed(self._codecs):
            data = codec.decode(data)
        return self._delegate.loads(data)
