"""Frame codec for the worker protocol.

A frame is a JSON header line followed by exactly the number of raw payload
bytes it declares. The blob stays raw rather than base64 inside the header so
the bytes on the wire are the wire-envelope bytes themselves. The same format
serves a pipe (prefork children) and a socket (an attached executor).

Mirrors ``crates/taskito-core/src/worker/protocol.rs``; the version constant is
read from the native module so it is never restated here.
"""

from __future__ import annotations

import json
from typing import Any, BinaryIO

from taskito._taskito import WORKER_PROTOCOL_VERSION

__all__ = [
    "MAX_HEADER_BYTES",
    "MAX_PAYLOAD_BYTES",
    "WORKER_PROTOCOL_VERSION",
    "ProtocolError",
    "declared_payload_len",
    "read_frame",
    "write_frame",
]

# Header cap, bounding a peer that never sends a newline.
MAX_HEADER_BYTES = 64 * 1024

# Payload cap, so a corrupt length field cannot allocate unboundedly.
MAX_PAYLOAD_BYTES = 64 * 1024 * 1024


class ProtocolError(Exception):
    """A frame could not be encoded or decoded."""


def declared_payload_len(header: dict[str, Any]) -> int:
    """Bytes of payload a header says follow it."""
    kind = header.get("type")
    if kind == "job":
        return int(header.get("payload_len") or 0)
    if kind == "success":
        result_len = header.get("result_len")
        return 0 if result_len is None else int(result_len)
    return 0


def write_frame(stream: BinaryIO, header: dict[str, Any], payload: bytes = b"") -> None:
    """Write one frame and flush it.

    A length disagreement would desync the reader, so it is rejected before
    anything reaches the wire.
    """
    declared = declared_payload_len(header)
    if declared != len(payload):
        raise ProtocolError(
            f"frame declared {declared} payload bytes but {len(payload)} were supplied"
        )
    stream.write(json.dumps(header, separators=(",", ":")).encode() + b"\n")
    if payload:
        stream.write(payload)
    stream.flush()


def read_frame(stream: BinaryIO) -> tuple[dict[str, Any], bytes]:
    """Read one frame. Raises ``EOFError`` when the peer closes between frames."""
    line = stream.readline(MAX_HEADER_BYTES + 1)
    if not line:
        raise EOFError("peer closed the connection")
    if not line.endswith(b"\n"):
        raise ProtocolError(f"frame header exceeds {MAX_HEADER_BYTES} bytes")

    try:
        header = json.loads(line)
    except json.JSONDecodeError as exc:
        raise ProtocolError(f"malformed frame header: {exc}") from exc
    if not isinstance(header, dict):
        raise ProtocolError("frame header must be a JSON object")

    length = declared_payload_len(header)
    if length > MAX_PAYLOAD_BYTES:
        raise ProtocolError(
            f"frame payload of {length} bytes exceeds the {MAX_PAYLOAD_BYTES} byte limit"
        )
    return header, _read_exact(stream, length)


def _read_exact(stream: BinaryIO, length: int) -> bytes:
    """Read exactly ``length`` bytes, looping because a raw stream may short-read."""
    if length == 0:
        return b""
    chunks: list[bytes] = []
    remaining = length
    while remaining:
        chunk = stream.read(remaining)
        if not chunk:
            raise ProtocolError(f"truncated frame payload: wanted {length} bytes")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)
