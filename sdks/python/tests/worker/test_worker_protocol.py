"""Frame codec guards: a malformed length must never become a read count."""

from __future__ import annotations

import io

import pytest

from taskito.worker_protocol import ProtocolError, read_frame, write_frame


def _frame(header: bytes, payload: bytes = b"") -> io.BytesIO:
    return io.BytesIO(header + b"\n" + payload)


def test_negative_payload_len_is_rejected() -> None:
    # read(-1) would drain the connection to EOF rather than read a payload.
    stream = _frame(b'{"type":"job","payload_len":-1}', b"trailing")
    with pytest.raises(ProtocolError, match="must not be negative"):
        read_frame(stream)


def test_non_integer_payload_len_is_rejected() -> None:
    stream = _frame(b'{"type":"job","payload_len":"12"}')
    with pytest.raises(ProtocolError, match="must be an integer"):
        read_frame(stream)


def test_oversized_payload_len_is_rejected() -> None:
    stream = _frame(b'{"type":"job","payload_len":99999999999}')
    with pytest.raises(ProtocolError, match="exceeds"):
        read_frame(stream)


def test_write_rejects_a_length_that_disagrees_with_the_payload() -> None:
    with pytest.raises(ProtocolError, match="declared"):
        write_frame(io.BytesIO(), {"type": "job", "payload_len": 4}, b"ab")


def test_round_trip_preserves_bytes_containing_newlines() -> None:
    payload = b'\n{"type":"success"}\n\x00\xff'
    sink = io.BytesIO()
    write_frame(sink, {"type": "job", "payload_len": len(payload)}, payload)
    header, read_back = read_frame(io.BytesIO(sink.getvalue()))
    assert header["type"] == "job"
    assert read_back == payload
