"""Unit tests for payload codecs and the codec serializer."""

import gzip
import threading
from collections.abc import Generator
from contextlib import AbstractContextManager, contextmanager
from pathlib import Path

import pytest

from flexiq import (
    AesGcmCodec,
    CodecSerializer,
    CryptoError,
    GzipCodec,
    HmacCodec,
    JsonSerializer,
    PayloadCodec,
    Queue,
    SerializationError,
    Serializer,
    SmartSerializer,
)

HMAC_KEY = b"codec-hmac-secret"
AES_KEY = b"0123456789abcdef0123456789abcdef"  # 32 bytes -> AES-256


def make_aes_codec() -> AesGcmCodec:
    pytest.importorskip("cryptography")
    return AesGcmCodec(AES_KEY)


class TestGzipCodec:
    def test_round_trip(self) -> None:
        codec = GzipCodec()
        data = b"hello codec world" * 100
        encoded = codec.encode(data)
        assert encoded != data
        assert len(encoded) < len(data)
        assert codec.decode(encoded) == data

    def test_round_trip_empty(self) -> None:
        codec = GzipCodec()
        assert codec.decode(codec.encode(b"")) == b""

    def test_decode_rejects_payload_exceeding_cap(self) -> None:
        big = b"a" * 1024
        encoded = GzipCodec().encode(big)
        with pytest.raises(SerializationError, match="exceeds"):
            GzipCodec(max_decompressed_bytes=16).decode(encoded)

    def test_decode_at_exact_cap_succeeds(self) -> None:
        data = b"a" * 64
        encoded = GzipCodec().encode(data)
        assert GzipCodec(max_decompressed_bytes=64).decode(encoded) == data

    def test_decode_rejects_corrupt_stream(self) -> None:
        with pytest.raises(SerializationError, match="decompression failed"):
            GzipCodec().decode(b"not gzip data")

    def test_constructor_rejects_non_positive_cap(self) -> None:
        with pytest.raises(ValueError, match="positive"):
            GzipCodec(max_decompressed_bytes=0)
        with pytest.raises(ValueError, match="positive"):
            GzipCodec(max_decompressed_bytes=-1)


class TestAesGcmCodec:
    def test_round_trip_and_hides_plaintext(self) -> None:
        codec = make_aes_codec()
        data = b"secret payload"
        encoded = codec.encode(data)
        assert data not in encoded
        assert codec.decode(encoded) == data

    def test_wire_format_nonce_prefix(self) -> None:
        # [12-byte nonce][ciphertext || 16-byte tag] — cross-SDK contract.
        codec = make_aes_codec()
        data = b"x" * 10
        encoded = codec.encode(data)
        assert len(encoded) == 12 + len(data) + 16

    def test_decode_rejects_tampered_payload(self) -> None:
        codec = make_aes_codec()
        encoded = bytearray(codec.encode(b"secret payload"))
        encoded[-1] ^= 0xFF
        with pytest.raises(CryptoError, match="decryption failed"):
            codec.decode(bytes(encoded))

    def test_decode_rejects_short_payload(self) -> None:
        codec = make_aes_codec()
        with pytest.raises(CryptoError, match="too short"):
            codec.decode(b"short")

    def test_constructor_rejects_bad_key_length(self) -> None:
        with pytest.raises(ValueError, match="16, 24, or 32"):
            AesGcmCodec(b"short-key")

    def test_constructor_rejects_non_bytes_key(self) -> None:
        with pytest.raises(TypeError, match="bytes"):
            AesGcmCodec("string-key")  # type: ignore[arg-type]


class TestHmacCodec:
    def test_round_trip(self) -> None:
        codec = HmacCodec(HMAC_KEY)
        data = b"authenticated payload"
        encoded = codec.encode(data)
        assert encoded[32:] == data  # [32-byte mac][body] — cross-SDK contract
        assert codec.decode(encoded) == data

    def test_decode_rejects_tampered_payload(self) -> None:
        codec = HmacCodec(HMAC_KEY)
        encoded = bytearray(codec.encode(b"authenticated payload"))
        encoded[-1] ^= 0xFF
        with pytest.raises(CryptoError, match="signature mismatch"):
            codec.decode(bytes(encoded))

    def test_decode_rejects_wrong_key(self) -> None:
        encoded = HmacCodec(HMAC_KEY).encode(b"payload")
        with pytest.raises(CryptoError, match="signature mismatch"):
            HmacCodec(b"different-key").decode(encoded)

    def test_decode_rejects_short_payload(self) -> None:
        with pytest.raises(CryptoError, match="too short"):
            HmacCodec(HMAC_KEY).decode(b"short")

    def test_constructor_rejects_non_bytes_key(self) -> None:
        with pytest.raises(TypeError, match="bytes"):
            HmacCodec("string-key")  # type: ignore[arg-type]

    def test_constructor_rejects_empty_key(self) -> None:
        with pytest.raises(ValueError, match="must not be empty"):
            HmacCodec(b"")


class TestPayloadCodecProtocol:
    def test_built_in_codecs_satisfy_protocol(self) -> None:
        assert isinstance(GzipCodec(), PayloadCodec)
        assert isinstance(HmacCodec(HMAC_KEY), PayloadCodec)


class TestCodecSerializer:
    @pytest.mark.parametrize("delegate", [SmartSerializer(), JsonSerializer()])
    def test_chain_is_reversible_in_reverse_order(self, delegate: Serializer) -> None:
        pytest.importorskip("cryptography")
        chain: list[PayloadCodec] = [GzipCodec(), make_aes_codec(), HmacCodec(HMAC_KEY)]
        serializer = CodecSerializer(delegate, chain)
        obj = {"numbers": [1, 2, 3], "text": "hello" * 50}
        assert serializer.loads(serializer.dumps(obj)) == obj

    def test_encoded_bytes_are_codec_framed(self) -> None:
        serializer = CodecSerializer(SmartSerializer(), [GzipCodec()])
        encoded = serializer.dumps({"key": "value"})
        assert encoded[:2] == b"\x1f\x8b"  # gzip magic
        assert gzip.decompress(encoded) == SmartSerializer().dumps({"key": "value"})

    def test_tamper_detected_before_decompression(self) -> None:
        # decode runs in reverse: HMAC verifies before gzip touches the bytes.
        serializer = CodecSerializer(SmartSerializer(), [GzipCodec(), HmacCodec(HMAC_KEY)])
        encoded = bytearray(serializer.dumps("payload"))
        encoded[-1] ^= 0xFF
        with pytest.raises(CryptoError, match="signature mismatch"):
            serializer.loads(bytes(encoded))

    def test_empty_chain_is_transparent(self) -> None:
        serializer = CodecSerializer(SmartSerializer(), [])
        obj = (1, "two", [3])
        assert serializer.loads(serializer.dumps(obj)) == obj
        assert serializer.dumps(obj) == SmartSerializer().dumps(obj)


class TestQueueCodecIntegration:
    @staticmethod
    def _run_worker(queue: Queue) -> AbstractContextManager[None]:
        @contextmanager
        def _ctx() -> Generator[None]:
            thread = threading.Thread(target=queue.run_worker, daemon=True)
            thread.start()
            try:
                yield
            finally:
                queue._inner.request_shutdown()
                thread.join(timeout=5)

        return _ctx()

    def test_global_chain_round_trips_through_worker(self, tmp_path: Path) -> None:
        queue = Queue(
            db_path=str(tmp_path / "chain.db"),
            workers=1,
            codec=[GzipCodec(), HmacCodec(HMAC_KEY)],
        )

        @queue.task()
        def double(x: int) -> int:
            return x * 2

        with self._run_worker(queue):
            result = double.delay(21)
            assert result.result(timeout=10) == 42
            # Both the payload and the result are codec-framed in storage.
            job = queue._inner.get_job(result.id)
            assert job is not None and job.result_bytes is not None
            hmac_codec = HmacCodec(HMAC_KEY)
            assert gzip.decompress(hmac_codec.decode(bytes(job.payload_bytes)))
            assert gzip.decompress(hmac_codec.decode(bytes(job.result_bytes)))

    def test_single_codec_accepted_without_list(self, tmp_path: Path) -> None:
        queue = Queue(db_path=str(tmp_path / "single.db"), workers=1, codec=GzipCodec())
        assert len(queue._codec_chain) == 1

    def test_per_task_codec_round_trips_through_worker(self, tmp_path: Path) -> None:
        queue = Queue(
            db_path=str(tmp_path / "pertask.db"),
            workers=1,
            codecs={"gzip": GzipCodec()},
        )

        @queue.task(codecs=["gzip"])
        def shout(text: str) -> str:
            return text.upper()

        @queue.task()
        def plain(text: str) -> str:
            return text

        with self._run_worker(queue):
            result = shout.delay("hello")
            assert result.result(timeout=10) == "HELLO"
            job = queue._inner.get_job(result.id)
            assert job is not None and job.result_bytes is not None
            # Payload is gzip-framed; the result skips per-task codecs and
            # stays on the plain queue serializer.
            assert bytes(job.payload_bytes[:2]) == b"\x1f\x8b"
            assert queue._serializer.loads(bytes(job.result_bytes)) == "HELLO"

            plain_result = plain.delay("untouched")
            assert plain_result.result(timeout=10) == "untouched"
            plain_job = queue._inner.get_job(plain_result.id)
            assert plain_job is not None
            assert bytes(plain_job.payload_bytes[:2]) != b"\x1f\x8b"

    def test_unregistered_codec_name_raises_at_enqueue(self, tmp_path: Path) -> None:
        queue = Queue(db_path=str(tmp_path / "missing.db"), workers=1)

        @queue.task(codecs=["nope"])
        def orphan() -> None:
            return None

        with pytest.raises(ValueError, match="no codec registered named 'nope'"):
            orphan.delay()

    def test_idempotency_key_stable_under_encryption_codec(self, tmp_path: Path) -> None:
        pytest.importorskip("cryptography")
        queue = Queue(
            db_path=str(tmp_path / "idem.db"),
            workers=1,
            codecs={"aes": make_aes_codec()},
        )

        @queue.task(codecs=["aes"], idempotent=True)
        def once(x: int) -> int:
            return x

        # AES adds a random nonce per encode; dedup keys hash the pre-codec
        # bytes, so the second enqueue dedups onto the first job.
        first = once.delay(7)
        second = once.delay(7)
        assert first.id == second.id
