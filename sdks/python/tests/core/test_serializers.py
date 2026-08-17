"""Tests for pluggable serializers."""

import json
import pickle

import pytest

from flexiq.serializers import (
    CborSerializer,
    CloudpickleSerializer,
    JsonSerializer,
    Serializer,
    SignedSerializer,
    SmartSerializer,
)


class TestJsonSerializer:
    def test_roundtrip_dict(self) -> None:
        s = JsonSerializer()
        data = {"key": "value", "num": 42, "nested": [1, 2, 3]}
        assert s.loads(s.dumps(data)) == data

    def test_roundtrip_list(self) -> None:
        s = JsonSerializer()
        data = [1, "two", None, True]
        assert s.loads(s.dumps(data)) == data

    def test_roundtrip_primitives(self) -> None:
        s = JsonSerializer()
        for val in [42, 3.14, "hello", True, None]:
            assert s.loads(s.dumps(val)) == val

    def test_dumps_returns_bytes(self) -> None:
        s = JsonSerializer()
        result = s.dumps({"a": 1})
        assert isinstance(result, bytes)

    def test_non_serializable_raises(self) -> None:
        s = JsonSerializer()
        with pytest.raises(TypeError):
            s.dumps(object())

    def test_invalid_bytes_raises(self) -> None:
        s = JsonSerializer()
        with pytest.raises((json.JSONDecodeError, UnicodeDecodeError, ValueError)):
            s.loads(b"\xff\xfe")


class TestCloudpickleSerializer:
    def test_roundtrip_dict(self) -> None:
        s = CloudpickleSerializer()
        data = {"key": "value", "num": 42}
        assert s.loads(s.dumps(data)) == data

    def test_roundtrip_lambda(self) -> None:
        s = CloudpickleSerializer()
        fn = lambda x: x * 2  # noqa: E731
        restored = s.loads(s.dumps(fn))
        assert restored(5) == 10

    def test_dumps_returns_bytes(self) -> None:
        s = CloudpickleSerializer()
        assert isinstance(s.dumps(42), bytes)

    def test_invalid_bytes_raises(self) -> None:
        s = CloudpickleSerializer()
        with pytest.raises((pickle.UnpicklingError, EOFError)):
            s.loads(b"not-valid-pickle")


class TestSerializerProtocol:
    def test_json_is_serializer(self) -> None:
        assert isinstance(JsonSerializer(), Serializer)

    def test_cloudpickle_is_serializer(self) -> None:
        assert isinstance(CloudpickleSerializer(), Serializer)


class TestMsgPackSerializer:
    def test_roundtrip(self) -> None:
        pytest.importorskip("msgpack")
        from flexiq.serializers import MsgPackSerializer

        s = MsgPackSerializer()
        data = {"key": "value", "num": 42}
        assert s.loads(s.dumps(data)) == data

    def test_dumps_returns_bytes(self) -> None:
        pytest.importorskip("msgpack")
        from flexiq.serializers import MsgPackSerializer

        s = MsgPackSerializer()
        assert isinstance(s.dumps([1, 2, 3]), bytes)


class TestEncryptedSerializer:
    def test_roundtrip(self) -> None:
        pytest.importorskip("cryptography")
        import os

        from flexiq.serializers import EncryptedSerializer

        key = os.urandom(32)
        s = EncryptedSerializer(JsonSerializer(), key)
        data = {"secret": "payload"}
        assert s.loads(s.dumps(data)) == data

    def test_wrong_key_fails(self) -> None:
        pytest.importorskip("cryptography")
        import os

        from cryptography.exceptions import InvalidTag

        from flexiq.serializers import EncryptedSerializer

        s1 = EncryptedSerializer(JsonSerializer(), os.urandom(32))
        s2 = EncryptedSerializer(JsonSerializer(), os.urandom(32))

        encrypted = s1.dumps({"data": 1})
        with pytest.raises(ValueError, match="Decryption failed") as excinfo:
            s2.loads(encrypted)
        # The original cryptography exception is preserved on the cause
        # chain so debugging surfaces still know it was a tag-validation
        # failure rather than a malformed-input ValueError.
        assert isinstance(excinfo.value.__cause__, InvalidTag)

    def test_tampered_ciphertext_fails(self) -> None:
        pytest.importorskip("cryptography")
        import os

        from cryptography.exceptions import InvalidTag

        from flexiq.serializers import EncryptedSerializer

        key = os.urandom(32)
        s = EncryptedSerializer(JsonSerializer(), key)
        encrypted = s.dumps("hello")

        tampered = encrypted[:-1] + bytes([encrypted[-1] ^ 0xFF])
        with pytest.raises(ValueError, match="Decryption failed") as excinfo:
            s.loads(tampered)
        assert isinstance(excinfo.value.__cause__, InvalidTag)

    def test_short_ciphertext_fails(self) -> None:
        """Inputs shorter than the AES-GCM nonce (12B) + tag (≥1B) are
        rejected before the cipher is ever consulted, with a distinct
        message so operators can tell parsing errors from key/tag failures.
        """
        pytest.importorskip("cryptography")
        import os

        from flexiq.serializers import EncryptedSerializer

        s = EncryptedSerializer(JsonSerializer(), os.urandom(32))
        with pytest.raises(ValueError, match="too short"):
            s.loads(b"only-twelve-")


class TestSmartSerializer:
    def test_plain_data_uses_msgpack_tag(self) -> None:
        s = SmartSerializer()
        encoded = s.dumps({"x": 1, "y": ["a", "b"]})
        assert encoded[:1] == b"\x01"
        assert s.loads(encoded) == {"x": 1, "y": ["a", "b"]}

    def test_namedtuple_preserved_via_cloudpickle(self) -> None:
        """Tuple subclasses (namedtuples) must keep their type, not be
        demoted to a plain tuple by the ExtType path."""
        from collections import namedtuple

        Point = namedtuple("Point", ["x", "y"])
        s = SmartSerializer()
        encoded = s.dumps(Point(1, 2))
        assert encoded[:1] == b"\x00"  # cloudpickle fallback, not msgpack
        restored = s.loads(encoded)
        assert restored == Point(1, 2)
        assert type(restored) is Point
        assert restored.x == 1

    def test_falls_back_to_cloudpickle_for_lambda(self) -> None:
        s = SmartSerializer()
        encoded = s.dumps(lambda x: x + 1)
        assert encoded[:1] == b"\x00"
        assert s.loads(encoded)(41) == 42

    def test_falls_back_to_cloudpickle_for_custom_class(self) -> None:
        s = SmartSerializer()

        class Point:
            def __init__(self, x: int) -> None:
                self.x = x

        encoded = s.dumps(Point(7))
        assert encoded[:1] == b"\x00"
        assert s.loads(encoded).x == 7

    def test_loads_legacy_untagged_cloudpickle(self) -> None:
        """Payloads written before the envelope existed are raw cloudpickle
        and must still deserialize."""
        legacy = CloudpickleSerializer().dumps({"legacy": True})
        # A protocol-2+ pickle starts with 0x80, never a codec tag.
        assert legacy[:1] not in (b"\x00", b"\x01")
        assert SmartSerializer().loads(legacy) == {"legacy": True}

    def test_preserves_tuples(self) -> None:
        """Tuples round-trip as tuples (not lists) via the msgpack ExtType,
        including nested ones — so task args/results keep exact semantics."""
        s = SmartSerializer()
        payload = ((1, "two", 3.0), {"k": ("nested", "tuple")})
        restored = s.loads(s.dumps(payload))
        assert restored == payload
        assert isinstance(restored[0], tuple)
        assert isinstance(restored[1]["k"], tuple)

    def test_empty_payload_rejected(self) -> None:
        with pytest.raises(ValueError, match="empty payload"):
            SmartSerializer().loads(b"")

    def test_satisfies_protocol(self) -> None:
        assert isinstance(SmartSerializer(), Serializer)

    def test_loads_cbor_wire_payload(self) -> None:
        """Cross-SDK CBOR payloads (tag 0x02) load without per-task config."""
        encoded = CborSerializer().dumps({"from": "another-sdk", "n": 2**80})
        assert SmartSerializer().loads(encoded) == {"from": "another-sdk", "n": 2**80}


class TestCborSerializer:
    def test_roundtrip_dict(self) -> None:
        s = CborSerializer()
        data = {"key": "value", "num": 42, "nested": [1, 2, 3], "raw": b"\x00\xff"}
        assert s.loads(s.dumps(data)) == data

    def test_wire_tag_is_0x02(self) -> None:
        assert CborSerializer().dumps([1, 2])[:1] == b"\x02"

    def test_big_int_roundtrip(self) -> None:
        """Integers beyond 2^53 (JS Number limit) and 2^64 (CBOR bignum)
        must survive — the reason CBOR is the cross-SDK default over JSON."""
        s = CborSerializer()
        for val in [2**53 + 1, 2**64 + 1, -(2**80)]:
            assert s.loads(s.dumps(val)) == val

    def test_datetime_roundtrip(self) -> None:
        from datetime import datetime, timezone

        s = CborSerializer()
        moment = datetime(2026, 7, 11, 12, 30, 45, tzinfo=timezone.utc)
        assert s.loads(s.dumps(moment)) == moment

    def test_call_envelope_shape(self) -> None:
        """An ``(args, kwargs)`` payload becomes a 2-element CBOR array —
        the cross-SDK call-body shape from BINDING_CONTRACT.md."""
        s = CborSerializer()
        restored = s.loads(s.dumps(((1, "a"), {"k": True})))
        assert restored == [[1, "a"], {"k": True}]

    def test_matches_binding_contract_vector(self) -> None:
        """Byte-exact against the BINDING_CONTRACT.md test vector for
        call ``f(1, "a")`` — the fixture every SDK's tests assert."""
        s = CborSerializer()
        assert s.dumps(((1, "a"), {})).hex() == "028282016161a0"
        assert s.loads(bytes.fromhex("028282016161a0")) == [[1, "a"], {}]

    def test_rejects_native_tagged_payload(self) -> None:
        native = SmartSerializer().dumps(lambda x: x)
        assert native[:1] == b"\x00"
        with pytest.raises(ValueError, match="native-tagged"):
            CborSerializer().loads(native)

    def test_rejects_untagged_payload(self) -> None:
        with pytest.raises(ValueError, match="not CBOR wire format"):
            CborSerializer().loads(b'{"json": true}')

    def test_empty_payload_rejected(self) -> None:
        with pytest.raises(ValueError, match="empty payload"):
            CborSerializer().loads(b"")

    def test_satisfies_protocol(self) -> None:
        assert isinstance(CborSerializer(), Serializer)


class TestSignedSerializer:
    def test_roundtrip(self) -> None:
        s = SignedSerializer(SmartSerializer(), b"k" * 32)
        assert s.loads(s.dumps({"hello": "world"})) == {"hello": "world"}

    def test_requires_bytes_key(self) -> None:
        with pytest.raises(TypeError, match="key must be bytes"):
            SignedSerializer(CloudpickleSerializer(), "string-key")  # type: ignore[arg-type]

    def test_rejects_short_key(self) -> None:
        with pytest.raises(ValueError, match="at least 32 bytes"):
            SignedSerializer(CloudpickleSerializer(), b"tooshort")

    def test_wrong_key_rejected(self) -> None:
        producer = SignedSerializer(CloudpickleSerializer(), b"a" * 32)
        attacker = SignedSerializer(CloudpickleSerializer(), b"b" * 32)
        data = producer.dumps({"x": 1})
        with pytest.raises(ValueError, match="integrity check failed"):
            attacker.loads(data)

    def test_tamper_detected(self) -> None:
        s = SignedSerializer(CloudpickleSerializer(), b"k" * 32)
        data = s.dumps({"x": 1})
        tampered = data[:-1] + bytes([data[-1] ^ 0xFF])
        with pytest.raises(ValueError, match="integrity check failed"):
            s.loads(tampered)

    def test_truncated_payload_rejected(self) -> None:
        s = SignedSerializer(CloudpickleSerializer(), b"k" * 32)
        with pytest.raises(ValueError, match="too short"):
            s.loads(b"short")

    def test_blocks_forged_unsigned_body(self) -> None:
        # An attacker who can write to storage but lacks the key cannot get a
        # forged cloudpickle body past verification (the RCE vector).
        s = SignedSerializer(CloudpickleSerializer(), b"k" * 32)
        forged = CloudpickleSerializer().dumps({"evil": True})
        with pytest.raises(ValueError, match="integrity check failed"):
            s.loads(b"\x00" * 32 + forged)
