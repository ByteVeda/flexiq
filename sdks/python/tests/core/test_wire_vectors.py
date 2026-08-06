"""Assert the shared cross-SDK wire vectors.

``contracts/wire-vectors.json`` pins the bytes of the CBOR call envelope. Every
SDK runs this same file against its own serializer, so an encoding change fails
the runtime that made it instead of quietly producing payloads its peers cannot
read.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from taskito.serializers import CborSerializer


def _vector_file() -> Path:
    """Walk up to the repository root rather than counting directories."""
    for parent in Path(__file__).resolve().parents:
        candidate = parent / "contracts" / "wire-vectors.json"
        if candidate.is_file():
            return candidate
    raise FileNotFoundError("contracts/wire-vectors.json not found above this test")


VECTORS = json.loads(_vector_file().read_text(encoding="utf-8"))


def _ids(cases: list[dict[str, Any]]) -> list[str]:
    return [case["name"] for case in cases]


@pytest.mark.parametrize("case", VECTORS["encode"], ids=_ids(VECTORS["encode"]))
def test_encodes_to_the_pinned_bytes(case: dict[str, Any]) -> None:
    payload = CborSerializer().dumps((tuple(case["args"]), case["kwargs"]))
    assert payload.hex() == case["hex"]


@pytest.mark.parametrize("case", VECTORS["encode"], ids=_ids(VECTORS["encode"]))
def test_decodes_the_pinned_bytes(case: dict[str, Any]) -> None:
    args, kwargs = CborSerializer().loads(bytes.fromhex(case["hex"]))
    assert list(args) == case["args"]
    assert kwargs == case["kwargs"]


@pytest.mark.parametrize("case", VECTORS["decode_only"], ids=_ids(VECTORS["decode_only"]))
def test_decode_only_vectors(case: dict[str, Any]) -> None:
    serializer = CborSerializer()
    raw = bytes.fromhex(case["hex"])
    args, kwargs = serializer.loads(raw)

    if case.get("round_trip_only"):
        # The value has no lossless JSON form, so re-encoding is the assertion.
        assert serializer.dumps((tuple(args), kwargs)).hex() == case["hex"]
    else:
        assert list(args) == case["args"]
        assert kwargs == case["kwargs"]


def test_vector_file_covers_the_documented_contract_vector() -> None:
    """The vector quoted in BINDING_CONTRACT.md must stay in the shared file."""
    by_name = {case["name"]: case for case in VECTORS["encode"]}
    assert by_name["contract-vector"]["hex"] == "028282016161a0"
