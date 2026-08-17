"""Assert the shared cross-SDK wire vectors.

``contracts/wire-vectors.json`` pins the bytes of the CBOR call envelope. Every
SDK runs this same file against its own serializer, so an encoding change fails
the runtime that made it instead of quietly producing payloads its peers cannot
read.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pytest

from flexiq.serializers import CborSerializer


def _repo_root() -> Path:
    """Walk up to the repository root rather than counting directories."""
    for parent in Path(__file__).resolve().parents:
        if (parent / "contracts" / "wire-vectors.json").is_file():
            return parent
    raise FileNotFoundError("contracts/wire-vectors.json not found above this test")


REPO_ROOT = _repo_root()
VECTORS = json.loads((REPO_ROOT / "contracts" / "wire-vectors.json").read_text(encoding="utf-8"))
BINDING_CONTRACT = REPO_ROOT / "crates" / "flexiq-core" / "BINDING_CONTRACT.md"

# A run of space-separated hex byte pairs inside backticks, as the doc writes them.
_DOCUMENTED_BYTES = re.compile(r"`((?:[0-9a-f]{2} )+[0-9a-f]{2})`")


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


def test_binding_contract_quotes_the_shared_contract_vector() -> None:
    """BINDING_CONTRACT.md restates the call vector; drift there is a silent contract change."""
    lines = BINDING_CONTRACT.read_text(encoding="utf-8").splitlines()
    quoted = next((line for line in lines if 'call `f(1, "a")`' in line), None)
    assert quoted is not None, f"no documented call vector in {BINDING_CONTRACT}"

    match = _DOCUMENTED_BYTES.search(quoted)
    assert match is not None, f"no hex byte run in: {quoted}"

    by_name = {case["name"]: case for case in VECTORS["encode"]}
    assert match.group(1).replace(" ", "") == by_name["contract-vector"]["hex"]
