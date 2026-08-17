"""Tests for MeshWorker configuration and integration."""

from __future__ import annotations

import json

import pytest

from flexiq.mesh import MeshWorker


class TestMeshWorkerValidation:
    def test_default_construction(self) -> None:
        mesh = MeshWorker()
        assert mesh.port == 7946
        assert mesh.seeds == []
        assert mesh.steal is True
        assert mesh.affinity_weight == 0.7
        assert mesh.local_buffer == 64
        assert mesh.steal_batch == 4
        assert mesh.steal_threshold == 2
        assert mesh.virtual_nodes == 150
        assert mesh.bind_addr == "0.0.0.0"
        assert mesh.encryption_key is None
        assert mesh.steal_rate_limit == 10

    def test_custom_construction(self) -> None:
        mesh = MeshWorker(
            port=8000,
            seeds=["host1:8000", "host2:8000"],
            steal=False,
            affinity_weight=0.5,
            local_buffer=128,
            steal_batch=8,
            steal_threshold=4,
            virtual_nodes=300,
            bind_addr="192.168.1.1",
            encryption_key="c2VjcmV0",
            steal_rate_limit=20,
        )
        assert mesh.port == 8000
        assert mesh.seeds == ["host1:8000", "host2:8000"]
        assert mesh.steal is False
        assert mesh.affinity_weight == 0.5
        assert mesh.local_buffer == 128
        assert mesh.steal_batch == 8
        assert mesh.steal_threshold == 4
        assert mesh.virtual_nodes == 300
        assert mesh.bind_addr == "192.168.1.1"
        assert mesh.encryption_key == "c2VjcmV0"
        assert mesh.steal_rate_limit == 20

    def test_seeds_none_normalizes_to_empty_list(self) -> None:
        mesh = MeshWorker(seeds=None)
        assert mesh.seeds == []

    def test_port_too_low(self) -> None:
        with pytest.raises(ValueError, match="port must be 1024-65535"):
            MeshWorker(port=80)

    def test_port_too_high(self) -> None:
        with pytest.raises(ValueError, match="port must be 1024-65535"):
            MeshWorker(port=70000)

    def test_port_boundary_low(self) -> None:
        mesh = MeshWorker(port=1024)
        assert mesh.port == 1024

    def test_port_boundary_high(self) -> None:
        mesh = MeshWorker(port=65535)
        assert mesh.port == 65535

    def test_affinity_weight_too_low(self) -> None:
        with pytest.raises(ValueError, match=r"affinity_weight must be 0\.0-1\.0"):
            MeshWorker(affinity_weight=-0.1)

    def test_affinity_weight_too_high(self) -> None:
        with pytest.raises(ValueError, match=r"affinity_weight must be 0\.0-1\.0"):
            MeshWorker(affinity_weight=1.1)

    def test_affinity_weight_boundaries(self) -> None:
        m0 = MeshWorker(affinity_weight=0.0)
        m1 = MeshWorker(affinity_weight=1.0)
        assert m0.affinity_weight == 0.0
        assert m1.affinity_weight == 1.0

    def test_local_buffer_zero(self) -> None:
        with pytest.raises(ValueError, match="local_buffer must be >= 1"):
            MeshWorker(local_buffer=0)

    def test_local_buffer_negative(self) -> None:
        with pytest.raises(ValueError, match="local_buffer must be >= 1"):
            MeshWorker(local_buffer=-1)

    def test_steal_batch_zero(self) -> None:
        with pytest.raises(ValueError, match="steal_batch must be >= 1"):
            MeshWorker(steal_batch=0)

    def test_virtual_nodes_zero(self) -> None:
        with pytest.raises(ValueError, match="virtual_nodes must be >= 1"):
            MeshWorker(virtual_nodes=0)


class TestMeshWorkerSerialization:
    def test_to_json_returns_valid_json(self) -> None:
        mesh = MeshWorker()
        parsed = json.loads(mesh.to_json())
        assert isinstance(parsed, dict)

    def test_to_json_field_mapping(self) -> None:
        mesh = MeshWorker(
            port=9000,
            seeds=["seed1:9000"],
            steal=False,
            affinity_weight=0.3,
            local_buffer=32,
            steal_batch=2,
            steal_threshold=1,
            virtual_nodes=50,
            bind_addr="10.0.0.1",
            encryption_key="a2V5",
            steal_rate_limit=5,
        )
        data = json.loads(mesh.to_json())

        assert data["gossip_port"] == 9000
        assert data["steal_port"] == 9001
        assert data["bind_addr"] == "10.0.0.1"
        assert data["seeds"] == ["seed1:9000"]
        assert data["enable_stealing"] is False
        assert data["affinity_weight"] == 0.3
        assert data["local_buffer_capacity"] == 32
        assert data["max_steal_batch"] == 2
        assert data["steal_threshold"] == 1
        assert data["virtual_nodes"] == 50
        assert data["encryption_key"] == "a2V5"
        assert data["steal_rate_limit"] == 5

    def test_steal_port_is_gossip_plus_one(self) -> None:
        mesh = MeshWorker(port=8500)
        data = json.loads(mesh.to_json())
        assert data["steal_port"] == 8501

    def test_hardcoded_protocol_constants(self) -> None:
        mesh = MeshWorker()
        data = json.loads(mesh.to_json())
        assert data["protocol_period_ms"] == 500
        assert data["indirect_ping_count"] == 3
        assert data["suspicion_multiplier"] == 4

    def test_encryption_key_none_serializes_as_null(self) -> None:
        mesh = MeshWorker(encryption_key=None)
        data = json.loads(mesh.to_json())
        assert data["encryption_key"] is None

    def test_empty_seeds_serializes_as_empty_list(self) -> None:
        mesh = MeshWorker(seeds=None)
        data = json.loads(mesh.to_json())
        assert data["seeds"] == []


class TestMeshWorkerRepr:
    def test_repr_includes_key_fields(self) -> None:
        mesh = MeshWorker(port=8000, seeds=["h1:8000"], steal=False)
        r = repr(mesh)
        assert "MeshWorker(" in r
        assert "port=8000" in r
        assert "seeds=['h1:8000']" in r
        assert "steal=False" in r

    def test_repr_default(self) -> None:
        mesh = MeshWorker()
        r = repr(mesh)
        assert "port=7946" in r
        assert "seeds=[]" in r
        assert "steal=True" in r
        assert "affinity_weight=0.7" in r
        assert "local_buffer=64" in r


class TestMeshWorkerSlots:
    def test_cannot_set_arbitrary_attributes(self) -> None:
        mesh = MeshWorker()
        with pytest.raises(AttributeError):
            mesh.foo = "bar"  # type: ignore[attr-defined]

    def test_all_slots_accessible(self) -> None:
        mesh = MeshWorker()
        for slot in MeshWorker.__slots__:
            assert hasattr(mesh, slot)
