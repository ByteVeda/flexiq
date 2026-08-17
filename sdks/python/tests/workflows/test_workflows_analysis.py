"""Tests for Phase 6 workflow graph analysis."""

from __future__ import annotations

import pytest

from flexiq.workflows import Workflow


class _FakeTask:
    _task_name = "fake"


def _linear() -> Workflow:
    """a → b → c"""
    wf = Workflow(name="linear")
    wf.step("a", _FakeTask())
    wf.step("b", _FakeTask(), after="a")
    wf.step("c", _FakeTask(), after="b")
    return wf


def _diamond() -> Workflow:
    """a → {b, c} → d"""
    wf = Workflow(name="diamond")
    wf.step("a", _FakeTask())
    wf.step("b", _FakeTask(), after="a")
    wf.step("c", _FakeTask(), after="a")
    wf.step("d", _FakeTask(), after=["b", "c"])
    return wf


# ── Ancestors / Descendants ──────────────────────────────────────


def test_ancestors_linear() -> None:
    wf = _linear()
    assert wf.ancestors("c") == ["a", "b"]
    assert wf.ancestors("b") == ["a"]
    assert wf.ancestors("a") == []


def test_descendants_linear() -> None:
    wf = _linear()
    assert wf.descendants("a") == ["b", "c"]
    assert wf.descendants("b") == ["c"]
    assert wf.descendants("c") == []


def test_ancestors_diamond() -> None:
    wf = _diamond()
    assert wf.ancestors("d") == ["a", "b", "c"]
    assert set(wf.ancestors("b")) == {"a"}


def test_ancestors_unknown_node() -> None:
    wf = _linear()
    with pytest.raises(KeyError, match="nonexistent"):
        wf.ancestors("nonexistent")


# ── Topological Levels ───────────────────────────────────────────


def test_topological_levels_linear() -> None:
    wf = _linear()
    assert wf.topological_levels() == [["a"], ["b"], ["c"]]


def test_topological_levels_diamond() -> None:
    wf = _diamond()
    levels = wf.topological_levels()
    assert levels[0] == ["a"]
    assert sorted(levels[1]) == ["b", "c"]
    assert levels[2] == ["d"]


# ── Stats ────────────────────────────────────────────────────────


def test_stats() -> None:
    wf = _diamond()
    s = wf.stats()
    assert s["nodes"] == 4
    assert s["edges"] == 4  # a→b, a→c, b→d, c→d
    assert s["depth"] == 3
    assert s["width"] == 2  # level 1 has b and c
    assert 0 < s["density"] <= 1.0


# ── Critical Path ────────────────────────────────────────────────


def test_critical_path_linear() -> None:
    wf = _linear()
    path, cost = wf.critical_path({"a": 1.0, "b": 2.0, "c": 3.0})
    assert path == ["a", "b", "c"]
    assert cost == 6.0


def test_critical_path_diamond() -> None:
    wf = _diamond()
    # b branch is heavier: a(1) + b(5) + d(1) = 7
    # c branch: a(1) + c(2) + d(1) = 4
    path, cost = wf.critical_path({"a": 1.0, "b": 5.0, "c": 2.0, "d": 1.0})
    assert path == ["a", "b", "d"]
    assert cost == 7.0


# ── Execution Plan ───────────────────────────────────────────────


def test_execution_plan_parallelism() -> None:
    wf = _diamond()
    plan = wf.execution_plan(max_workers=2)
    # Level 0: [a], Level 1: [b, c] fits in 1 batch, Level 2: [d]
    assert plan == [["a"], ["b", "c"], ["d"]]


def test_execution_plan_worker_limit() -> None:
    wf = _diamond()
    plan = wf.execution_plan(max_workers=1)
    # Level 1 splits into two batches
    assert plan == [["a"], ["b"], ["c"], ["d"]]


# ── Bottleneck Analysis ──────────────────────────────────────────


def test_bottleneck_analysis() -> None:
    wf = _diamond()
    result = wf.bottleneck_analysis({"a": 1.0, "b": 5.0, "c": 2.0, "d": 1.0})
    assert result["node"] == "b"
    assert result["cost"] == 5.0
    assert result["percentage"] > 50
    assert "b" in result["suggestion"]
    assert result["critical_path"] == ["a", "b", "d"]
