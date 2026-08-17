"""Pure-function tests for ``compute_desired_workers``.

The decision formula is exhaustively covered here so we don't need to
spin up subprocesses to verify scaling correctness.
"""

from __future__ import annotations

from typing import Any

from flexiq.autoscale import AutoscaleConfig, compute_desired_workers


def _cfg(**overrides: Any) -> AutoscaleConfig:
    base: dict[str, Any] = {
        "app_path": "myapp:queue",
        "min_workers": 1,
        "max_workers": 10,
        "target_queue_depth_per_worker": 10,
        "target_utilisation": 0.75,
        "tolerance": 0.1,
        "threads_per_worker": 4,
    }
    base.update(overrides)
    return AutoscaleConfig(**base)


def test_stable_when_utilisation_at_target() -> None:
    """utilisation == target_utilisation → desired equals current (within tolerance)."""
    cfg = _cfg()
    # 2 workers, 4 threads each = 8 capacity. At 75% util, running=6.
    # Depth signal: pending=0 -> 0 workers, clamped up to min.
    decision = compute_desired_workers(pending=0, running=6, current_workers=2, config=cfg)
    assert decision.desired_workers == 2


def test_scale_up_on_high_depth() -> None:
    """pending=100 + target=10 per worker → desired ~10."""
    cfg = _cfg(max_workers=20)
    decision = compute_desired_workers(pending=100, running=0, current_workers=2, config=cfg)
    assert decision.desired_workers == 10
    assert "scale-up" in decision.rationale


def test_scale_down_on_zero_load() -> None:
    """Zero pending and zero running with overshoot → scale down to min."""
    cfg = _cfg(min_workers=1)
    decision = compute_desired_workers(pending=0, running=0, current_workers=8, config=cfg)
    assert decision.desired_workers == 1
    assert "scale-down" in decision.rationale


def test_tolerance_band_suppresses_noise() -> None:
    """Small fluctuations within ±10% don't change the worker count."""
    cfg = _cfg(tolerance=0.5)  # very wide tolerance
    # depth=11 -> 2 desired; current=2 -> within tolerance
    decision = compute_desired_workers(pending=11, running=5, current_workers=2, config=cfg)
    assert decision.desired_workers == 2


def test_overload_bypasses_tolerance() -> None:
    """running > capacity forces +1 even when delta is within tolerance."""
    cfg = _cfg(tolerance=0.9)  # would otherwise suppress everything
    # 2 workers * 4 threads = 8 capacity. running=10 > 8 → overload.
    decision = compute_desired_workers(pending=0, running=10, current_workers=2, config=cfg)
    assert decision.desired_workers >= 3


def test_min_workers_floor() -> None:
    """Desired never drops below min_workers."""
    cfg = _cfg(min_workers=3, max_workers=10)
    decision = compute_desired_workers(pending=0, running=0, current_workers=2, config=cfg)
    assert decision.desired_workers >= 3


def test_max_workers_ceiling() -> None:
    """Desired never exceeds max_workers, even with huge pending."""
    cfg = _cfg(min_workers=1, max_workers=4)
    decision = compute_desired_workers(pending=10_000, running=0, current_workers=2, config=cfg)
    assert decision.desired_workers == 4


def test_zero_current_workers_still_recommends_scale_up_on_depth() -> None:
    """Cold-start: current=0, pending>0 → desired >= min_workers."""
    cfg = _cfg(min_workers=1)
    decision = compute_desired_workers(pending=50, running=0, current_workers=0, config=cfg)
    assert decision.desired_workers >= 1


def test_hpa_formula_matches_kubernetes_doc_example() -> None:
    """HPA reference: desired = ceil(current * (metric / target)).

    With current=2, utilisation=1.0 (saturated), target=0.5:
      desired = ceil(2 * (1.0 / 0.5)) = 4.
    """
    cfg = _cfg(
        target_utilisation=0.5,
        threads_per_worker=2,
        target_queue_depth_per_worker=1_000,
    )
    # 2 workers * 2 threads = 4 capacity. running=4 -> util=1.0.
    decision = compute_desired_workers(pending=0, running=4, current_workers=2, config=cfg)
    # Overload bypass kicks in because running == capacity is the edge case;
    # the formula yields desired = ceil(2 * (1.0 / 0.5)) = 4.
    assert decision.desired_workers >= 4
