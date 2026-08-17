"""Validation tests for AutoscaleConfig."""

from __future__ import annotations

import pytest

from flexiq.autoscale import AutoscaleConfig


def test_default_values_pass_validation() -> None:
    cfg = AutoscaleConfig(app_path="myapp:queue")
    assert cfg.app_path == "myapp:queue"
    assert cfg.min_workers == 1
    assert cfg.max_workers == 10
    assert cfg.target_queue_depth_per_worker == 15
    assert cfg.target_utilisation == 0.75
    assert cfg.tolerance == 0.1
    assert cfg.scale_up_window_sec == 0
    assert cfg.scale_down_window_sec == 300


def test_missing_app_path_rejected() -> None:
    with pytest.raises(ValueError, match="app_path"):
        AutoscaleConfig(app_path="")


def test_min_greater_than_max_rejected() -> None:
    with pytest.raises(ValueError, match="max_workers"):
        AutoscaleConfig(app_path="x", min_workers=5, max_workers=2)


def test_negative_min_rejected() -> None:
    with pytest.raises(ValueError, match="min_workers"):
        AutoscaleConfig(app_path="x", min_workers=-1)


def test_zero_max_rejected() -> None:
    with pytest.raises(ValueError, match="max_workers"):
        AutoscaleConfig(app_path="x", max_workers=0)


def test_utilisation_out_of_range_rejected() -> None:
    with pytest.raises(ValueError, match="target_utilisation"):
        AutoscaleConfig(app_path="x", target_utilisation=0.0)
    with pytest.raises(ValueError, match="target_utilisation"):
        AutoscaleConfig(app_path="x", target_utilisation=1.0)


def test_tolerance_out_of_range_rejected() -> None:
    with pytest.raises(ValueError, match="tolerance"):
        AutoscaleConfig(app_path="x", tolerance=1.0)
    with pytest.raises(ValueError, match="tolerance"):
        AutoscaleConfig(app_path="x", tolerance=-0.1)


def test_zero_target_depth_rejected() -> None:
    with pytest.raises(ValueError, match="target_queue_depth_per_worker"):
        AutoscaleConfig(app_path="x", target_queue_depth_per_worker=0)


def test_zero_poll_interval_rejected() -> None:
    with pytest.raises(ValueError, match="poll_interval_sec"):
        AutoscaleConfig(app_path="x", poll_interval_sec=0)
