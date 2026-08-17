"""Regression tests for the per-task middleware-chain cache.

``_get_middleware_chain`` runs on every job dispatch, so it must not hit
storage (to read the dashboard disable list) on every call. The chain is
cached per task and invalidated immediately on same-process disable changes
via a version counter.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from flexiq import Queue


def test_middleware_chain_caches_disable_reads(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Repeated dispatches read the disable list from storage at most once
    within the TTL window."""
    # Pin the TTL large so the assertion can't flake if the loop happens to
    # straddle the real 1s window under load.
    monkeypatch.setattr("flexiq.mixins.decorators._MW_CHAIN_TTL", 1_000_000.0)
    queue = Queue(db_path=str(tmp_path / "cache.db"))

    reads = {"count": 0}
    real_get_setting = queue.get_setting

    def counting_get_setting(key: str) -> str | None:
        reads["count"] += 1
        return real_get_setting(key)

    queue.get_setting = counting_get_setting  # type: ignore[method-assign]

    for _ in range(50):
        assert queue._get_middleware_chain("some.task") == []
    assert reads["count"] == 1, "disable list should be read once, then cached"


def test_disable_version_bump_invalidates_cache(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A same-process disable change re-reads the disable list on the next call."""
    # Pin the TTL so only the version bump (not expiry) can trigger a re-read.
    monkeypatch.setattr("flexiq.mixins.decorators._MW_CHAIN_TTL", 1_000_000.0)
    queue = Queue(db_path=str(tmp_path / "cache.db"))

    reads = {"count": 0}
    real_get_setting = queue.get_setting

    def counting_get_setting(key: str) -> str | None:
        reads["count"] += 1
        return real_get_setting(key)

    queue.get_setting = counting_get_setting  # type: ignore[method-assign]

    queue._get_middleware_chain("some.task")
    assert reads["count"] == 1

    # Simulate a dashboard disable-list change.
    queue._bump_mw_disable_version()
    queue._get_middleware_chain("some.task")
    assert reads["count"] == 2, "version bump should invalidate the cached chain"
