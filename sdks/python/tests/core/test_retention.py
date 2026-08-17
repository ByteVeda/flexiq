"""Tests for queue-wide retention config validation."""

from __future__ import annotations

import threading
from pathlib import Path

import pytest

from flexiq import Queue, Retention, RetentionPreview


def test_negative_result_ttl_rejected(tmp_path: Path) -> None:
    # A negative TTL inverts the auto-cleanup cutoff into the future, which
    # matches every archived job — so it must never reach the scheduler.
    with pytest.raises(ValueError, match="result_ttl must be non-negative"):
        Queue(db_path=str(tmp_path / "retention.db"), result_ttl=-1)


def test_overflowing_result_ttl_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="result_ttl too large"):
        Queue(db_path=str(tmp_path / "retention.db"), result_ttl=2**63 // 1000 + 1)


def test_zero_result_ttl_accepted(tmp_path: Path) -> None:
    # Zero is a valid window: purge as soon as a job completes.
    queue = Queue(db_path=str(tmp_path / "retention.db"), result_ttl=0)
    assert queue is not None


def test_retention_map_accepted(tmp_path: Path) -> None:
    queue = Queue(
        db_path=str(tmp_path / "retention.db"),
        retention=Retention(archived_jobs=604_800, dead_letter=2_592_000, task_logs=259_200),
    )
    assert queue is not None


def test_empty_retention_overrides_result_ttl(tmp_path: Path) -> None:
    # An explicit empty Retention disables all windows, winning over result_ttl
    # rather than falling back to it — the precedence contract.
    queue = Queue(
        db_path=str(tmp_path / "retention.db"),
        result_ttl=3600,
        retention=Retention(),
    )
    assert queue is not None


def test_negative_retention_window_rejected_at_construction() -> None:
    # Fail fast on the dataclass, before it ever reaches a Queue.
    with pytest.raises(ValueError, match="retention window 'task_logs' must be non-negative"):
        Retention(task_logs=-1)


def test_retention_map_omits_none_fields() -> None:
    # Only the set windows reach the native layer.
    assert Retention(archived_jobs=7, task_logs=3)._as_map() == {
        "archived_jobs": 7,
        "task_logs": 3,
    }


def test_empty_retention_is_the_opt_out_signal() -> None:
    # Retention is on by default, so opting out is passing an empty Retention():
    # it maps to an empty window set, which the native layer reads as "disable"
    # rather than the omitted case that applies the recommended defaults.
    assert Retention()._as_map() == {}


# ── Dry-run preview ─────────────────────────────────────


def test_dry_run_on_an_empty_queue_counts_nothing(tmp_path: Path) -> None:
    # No data yet, but with nothing configured the defaults are still "on".
    queue = Queue(db_path=str(tmp_path / "dryrun.db"))
    preview = queue.dry_run_retention()
    assert isinstance(preview, RetentionPreview)
    assert preview.enabled
    assert preview.defaulted, "nothing configured → recommended defaults"
    assert preview.total == 0
    assert preview.counts == {
        "task_logs": 0,
        "archived_jobs": 0,
        "job_errors": 0,
        "task_metrics": 0,
        "dead_letter": 0,
    }


def test_dry_run_counts_purgeable_rows_without_deleting(tmp_path: Path) -> None:
    # archived_jobs=0 makes a completed job immediately eligible; the default
    # cleanup cadence (~60s) means no sweep fires during the test, so the rows
    # survive for the dry-run to count — and must still survive afterwards.
    queue = Queue(
        db_path=str(tmp_path / "dryrun.db"),
        retention=Retention(archived_jobs=0, task_metrics=0),
    )

    @queue.task()
    def noop(x: int) -> int:
        return x

    job = noop.delay(1)
    worker = threading.Thread(target=queue.run_worker, daemon=True)
    worker.start()
    try:
        job.result(timeout=10)

        preview = queue.dry_run_retention()
        assert preview.enabled
        assert not preview.defaulted, "an explicit config is not defaulted"
        assert preview.counts["archived_jobs"] >= 1, "the completed job is purgeable"
        assert preview.counts["task_metrics"] >= 1, "its metric is purgeable"
        assert preview.total >= preview.counts["archived_jobs"]

        # Read-only: the row the count reported is still there.
        assert queue.get_job(job.id) is not None
    finally:
        queue.shutdown()
        worker.join(timeout=5)


def test_dry_run_previews_candidate_windows(tmp_path: Path) -> None:
    # An operator sizing a window can preview candidate windows without touching
    # the queue's own config: passing `Retention` overrides it for the count.
    # The queue is configured to keep everything (empty), but the candidate
    # windows still report a purgeable job.
    queue = Queue(db_path=str(tmp_path / "dryrun.db"), retention=Retention())

    @queue.task()
    def noop(x: int) -> int:
        return x

    job = noop.delay(1)
    worker = threading.Thread(target=queue.run_worker, daemon=True)
    worker.start()
    try:
        job.result(timeout=10)

        # The queue's own (disabled) config counts nothing.
        assert queue.dry_run_retention().total == 0

        # A candidate archived window of 0 makes the completed job purgeable.
        preview = queue.dry_run_retention(Retention(archived_jobs=0))
        assert preview.enabled
        assert not preview.defaulted
        assert preview.counts["archived_jobs"] >= 1
        assert preview.windows["archived_jobs"] == 0
        assert preview.windows["dead_letter"] is None, "unset candidate windows keep forever"

        # Nothing was deleted by either preview.
        assert queue.get_job(job.id) is not None
    finally:
        queue.shutdown()
        worker.join(timeout=5)


def test_dry_run_reports_disabled_for_an_empty_config(tmp_path: Path) -> None:
    queue = Queue(db_path=str(tmp_path / "dryrun.db"), retention=Retention())
    preview = queue.dry_run_retention()
    assert not preview.enabled, "an empty Retention() disables the windows"
    assert not preview.defaulted
    assert all(window is None for window in preview.windows.values())
    assert preview.total == 0


async def test_adry_run_retention_matches_sync(tmp_path: Path) -> None:
    queue = Queue(db_path=str(tmp_path / "dryrun.db"), retention=Retention(archived_jobs=0))
    preview = await queue.adry_run_retention()
    assert isinstance(preview, RetentionPreview)
    assert preview.enabled
    assert preview.total == 0
