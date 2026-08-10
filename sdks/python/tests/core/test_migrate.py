"""A deployment that gates DDL opens unmigrated and applies the schema itself.

Until ``migrate`` has run there are no tables, so every query fails — that is
the gate working, not a fault.
"""

from __future__ import annotations

from typing import Any

import pytest

from taskito import Queue


def test_an_unmigrated_queue_applies_its_own_schema(tmp_path: Any) -> None:
    queue = Queue(db_path=str(tmp_path / "q.db"), auto_migrate=False)

    with pytest.raises(RuntimeError, match="no such table"):
        queue.stats()

    report = queue.migrate()
    assert report["applied"], "the first migrate applies the whole history"
    assert report["workflow_applied"], "workflow tables come with it"
    assert report["schemaless"] is False
    queue.stats()

    again = queue.migrate()
    assert again["applied"] == []
    assert again["workflow_applied"] == []
    assert again["archived_jobs"] == 0


def test_an_auto_migrated_queue_has_only_workflow_tables_left(tmp_path: Any) -> None:
    # Opening applies the core schema; workflow tables are built on first
    # workflow use, so an explicit migrate is what brings them forward for a
    # deployment that wants no DDL at runtime.
    queue = Queue(db_path=str(tmp_path / "q.db"))

    report = queue.migrate()
    assert report["applied"] == []
    assert report["workflow_applied"]

    assert queue.migrate()["workflow_applied"] == []


def test_a_gated_queue_is_fully_usable_after_migrating(tmp_path: Any) -> None:
    queue = Queue(db_path=str(tmp_path / "q.db"), auto_migrate=False, workers=1)
    queue.migrate()

    @queue.task()
    def add(left: int, right: int) -> int:
        return left + right

    job_id = add.delay(2, 3).id
    assert queue.get_job(job_id) is not None
    assert queue.stats()["pending"] == 1
