"""Keyset (cursor) pagination — S12."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from conftest import join_worker
from flexiq import Queue
from flexiq.pagination import Page

PollUntil = Any  # the conftest fixture's runtime type


def _page_all_job_ids(queue: Queue, **kwargs: Any) -> list[str]:
    """Page through ``list_jobs_after`` fully, returning every job id seen."""
    seen: list[str] = []
    cursor: str | None = None
    while True:
        page = queue.list_jobs_after(after=cursor, **kwargs)
        assert isinstance(page, Page)
        seen.extend(j.id for j in page.items)
        cursor = page.next_cursor
        if cursor is None:
            break
    return seen


def test_page_is_iterable_and_sized() -> None:
    page: Page[int] = Page(items=[1, 2, 3], next_cursor="abc")
    assert len(page) == 3
    assert list(page) == [1, 2, 3]
    assert page.next_cursor == "abc"


def test_list_jobs_after_pages_every_job_once(tmp_path: Path) -> None:
    queue = Queue(db_path=str(tmp_path / "t.db"))

    @queue.task()
    def noop() -> None:
        pass

    expected = {noop.delay().id for _ in range(25)}

    seen = _page_all_job_ids(queue, status="pending", limit=10)
    assert set(seen) == expected
    assert len(seen) == len(expected), "no job may appear on two pages"


def test_list_jobs_after_empty(tmp_path: Path) -> None:
    queue = Queue(db_path=str(tmp_path / "t.db"))

    @queue.task()
    def noop() -> None:
        pass

    page = queue.list_jobs_after(status="pending", limit=10)
    assert page.items == []
    assert page.next_cursor is None


def test_list_jobs_after_last_page_has_no_cursor(tmp_path: Path) -> None:
    queue = Queue(db_path=str(tmp_path / "t.db"))

    @queue.task()
    def noop() -> None:
        pass

    for _ in range(3):
        noop.delay()

    # Fewer rows than the limit → this is the last page, cursor is None.
    page = queue.list_jobs_after(status="pending", limit=10)
    assert len(page.items) == 3
    assert page.next_cursor is None


def test_list_jobs_after_descending_order(tmp_path: Path) -> None:
    queue = Queue(db_path=str(tmp_path / "t.db"))

    @queue.task()
    def noop() -> None:
        pass

    for _ in range(10):
        noop.delay()

    page = queue.list_jobs_after(status="pending", limit=10)
    # UUIDv7 ids are time-ordered, and the page is (created_at, id) descending,
    # so the ids come back strictly descending (newest first).
    ids = [j.id for j in page.items]
    assert ids == sorted(ids, reverse=True), "page must be newest-first"


def test_list_jobs_after_stable_under_insert(tmp_path: Path) -> None:
    """Rows inserted mid-pagination never duplicate or skip earlier rows."""
    queue = Queue(db_path=str(tmp_path / "t.db"))

    @queue.task()
    def noop() -> None:
        pass

    original = {noop.delay().id for _ in range(20)}

    seen: list[str] = []
    cursor: str | None = None
    inserted = False
    while True:
        page = queue.list_jobs_after(status="pending", limit=5, after=cursor)
        seen.extend(j.id for j in page.items)
        cursor = page.next_cursor
        if not inserted:
            # Insert newer rows; they sort ahead of the cursor and must not
            # appear in the remaining pages of the original scan.
            for _ in range(5):
                noop.delay()
            inserted = True
        if cursor is None:
            break

    # Every original row seen exactly once. Newer inserts may or may not appear
    # depending on where they sort, but the originals must be complete + unique.
    for job_id in original:
        assert seen.count(job_id) == 1


def test_list_jobs_filtered_after(tmp_path: Path) -> None:
    queue = Queue(db_path=str(tmp_path / "t.db"))

    @queue.task()
    def alpha() -> None:
        pass

    @queue.task()
    def beta() -> None:
        pass

    alpha_ids = {alpha.delay().id for _ in range(8)}
    for _ in range(8):
        beta.delay()

    seen: list[str] = []
    cursor: str | None = None
    while True:
        page = queue.list_jobs_filtered_after(
            status="pending", task_name=alpha.name, limit=3, after=cursor
        )
        seen.extend(j.id for j in page.items)
        cursor = page.next_cursor
        if cursor is None:
            break

    assert set(seen) == alpha_ids


def test_dead_letters_after(tmp_path: Path, poll_until: PollUntil) -> None:
    queue = Queue(db_path=str(tmp_path / "t.db"), workers=2)

    @queue.task(max_retries=0)
    def boom() -> None:
        raise ValueError("boom")

    for _ in range(6):
        boom.delay()

    import threading

    thread = threading.Thread(target=queue.run_worker, daemon=True)
    thread.start()
    try:
        poll_until(
            lambda: len(queue.dead_letters(limit=100)) >= 6,
            timeout=15,
            message="jobs never dead-lettered",
        )
    finally:
        queue.shutdown()
        join_worker(thread)

    seen: list[str] = []
    cursor: str | None = None
    while True:
        page = queue.dead_letters_after(limit=2, after=cursor)
        assert isinstance(page, Page)
        seen.extend(entry["id"] for entry in page.items)
        cursor = page.next_cursor
        if cursor is None:
            break

    assert len(seen) >= 6
    assert len(set(seen)) == len(seen), "no DLQ entry may appear twice"


def test_list_archived_after(tmp_path: Path, poll_until: PollUntil) -> None:
    queue = Queue(db_path=str(tmp_path / "t.db"), workers=2)

    @queue.task()
    def ok(x: int) -> int:
        return x

    for i in range(6):
        ok.delay(i)

    import threading

    thread = threading.Thread(target=queue.run_worker, daemon=True)
    thread.start()
    try:
        poll_until(
            lambda: len(queue.list_archived(limit=100)) >= 6,
            timeout=15,
            message="jobs never archived",
        )
    finally:
        queue.shutdown()
        join_worker(thread)

    seen: list[str] = []
    cursor: str | None = None
    while True:
        page = queue.list_archived_after(limit=2, after=cursor)
        seen.extend(j.id for j in page.items)
        cursor = page.next_cursor
        if cursor is None:
            break

    assert len(seen) >= 6
    assert len(set(seen)) == len(seen), "no archived job may appear twice"
