"""Tests for the superseded-cache rule in cache_budget.py. Run from scripts/:
``python3 -m unittest test_cache_budget``.
"""

from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from cache_budget import Entry, superseded

NOW = datetime(2026, 10, 8, 9, 0, tzinfo=timezone.utc)
MASTER = "refs/heads/master"
CARGO = "v1-rust-bin-fix"


def entry(
    key: str, created_days_ago: float, used_days_ago: float, *, ref: str = MASTER, id: int = 0
) -> Entry:
    return Entry(
        id=id,
        size=1,
        ref=ref,
        key=key,
        created=NOW - timedelta(days=created_days_ago),
        last_used=NOW - timedelta(days=used_days_ago),
    )


def stale_ids(entries: list[Entry]) -> set[int]:
    return {item.id for item in superseded(entries, NOW, CARGO)}


class SupersededTest(unittest.TestCase):
    def test_replaced_rust_cache_goes(self) -> None:
        old = entry(f"{CARGO}-release-Linux-x64-f3ce3a4d-ee4c8f7c", 7, 3, id=1)
        new = entry(f"{CARGO}-release-Linux-x64-f3ce3a4d-77b48f8a", 2, 0, id=2)
        self.assertEqual(stale_ids([old, new]), {1})

    def test_rust_jobs_with_other_env_hash_are_independent(self) -> None:
        # Same shared-key, different job env: two lineages, not a replacement.
        one = entry(f"{CARGO}-release-Linux-x64-f3ce3a4d-ee4c8f7c", 7, 3, id=1)
        two = entry(f"{CARGO}-release-Linux-x64-0da010fa-77b48f8a", 2, 0, id=2)
        self.assertEqual(stale_ids([one, two]), set())

    def test_recently_replaced_entry_gets_a_day(self) -> None:
        # A run still on the old key may be mid-flight; wait out the idle window.
        old = entry(f"{CARGO}-dev-Linux-x64-f3ce3a4d-ee4c8f7c", 7, 0.5, id=1)
        new = entry(f"{CARGO}-dev-Linux-x64-f3ce3a4d-77b48f8a", 0.4, 0, id=2)
        self.assertEqual(stale_ids([old, new]), set())

    def test_only_newest_codeql_commit_key_stays(self) -> None:
        keys = [
            entry(f"codeql-trap-1-2.27.1-javascript-{n:040x}", 4 - n, 4 - n, id=n)
            for n in range(4)
        ]
        self.assertEqual(stale_ids(keys), {0, 1, 2})

    def test_idle_lockfile_cache_beside_a_newer_one_stays(self) -> None:
        # The docs lockfile cache sat idle while another lockfile's key changed:
        # the key cannot tell those apart from a replacement, so pnpm is skipped.
        docs = entry("node-cache-Linux-x64-pnpm-24ea7dc5df44", 25, 3, id=1)
        sdk = entry("node-cache-Linux-x64-pnpm-a07b6afd81c9", 2, 0, id=2)
        self.assertEqual(stale_ids([docs, sdk]), set())

    def test_buildkit_blobs_are_never_pruned(self) -> None:
        old = entry("buildkit-blob-1-sha256:aaaaaaaaaaaa", 6, 5, id=1)
        new = entry("buildkit-blob-1-sha256:bbbbbbbbbbbb", 1, 0, id=2)
        self.assertEqual(stale_ids([old, new]), set())

    def test_lineages_are_per_ref(self) -> None:
        old = entry(f"{CARGO}-release-Linux-x64-f3ce3a4d-ee4c8f7c", 6, 5, ref="refs/pull/1/merge")
        new = entry(f"{CARGO}-release-Linux-x64-f3ce3a4d-77b48f8a", 1, 0, id=2)
        self.assertEqual(stale_ids([old, new]), set())


if __name__ == "__main__":
    unittest.main()
