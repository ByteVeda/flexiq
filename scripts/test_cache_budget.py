"""Tests for the superseded-cache rule in cache_budget.py. Run from scripts/:
``python3 -m unittest test_cache_budget``.
"""

from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from cache_budget import Entry, superseded

NOW = datetime(2026, 10, 8, 9, 0, tzinfo=timezone.utc)
MASTER = "refs/heads/master"


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


def ids(entries: list[Entry]) -> set[int]:
    return {item.id for item in entries}


class SupersededTest(unittest.TestCase):
    def test_replaced_rust_cache_goes(self) -> None:
        old = entry("v1-rust-bin-fix-release-Linux-x64-f3ce3a4d-ee4c8f7c", 7, 3, id=1)
        new = entry("v1-rust-bin-fix-release-Linux-x64-f3ce3a4d-77b48f8a", 2, 0, id=2)
        self.assertEqual(ids(superseded([old, new], NOW)), {1})

    def test_two_live_lockfile_caches_both_stay(self) -> None:
        # One family, two lockfiles: both read after the newer one was saved.
        docs = entry("node-cache-Linux-x64-pnpm-24ea7dc5df44", 25, 0, id=1)
        sdk = entry("node-cache-Linux-x64-pnpm-a07b6afd81c9", 2, 0, id=2)
        self.assertEqual(superseded([docs, sdk], NOW), [])

    def test_recently_replaced_entry_gets_a_day(self) -> None:
        # A run still on the old key may be mid-flight; wait out the idle window.
        old = entry("node-cache-macOS-arm64-pnpm-ca27f33fbf55", 25, 0.5, id=1)
        new = entry("node-cache-macOS-arm64-pnpm-eab54a975b96", 0.4, 0, id=2)
        self.assertEqual(superseded([old, new], NOW), [])

    def test_only_newest_codeql_commit_key_stays(self) -> None:
        keys = [
            entry(f"codeql-trap-1-2.27.1-javascript-{n:040x}", 4 - n, 4 - n, id=n)
            for n in range(4)
        ]
        self.assertEqual(ids(superseded(keys, NOW)), {0, 1, 2})

    def test_lone_idle_entry_stays(self) -> None:
        self.assertEqual(superseded([entry("setup-go-Linux-x64-go-1.25-abcdef12", 6, 6)], NOW), [])

    def test_buildkit_blobs_are_never_pruned(self) -> None:
        old = entry("buildkit-blob-1-sha256:aaaaaaaaaaaa", 6, 5, id=1)
        new = entry("buildkit-blob-1-sha256:bbbbbbbbbbbb", 1, 0, id=2)
        self.assertEqual(superseded([old, new], NOW), [])

    def test_families_are_per_ref(self) -> None:
        old = entry("node-cache-Linux-x64-pnpm-ca27f33fbf55", 6, 5, ref="refs/pull/1/merge", id=1)
        new = entry("node-cache-Linux-x64-pnpm-eab54a975b96", 1, 0, id=2)
        self.assertEqual(superseded([old, new], NOW), [])


if __name__ == "__main__":
    unittest.main()
