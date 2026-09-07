#!/usr/bin/env python3
"""Report GitHub Actions cache usage and enforce this repo's cache layout.

Past 10 GB, GitHub evicts least-recently-used entries silently — the large
master caches every run restores from. Usage is checked here before that, along
with the rule that keeps it down: Cargo caches live on master only.

    gh api "repos/$REPO/actions/caches?per_page=100" --paginate \\
      --jq '.actions_caches[] | {size: .size_in_bytes, ref: .ref, key: .key}' \\
      | jq -s '.' | python3 scripts/cache_budget.py -

Prints Markdown, exits non-zero on a violation.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

GIB = 1_000_000_000

#: Where a Cargo cache is allowed to exist. Every other ref restores from it.
CARGO_HOME_REF = "refs/heads/master"

#: buildkit names every layer after its digest; collapse them to one row each.
DIGEST_FAMILIES = (
    ("buildkit-blob", "buildkit layer blobs"),
    ("index-server-image", "buildkit image index"),
)

#: Trailing hash segments rust-cache and friends append to a stable prefix.
_HASH_SUFFIX = re.compile(r"-(?:[0-9a-f]{8,}|\d+)(?:#\d+)?$")


@dataclass(frozen=True)
class Entry:
    """One cache entry as the API reports it."""

    size: int
    ref: str
    key: str


@dataclass
class Bucket:
    """Running total for one cache family."""

    size: int = 0
    count: int = 0

    def add(self, entry: Entry) -> None:
        self.size += entry.size
        self.count += 1


@dataclass(frozen=True)
class Violation:
    """A broken rule, with the entries that broke it."""

    message: str
    details: tuple[str, ...] = ()


def family_of(key: str) -> str:
    """The reporting bucket a cache key belongs to.

    Non-digest keys keep their stable prefix, so a new cache gets its own row
    rather than joining someone else's total.
    """
    for prefix, label in DIGEST_FAMILIES:
        if key.startswith(prefix):
            return label
    stripped = key
    while True:
        shorter = _HASH_SUFFIX.sub("", stripped)
        if shorter == stripped:
            return stripped
        stripped = shorter


def cargo_prefix_from(action: Path) -> str:
    """Read `prefix-key` out of setup-rust, so a bump cannot disarm the check.

    Hardcoding it here would leave the rule silently passing the day someone
    moves to `v2-rust-bin`.
    """
    for line in action.read_text().splitlines():
        stripped = line.strip()
        if stripped.startswith("prefix-key:"):
            return stripped.split(":", 1)[1].strip().strip("\"'")
    raise SystemExit(f"no prefix-key in {action}")


def load(source: str) -> list[Entry]:
    """Read the caches array from a file, or from stdin when given ``-``."""
    raw = sys.stdin.read() if source == "-" else Path(source).read_text()
    return [
        Entry(size=int(item["size"]), ref=item["ref"], key=item["key"]) for item in json.loads(raw)
    ]


def render(entries: list[Entry], total: int, limit_gb: float, rows: int) -> list[str]:
    """The Markdown report, largest family first, tail folded into one row."""
    by_family: dict[str, Bucket] = defaultdict(Bucket)
    by_ref: dict[str, Bucket] = defaultdict(Bucket)
    for entry in entries:
        by_family[family_of(entry.key)].add(entry)
        by_ref["master" if entry.ref == CARGO_HOME_REF else "other refs"].add(entry)

    share = 100 * total / (limit_gb * GIB) if limit_gb else 0
    lines = [
        "## Actions cache budget",
        "",
        f"**{total / GIB:.2f} GB** across {len(entries)} entries "
        f"— {share:.0f}% of the {limit_gb:g} GB limit.",
        "",
        "| Family | GB | Entries | Share |",
        "| --- | ---: | ---: | ---: |",
    ]
    ranked = sorted(by_family.items(), key=lambda item: -item[1].size)
    for name, bucket in ranked[:rows]:
        percent = 100 * bucket.size / total if total else 0
        lines.append(f"| `{name}` | {bucket.size / GIB:.2f} | {bucket.count} | {percent:.0f}% |")
    if len(ranked) > rows:
        tail = [bucket for _, bucket in ranked[rows:]]
        size = sum(bucket.size for bucket in tail)
        percent = 100 * size / total if total else 0
        lines.append(
            f"| _{len(tail)} smaller families_ | {size / GIB:.2f} | "
            f"{sum(bucket.count for bucket in tail)} | {percent:.0f}% |"
        )
    lines += ["", "| Ref | GB | Entries |", "| --- | ---: | ---: |"]
    for name, bucket in sorted(by_ref.items(), key=lambda item: -item[1].size):
        lines.append(f"| {name} | {bucket.size / GIB:.2f} | {bucket.count} |")
    return lines


def violations(
    entries: list[Entry], total: int, budget_gb: float, cargo_prefix: str
) -> list[Violation]:
    """Every rule this snapshot breaks. Empty means healthy."""
    found = []
    if total > budget_gb * GIB:
        found.append(
            Violation(
                f"Cache is {total / GIB:.2f} GB, over the {budget_gb:g} GB "
                "budget; past 10 GB GitHub evicts the entries builds restore "
                "from."
            )
        )

    stray = [
        entry
        for entry in entries
        if entry.key.startswith(cargo_prefix) and entry.ref != CARGO_HOME_REF
    ]
    if stray:
        wasted = sum(entry.size for entry in stray) / GIB
        found.append(
            Violation(
                f"{len(stray)} Cargo cache(s) outside {CARGO_HOME_REF}, holding "
                f"{wasted:.2f} GB. Only master's writer may save one. A job that "
                "saves anyway is usually reaching setup-rust through a wrapper, "
                "which loses `save-if` in rust-cache's post step.",
                tuple(f"{entry.ref}  {entry.key}" for entry in stray),
            )
        )
    return found


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("caches", help="JSON array of caches, or - for stdin")
    parser.add_argument("--limit-gb", type=float, default=10.0)
    parser.add_argument("--budget-gb", type=float, default=8.5)
    parser.add_argument(
        "--setup-rust",
        type=Path,
        default=Path(".github/actions/setup-rust/action.yml"),
        help="action to read the Cargo cache prefix-key from",
    )
    parser.add_argument("--rows", type=int, default=12, help="families to list")
    args = parser.parse_args()

    cargo_prefix = cargo_prefix_from(args.setup_rust)
    entries = load(args.caches)
    total = sum(entry.size for entry in entries)

    report = render(entries, total, args.limit_gb, args.rows)
    broken = violations(entries, total, args.budget_gb, cargo_prefix)
    if broken:
        report += ["", "### Over budget", ""]
        for violation in broken:
            report.append(f"- {violation.message}")
            report += [f"  - `{detail}`" for detail in violation.details]
    print("\n".join(report))

    for violation in broken:
        print(f"::error::{violation.message}", file=sys.stderr)
        for detail in violation.details:
            print(f"  {detail}", file=sys.stderr)
    return 1 if broken else 0


if __name__ == "__main__":
    sys.exit(main())
