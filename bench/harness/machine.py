"""What this run was measured on.

A throughput number without a machine beside it is a rumour. Everything here
ends up in the results file so that a reader can decide whether our numbers
have any bearing on theirs — and, when they do not, reproduce them on hardware
that does.
"""

from __future__ import annotations

import json
import os
import platform
import re
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


def _cpu_model() -> str:
    try:
        text = Path("/proc/cpuinfo").read_text()
    except OSError:
        return platform.processor() or "unknown"
    match = re.search(r"^model name\s*:\s*(.+)$", text, re.MULTILINE)
    return match.group(1).strip() if match else platform.processor() or "unknown"


def _total_ram_gb() -> float | None:
    try:
        text = Path("/proc/meminfo").read_text()
    except OSError:
        return None
    match = re.search(r"^MemTotal:\s+(\d+) kB$", text, re.MULTILINE)
    return round(int(match.group(1)) / 1024 / 1024, 1) if match else None


def _command(*argv: str) -> str | None:
    if shutil.which(argv[0]) is None:
        return None
    try:
        out = subprocess.run(argv, capture_output=True, text=True, timeout=30, check=True)
    except (subprocess.SubprocessError, OSError):
        return None
    return out.stdout.strip() or None


def git_revision() -> dict[str, object]:
    """The commit the harness was run from, and whether the tree was clean.

    A dirty tree is recorded rather than refused: a benchmark is often run
    mid-change. It is recorded because a number from an uncommitted tree cannot
    be reproduced from the repository alone, and a reader deserves to know that
    before quoting it.
    """
    sha = _command("git", "-C", str(REPO_ROOT), "rev-parse", "HEAD")
    status = _command("git", "-C", str(REPO_ROOT), "status", "--porcelain")
    return {"commit": sha, "dirty": bool(status)}


def python_versions(packages: list[str]) -> dict[str, str | None]:
    """Installed version of each package, as resolved in *this* interpreter."""
    from importlib.metadata import PackageNotFoundError, version

    out: dict[str, str | None] = {}
    for name in packages:
        try:
            out[name] = version(name)
        except PackageNotFoundError:
            out[name] = None
    return out


def node_package_versions(package_dir: Path, packages: list[str]) -> dict[str, str | None]:
    """Versions actually installed under a Node package directory.

    Read from each dependency's own `package.json` rather than the manifest's
    range, for the same reason the Python side reads installed metadata: `^6.3`
    is not a version, it is a hope.
    """
    out: dict[str, str | None] = {}
    for name in packages:
        manifest = package_dir / "node_modules" / name / "package.json"
        try:
            out[name] = json.loads(manifest.read_text())["version"]
        except (OSError, KeyError, json.JSONDecodeError):
            out[name] = None
    return out


def slug() -> str:
    """Filename-safe machine tag: `<cores>core-<arch>-<host>`."""
    host = re.sub(r"[^a-z0-9]+", "-", platform.node().lower()).strip("-") or "unknown"
    return f"{os.cpu_count() or 0}core-{platform.machine()}-{host}"


def fingerprint(label: str, notes: str) -> dict[str, object]:
    """The machine block written into every results file."""
    return {
        "label": label,
        "notes": notes,
        "measured_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "cpu_model": _cpu_model(),
        "cpu_count": os.cpu_count(),
        "ram_gb": _total_ram_gb(),
        "kernel": platform.release(),
        "platform": platform.platform(),
        "python": sys.version.split()[0],
        "node": _command("node", "--version"),
        "redis_server": _command("redis-server", "--version"),
        "git": git_revision(),
    }
