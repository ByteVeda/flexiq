"""Fixtures shared by the deferred-registration tests."""

from __future__ import annotations

import sys
import uuid
from collections.abc import Callable, Generator
from pathlib import Path

import pytest

PackageWriter = Callable[[dict[str, str]], str]


@pytest.fixture
def write_package(tmp_path: Path) -> Generator[PackageWriter]:
    """Write an importable package tree into ``tmp_path`` and return its name.

    The package name is unique per call so a second test writing the same
    module layout is not served the first one out of ``sys.modules``.
    """
    created: list[str] = []
    sys.path.insert(0, str(tmp_path))

    def _write(modules: dict[str, str]) -> str:
        pkg = f"fq_discovery_{uuid.uuid4().hex[:8]}"
        created.append(pkg)
        for rel, source in modules.items():
            path = tmp_path / pkg / rel
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(source.replace("<PKG>", pkg))
        return pkg

    try:
        yield _write
    finally:
        sys.path.remove(str(tmp_path))
        for pkg in created:
            for name in [m for m in sys.modules if m == pkg or m.startswith(f"{pkg}.")]:
                del sys.modules[name]
