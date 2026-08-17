"""Tests for dashboard static asset resolution and Content-Type mapping."""

from __future__ import annotations

from pathlib import Path

import pytest

from flexiq.dashboard import _content_type_for, _resolve_static_node


@pytest.fixture
def static_root(tmp_path: Path) -> Path:
    """Layout mirroring a built Vite SPA tree."""
    (tmp_path / "index.html").write_text("<html></html>")
    assets = tmp_path / "assets"
    assets.mkdir()
    (assets / "index-abc.js").write_text("// js")
    (assets / "index-abc.css").write_text("/* css */")
    (assets / "nested").mkdir()
    (assets / "nested" / "deep.png").write_bytes(b"\x89PNG")
    return tmp_path


# ── _resolve_static_node ────────────────────────────────────────────


def test_resolve_index_html(static_root: Path) -> None:
    node = _resolve_static_node(static_root, "/index.html")
    assert node is not None
    assert node.read_text() == "<html></html>"


def test_resolve_hashed_asset(static_root: Path) -> None:
    node = _resolve_static_node(static_root, "/assets/index-abc.js")
    assert node is not None
    assert node.read_text() == "// js"


def test_resolve_nested_asset(static_root: Path) -> None:
    node = _resolve_static_node(static_root, "/assets/nested/deep.png")
    assert node is not None
    assert node.read_bytes() == b"\x89PNG"


def test_resolve_missing_file_returns_none(static_root: Path) -> None:
    assert _resolve_static_node(static_root, "/assets/missing.js") is None


def test_resolve_directory_returns_none(static_root: Path) -> None:
    # A directory matches joinpath but is_file() is False
    assert _resolve_static_node(static_root, "/assets") is None


def test_resolve_empty_path_returns_none(static_root: Path) -> None:
    assert _resolve_static_node(static_root, "") is None
    assert _resolve_static_node(static_root, "/") is None


def test_resolve_rejects_parent_traversal(static_root: Path) -> None:
    assert _resolve_static_node(static_root, "/../secret") is None
    assert _resolve_static_node(static_root, "/assets/../../secret") is None


def test_resolve_rejects_current_directory(static_root: Path) -> None:
    assert _resolve_static_node(static_root, "/./index.html") is None


def test_resolve_rejects_null_byte(static_root: Path) -> None:
    assert _resolve_static_node(static_root, "/index.html\x00.png") is None


def test_resolve_rejects_backslash(static_root: Path) -> None:
    # Windows-style separators should be rejected to avoid ambiguity
    assert _resolve_static_node(static_root, "/assets\\index-abc.js") is None


def test_resolve_rejects_double_slash(static_root: Path) -> None:
    # Empty segments from double slashes are rejected
    assert _resolve_static_node(static_root, "/assets//index-abc.js") is None


# ── _content_type_for ──────────────────────────────────────────────


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        ("/index.html", "text/html; charset=utf-8"),
        ("/assets/index-abc.js", "application/javascript; charset=utf-8"),
        ("/assets/index-abc.mjs", "application/javascript; charset=utf-8"),
        ("/assets/index-abc.css", "text/css; charset=utf-8"),
        ("/icon.svg", "image/svg+xml"),
        ("/icon.png", "image/png"),
        ("/favicon.ico", "image/x-icon"),
        ("/fonts/inter.woff2", "font/woff2"),
        ("/fonts/inter.woff", "font/woff"),
        ("/app.webmanifest", "application/manifest+json"),
        ("/data.json", "application/json; charset=utf-8"),
        ("/unknown.bin", "application/octet-stream"),
        ("/no-extension", "application/octet-stream"),
    ],
)
def test_content_type_for(path: str, expected: str) -> None:
    assert _content_type_for(path) == expected


def test_content_type_case_insensitive() -> None:
    # Uppercase extensions should still match
    assert _content_type_for("/IMAGE.PNG") == "image/png"
    assert _content_type_for("/script.JS") == "application/javascript; charset=utf-8"


# ── _safe_path (log injection guard) ─────────────────────────────────


def test_safe_path_strips_crlf() -> None:
    from flexiq.dashboard.server import _safe_path

    assert _safe_path("/api/jobs\r\nFAKE LOG ENTRY") == "/api/jobsFAKE LOG ENTRY"


def test_safe_path_strips_null_byte() -> None:
    from flexiq.dashboard.server import _safe_path

    assert _safe_path("/api/jobs\x00admin") == "/api/jobsadmin"


def test_safe_path_strips_all_control_chars_except_tab() -> None:
    from flexiq.dashboard.server import _safe_path

    raw = "/api\x01\x02\x1f\x7fpath\twith-tab"
    assert _safe_path(raw) == "/apipath\twith-tab"


def test_safe_path_truncates_long_input() -> None:
    from flexiq.dashboard.server import _safe_path

    raw = "/api/" + "x" * 1000
    out = _safe_path(raw)
    assert len(out) == 256
    assert out.startswith("/api/x")
