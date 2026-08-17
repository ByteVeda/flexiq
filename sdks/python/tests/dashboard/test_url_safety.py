"""Tests for the URL-safety helpers used by the dashboard."""

from __future__ import annotations

import pytest

from flexiq.dashboard.url_safety import is_safe_redirect


@pytest.mark.parametrize(
    "path",
    [
        "/",
        "/dashboard",
        "/dashboard/jobs",
        "/dashboard?tab=overview",
        "/dashboard/jobs#section",
    ],
)
def test_is_safe_redirect_accepts_relative_paths(path: str) -> None:
    assert is_safe_redirect(path) is True


@pytest.mark.parametrize(
    "path",
    [
        "",
        None,
        "dashboard",  # no leading slash
        "//evil.com/x",  # protocol-relative URL
        "/\\evil.com",  # backslash variant
        "http://evil.com/x",
        "https://evil.com/x",
        "javascript:alert(1)",
        "data:text/html,xss",
        "\\\\evil.com",
    ],
)
def test_is_safe_redirect_rejects_unsafe(path: str | None) -> None:
    assert is_safe_redirect(path) is False
