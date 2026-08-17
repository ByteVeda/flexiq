"""Tests for JobResult helpers."""

from __future__ import annotations

from flexiq.result import _ERROR_SUMMARY_MAX, _summarize_error


class TestErrorSummary:
    """``to_dict`` exposes only the final traceback line so the job list API
    doesn't broadcast frame source/locals to every viewer (M8)."""

    def test_returns_last_line(self) -> None:
        tb = (
            "Traceback (most recent call last):\n"
            '  File "x.py", line 3, in f\n'
            '    do(secret="sk-abc123")\n'
            "ValueError: bad value 42"
        )
        assert _summarize_error(tb) == "ValueError: bad value 42"

    def test_none_passes_through(self) -> None:
        assert _summarize_error(None) is None

    def test_empty_passes_through(self) -> None:
        assert _summarize_error("") == ""

    def test_long_line_capped(self) -> None:
        out = _summarize_error("E" * 5000)
        assert out is not None
        assert len(out) == _ERROR_SUMMARY_MAX + 1  # +1 for the ellipsis
        assert out.endswith("…")

    def test_single_line_unchanged(self) -> None:
        assert _summarize_error("RuntimeError: boom") == "RuntimeError: boom"
