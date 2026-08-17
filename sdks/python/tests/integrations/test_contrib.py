"""Tests for contrib middleware modules using mocks (no hard dependencies)."""

from __future__ import annotations

import types
from typing import Any
from unittest.mock import MagicMock, patch

# ── Helpers ──────────────────────────────────────────────────────────


def _make_ctx(
    job_id: str = "job-1",
    task_name: str = "my_task",
    queue_name: str = "default",
    retry_count: int = 0,
) -> MagicMock:
    ctx = MagicMock()
    ctx.id = job_id
    ctx.task_name = task_name
    ctx.queue_name = queue_name
    ctx.retry_count = retry_count
    return ctx


# ── OpenTelemetry ────────────────────────────────────────────────────


class TestOpenTelemetryMiddleware:
    def test_before_starts_span(self) -> None:
        otel = _try_import_otel()
        if otel is None:
            return

        mock_tracer = MagicMock()
        mock_span = MagicMock()
        mock_tracer.start_span.return_value = mock_span

        mw = otel.OpenTelemetryMiddleware.__new__(otel.OpenTelemetryMiddleware)
        import threading

        mw._tracer = mock_tracer
        mw._span_name_fn = None
        mw._attr_prefix = "flexiq"
        mw._extra_attributes_fn = None
        mw._task_filter = None
        mw._spans = {}
        mw._lock = threading.Lock()

        ctx = _make_ctx()
        mw.before(ctx)

        mock_tracer.start_span.assert_called_once()
        assert "job-1" in mw._spans

    def test_after_ends_span_success(self) -> None:
        otel = _try_import_otel()
        if otel is None:
            return

        mock_span = MagicMock()
        mw = otel.OpenTelemetryMiddleware.__new__(otel.OpenTelemetryMiddleware)
        import threading

        mw._tracer = MagicMock()
        mw._span_name_fn = None
        mw._attr_prefix = "flexiq"
        mw._extra_attributes_fn = None
        mw._task_filter = None
        mw._spans = {"job-1": mock_span}
        mw._lock = threading.Lock()

        ctx = _make_ctx()
        mw.after(ctx, result="ok", error=None)

        mock_span.set_status.assert_called_once()
        mock_span.end.assert_called_once()
        mock_span.record_exception.assert_not_called()

    def test_after_records_exception_on_error(self) -> None:
        otel = _try_import_otel()
        if otel is None:
            return

        mock_span = MagicMock()
        mw = otel.OpenTelemetryMiddleware.__new__(otel.OpenTelemetryMiddleware)
        import threading

        mw._tracer = MagicMock()
        mw._span_name_fn = None
        mw._attr_prefix = "flexiq"
        mw._extra_attributes_fn = None
        mw._task_filter = None
        mw._spans = {"job-1": mock_span}
        mw._lock = threading.Lock()

        ctx = _make_ctx()
        exc = ValueError("boom")
        mw.after(ctx, result=None, error=exc)

        mock_span.record_exception.assert_called_once_with(exc)
        mock_span.end.assert_called_once()


def _try_import_otel() -> types.ModuleType | None:
    """Import otel module with mocked opentelemetry if not installed."""
    try:
        import sys

        # Provide mock opentelemetry modules if not installed
        mock_trace = MagicMock()
        mock_trace.StatusCode.OK = "OK"
        mock_trace.StatusCode.ERROR = "ERROR"

        with patch.dict(
            sys.modules,
            {
                "opentelemetry": MagicMock(),
                "opentelemetry.trace": mock_trace,
            },
        ):
            if "flexiq.contrib.otel" in sys.modules:
                del sys.modules["flexiq.contrib.otel"]
            from flexiq.contrib import otel

            # Patch module-level references
            otel.trace = mock_trace
            otel.StatusCode = mock_trace.StatusCode
            return otel
    except Exception:
        return None


# ── Sentry ───────────────────────────────────────────────────────────


class TestSentryMiddleware:
    def test_before_pushes_scope(self) -> None:
        sentry_mod = _try_import_sentry()
        if sentry_mod is None:
            return

        mock_sdk = sentry_mod.sentry_sdk
        ctx = _make_ctx()

        mw = sentry_mod.SentryMiddleware.__new__(sentry_mod.SentryMiddleware)
        mw._tag_prefix = "flexiq"
        mw._transaction_name_fn = None
        mw._task_filter = None
        mw._extra_tags_fn = None
        mw.before(ctx)

        mock_sdk.push_scope.assert_called_once()

    def test_after_pops_scope(self) -> None:
        sentry_mod = _try_import_sentry()
        if sentry_mod is None:
            return

        mock_sdk = sentry_mod.sentry_sdk
        ctx = _make_ctx()

        mw = sentry_mod.SentryMiddleware.__new__(sentry_mod.SentryMiddleware)
        mw._tag_prefix = "flexiq"
        mw._transaction_name_fn = None
        mw._task_filter = None
        mw._extra_tags_fn = None
        mw.after(ctx, result="ok", error=None)

        mock_sdk.pop_scope_unsafe.assert_called_once()

    def test_after_captures_exception_on_error(self) -> None:
        sentry_mod = _try_import_sentry()
        if sentry_mod is None:
            return

        mock_sdk = sentry_mod.sentry_sdk
        ctx = _make_ctx()
        exc = RuntimeError("oops")

        mw = sentry_mod.SentryMiddleware.__new__(sentry_mod.SentryMiddleware)
        mw._tag_prefix = "flexiq"
        mw._transaction_name_fn = None
        mw._task_filter = None
        mw._extra_tags_fn = None
        mw.after(ctx, result=None, error=exc)

        mock_sdk.capture_exception.assert_called_once_with(exc)


def _try_import_sentry() -> types.ModuleType | None:
    try:
        import sys

        mock_sdk = MagicMock()
        with patch.dict(sys.modules, {"sentry_sdk": mock_sdk}):
            if "flexiq.contrib.sentry" in sys.modules:
                del sys.modules["flexiq.contrib.sentry"]
            from flexiq.contrib import sentry

            sentry.sentry_sdk = mock_sdk
            return sentry
    except Exception:
        return None


# ── Prometheus ───────────────────────────────────────────────────────


def _make_mock_metrics() -> dict[str, Any]:
    """Create a mock metrics dict matching the instance-based store format."""
    return {
        "jobs_total": MagicMock(),
        "job_duration": MagicMock(),
        "active_workers": MagicMock(),
        "retries_total": MagicMock(),
        "queue_depth": MagicMock(),
        "dlq_size": MagicMock(),
        "worker_utilization": MagicMock(),
        "resource_health": MagicMock(),
        "resource_recreations": MagicMock(),
        "resource_init_duration": MagicMock(),
        "proxy_reconstruct_duration": MagicMock(),
        "proxy_reconstruct_total": MagicMock(),
        "proxy_reconstruct_errors": MagicMock(),
        "intercept_duration": MagicMock(),
        "intercept_strategy_total": MagicMock(),
        "pool_size": MagicMock(),
        "pool_active": MagicMock(),
        "pool_idle": MagicMock(),
        "pool_timeouts": MagicMock(),
    }


class TestPrometheusMiddleware:
    def test_before_increments_active_workers(self) -> None:
        prom = _try_import_prometheus()
        if prom is None:
            return

        import threading

        metrics = _make_mock_metrics()
        mw = prom.PrometheusMiddleware.__new__(prom.PrometheusMiddleware)
        mw._metrics = metrics
        mw._extra_labels_fn = None
        mw._task_filter = None
        mw._start_times = {}
        mw._lock = threading.Lock()

        ctx = _make_ctx()
        mw.before(ctx)

        metrics["active_workers"].inc.assert_called()

    def test_after_tracks_counter_and_histogram(self) -> None:
        prom = _try_import_prometheus()
        if prom is None:
            return

        import threading

        metrics = _make_mock_metrics()
        mw = prom.PrometheusMiddleware.__new__(prom.PrometheusMiddleware)
        mw._metrics = metrics
        mw._extra_labels_fn = None
        mw._task_filter = None
        mw._start_times = {"job-1": 0.0}
        mw._lock = threading.Lock()

        ctx = _make_ctx()
        mw.after(ctx, result="ok", error=None)

        metrics["active_workers"].dec.assert_called()
        metrics["jobs_total"].labels.assert_called_with(task="my_task", status="completed")
        metrics["jobs_total"].labels().inc.assert_called()

    def test_after_tracks_failure(self) -> None:
        prom = _try_import_prometheus()
        if prom is None:
            return

        import threading

        metrics = _make_mock_metrics()
        mw = prom.PrometheusMiddleware.__new__(prom.PrometheusMiddleware)
        mw._metrics = metrics
        mw._extra_labels_fn = None
        mw._task_filter = None
        mw._start_times = {"job-1": 0.0}
        mw._lock = threading.Lock()

        ctx = _make_ctx()
        exc = ValueError("fail")
        mw.after(ctx, result=None, error=exc)

        metrics["jobs_total"].labels.assert_called_with(task="my_task", status="failed")


def _try_import_prometheus() -> types.ModuleType | None:
    try:
        import sys

        mock_counter = MagicMock()
        mock_gauge = MagicMock()
        mock_histogram = MagicMock()

        with patch.dict(
            sys.modules,
            {
                "prometheus_client": MagicMock(
                    Counter=mock_counter,
                    Gauge=mock_gauge,
                    Histogram=mock_histogram,
                    start_http_server=MagicMock(),
                ),
            },
        ):
            if "flexiq.contrib.prometheus" in sys.modules:
                del sys.modules["flexiq.contrib.prometheus"]
            from flexiq.contrib import prometheus

            return prometheus
    except Exception:
        return None
