"""Tests for KEDA-compatible /api/scaler endpoint contract."""

import json
import threading
import urllib.request
from collections.abc import Generator
from http.server import ThreadingHTTPServer
from pathlib import Path

import pytest

from flexiq import Queue
from flexiq.dashboard import build_scaler_response


@pytest.fixture()
def empty_queue(tmp_path: Path) -> Queue:
    """Queue with no pending jobs."""
    return Queue(db_path=str(tmp_path / "keda.db"), workers=4)


@pytest.fixture()
def populated_queue(tmp_path: Path) -> Queue:
    """Queue with pending jobs across two queues."""
    q = Queue(db_path=str(tmp_path / "keda.db"), workers=4)

    @q.task(queue="emails")
    def send_email(to: str) -> None:
        pass

    @q.task(queue="reports")
    def generate_report(name: str) -> None:
        pass

    for i in range(5):
        send_email.delay(f"user{i}@example.com")
    for i in range(3):
        generate_report.delay(f"report_{i}")

    return q


@pytest.fixture()
def scaler_server(empty_queue: Queue) -> Generator[str]:
    """Start a scaler HTTP server on a random port, yield the base URL."""
    from flexiq.scaler import _make_scaler_handler

    handler = _make_scaler_handler(empty_queue, target_queue_depth=10)
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{port}"
    server.shutdown()


# ─── Unit tests: build_scaler_response() ───


class TestScalerResponseShape:
    def test_required_fields(self, empty_queue: Queue) -> None:
        resp = build_scaler_response(empty_queue)
        assert "metricName" in resp
        assert "metricValue" in resp
        assert "isActive" in resp
        assert "liveWorkers" in resp
        assert "totalCapacity" in resp
        assert "targetQueueDepth" in resp

    def test_metric_value_is_pending_count(self, populated_queue: Queue) -> None:
        resp = build_scaler_response(populated_queue)
        assert resp["metricValue"] == 8  # 5 emails + 3 reports

    def test_is_active_false_when_empty(self, empty_queue: Queue) -> None:
        resp = build_scaler_response(empty_queue)
        assert resp["isActive"] is False
        assert resp["metricValue"] == 0

    def test_is_active_true_when_pending(self, populated_queue: Queue) -> None:
        resp = build_scaler_response(populated_queue)
        assert resp["isActive"] is True

    def test_per_queue_filter(self, populated_queue: Queue) -> None:
        resp = build_scaler_response(populated_queue, queue_name="emails")
        assert resp["metricValue"] == 5
        assert resp["isActive"] is True

    def test_metric_name_namespaced_for_queue(self, populated_queue: Queue) -> None:
        resp = build_scaler_response(populated_queue, queue_name="reports")
        assert resp["metricName"] == "flexiq_queue_depth_reports"

    def test_default_metric_name(self, empty_queue: Queue) -> None:
        resp = build_scaler_response(empty_queue)
        assert resp["metricName"] == "flexiq_queue_depth"

    def test_worker_utilization_present(self, empty_queue: Queue) -> None:
        resp = build_scaler_response(empty_queue)
        # workers=4, 0 running → utilization = 0.0
        assert "workerUtilization" in resp
        assert resp["workerUtilization"] == 0.0

    def test_worker_utilization_is_zero_when_idle(self, empty_queue: Queue) -> None:
        resp = build_scaler_response(empty_queue)
        assert resp["workerUtilization"] == 0.0

    def test_per_queue_stats_present(self, populated_queue: Queue) -> None:
        resp = build_scaler_response(populated_queue)
        assert "perQueue" in resp
        assert "emails" in resp["perQueue"]
        assert "reports" in resp["perQueue"]
        assert resp["perQueue"]["emails"]["pending"] == 5
        assert resp["perQueue"]["reports"]["pending"] == 3

    def test_target_queue_depth_default(self, empty_queue: Queue) -> None:
        resp = build_scaler_response(empty_queue)
        assert resp["targetQueueDepth"] == 10

    def test_target_queue_depth_custom(self, empty_queue: Queue) -> None:
        resp = build_scaler_response(empty_queue, target_queue_depth=25)
        assert resp["targetQueueDepth"] == 25

    def test_live_workers_zero_before_start(self, empty_queue: Queue) -> None:
        resp = build_scaler_response(empty_queue)
        assert resp["liveWorkers"] == 0

    def test_total_capacity_matches_config(self, empty_queue: Queue) -> None:
        resp = build_scaler_response(empty_queue)
        assert resp["totalCapacity"] == 4


# ─── Integration tests: HTTP server ───


class TestScalerHTTP:
    def test_scaler_endpoint_returns_json(self, scaler_server: str) -> None:
        resp = urllib.request.urlopen(f"{scaler_server}/api/scaler")
        assert resp.status == 200
        data = json.loads(resp.read())
        assert "metricName" in data
        assert "metricValue" in data

    def test_health_endpoint(self, scaler_server: str) -> None:
        resp = urllib.request.urlopen(f"{scaler_server}/health")
        assert resp.status == 200
        data = json.loads(resp.read())
        assert data["status"] == "ok"

    def test_unknown_path_returns_404(self, scaler_server: str) -> None:
        try:
            urllib.request.urlopen(f"{scaler_server}/unknown")
            pytest.fail("Expected HTTPError")
        except urllib.error.HTTPError as e:
            assert e.code == 404
