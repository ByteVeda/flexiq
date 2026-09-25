"""Tests for ``Queue(event_sinks=...)``: job events sent out as CloudEvents."""

import json
import threading
from collections.abc import Generator
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any

import pytest

from conftest import PollUntil, join_worker
from flexiq import Queue

SINK = "receiver"


@pytest.fixture
def receiver() -> Generator[tuple[str, list[dict[str, Any]]]]:
    """A loopback HTTP server recording every CloudEvent posted to it."""
    received: list[dict[str, Any]] = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length))
            # A batching sink posts an array; one event per request otherwise.
            received.extend(body if isinstance(body, list) else [body])
            self.send_response(200)
            self.end_headers()

        def log_message(self, *args: Any) -> None:
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}/events", received
    finally:
        server.shutdown()
        server.server_close()


def _http_sinks(url: str) -> dict[str, Any]:
    return {
        "sinks": [
            {
                "kind": "http",
                "name": SINK,
                "url": url,
                "allow": ["127.0.0.1"],
                "allow_loopback": True,
            }
        ]
    }


def test_invalid_document_raises_value_error(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="names no sinks"):
        Queue(db_path=str(tmp_path / "q.db"), event_sinks={"sinks": []})


def test_malformed_json_raises_value_error(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="events config is not valid"):
        Queue(db_path=str(tmp_path / "q.db"), event_sinks="{not json")


def test_document_is_read_from_a_path(tmp_path: Path) -> None:
    """An ``os.PathLike`` is read as a file; the bad document inside it is what raises."""
    document = tmp_path / "events.json"
    document.write_text(json.dumps({"sinks": []}), encoding="utf-8")
    with pytest.raises(ValueError, match="names no sinks"):
        Queue(db_path=str(tmp_path / "q.db"), event_sinks=document)


@pytest.mark.parametrize("drain", [-1.0, float("inf"), float("nan")])
def test_bad_drain_raises_value_error(tmp_path: Path, drain: float) -> None:
    with pytest.raises(ValueError, match="event_sinks_drain"):
        Queue(db_path=str(tmp_path / "q.db"), event_sinks_drain=drain)


def test_stats_are_empty_without_a_running_worker(tmp_path: Path) -> None:
    queue = Queue(db_path=str(tmp_path / "q.db"), event_sinks=_http_sinks("http://127.0.0.1:9/"))
    assert queue.event_sink_stats() == []


def test_worker_sends_started_and_completed(
    tmp_path: Path,
    receiver: tuple[str, list[dict[str, Any]]],
    poll_until: PollUntil,
) -> None:
    url, received = receiver
    queue = Queue(db_path=str(tmp_path / "q.db"), workers=2, event_sinks=_http_sinks(url))

    @queue.task()
    def echo(x: str) -> str:
        return x

    worker = threading.Thread(target=queue.run_worker, daemon=True)
    worker.start()
    try:
        job = echo.delay("hello")
        assert job.result(timeout=10) == "hello"

        def job_events() -> dict[str, dict[str, Any]]:
            return {e["type"]: e for e in list(received) if e["subject"] == job.id}

        started = "org.byteveda.flexiq.job.started"
        completed = "org.byteveda.flexiq.job.completed"
        poll_until(
            lambda: {started, completed} <= job_events().keys(),
            timeout=10,
            message="job.started and job.completed not both received",
        )
        for event in (job_events()[started], job_events()[completed]):
            assert event["flexiqqueue"] == "default"
            assert event["flexiqtask"] == echo.name
            assert event["data"]["job_id"] == job.id
            # No sink set `include_payload`, so no payload leaves the process.
            assert "payload_base64" not in event["data"]

        def delivered() -> int:
            return sum(s["delivered"] for s in queue.event_sink_stats() if s["name"] == SINK)

        # The counter moves once the receiver's reply is read, just after the post.
        poll_until(
            lambda: delivered() >= 2,
            message="event_sink_stats never counted the deliveries",
        )
        (stats,) = queue.event_sink_stats()
        assert stats["kind"] == "http"
        assert stats["dropped_rejected"] == 0
        assert stats["dropped_failed"] == 0
    finally:
        queue.shutdown()
        join_worker(worker)

    # The drained hub stays readable, so the final counts survive the run.
    (final,) = queue.event_sink_stats()
    assert final["name"] == SINK
    assert final["delivered"] >= 2
    assert final["queued"] == 0
