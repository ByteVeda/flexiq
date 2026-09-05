"""Regression tests: results are written with the queue serializer.

Before the fix, worker paths wrote results as raw cloudpickle regardless of
the configured serializer, while ``JobResult.get()`` read them back with the
queue serializer — silently broken for any non-default serializer.
"""

import json
import threading
from collections.abc import Generator
from pathlib import Path

import cloudpickle
import pytest

from conftest import join_worker
from flexiq import JsonSerializer, Queue, SmartSerializer


@pytest.fixture
def json_queue(tmp_path: Path) -> Queue:
    return Queue(db_path=str(tmp_path / "json.db"), workers=1, serializer=JsonSerializer())


@pytest.fixture
def run_json_worker(json_queue: Queue) -> Generator[threading.Thread]:
    thread = threading.Thread(target=json_queue.run_worker, daemon=True)
    thread.start()
    yield thread
    json_queue._inner.request_shutdown()
    join_worker(thread)


def test_result_round_trips_with_json_serializer(
    json_queue: Queue, run_json_worker: threading.Thread
) -> None:
    @json_queue.task()
    def build_report(name: str) -> dict:
        return {"report": name, "rows": [1, 2, 3]}

    result = build_report.delay("weekly")
    assert result.result(timeout=10) == {"report": "weekly", "rows": [1, 2, 3]}


def test_result_bytes_use_queue_serializer(
    json_queue: Queue, run_json_worker: threading.Thread
) -> None:
    @json_queue.task()
    def echo(value: str) -> str:
        return value

    result = echo.delay("hello")
    result.result(timeout=10)

    job = json_queue._inner.get_job(result.id)
    assert job is not None and job.result_bytes is not None
    # Valid JSON proves the queue serializer produced the bytes, not cloudpickle.
    assert json.loads(job.result_bytes.decode("utf-8")) == "hello"


def test_smart_serializer_reads_legacy_cloudpickle_results() -> None:
    # Results persisted before the fix are raw cloudpickle; the default
    # serializer's untagged-bytes fallback keeps them readable after upgrade.
    legacy = cloudpickle.dumps({"legacy": True})
    assert SmartSerializer().loads(legacy) == {"legacy": True}
