"""Tests for partial result streaming via current_job.publish() and job.stream()."""

from __future__ import annotations

import threading
import time
from pathlib import Path

from flexiq import Queue


def test_publish_writes_result_log(tmp_path: Path) -> None:
    """publish() stores data as a task log with level='result'."""
    queue = Queue(db_path=str(tmp_path / "test.db"))

    @queue.task()
    def emit_data() -> str:
        from flexiq.context import current_job

        current_job.publish({"step": 1, "value": "hello"})
        current_job.publish({"step": 2, "value": "world"})
        return "done"

    job = emit_data.delay()

    worker = threading.Thread(target=queue.run_worker, daemon=True)
    worker.start()

    result = job.result(timeout=10)
    assert result == "done"

    # Check that partial results are stored as task logs
    logs = queue.task_logs(job.id)
    result_logs = [lg for lg in logs if lg["level"] == "result"]
    assert len(result_logs) == 2
    assert '"step": 1' in result_logs[0]["extra"]
    assert '"step": 2' in result_logs[1]["extra"]

    queue._inner.request_shutdown()


def test_stream_yields_partial_results(tmp_path: Path) -> None:
    """job.stream() yields published partial results."""
    queue = Queue(db_path=str(tmp_path / "test.db"))

    @queue.task()
    def batch_process() -> str:
        from flexiq.context import current_job

        for i in range(3):
            current_job.publish({"item": i, "status": "processed"})
            time.sleep(0.1)
        return "all done"

    job = batch_process.delay()

    worker = threading.Thread(target=queue.run_worker, daemon=True)
    worker.start()

    # Collect streamed results
    results = list(job.stream(timeout=15, poll_interval=0.3))

    assert len(results) == 3
    assert results[0]["item"] == 0
    assert results[1]["item"] == 1
    assert results[2]["item"] == 2
    assert all(r["status"] == "processed" for r in results)

    queue._inner.request_shutdown()


def test_stream_stops_on_completion(tmp_path: Path) -> None:
    """stream() stops iterating when the job completes."""
    queue = Queue(db_path=str(tmp_path / "test.db"))

    @queue.task()
    def quick_task() -> int:
        from flexiq.context import current_job

        current_job.publish({"msg": "started"})
        return 42

    job = quick_task.delay()

    worker = threading.Thread(target=queue.run_worker, daemon=True)
    worker.start()

    results = list(job.stream(timeout=10, poll_interval=0.2))
    assert len(results) >= 1
    assert results[0]["msg"] == "started"

    queue._inner.request_shutdown()


async def test_astream_async(tmp_path: Path) -> None:
    """astream() works as an async iterator."""
    queue = Queue(db_path=str(tmp_path / "test.db"))

    @queue.task()
    def async_batch() -> str:
        from flexiq.context import current_job

        current_job.publish({"phase": "init"})
        current_job.publish({"phase": "done"})
        return "ok"

    job = async_batch.delay()

    worker = threading.Thread(target=queue.run_worker, daemon=True)
    worker.start()

    results: list[dict] = []
    async for partial in job.astream(timeout=10, poll_interval=0.3):
        results.append(partial)

    assert len(results) >= 1
    assert results[0]["phase"] == "init"

    queue._inner.request_shutdown()


def test_publish_non_dict_data(tmp_path: Path) -> None:
    """publish() handles non-dict data (strings, lists, numbers).

    Since stream() polls, we verify via task_logs directly to avoid
    timing-dependent polling issues.
    """
    queue = Queue(db_path=str(tmp_path / "test.db"))

    @queue.task()
    def varied_output() -> str:
        from flexiq.context import current_job

        current_job.publish("plain string")
        current_job.publish([1, 2, 3])
        current_job.publish(42)
        return "done"

    job = varied_output.delay()

    worker = threading.Thread(target=queue.run_worker, daemon=True)
    worker.start()

    job.result(timeout=10)

    # Verify all published data via task logs
    logs = queue.task_logs(job.id)
    result_logs = [lg for lg in logs if lg["level"] == "result"]
    extras = [lg["extra"] for lg in result_logs]
    assert '"plain string"' in extras
    assert "[1, 2, 3]" in extras
    assert "42" in extras

    queue._inner.request_shutdown()
