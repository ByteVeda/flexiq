"""Structured task-error encoding/decoding and end-to-end propagation."""

import json
import threading
from typing import Any

import pytest

from flexiq import Queue
from flexiq.exceptions import MaxRetriesExceededError, TaskFailedError
from flexiq.result import _summarize_error
from flexiq.task_errors import decode_task_error, encode_from_parts, encode_task_error


def _raise_and_capture() -> BaseException:
    try:
        raise ValueError("bad value 42")
    except ValueError as exc:
        return exc


class TestEncode:
    def test_matches_contract_vector(self) -> None:
        """Byte-exact against the BINDING_CONTRACT.md test vector."""

        class BoomError(Exception):
            pass

        exc = BoomError("it broke")
        encoded = encode_from_parts(BoomError, exc, None)
        decoded = json.loads(encoded)
        decoded["traceback"] = ["frame1", "frame2"]
        assert (
            json.dumps(decoded, separators=(",", ":"))
            == '{"errtype":"TestEncode.test_matches_contract_vector.<locals>.BoomError",'
            '"message":"it broke","traceback":["frame1","frame2"]}'
        )

    def test_encodes_type_message_and_frames(self) -> None:
        exc = _raise_and_capture()
        decoded = json.loads(encode_task_error(exc))
        assert decoded["errtype"] == "ValueError"
        assert decoded["message"] == "bad value 42"
        assert any("raise ValueError" in line for line in decoded["traceback"])

    def test_key_order_and_compactness(self) -> None:
        exc = _raise_and_capture()
        encoded = encode_task_error(exc)
        assert encoded.startswith('{"errtype":"ValueError","message":"bad value 42","traceback":[')
        assert ": " not in encoded.split('"traceback"')[0]


class TestDecode:
    def test_round_trip(self) -> None:
        exc = _raise_and_capture()
        decoded = decode_task_error(encode_task_error(exc))
        assert decoded is not None
        assert decoded.errtype == "ValueError"
        assert decoded.message == "bad value 42"
        assert decoded.summary() == "ValueError: bad value 42"

    @pytest.mark.parametrize(
        "raw",
        [
            None,
            "",
            "plain legacy traceback\nValueError: nope",
            "job timed out after 5000ms",
            "[1, 2]",
            '{"no_message_key": true}',
            '{"message": 42}',
            "{not json",
        ],
    )
    def test_legacy_and_malformed_return_none(self, raw: str | None) -> None:
        assert decode_task_error(raw) is None


class TestSummarize:
    def test_structured_error_summarized_from_fields(self) -> None:
        exc = _raise_and_capture()
        assert _summarize_error(encode_task_error(exc)) == "ValueError: bad value 42"

    def test_legacy_traceback_still_uses_last_line(self) -> None:
        tb = "Traceback (most recent call last):\n  ...\nValueError: legacy"
        assert _summarize_error(tb) == "ValueError: legacy"


class TestEndToEnd:
    def test_failed_job_stores_structured_error(
        self, queue: Queue, run_worker: threading.Thread, poll_until: Any
    ) -> None:
        @queue.task(max_retries=0)
        def explode() -> None:
            raise RuntimeError("kaboom")

        result = explode.delay()

        def job_failed() -> bool:
            job = queue.get_job(result.id)
            return job is not None and job.status in ("failed", "dead")

        poll_until(job_failed, message="job should fail")
        job = queue.get_job(result.id)
        assert job is not None
        decoded = decode_task_error(job.error)
        assert decoded is not None
        assert decoded.errtype == "RuntimeError"
        assert decoded.message == "kaboom"
        assert decoded.traceback

    def test_result_raises_with_structured_details(
        self, queue: Queue, run_worker: threading.Thread
    ) -> None:
        @queue.task(max_retries=0)
        def explode() -> None:
            raise RuntimeError("kaboom")

        result = explode.delay()
        with pytest.raises((TaskFailedError, MaxRetriesExceededError)) as excinfo:
            result.result(timeout=10)
        error = excinfo.value
        assert isinstance(error, (TaskFailedError, MaxRetriesExceededError))
        assert "RuntimeError: kaboom" in str(error)
        assert error.errtype == "RuntimeError"
        assert error.job_id == result.id
        assert error.traceback
        assert error.raw_error is not None and error.raw_error.startswith("{")
