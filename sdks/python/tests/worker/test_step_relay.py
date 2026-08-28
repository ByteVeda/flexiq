"""Unit tests for the prefork child's step relay.

The end-to-end path is covered by ``test_executor_attach.py``, which drives a
real executor against a socket scheduler. What that cannot reach is the relay's
behaviour once the frame stream is *gone* — the pool has to be dead for those,
and a dead pool takes the assertions with it. So they are driven directly here.
"""

from __future__ import annotations

import threading
import time
from typing import Any

from flexiq.prefork.steps import _ACK_BACKSTOP_S, StepRelay


def _sent() -> tuple[list[tuple[dict[str, Any], bytes]], Any]:
    """A recording stand-in for the child's framed writer."""
    frames: list[tuple[dict[str, Any], bytes]] = []

    def send(header: dict[str, Any], payload: bytes) -> None:
        frames.append((header, payload))

    return frames, send


def test_a_commit_frames_its_result_and_waits_for_the_ack() -> None:
    """The header is the wire's, and the blob rides behind it."""
    frames, send = _sent()
    relay = StepRelay(send)
    answered: list[dict[str, Any]] = []

    def commit() -> None:
        answered.append(relay.commit("job-1", 0, "charge#0", "run", None, b"receipt"))

    thread = threading.Thread(target=commit)
    thread.start()
    deadline = time.monotonic() + 5
    while not frames:
        assert time.monotonic() < deadline, "the commit was never framed"
        time.sleep(0.01)

    header, payload = frames[0]
    assert header["type"] == "step_commit"
    assert header["step_key"] == "charge#0"
    assert header["payload_len"] == len(payload) == len(b"receipt")
    # No owner, ever: one an executor fills in is one it can forge.
    assert "owner" not in header

    relay.deliver({"type": "step_ack", "job_id": "job-1", "seq": 0, "ok": True})
    thread.join(timeout=5)
    assert answered == [{"type": "step_ack", "job_id": "job-1", "seq": 0, "ok": True}]


def test_a_commit_in_flight_when_the_stream_ends_is_released() -> None:
    """A dead pool must read as a disconnect, not as a backstop wait."""
    _, send = _sent()
    relay = StepRelay(send)
    answered: list[dict[str, Any]] = []

    thread = threading.Thread(
        target=lambda: answered.append(relay.commit("job-1", 0, "charge#0", "run", None, b"x"))
    )
    thread.start()
    time.sleep(0.05)
    relay.abandon()

    thread.join(timeout=5)
    assert not thread.is_alive(), "abandon must release a parked commit"
    assert answered[0]["ok"] is False
    assert answered[0]["failure"] == "retryable", "nothing confirmed the write landed"


def test_a_commit_made_after_the_stream_ends_refuses_at_once() -> None:
    """The job thread runs on past the reader's exit, and may take another step.

    Without the latch that commit registers a waiter nothing can settle — the
    reader has already returned — and parks for the whole 120s backstop. A job
    with an execution timeout is reaped by the watchdog; one without is not.
    """
    _, send = _sent()
    relay = StepRelay(send)
    relay.abandon()

    started = time.monotonic()
    answer = relay.commit("job-1", 0, "charge#0", "run", None, b"x")
    elapsed = time.monotonic() - started

    # Relative to the backstop, not an absolute budget: that is what makes the
    # assertion fail when the latch is removed rather than when CI is slow.
    assert elapsed < _ACK_BACKSTOP_S / 10, (
        f"the commit parked for {elapsed:.1f}s instead of refusing at once"
    )
    assert answer["ok"] is False
    assert answer["failure"] == "retryable"
    assert "ended" in answer["error"]


def test_an_ack_nobody_waits_on_is_dropped() -> None:
    """A duplicate, or one for a commit that already gave up. Neither is a fault."""
    _, send = _sent()
    relay = StepRelay(send)

    relay.deliver({"type": "step_ack", "job_id": "job-1", "seq": 0, "ok": True})
    # Malformed ones too: the commit stays durable either way, and raising here
    # would take the child's stdin reader down with it.
    relay.deliver({"type": "step_ack"})
    relay.deliver({"type": "step_ack", "job_id": "job-1", "seq": "0", "ok": True})


def test_a_broken_writer_refuses_without_parking() -> None:
    """Nothing was sent, so the replay re-runs the step under the same key."""

    def send(header: dict[str, Any], payload: bytes) -> None:
        raise BrokenPipeError("the pool is gone")

    answer = StepRelay(send).commit("job-1", 0, "charge#0", "run", None, b"x")
    assert answer["ok"] is False
    assert answer["failure"] == "retryable"
    assert "could not be sent" in answer["error"]
