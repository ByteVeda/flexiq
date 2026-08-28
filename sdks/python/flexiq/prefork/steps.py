"""Framing a child's durable-step commits to the pool that spawned it.

A child under an attached executor holds neither storage nor the scheduler
connection, so a step commit travels two hops: this frame to the pool, and the
pool's own to the scheduler that owns the database. The answer comes back the
same way, and the task is blocked on it — an unconfirmed commit is
indistinguishable from one that never happened.

Unlike progress and task logs, none of this is fire-and-forget. A dropped log
line is a missing line; a dropped step commit is a charge made twice.
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from typing import Any

from flexiq.worker_protocol import ProtocolError

__all__ = ["StepRelay"]

#: Backstop on how long a commit waits, in seconds.
#:
#: Not the normal bound. The pool answers every commit exactly once, and a pool
#: that died closes this child's stdin — which releases every waiter as a
#: disconnect. A job with an execution timeout is killed by the pool's watchdog
#: long before this fires. It exists so a pool that is *broken* rather than gone
#: still ends the attempt instead of parking it forever.
_ACK_BACKSTOP_S = 120.0

#: What a commit answers with when no ack reached it.
#:
#: Retryable, and honestly so: nothing confirmed the write landed, and the
#: replay re-runs the step under the same downstream idempotency key.
_RETRYABLE = "retryable"

#: Refusal for a commit the frame stream can no longer carry — one caught
#: mid-flight when the reader ended, and one made after it did.
_ENDED = (
    "the connection to the executor ended before step '{step_key}' of "
    "job {job_id} was acknowledged"
)


class _Waiter:
    """One commit's parking spot, and where its answer lands."""

    __slots__ = ("answer", "arrived")

    def __init__(self) -> None:
        self.arrived = threading.Event()
        self.answer: dict[str, Any] | None = None

    def settle(self, answer: dict[str, Any]) -> None:
        self.answer = answer
        self.arrived.set()


class StepRelay:
    """Sends this child's step commits to its pool and waits for each ack.

    ``send`` writes one frame — the same serialized writer the rest of the
    child's frames go through, so a commit can never interleave with a result.
    """

    def __init__(self, send: Callable[[dict[str, Any], bytes], None]) -> None:
        self._send = send
        self._lock = threading.Lock()
        self._waiters: dict[tuple[str, int], _Waiter] = {}
        # Latched once the frame stream has ended. Without it a commit made
        # *after* the reader returned would register a waiter nothing can ever
        # settle and park for the whole backstop — reachable, because the job
        # thread runs on past the reader's exit and may take another step.
        self._over = False

    def commit(
        self,
        job_id: str,
        seq: int,
        step_key: str,
        kind: str,
        wake_at: int | None,
        result: bytes,
    ) -> dict[str, Any]:
        """Frame one commit and block until the pool answers it.

        Always returns an ack, never raises: the caller is a step store, and a
        refusal carrying a reason is what it knows how to fail an attempt with.
        """
        waiter = _Waiter()
        key = (job_id, seq)
        # Registered before the frame goes out, or a fast pool could answer
        # before there is anything to answer to.
        with self._lock:
            if self._over:
                return _refused(_ENDED.format(step_key=step_key, job_id=job_id))
            self._waiters[key] = waiter

        header: dict[str, Any] = {
            "type": "step_commit",
            "job_id": job_id,
            "seq": seq,
            "step_key": step_key,
            "kind": kind,
            "payload_len": len(result),
        }
        if wake_at is not None:
            header["wake_at"] = wake_at

        try:
            self._send(header, result)
        except (OSError, EOFError, ValueError, ProtocolError) as broken:
            self._forget(key)
            return _refused(f"step '{step_key}' of job {job_id} could not be sent: {broken}")

        # Released while parked, so the thread reading acks off the pipe can run
        # and settle this waiter.
        if not waiter.arrived.wait(_ACK_BACKSTOP_S):
            self._forget(key)
            return _refused(
                f"the executor did not acknowledge step '{step_key}' of job {job_id} "
                f"within {_ACK_BACKSTOP_S:.0f}s"
            )

        answer = waiter.answer
        if answer is None:
            return _refused(_ENDED.format(step_key=step_key, job_id=job_id))
        return answer

    def deliver(self, ack: dict[str, Any]) -> None:
        """Hand one ack to whoever is blocked on it.

        An ack nothing is waiting on is a duplicate, or one for a commit that
        already gave up. The attempt has moved on either way, and the commit
        stays durable — so it is dropped rather than reported.
        """
        job_id = ack.get("job_id")
        seq = ack.get("seq")
        if not isinstance(job_id, str) or not isinstance(seq, int):
            return
        waiter = self._forget((job_id, seq))
        if waiter is not None:
            waiter.settle(ack)

    def abandon(self) -> None:
        """Release everyone blocked on an ack, because none is coming.

        Called when the frame stream ends. Waking each waiter with no answer
        turns a dead pool into a disconnect rather than a full backstop wait.
        """
        with self._lock:
            self._over = True
            waiters = list(self._waiters.values())
            self._waiters.clear()
        for waiter in waiters:
            waiter.arrived.set()

    def _forget(self, key: tuple[str, int]) -> _Waiter | None:
        with self._lock:
            return self._waiters.pop(key, None)


def _refused(message: str) -> dict[str, Any]:
    """An ack this side produced, shaped exactly like one off the wire."""
    return {"ok": False, "already": False, "error": message, "failure": _RETRYABLE}
