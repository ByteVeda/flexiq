"""Worker-dispatch predicate gating tests."""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Any

import pytest

from conftest import join_worker
from flexiq.app import Queue
from flexiq.exceptions import PredicateRejectedError
from flexiq.predicates import (
    Cancel,
    Defer,
    Predicate,
    PredicateContext,
)


@dataclass(frozen=True)
class _Const(Predicate):
    value: Any

    def evaluate(self, ctx: PredicateContext) -> Any:
        return self.value


class _DispatchOnly(Predicate):
    """Allow enqueue (job_id None); apply ``dispatch_value`` at dispatch."""

    def __init__(self, dispatch_value: Any) -> None:
        self.dispatch_value = dispatch_value
        self.dispatch_calls = 0

    def evaluate(self, ctx: PredicateContext) -> Any:
        if ctx.job_id is None:
            return True
        self.dispatch_calls += 1
        return self.dispatch_value


def _start_worker(queue: Queue) -> threading.Thread:
    thread = threading.Thread(target=queue.run_worker, daemon=True)
    thread.start()
    return thread


def _stop_worker(queue: Queue, thread: threading.Thread) -> None:
    queue._inner.request_shutdown()
    join_worker(thread)


def _wait_for_status(queue: Queue, job_id: str, statuses: set[str], timeout: float = 8.0) -> str:
    deadline = time.monotonic() + timeout
    last_status = ""
    while time.monotonic() < deadline:
        job = queue._inner.get_job(job_id)
        if job is not None:
            last_status = job.status
            if last_status in statuses:
                return last_status
        time.sleep(0.05)
    return last_status


def _wait_for_completed_task(queue: Queue, task_name: str, timeout: float = 10.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        jobs = queue._inner.list_jobs(status="complete", task_name=task_name)
        if jobs:
            return True
        time.sleep(0.1)
    return False


def test_dispatch_predicate_allows_normal_run(queue: Queue) -> None:
    @queue.task(predicate=_Const(True))
    def t() -> str:
        return "ran"

    job = t.delay()
    thread = _start_worker(queue)
    try:
        assert _wait_for_status(queue, job.id, {"complete"}) == "complete"
    finally:
        _stop_worker(queue, thread)


def test_dispatch_predicate_cancel_terminates_job(queue: Queue) -> None:
    @queue.task(predicate=_DispatchOnly(Cancel(reason="blocked")))
    def t() -> str:
        return "ran"

    job = t.delay()
    thread = _start_worker(queue)
    try:
        assert _wait_for_status(queue, job.id, {"cancelled"}) == "cancelled"
    finally:
        _stop_worker(queue, thread)


def test_dispatch_predicate_false_with_cancel_action(queue: Queue) -> None:
    @queue.task(predicate=_DispatchOnly(False), on_false="cancel")
    def t() -> str:
        return "ran"

    job = t.delay()
    thread = _start_worker(queue)
    try:
        assert _wait_for_status(queue, job.id, {"cancelled"}) == "cancelled"
    finally:
        _stop_worker(queue, thread)


def test_dispatch_predicate_defer_reenqueues(queue: Queue) -> None:
    # First dispatch -> Defer(seconds=1); second dispatch -> True.
    # The original job is cancelled and a new one is enqueued with 1s delay.
    state = {"calls": 0}

    class _Pred(Predicate):
        def evaluate(self, ctx: PredicateContext) -> Any:
            if ctx.job_id is None:
                return True  # allow enqueue
            state["calls"] += 1
            if state["calls"] == 1:
                return Defer(seconds=1.0)
            return True

    @queue.task(predicate=_Pred())
    def t() -> str:
        return "ran"

    t.delay()
    thread = _start_worker(queue)
    try:
        assert _wait_for_completed_task(queue, t.name, timeout=15.0), (
            "deferred job never completed after re-enqueue"
        )
        # Verify the predicate was called at least twice (defer + run)
        assert state["calls"] >= 2
    finally:
        _stop_worker(queue, thread)


def test_dispatch_predicate_fail_closed_on_exception_at_dispatch(
    queue: Queue,
) -> None:
    class _DispatchBoom(Predicate):
        def evaluate(self, ctx: PredicateContext) -> bool:
            if ctx.job_id is None:
                return True  # let enqueue through
            raise RuntimeError("boom")

    @queue.task(predicate=_DispatchBoom(), on_false="cancel")
    def t() -> str:
        return "ran"

    job = t.delay()
    thread = _start_worker(queue)
    try:
        # Fail-closed → False → on_false=cancel → cancelled
        assert _wait_for_status(queue, job.id, {"cancelled"}) == "cancelled"
    finally:
        _stop_worker(queue, thread)


def test_dispatch_metrics_record_outcome(queue: Queue) -> None:
    @queue.task(predicate=_DispatchOnly(Cancel(reason="x")))
    def t() -> str:
        return "ran"

    job = t.delay()
    thread = _start_worker(queue)
    try:
        _wait_for_status(queue, job.id, {"cancelled"})
    finally:
        _stop_worker(queue, thread)
    snap = queue._predicate_metrics.snapshot()
    # allowed=1 (enqueue), cancelled=1 (dispatch)
    assert snap["allowed"] >= 1
    assert snap["cancelled"] >= 1


def test_enqueue_time_cancel_still_raises_for_dispatch_predicate(queue: Queue) -> None:
    # If predicate cancels at enqueue (regardless of dispatch behavior),
    # the enqueue raises and no job is created.
    @queue.task(predicate=_Const(Cancel(reason="nope")))
    def t() -> str:
        return "ran"

    with pytest.raises(PredicateRejectedError):
        t.delay()
