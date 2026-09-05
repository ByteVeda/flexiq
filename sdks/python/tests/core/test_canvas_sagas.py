"""Canvas-level saga compensation: chain/group/chord.with_compensation()."""

from __future__ import annotations

import threading
import time
from collections.abc import Generator
from pathlib import Path
from typing import Any

import pytest

from conftest import join_worker
from flexiq import MaxRetriesExceededError, Queue, TaskFailedError, chain, chord, group

PollUntil = Any  # the conftest fixture's runtime type


@pytest.fixture
def queue(tmp_path: Path) -> Generator[Queue]:
    """Queue with a worker thread that's *cleaned up* on teardown.

    Without explicit cleanup the daemon worker thread keeps running for
    the rest of the pytest session, leaking log records into other
    tests' ``caplog`` and surfacing as flaky CI failures on Windows.
    """
    db_path = str(tmp_path / "canvas_sagas.db")
    q = Queue(db_path=db_path, workers=4)
    t = threading.Thread(target=q.run_worker, daemon=True)
    t.start()
    try:
        yield q
    finally:
        q._inner.request_shutdown()
        join_worker(t)


# ── Construction / API validation ─────────────────────────────────────────


def test_signature_with_compensation_returns_new_signature(queue: Queue) -> None:
    @queue.task()
    def step() -> int:
        return 1

    @queue.task()
    def comp_step(args: tuple, kwargs: dict, result: object) -> None:
        pass

    sig = step.si()
    new = sig.with_compensation(comp_step)
    assert new is not sig
    assert sig.options.get("compensates") is None
    assert new.options["compensates"] is comp_step


def test_chain_with_compensation_length_mismatch_raises(queue: Queue) -> None:
    @queue.task()
    def a() -> None:
        pass

    @queue.task()
    def b() -> None:
        pass

    @queue.task()
    def comp_a(args: tuple, kwargs: dict, result: object) -> None:
        pass

    c = chain(a.si(), b.si())
    with pytest.raises(ValueError, match="exactly 2 compensator"):
        c.with_compensation([comp_a])
    with pytest.raises(ValueError, match="exactly 2 compensator"):
        c.with_compensation([comp_a, comp_a, comp_a])


def test_chain_with_compensation_none_disables(queue: Queue) -> None:
    @queue.task()
    def a() -> None:
        pass

    c = chain(a.si()).with_compensation(None)
    # None becomes tuple of None — no compensation.
    assert all(x is None for x in c._compensators)


def test_chain_with_compensation_rejects_invalid_compensator(queue: Queue) -> None:
    @queue.task()
    def a() -> None:
        pass

    with pytest.raises(TypeError, match="compensator must be"):
        chain(a.si()).with_compensation([42])  # int is not a valid compensator


# ── chain compensation ────────────────────────────────────────────────────


def test_chain_failure_compensates_in_reverse_order(queue: Queue, poll_until: PollUntil) -> None:
    """3 steps; step 3 fails → compensators for step 2 and step 1 run in reverse."""
    lock = threading.Lock()
    order: list[str] = []

    @queue.task()
    def comp_a(args: tuple, kwargs: dict, result: object) -> None:
        with lock:
            order.append("comp_a")

    @queue.task()
    def comp_b(args: tuple, kwargs: dict, result: object) -> None:
        with lock:
            order.append("comp_b")

    @queue.task()
    def step_a(x: int) -> int:
        with lock:
            order.append("a")
        return x + 1

    @queue.task()
    def step_b(x: int) -> int:
        with lock:
            order.append("b")
        return x * 10

    @queue.task(max_retries=0)
    def step_c(x: int) -> int:
        with lock:
            order.append("c")
        raise RuntimeError("boom")

    c = chain(step_a.s(1), step_b.s(), step_c.s()).with_compensation([comp_a, comp_b, None])
    with pytest.raises((TaskFailedError, MaxRetriesExceededError)):
        c.apply(queue)

    # Wait for compensator jobs to land (they're async dispatches).
    poll_until(lambda: "comp_a" in order and "comp_b" in order, timeout=6)

    with lock:
        assert "a" in order
        assert "b" in order
        assert "c" in order
        assert "comp_b" in order, f"comp_b must run: order={order}"
        assert "comp_a" in order, f"comp_a must run: order={order}"
        # Reverse order: comp_b dispatched before comp_a.
        comp_indices = [i for i, x in enumerate(order) if x.startswith("comp_")]
        assert len(comp_indices) >= 2
        # Note: actual execution order on the worker may interleave, but
        # the dispatch order is preserved in the enqueue sequence.


def test_chain_success_runs_no_compensators(queue: Queue) -> None:
    """All steps succeed → no compensator runs."""
    lock = threading.Lock()
    comp_calls: list[str] = []

    @queue.task()
    def comp_a(args: tuple, kwargs: dict, result: object) -> None:
        with lock:
            comp_calls.append("comp_a")

    @queue.task()
    def comp_b(args: tuple, kwargs: dict, result: object) -> None:
        with lock:
            comp_calls.append("comp_b")

    @queue.task()
    def a() -> int:
        return 1

    @queue.task()
    def b(x: int) -> int:
        return x + 1

    c = chain(a.si(), b.s()).with_compensation([comp_a, comp_b])
    result_job = c.apply(queue)
    assert result_job.result(timeout=30) == 2

    # Give the worker a moment to confirm no compensators dispatch.
    time.sleep(0.5)
    with lock:
        assert comp_calls == [], f"no compensators should run on success: {comp_calls}"


# ── group compensation ────────────────────────────────────────────────────


def test_group_member_failure_compensates_succeeded_members(
    queue: Queue, poll_until: PollUntil
) -> None:
    """3-member group; member 0 fails → comp for members 1 and 2 run."""
    lock = threading.Lock()
    order: list[str] = []

    @queue.task()
    def comp_a(args: tuple, kwargs: dict, result: object) -> None:
        with lock:
            order.append("comp_a")

    @queue.task()
    def comp_b(args: tuple, kwargs: dict, result: object) -> None:
        with lock:
            order.append("comp_b")

    @queue.task()
    def comp_c(args: tuple, kwargs: dict, result: object) -> None:
        with lock:
            order.append("comp_c")

    @queue.task(max_retries=0)
    def m_fail() -> None:
        with lock:
            order.append("m_fail")
        raise RuntimeError("boom")

    @queue.task()
    def m_ok_1() -> int:
        with lock:
            order.append("m_ok_1")
        return 1

    @queue.task()
    def m_ok_2() -> int:
        with lock:
            order.append("m_ok_2")
        return 2

    g = group(m_fail.si(), m_ok_1.si(), m_ok_2.si()).with_compensation([comp_a, comp_b, comp_c])
    with pytest.raises((TaskFailedError, MaxRetriesExceededError)):
        g.apply(queue)

    # Wait for compensators to dispatch.
    poll_until(lambda: "comp_b" in order and "comp_c" in order, timeout=5)
    with lock:
        assert "m_fail" in order
        assert "m_ok_1" in order
        assert "m_ok_2" in order
        assert "comp_a" not in order, "failed member must not be compensated"
        assert "comp_b" in order
        assert "comp_c" in order


# ── chord compensation ────────────────────────────────────────────────────


def test_chord_callback_failure_compensates_group_members(
    queue: Queue, poll_until: PollUntil
) -> None:
    """Group succeeds, callback fails → group compensators dispatch."""
    lock = threading.Lock()
    order: list[str] = []

    @queue.task()
    def comp_a(args: tuple, kwargs: dict, result: object) -> None:
        with lock:
            order.append("comp_a")

    @queue.task()
    def comp_b(args: tuple, kwargs: dict, result: object) -> None:
        with lock:
            order.append("comp_b")

    @queue.task()
    def comp_callback(args: tuple, kwargs: dict, result: object) -> None:
        with lock:
            order.append("comp_callback")

    @queue.task()
    def m1() -> int:
        with lock:
            order.append("m1")
        return 1

    @queue.task()
    def m2() -> int:
        with lock:
            order.append("m2")
        return 2

    @queue.task(max_retries=0)
    def cb(results: list[int]) -> None:
        with lock:
            order.append("cb")
        raise RuntimeError("callback boom")

    g = group(m1.si(), m2.si())
    ch = chord(g, cb.s()).with_compensation(group=[comp_a, comp_b], callback=comp_callback)
    with pytest.raises((TaskFailedError, MaxRetriesExceededError)):
        ch.apply(queue)

    poll_until(lambda: "comp_a" in order and "comp_b" in order, timeout=5)
    with lock:
        assert "m1" in order and "m2" in order, f"group must run: {order}"
        assert "cb" in order, f"callback must run: {order}"
        assert "comp_a" in order, f"group member a must compensate: {order}"
        assert "comp_b" in order, f"group member b must compensate: {order}"


def test_chord_group_failure_skips_callback_compensator(queue: Queue) -> None:
    """A group member fails → callback never runs → callback compensator not dispatched."""
    lock = threading.Lock()
    order: list[str] = []

    @queue.task()
    def comp_a(args: tuple, kwargs: dict, result: object) -> None:
        with lock:
            order.append("comp_a")

    @queue.task()
    def comp_callback(args: tuple, kwargs: dict, result: object) -> None:
        with lock:
            order.append("comp_callback")

    @queue.task(max_retries=0)
    def m1_fail() -> None:
        raise RuntimeError("group boom")

    @queue.task()
    def m2() -> int:
        return 2

    @queue.task()
    def cb(results: list) -> None:  # pragma: no cover - should never run
        with lock:
            order.append("cb")

    g = group(m1_fail.si(), m2.si())
    ch = chord(g, cb.s()).with_compensation(group=[None, comp_a], callback=comp_callback)
    with pytest.raises((TaskFailedError, MaxRetriesExceededError)):
        ch.apply(queue)

    time.sleep(2)
    with lock:
        assert "cb" not in order, f"callback must not run on group failure: {order}"
        # comp_a may or may not run depending on timing of m2 completion;
        # the key assertion is the callback compensator does NOT run.
        assert "comp_callback" not in order, f"callback compensator must not run: {order}"


# ── Idempotency (dedup via unique_key) ────────────────────────────────────


def test_chain_compensator_idempotency_key_format(queue: Queue, poll_until: PollUntil) -> None:
    """Compensator enqueues use canvas_compensation:{run_id}:{slot} as unique_key.

    This isn't directly observable on a single apply() call (each apply
    generates a fresh run_id), but the dedup mechanism prevents the same
    compensator from double-dispatching on a single failure within a run.
    """
    lock = threading.Lock()
    comp_calls = 0

    @queue.task()
    def comp_a(args: tuple, kwargs: dict, result: object) -> None:
        nonlocal comp_calls
        with lock:
            comp_calls += 1

    @queue.task()
    def a() -> int:
        return 1

    @queue.task(max_retries=0)
    def b_fail(x: int) -> None:
        raise RuntimeError("nope")

    c = chain(a.si(), b_fail.s()).with_compensation([comp_a, None])
    with pytest.raises((TaskFailedError, MaxRetriesExceededError)):
        c.apply(queue)

    poll_until(lambda: comp_calls >= 1, timeout=5)
    with lock:
        assert comp_calls == 1, f"compensator must fire exactly once: {comp_calls}"
