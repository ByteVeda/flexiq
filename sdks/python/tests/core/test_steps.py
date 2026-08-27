"""Durable inline steps: ``current_job.step.run`` and ``current_job.step.sleep``.

Every test here runs a real worker against a real database, because the whole
point of a step is what survives a crash — an in-process stub would prove
nothing about the memo.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import subprocess
import sys
import threading
from collections.abc import Callable, Generator
from functools import partial
from pathlib import Path
from typing import Any

import pytest

from flexiq import Queue, _flexiq
from flexiq._active_context import _ActiveContext
from flexiq.codecs import GzipCodec
from flexiq.context import current_job
from flexiq.detached import DetachedNative
from flexiq.middleware import TaskMiddleware
from flexiq.steps import StepContext, StepError, StepUnavailableError

PollUntil = Any  # the conftest fixture's runtime type
WorkerFactory = Callable[..., threading.Thread]

# Only an ``async def`` task reaches the native executor, and only that path
# reports a sleep through ``try_report_slept``.
requires_native_async = pytest.mark.skipif(
    not hasattr(_flexiq, "PyResultSender"),
    reason="wheel built without the native-async feature",
)


@pytest.fixture
def start_worker() -> Generator[WorkerFactory]:
    """Start workers and stop every one of them at teardown.

    Leaking a worker thread between tests leaves it polling a deleted temp
    database, which shows up as unrelated "database is locked" noise in
    whichever test runs next.
    """
    started: list[tuple[Queue, threading.Thread]] = []

    def _start(queue: Queue, queues: list[str] | None = None) -> threading.Thread:
        thread = threading.Thread(target=partial(queue.run_worker, queues=queues), daemon=True)
        thread.start()
        started.append((queue, thread))
        return thread

    yield _start

    for queue, thread in started:
        queue.shutdown()
        thread.join(timeout=5)


# Runs in its own interpreter — see ``_query``. Emits JSON so blobs survive the
# hop as hex.
_READER = """
import json, sqlite3, sys
conn = sqlite3.connect(sys.argv[1])
rows = conn.execute(sys.argv[2]).fetchall()
conn.commit()
print(json.dumps(rows, default=lambda blob: blob.hex()))
"""


def _query(queue: Queue, sql: str) -> list[list[Any]]:
    """Run one statement against the queue's database, in a subprocess.

    Not in this process, on purpose. The worker's SQLite is the one Rust links;
    ``sqlite3`` here is the interpreter's own. Two SQLite libraries in one
    process do not share the WAL index, so an in-process reader sees a stale
    snapshot — this file's first version silently read an empty ``job_steps``
    while the worker was committing to it. A fresh interpreter has exactly one
    SQLite and sees what was committed.
    """
    out = subprocess.run(
        [sys.executable, "-c", _READER, queue._db_path, sql],
        capture_output=True,
        text=True,
        check=True,
    )
    rows: list[list[Any]] = json.loads(out.stdout)
    return rows


def _step_rows(queue: Queue) -> list[list[Any]]:
    """The job's committed step rows: job_id, seq, step_key, kind, result, wake_at.

    Read raw rather than through the SDK: the codec test has to assert on the
    bytes as stored, and anything that went back through the queue would decode
    them on the way out.
    """
    return _query(queue, "SELECT job_id, seq, step_key, kind, result, wake_at FROM job_steps")


# ---------------------------------------------------------------- memoization


def test_a_committed_step_is_not_re_run_after_a_retry(
    queue: Queue, start_worker: WorkerFactory
) -> None:
    """The whole feature: a retry replays the step instead of running it."""
    charges: list[str] = []
    attempts: list[int] = []

    @queue.task(max_retries=2, retry_backoff=0)
    def checkout() -> str:
        charge = current_job.step.run("charge", lambda: _charge(charges))
        attempts.append(1)
        if len(attempts) == 1:
            raise ValueError("crashed after the charge, before returning")
        return charge

    job = checkout.delay()
    start_worker(queue)

    assert job.result(timeout=30) == "ch_0"
    assert len(attempts) == 2, "the task should have run twice"
    assert charges == ["ch_0"], "the charge should have happened exactly once"


def test_a_memo_hit_returns_the_stored_value(queue: Queue, start_worker: WorkerFactory) -> None:
    """The replayed value is the committed one, not a freshly computed one."""
    counter = {"n": 0}
    seen: list[dict] = []

    @queue.task(max_retries=2, retry_backoff=0)
    def build() -> dict:
        counter["n"] += 1
        value = current_job.step.run("payload", lambda: {"n": counter["n"], "items": [1, 2]})
        seen.append(value)
        if len(seen) == 1:
            raise ValueError("retry me")
        return value

    job = build.delay()
    start_worker(queue)

    assert job.result(timeout=30) == {"n": 1, "items": [1, 2]}
    assert seen == [{"n": 1, "items": [1, 2]}, {"n": 1, "items": [1, 2]}]


def test_a_keyed_step_is_matched_wherever_it_sits(
    queue: Queue, start_worker: WorkerFactory
) -> None:
    """A keyed step survives a reorder — which is what ``key=`` exists for."""
    ran: list[str] = []
    attempts: list[int] = []

    @queue.task(max_retries=2, retry_backoff=0)
    def fan_out() -> list[str]:
        attempts.append(1)
        # The second attempt walks the same set in the opposite order, which is
        # what an unordered collection does in practice.
        order = ["a", "b"] if len(attempts) == 1 else ["b", "a"]
        results = [
            current_job.step.run("item", partial(_mark, ran, item), key=item) for item in order
        ]
        if len(attempts) == 1:
            raise ValueError("retry me")
        return sorted(results)

    job = fan_out.delay()
    start_worker(queue)

    assert job.result(timeout=30) == ["done:a", "done:b"]
    assert sorted(ran) == ["a", "b"], "each item should have run exactly once"


def test_a_step_needs_a_name(queue: Queue) -> None:
    """An unnamed step is refused, never inferred from the callable.

    And refused *permanently*: the name is in the code, so every later attempt
    rejects it in the same place. Raised as a step failure rather than the bare
    ``TypeError`` it once was, because that carried no verdict and the retry
    filters kept re-running it to the same end.
    """
    with queue.test_mode(propagate_errors=True):

        @queue.task()
        def unnamed() -> None:
            current_job.step.run("", lambda: None)

        with pytest.raises(StepError, match="a step needs a name") as refused:
            unnamed.delay()

    assert refused.value.flexiq_should_retry is False


def test_an_unparseable_sleep_is_refused_permanently(queue: Queue) -> None:
    """A duration the grammar rejects ends the attempt without spending a retry."""
    with queue.test_mode(propagate_errors=True):

        @queue.task()
        def naps() -> None:
            current_job.step.sleep("next tuesday", name="nap")

        with pytest.raises(StepError, match="is not a duration") as refused:
            naps.delay()

    assert refused.value.flexiq_should_retry is False


# ---------------------------------------------------------- idempotency keys


def test_the_idempotency_key_is_stable_across_a_retry(
    queue: Queue, start_worker: WorkerFactory
) -> None:
    """Same run, same step, same key — that is what closes the crash window."""
    keys: list[str] = []
    attempts: list[int] = []

    @queue.task(max_retries=2, retry_backoff=0)
    def charge() -> str:
        attempts.append(1)
        # A fresh step name each attempt would produce a fresh key, so the
        # first step is deliberately re-issued under the same name and its key
        # recorded from inside the body.
        return current_job.step.run(
            f"charge{len(attempts)}",
            lambda: _record_key(keys),
        )

    job = charge.delay()
    start_worker(queue)

    assert job.result(timeout=30)
    assert len(keys) == 1

    run_key, step_key = keys[0].split(":", 1)
    assert run_key == job.id
    assert step_key == "charge1#0"


def test_the_key_is_only_readable_inside_a_step(queue: Queue) -> None:
    """Outside a step body there is no step for the key to name."""
    with queue.test_mode(propagate_errors=True):

        @queue.task()
        def early() -> None:
            _ = current_job.step.idempotency_key

        with pytest.raises(RuntimeError, match="only readable inside a step body"):
            early.delay()


# --------------------------------------------------------------- divergence


def test_a_diverged_sequence_dead_letters_without_retrying(
    queue: Queue, start_worker: WorkerFactory, poll_until: PollUntil
) -> None:
    """Changing the step sequence mid-retry fails loudly rather than lying.

    A wrong memoized result is worse than a re-run, so the divergence is
    permanent: it burns no retry budget reproducing an error the code will
    keep making.
    """
    attempts: list[int] = []

    @queue.task(max_retries=5, retry_backoff=0)
    def shifting() -> str:
        attempts.append(1)
        # Attempt 1 records `first#0`; attempt 2 asks for `second#0` at the
        # same position, which is exactly the mid-retry deploy §3 exists for.
        name = "first" if len(attempts) == 1 else "second"
        value = current_job.step.run(name, lambda: "v")
        raise ValueError(f"retry me after {value}")

    shifting.delay()
    start_worker(queue)

    poll_until(
        lambda: len(queue.dead_letters()) >= 1,
        timeout=20,
        message="a diverged step should have dead-lettered",
    )
    assert len(attempts) == 2, "a divergence must not be retried"
    assert "second#0" in queue.dead_letters()[0]["error"]


# -------------------------------------------------------------------- sleep


def test_sleep_ends_the_attempt_and_replays_the_earlier_steps(
    queue: Queue, start_worker: WorkerFactory
) -> None:
    """A sleep unwinds the body; the wake replays it with every step memoized."""
    prepared: list[str] = []
    bodies: list[int] = []

    @queue.task(max_retries=0)
    def deferred() -> str:
        bodies.append(1)
        value = current_job.step.run("prepare", lambda: _mark(prepared, "once"))
        current_job.step.sleep("200ms", name="cool_off")
        return f"{value}/{len(bodies)}"

    job = deferred.delay()
    start_worker(queue)

    assert job.result(timeout=30) == "done:once/2"
    assert prepared == ["once"], "the step before the sleep must not run twice"


def test_a_sleep_costs_no_retry(queue: Queue, start_worker: WorkerFactory) -> None:
    """A sleep is not a failure: nothing about the retry accounting moves."""

    @queue.task(max_retries=0, retry_backoff=0)
    def naps() -> str:
        current_job.step.sleep("150ms", name="nap")
        return "awake"

    job = naps.delay()
    start_worker(queue)

    assert job.result(timeout=30) == "awake"
    finished = queue.get_job(job.id)
    assert finished is not None
    assert finished.to_dict()["retry_count"] == 0


def test_a_replayed_sleep_keeps_its_first_deadline(
    queue: Queue, start_worker: WorkerFactory, poll_until: PollUntil
) -> None:
    """The first commit fixes the deadline; re-issuing the sleep never moves it.

    A binding that recomputed ``now + duration`` on each replay would push the
    deadline a full duration further out every time an attempt came back to it
    — a sleep that outlives the job, produced by the recovery path itself. The
    replay is forced by pulling the sleeping job's schedule forward, which is
    what a reclaim or an operator requeue does.
    """

    bodies: list[int] = []

    @queue.task(max_retries=0)
    def holds() -> str:
        bodies.append(1)
        current_job.step.sleep("30s", name="hold")
        return "through"

    job = holds.delay()
    start_worker(queue)

    poll_until(
        lambda: _sleep_deadline(queue) is not None,
        timeout=20,
        message="the job never slept",
    )
    first = _sleep_deadline(queue)

    _pull_forward(queue, job.id)
    poll_until(
        lambda: len(bodies) == 2,
        timeout=20,
        message="the pulled-forward job never ran again",
    )
    poll_until(
        lambda: _scheduled_at(queue, job.id) == first,
        timeout=20,
        message="the job was not rescheduled to its stored deadline",
    )

    assert _sleep_deadline(queue) == first, "the stored deadline must stand on a replay"


# ------------------------------------------------------------ swallow layers


def test_a_bare_except_does_not_catch_a_sleep(queue: Queue, start_worker: WorkerFactory) -> None:
    """``except Exception`` misses a control signal, like KeyboardInterrupt."""
    caught: list[str] = []

    @queue.task(max_retries=0)
    def guarded() -> str:
        try:
            current_job.step.sleep("100ms", name="nap")
        except Exception as exc:
            caught.append(type(exc).__name__)
        return "returned"

    job = guarded.delay()
    start_worker(queue)

    assert job.result(timeout=30) == "returned"
    assert caught == [], "a bare `except Exception` must not see the sleep"


def test_swallowing_a_divergence_fails_the_attempt(
    queue: Queue, start_worker: WorkerFactory, poll_until: PollUntil
) -> None:
    """The latch, in the case where it is the only defence.

    A swallowed divergence leaves the attempt holding its claim, so nothing
    downstream would question the value it goes on to return — the runner has
    to refuse it here. Permanent, like the divergence it hid.
    """
    attempts: list[int] = []

    @queue.task(max_retries=5, retry_backoff=0)
    def swallows() -> str:
        attempts.append(1)
        name = "first" if len(attempts) == 1 else "second"
        with contextlib.suppress(BaseException):
            current_job.step.run(name, lambda: "v")
        if len(attempts) == 1:
            raise ValueError("retry me")
        return "should not be reported"

    swallows.delay()
    start_worker(queue)

    poll_until(
        lambda: len(queue.dead_letters()) >= 1,
        timeout=20,
        message="a swallowed divergence should have failed the attempt",
    )
    assert "swallowed" in queue.dead_letters()[0]["error"]
    assert len(attempts) == 2, "a swallowed divergence must not be retried"


def test_swallowing_a_sleep_loses_the_attempt_but_not_the_job(
    queue: Queue, start_worker: WorkerFactory
) -> None:
    """A swallowed sleep costs the attempt; the job still wakes and finishes.

    The latch fails the attempt, but by then the sleep has committed and
    released the claim — so the scheduler's own ``(owner, attempt)`` fence sees
    a job that has moved on and drops the failure rather than dead-lettering a
    job that is sleeping correctly. On wake the sleep is a memo hit, no signal
    is raised, and the body completes under a claim it actually holds.
    """
    bodies: list[int] = []

    @queue.task(max_retries=0)
    def swallows() -> str:
        bodies.append(1)
        with contextlib.suppress(BaseException):
            current_job.step.sleep("150ms", name="nap")
        return f"finished on pass {len(bodies)}"

    job = swallows.delay()
    start_worker(queue)

    assert job.result(timeout=30) == "finished on pass 2"
    assert queue.dead_letters() == []


# ------------------------------------------------------------- serialization


def test_step_results_go_through_the_queue_codec(
    tmp_path: Path, start_worker: WorkerFactory, poll_until: PollUntil
) -> None:
    """A codec on the queue reaches ``job_steps`` with no extra plumbing.

    Asserted on the raw stored row rather than on what comes back out: the
    round trip would pass even if the bytes were written in plaintext.
    """
    queue = Queue(
        db_path=str(tmp_path / "codec.db"),
        workers=1,
        codec=GzipCodec(),
    )
    marker = "plaintext-canary-" + "x" * 400

    # The sleep is what keeps the row readable: a job's step rows are deleted
    # inside the transaction that ends it, so a completed job leaves nothing to
    # inspect. Sleeping parks the job Pending with its steps intact.
    @queue.task(max_retries=0)
    def stores() -> str:
        value = current_job.step.run("blob", lambda: marker)
        current_job.step.sleep("30s", name="hold")
        return value

    stores.delay()
    start_worker(queue)

    poll_until(
        lambda: any(row[2] == "blob#0" for row in _step_rows(queue)),
        timeout=20,
        message="the step was never committed",
    )
    rows = [row for row in _step_rows(queue) if row[2] == "blob#0"]
    assert len(rows) == 1
    stored = bytes.fromhex(rows[0][4])
    assert marker.encode() not in stored, "the codec must have encoded the step blob"
    assert len(stored) < len(marker), "gzip should have shrunk a 400-byte run of x"


# --------------------------------------------------------------- middleware


def test_a_sleep_calls_on_sleep_rather_than_after(
    tmp_path: Path, start_worker: WorkerFactory
) -> None:
    """Every ``before`` is matched by exactly one of ``after`` / ``on_sleep``.

    ``after(ctx, None, None)`` would be indistinguishable from "the task
    returned None", which is how a sleep silently becomes a success in a
    tracing span or a metrics counter.
    """
    events: list[str] = []

    class Recorder(TaskMiddleware):
        def before(self, ctx: Any) -> None:
            events.append("before")

        def after(self, ctx: Any, result: Any, error: Exception | None) -> None:
            events.append("after")

        def on_sleep(self, ctx: Any, wake_at: int) -> None:
            events.append(f"on_sleep:{wake_at > 0}")

    queue = Queue(db_path=str(tmp_path / "hooks.db"), workers=1, middleware=[Recorder()])

    @queue.task(max_retries=0)
    def naps() -> str:
        current_job.step.sleep("150ms", name="nap")
        return "awake"

    job = naps.delay()
    start_worker(queue)

    assert job.result(timeout=30) == "awake"
    assert events == ["before", "on_sleep:True", "before", "after"]


# ------------------------------------------------------- claims per worker


def test_each_worker_fences_on_its_own_claim(queue: Queue, start_worker: WorkerFactory) -> None:
    """Two workers off one ``Queue`` both commit their steps.

    The owner half of the ``(owner, attempt)`` fence belongs to the worker that
    won the claim. Held on the queue handle instead, the second ``run_worker``
    overwrote the first's id, every step the first worker went on to commit was
    refused as superseded, and its jobs dead-lettered instead of running.
    """
    ran: list[str] = []

    @queue.task(queue="alpha", max_retries=0)
    def on_alpha() -> str:
        return current_job.step.run("work", partial(_mark, ran, "alpha"))

    @queue.task(queue="beta", max_retries=0)
    def on_beta() -> str:
        return current_job.step.run("work", partial(_mark, ran, "beta"))

    # Both started before either job is enqueued, so whichever worker a shared
    # slot stranded would be holding a claim it no longer names.
    start_worker(queue, queues=["alpha"])
    start_worker(queue, queues=["beta"])

    first, second = on_alpha.delay(), on_beta.delay()

    assert first.result(timeout=30) == "done:alpha"
    assert second.result(timeout=30) == "done:beta"
    assert sorted(ran) == ["alpha", "beta"]
    assert queue.dead_letters() == []


# ------------------------------------------------------------------ refusal


def test_steps_refuse_without_an_execution_claim(queue: Queue) -> None:
    """No claim, no fence — so the step refuses instead of running un-memoized.

    A context carries the step handle of the worker that dispatched the job;
    nothing dispatched this one, so there is no claim to fence a commit on and
    a silently lost memo is worse than a failure naming the reason.
    """
    ctx = _ActiveContext(job_id="j", task_name="t", retry_count=0, queue_name="default")

    with pytest.raises(StepUnavailableError, match="execution claim") as refusal:
        StepContext(ctx, queue).run("charge", lambda: "x")

    assert refusal.value.flexiq_should_retry, "the next attempt may land on a worker that can"


def test_steps_refuse_on_an_attached_executor(queue: Queue) -> None:
    """An executor holds no claim of its own, so steps stop there.

    Its queue is storage-free, and nothing in the step path touches it: the
    refusal is a step failure naming the reason, never an ``AttributeError``
    escaping the detached stand-in, and the task body cannot catch it away.
    """
    queue._inner = DetachedNative()  # type: ignore[assignment]
    ctx = _ActiveContext(job_id="j", task_name="t", retry_count=0, queue_name="default")

    with pytest.raises(StepUnavailableError, match="attached executor"):
        StepContext(ctx, queue).run("charge", lambda: "x")


# ------------------------------------------------------------------- inline


def test_test_mode_runs_steps_without_memoizing(queue: Queue) -> None:
    """``test_mode`` has no job row, so a step runs and a sleep is a no-op."""
    ran: list[str] = []

    with queue.test_mode() as tq:

        @queue.task()
        def flow() -> str:
            value = current_job.step.run("work", lambda: _mark(ran, "x"))
            current_job.step.sleep("1h", name="nap")
            return value

        flow.delay()

    assert ran == ["x"]
    assert tq[0].return_value == "done:x"


# ------------------------------------------------------------------- async


def test_arun_awaits_a_coroutine_step(queue: Queue) -> None:
    """The async twin awaits an awaitable body and returns what it resolved to.

    Driven synchronously: test mode runs the coroutine itself, and nesting that
    inside an already-running loop is what the async worker path exists for.
    """
    ran: list[str] = []

    with queue.test_mode() as tq:

        @queue.task()
        async def flow() -> str:
            return await current_job.step.arun("work", lambda: _async_mark(ran))

        flow.delay()

    assert ran == ["async"]
    assert tq[0].return_value == "done:async"


@requires_native_async
def test_an_async_task_sleeps_on_the_native_executor(
    queue: Queue, start_worker: WorkerFactory
) -> None:
    """The native-async path reports a slept attempt on its own channel.

    A sync task on this pool still runs on a blocking thread, so only an
    ``async def`` reaches ``AsyncTaskExecutor`` — and only it exercises
    ``try_report_slept``, the one sleep-reporting path the other tests miss.
    """
    bodies: list[int] = []

    @queue.task(max_retries=0)
    async def naps() -> str:
        bodies.append(1)
        await current_job.step.asleep("150ms", name="nap")
        return f"awake on pass {len(bodies)}"

    job = naps.delay()
    start_worker(queue)

    assert job.result(timeout=30) == "awake on pass 2"


@requires_native_async
def test_gathered_steps_are_refused_rather_than_interleaved(
    queue: Queue, start_worker: WorkerFactory, poll_until: PollUntil
) -> None:
    """Two steps at once have no position to take, so the attempt fails.

    A step's identity *is* its place in the sequence, so the core refuses a
    second `begin_run` while one is uncommitted — before the second closure
    runs, and before it could read a key. Pinned because the obvious "fix" for
    the shared current-key slot is a ContextVar, which would make the key look
    right for a mode the sequence cannot support.
    """
    keys: list[str] = []

    async def body(tag: str) -> str:
        await asyncio.sleep(0.05)
        keys.append(current_job.step.idempotency_key)
        return tag

    @queue.task(max_retries=0)
    async def gathered() -> str:
        # Bound before the gather so each infers its own step type rather than
        # being solved against the gather's expected one.
        a = current_job.step.arun("a", lambda: body("a"))
        b = current_job.step.arun("b", lambda: body("b"))
        first, second = await asyncio.gather(a, b)
        return f"{first}{second}"

    gathered.delay()
    start_worker(queue)

    poll_until(
        lambda: len(queue.dead_letters()) >= 1,
        timeout=20,
        message="gathered steps should have failed the attempt",
    )
    assert "still uncommitted" in queue.dead_letters()[0]["error"]
    # `gather` leaves the surviving coroutine running when its sibling raises,
    # and this one sleeps before reading its key — so the dead letter lands
    # first. Wait for the read, or the assertion below passes on an empty list.
    poll_until(
        lambda: bool(keys),
        timeout=20,
        message="the surviving step body never read its key",
    )
    assert all(key.endswith(":a#0") for key in keys), keys


def test_test_mode_refuses_gathered_steps_too(queue: Queue) -> None:
    """Test mode holds the one-at-a-time rule, so a test fails what a worker fails.

    Inline steps have no session, so nothing in the core sees them — without a
    guard here, two gathered bodies overwrite each other's key and the test
    passes for code that dead-letters in production.
    """
    keys: list[str] = []

    async def body(tag: str) -> str:
        await asyncio.sleep(0.05)
        keys.append(current_job.step.idempotency_key)
        return tag

    with queue.test_mode(propagate_errors=True):

        @queue.task()
        async def gathered() -> str:
            a = current_job.step.arun("a", lambda: body("a"))
            b = current_job.step.arun("b", lambda: body("b"))
            first, second = await asyncio.gather(a, b)
            return f"{first}{second}"

        with pytest.raises(StepError, match="still uncommitted"):
            gathered.delay()

    # `asyncio.run` cancels the surviving task on its way out, so `keys` is
    # normally empty — but emptiness alone would also be what a cancelled
    # *corrupted* read looks like. Assert the invariant instead: whatever was
    # read belonged to the step that read it.
    assert all(key.endswith(":a#0") for key in keys), keys


@pytest.mark.parametrize(
    ("key", "message"),
    [
        ("", "empty key"),
        ("k" * 600, "over the"),
    ],
)
def test_test_mode_refuses_the_keys_a_worker_refuses(queue: Queue, key: str, message: str) -> None:
    """Inline steps derive their identity through the core, so the rules match.

    `key=""` used to fall back to numbering by occurrence here while a worker
    raised — a test passing for a key the real run rejects.
    """
    with queue.test_mode(propagate_errors=True):

        @queue.task()
        def keyed() -> str:
            return current_job.step.run("charge", lambda: "x", key=key)

        with pytest.raises(StepError, match=message):
            keyed.delay()


def test_a_worker_refuses_an_empty_key_the_same_way(
    queue: Queue, start_worker: WorkerFactory, poll_until: PollUntil
) -> None:
    """The other half of the pair above: same rule, same message, real session."""

    @queue.task(max_retries=0)
    def keyed() -> str:
        return current_job.step.run("charge", lambda: "x", key="")

    keyed.delay()
    start_worker(queue)

    poll_until(
        lambda: len(queue.dead_letters()) >= 1,
        timeout=20,
        message="an empty key should have failed the attempt",
    )
    assert "empty key" in queue.dead_letters()[0]["error"]


def test_a_refused_inline_step_does_not_spend_its_occurrence(queue: Queue) -> None:
    """A step the guard refuses must not move the next one's key.

    The occurrence counter is what an unkeyed step's identity is built from, so
    a refused call that took a number would shift every later one — and the
    whole point of the downstream key is that it does not move. Driven
    re-entrantly rather than through ``gather`` so the ordering is exact.
    """
    ctx = _ActiveContext(job_id="j", task_name="t", retry_count=0, queue_name="default")
    step = StepContext(ctx, queue)
    keys: list[str] = []

    with queue.test_mode():

        def outer() -> str:
            # Refused: `a#0` is still in flight. It derives `a#1` on the way to
            # being refused, which is exactly the number it must not keep.
            with pytest.raises(StepError, match="still uncommitted"):
                step.run("a", lambda: "inner")
            return "outer"

        assert step.run("a", outer) == "outer"
        step.run("a", lambda: keys.append(step.idempotency_key))

    assert keys == ["j:a#1"], "the refused step spent an occurrence it never used"


# ------------------------------------------------------------------ helpers


def _charge(charges: list[str]) -> str:
    charge_id = f"ch_{len(charges)}"
    charges.append(charge_id)
    return charge_id


def _mark(seen: list[Any], item: str) -> str:
    seen.append(item)
    return f"done:{item}"


async def _async_mark(seen: list[Any]) -> str:
    seen.append("async")
    return "done:async"


def _record_key(keys: list[str]) -> str:
    keys.append(current_job.step.idempotency_key)
    return "charged"


def _sleep_deadline(queue: Queue) -> int | None:
    """The deadline on the job's committed sleep row, if it has one."""
    rows = [row for row in _step_rows(queue) if row[3] == "sleep"]
    return int(rows[0][5]) if rows else None


def _scheduled_at(queue: Queue, job_id: str) -> int:
    rows = _query(queue, f"SELECT scheduled_at FROM jobs WHERE id = '{job_id}'")
    return int(rows[0][0]) if rows else -1


def _pull_forward(queue: Queue, job_id: str) -> None:
    """Make a sleeping job eligible now, as a reclaim or a requeue would.

    ``requeue_job`` cannot do it: a sleeping job is Pending, and requeue only
    releases a *Running* claim. Moving ``scheduled_at`` is the same observable
    change without waiting out a real deadline.
    """
    _query(queue, f"UPDATE jobs SET scheduled_at = 0 WHERE id = '{job_id}'")
