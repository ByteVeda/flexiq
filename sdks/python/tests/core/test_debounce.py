"""Tests for the debounce options on @queue.task, enqueue, and aenqueue."""

from __future__ import annotations

import threading
import time

import pytest

from flexiq import Queue
from flexiq.debounce import parse_duration_ms
from flexiq.result import JobResult


def _timing(job_dict: dict) -> tuple[int, int]:
    """``(created_at, scheduled_at)`` for a job, in unix milliseconds."""
    return job_dict["created_at"], job_dict["scheduled_at"]


# ── Duration parsing ──────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("500ms", 500),
        ("5s", 5_000),
        ("2.5m", 150_000),
        ("1h", 3_600_000),
        ("1d", 86_400_000),
        ("  30M  ", 1_800_000),
        (5, 5_000),
        (0.25, 250),
    ],
)
def test_parse_duration_ms_accepts_strings_and_seconds(value: str | float, expected: int) -> None:
    assert parse_duration_ms(value, param="debounce") == expected


@pytest.mark.parametrize("value", ["5", "5 minutes", "", "-3s", "m"])
def test_parse_duration_ms_rejects_unusable_strings(value: str) -> None:
    """A bare number in a string has no agreed unit, so it is refused."""
    with pytest.raises(ValueError, match="debounce"):
        parse_duration_ms(value, param="debounce")


def test_parse_duration_ms_rejects_non_positive_numbers() -> None:
    with pytest.raises(ValueError, match="positive"):
        parse_duration_ms(0, param="debounce")


@pytest.mark.parametrize("value", [float("inf"), float("-inf"), float("nan"), 1e308])
def test_parse_duration_ms_rejects_non_finite_durations(value: float) -> None:
    """``round`` would raise OverflowError on these; the contract is ValueError."""
    with pytest.raises(ValueError, match="finite"):
        parse_duration_ms(value, param="debounce")


def test_parse_duration_ms_rejects_durations_storage_cannot_hold() -> None:
    """Past i64 milliseconds the value has no representation to travel in."""
    with pytest.raises(ValueError, match="ceiling"):
        parse_duration_ms("999999999999999999999999d", param="debounce")


def test_parse_duration_ms_rejects_wrong_types() -> None:
    with pytest.raises(TypeError, match="duration string"):
        parse_duration_ms(None, param="debounce")  # type: ignore[arg-type]


# ── Coalescing ────────────────────────────────────────────────────────


def test_burst_collapses_into_one_job(queue: Queue) -> None:
    """Repeated submissions on one key slide a single pending job."""

    @queue.task(debounce="5m", debounce_key="report:{user_id}", debounce_max_wait="30m")
    def build_report(user_id: str) -> str:
        return user_id

    jobs = [build_report.delay("u1") for _ in range(5)]

    assert len({job.id for job in jobs}) == 1
    assert queue.stats()["pending"] == 1


def test_distinct_keys_stay_independent(queue: Queue) -> None:
    """Two users debounce separately — the key is payload-derived."""

    @queue.task(debounce="5m", debounce_key="report:{user_id}", debounce_max_wait="30m")
    def build_report(user_id: str) -> str:
        return user_id

    a1 = build_report.delay("u1")
    b1 = build_report.delay("u2")
    a2 = build_report.delay("u1")
    b2 = build_report.delay("u2")

    assert a1.id == a2.id
    assert b1.id == b2.id
    assert a1.id != b1.id
    assert queue.stats()["pending"] == 2


def test_key_resolves_from_keyword_arguments(queue: Queue) -> None:
    """A parameter is addressable by name however the caller passed it."""

    @queue.task(debounce="5m", debounce_key="report:{user_id}", debounce_max_wait="30m")
    def build_report(user_id: str) -> str:
        return user_id

    positional = build_report.delay("u1")
    keyword = build_report.delay(user_id="u1")

    assert positional.id == keyword.id


def test_window_slides_the_deadline_forward(queue: Queue) -> None:
    """A second submission inside the window pushes the run further out."""

    @queue.task(debounce="1s", debounce_key="slide:{name}", debounce_max_wait="1h")
    def rebuild(name: str) -> str:
        return name

    first = rebuild.delay("index")
    _, first_deadline = _timing(first.to_dict())

    time.sleep(0.15)
    second = rebuild.delay("index")
    _, second_deadline = _timing(second.to_dict())

    assert second.id == first.id
    assert second_deadline > first_deadline


def test_max_wait_caps_the_slide(queue: Queue) -> None:
    """The deadline never moves past ``first_seen + max_wait``.

    ``max_wait`` equal to the window is the tightest legal setting, so the cap
    binds on the very first slide and the assertion needs no sleep long enough
    to race the scheduler.
    """

    @queue.task(debounce="1s", debounce_key="cap:{name}", debounce_max_wait="1s")
    def rebuild(name: str) -> str:
        return name

    first = rebuild.delay("index")
    created_at, first_deadline = _timing(first.to_dict())
    assert first_deadline == created_at + 1_000

    time.sleep(0.2)
    second = rebuild.delay("index")
    _, capped_deadline = _timing(second.to_dict())

    assert second.id == first.id
    # Uncapped, the second call would have scheduled ~200ms further out.
    assert capped_deadline == created_at + 1_000


def test_payload_is_kept_unless_replacement_is_requested(queue: Queue) -> None:
    """A repeat submission is a vote to run again, not a redefinition."""

    @queue.task(debounce="5m", debounce_key="notify:{user_id}", debounce_max_wait="30m")
    def notify(user_id: str, body: str) -> str:
        return body

    first = notify.delay("u1", "first")
    notify.delay("u1", "second")

    args, kwargs = queue._deserialize_payload(notify.name, first._py_job.payload_bytes)
    assert args == ("u1", "first")
    assert kwargs == {}


def test_replace_payload_takes_the_newest_arguments(queue: Queue) -> None:
    @queue.task(
        debounce="5m",
        debounce_key="notify:{user_id}",
        debounce_max_wait="30m",
        debounce_replace_payload=True,
    )
    def notify(user_id: str, body: str) -> str:
        return body

    first = notify.delay("u1", "first")
    notify.delay("u1", "second")

    refreshed = queue._inner.get_job(first.id)
    assert refreshed is not None
    args, _kwargs = queue._deserialize_payload(notify.name, refreshed.payload_bytes)
    assert args == ("u1", "second")


def test_debounced_job_runs_once(queue: Queue) -> None:
    """End to end: a burst produces exactly one execution."""
    runs: list[str] = []

    @queue.task(debounce="0.2s", debounce_key="run:{name}", debounce_max_wait="1s")
    def record(name: str) -> str:
        runs.append(name)
        return name

    job = record.delay("a")
    for _ in range(4):
        record.delay("a")

    # Started after the burst: the whole point is that five submissions leave
    # one job behind, and a worker racing the burst would not prove it.
    worker = threading.Thread(target=queue.run_worker, daemon=True)
    worker.start()
    try:
        assert job.result(timeout=10) == "a"
        assert runs == ["a"]
        assert queue.stats()["pending"] == 0
    finally:
        queue.shutdown()
        worker.join(timeout=5)


# ── Key template resolution ───────────────────────────────────────────


def test_unresolvable_placeholder_raises_at_enqueue(queue: Queue) -> None:
    """A key naming something the call lacks is an error, not a global key."""

    @queue.task(debounce="5m", debounce_key="report:{tenant}", debounce_max_wait="30m")
    def build_report(user_id: str) -> str:
        return user_id

    with pytest.raises(ValueError, match="does not provide"):
        build_report.delay("u1")

    assert queue.stats()["pending"] == 0


def test_key_template_mismatching_the_signature_raises(queue: Queue) -> None:
    @queue.task(debounce="5m", debounce_key="report:{user_id}", debounce_max_wait="30m")
    def build_report(user_id: str) -> str:
        return user_id

    with pytest.raises(ValueError, match="does not match the task signature"):
        build_report.apply_async(kwargs={"nope": 1})


def test_positional_placeholder_resolves(queue: Queue) -> None:
    @queue.task(debounce="5m", debounce_key="report:{0}", debounce_max_wait="30m")
    def build_report(user_id: str) -> str:
        return user_id

    assert build_report.delay("u1").id == build_report.delay("u1").id


def test_literal_key_debounces_the_whole_task(queue: Queue) -> None:
    """A template with no placeholder is a deliberate queue-wide window."""

    @queue.task(debounce="5m", debounce_key="rebuild-index", debounce_max_wait="30m")
    def rebuild(shard: int) -> int:
        return shard

    assert rebuild.delay(1).id == rebuild.delay(2).id


def test_key_resolving_to_empty_raises(queue: Queue) -> None:
    @queue.task(debounce="5m", debounce_key="{user_id}", debounce_max_wait="30m")
    def build_report(user_id: str) -> str:
        return user_id

    with pytest.raises(ValueError, match="empty key"):
        build_report.delay("")


# ── Validation ────────────────────────────────────────────────────────


def test_debounce_without_max_wait_is_refused(queue: Queue) -> None:
    """An unbounded debounce starves the job, so it fails loudly."""
    with pytest.raises(ValueError, match="debounce_max_wait"):

        @queue.task(debounce="5m", debounce_key="k:{a}")
        def build(a: int) -> int:
            return a


def test_debounce_without_key_is_refused(queue: Queue) -> None:
    with pytest.raises(ValueError, match="debounce_key"):

        @queue.task(debounce="5m", debounce_max_wait="30m")
        def build(a: int) -> int:
            return a


def test_max_wait_shorter_than_window_is_refused(queue: Queue) -> None:
    with pytest.raises(ValueError, match="at least as long as"):

        @queue.task(debounce="5m", debounce_key="k:{a}", debounce_max_wait="1m")
        def build(a: int) -> int:
            return a


def test_debounce_key_without_a_window_is_refused(queue: Queue) -> None:
    with pytest.raises(ValueError, match="requires debounce="):

        @queue.task(debounce_key="k:{a}")
        def build(a: int) -> int:
            return a


def test_debounce_with_idempotent_is_refused(queue: Queue) -> None:
    with pytest.raises(ValueError, match="incompatible with idempotent"):

        @queue.task(
            debounce="5m",
            debounce_key="k:{a}",
            debounce_max_wait="30m",
            idempotent=True,
        )
        def build(a: int) -> int:
            return a


def test_debounce_with_batch_is_refused(queue: Queue) -> None:
    with pytest.raises(ValueError, match="incompatible with batch"):

        @queue.task(debounce="5m", debounce_key="k:{a}", debounce_max_wait="30m", batch=True)
        def build(a: list[int]) -> int:
            return len(a)


def test_per_call_dedup_key_with_debounce_is_refused(queue: Queue) -> None:
    @queue.task(debounce="5m", debounce_key="k:{a}", debounce_max_wait="30m")
    def build(a: int) -> int:
        return a

    with pytest.raises(ValueError, match="dedup key with debounce"):
        build.apply_async(args=(1,), idempotency_key="explicit")


def test_a_lone_debounce_field_is_refused_at_the_binding(queue: Queue) -> None:
    """The low-level binding refuses them too, not just ``normalize_debounce``.

    On its own either field is a window the caller thinks they configured and
    did not, so neither may fall through to a plain insert — which would drop
    the admission cap silently.
    """
    with pytest.raises(ValueError, match="debounce_replace_payload and debounce_max_pending"):
        queue._inner.enqueue(task_name="t", payload=b"x", debounce_replace_payload=True)

    with pytest.raises(ValueError, match="debounce_replace_payload and debounce_max_pending"):
        queue._inner.enqueue(task_name="t", payload=b"x", debounce_max_pending=5)


def test_delay_with_debounce_is_refused(queue: Queue) -> None:
    """The window owns the deadline, so a delay would be silently dropped."""

    @queue.task(debounce="5m", debounce_key="k:{a}", debounce_max_wait="30m")
    def build(a: int) -> int:
        return a

    with pytest.raises(ValueError, match="combine delay with debounce"):
        build.apply_async(args=(1,), delay=30)


def test_batch_enqueue_of_a_debounced_task_is_refused(queue: Queue) -> None:
    """``.map()`` has no per-item window, so it refuses rather than bypasses."""

    @queue.task(debounce="5m", debounce_key="k:{a}", debounce_max_wait="30m")
    def build(a: int) -> int:
        return a

    with pytest.raises(ValueError, match="batch-enqueue debounced task"):
        build.map([(1,), (2,)])


def test_re_registering_without_debounce_clears_the_window(queue: Queue) -> None:
    """Re-registering a name replaces the task, window included."""

    @queue.task(name="rebuild", debounce="5m", debounce_key="k", debounce_max_wait="30m")
    def first(a: int) -> int:
        return a

    @queue.task(name="rebuild")
    def second(a: int) -> int:
        return a

    assert second.delay(1).id != second.delay(1).id


# ── Imperative and async forms ────────────────────────────────────────


def test_imperative_enqueue_debounces(queue: Queue) -> None:
    @queue.task()
    def build_report(user_id: str) -> str:
        return user_id

    def submit(user_id: str) -> JobResult:
        return queue.enqueue(
            build_report.name,
            args=(user_id,),
            debounce="5m",
            debounce_key="report:{user_id}",
            debounce_max_wait="30m",
        )

    first = submit("u1")
    second = submit("u1")
    other = submit("u2")

    assert first.id == second.id
    assert other.id != first.id


def test_per_call_options_override_the_registered_window(queue: Queue) -> None:
    """Per-call debounce options replace the task's window as a set."""

    @queue.task(debounce="5m", debounce_key="report:{user_id}", debounce_max_wait="30m")
    def build_report(user_id: str) -> str:
        return user_id

    task_window = build_report.delay("u1")
    call_window = build_report.apply_async(
        args=("u1",),
        debounce="1s",
        debounce_key="other:{user_id}",
        debounce_max_wait="1h",
    )

    assert call_window.id != task_window.id


def test_per_call_partial_options_are_refused(queue: Queue) -> None:
    """A per-call window is validated on its own, not merged with the task's."""

    @queue.task(debounce="5m", debounce_key="report:{user_id}", debounce_max_wait="30m")
    def build_report(user_id: str) -> str:
        return user_id

    with pytest.raises(ValueError, match="debounce_max_wait"):
        build_report.apply_async(args=("u1",), debounce="1s")


async def test_aenqueue_debounces(queue: Queue) -> None:
    @queue.task()
    def build_report(user_id: str) -> str:
        return user_id

    async def submit() -> JobResult:
        return await queue.aenqueue(
            task_name=build_report.name,
            args=("u1",),
            debounce="5m",
            debounce_key="report:{user_id}",
            debounce_max_wait="30m",
        )

    first = await submit()
    second = await submit()

    assert first.id == second.id
    assert queue.stats()["pending"] == 1
