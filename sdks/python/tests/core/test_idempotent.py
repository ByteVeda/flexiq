"""Tests for auto-derived idempotency on @queue.task(idempotent=True)."""

from __future__ import annotations

import threading

from flexiq import Queue


def test_idempotent_task_dedupes_by_args(queue: Queue) -> None:
    """Two enqueues with identical args under idempotent=True share a job."""

    @queue.task(idempotent=True)
    def charge(customer_id: int, amount: int) -> int:
        return amount

    job1 = charge.delay(42, 1000)
    job2 = charge.delay(42, 1000)

    assert job1.id == job2.id


def test_idempotent_task_distinct_args_distinct_jobs(queue: Queue) -> None:
    """Different arguments produce different auto-keys → different jobs."""

    @queue.task(idempotent=True)
    def charge(customer_id: int, amount: int) -> int:
        return amount

    job_a = charge.delay(1, 100)
    job_b = charge.delay(2, 100)
    job_c = charge.delay(1, 200)

    assert len({job_a.id, job_b.id, job_c.id}) == 3


def test_idempotent_per_call_key_overrides_auto(queue: Queue) -> None:
    """An explicit idempotency_key wins over the auto-derived key."""

    @queue.task(idempotent=True)
    def process(data: str) -> str:
        return data

    auto_job = process.delay("payload")
    explicit_job = process.apply_async(args=("payload",), idempotency_key="custom-key")

    # Auto-key and explicit-key collide on the same args only by coincidence —
    # since the explicit key is "custom-key", they should be different jobs.
    assert auto_job.id != explicit_job.id


def test_idempotent_per_call_disable_creates_new_job(queue: Queue) -> None:
    """idempotent=False on apply_async overrides the per-task default."""

    @queue.task(idempotent=True)
    def process(data: str) -> str:
        return data

    auto_job = process.delay("same-args")
    forced_new = process.apply_async(args=("same-args",), idempotent=False)

    assert auto_job.id != forced_new.id


def test_non_idempotent_task_allows_duplicates(queue: Queue) -> None:
    """Without idempotent=True, identical calls produce distinct jobs."""

    @queue.task()
    def process(data: str) -> str:
        return data

    job1 = process.delay("payload")
    job2 = process.delay("payload")

    assert job1.id != job2.id


def test_idempotent_clears_after_completion(queue: Queue) -> None:
    """After an idempotent job finishes, the next call creates a new job."""

    @queue.task(idempotent=True)
    def fast() -> str:
        return "done"

    job1 = fast.delay()

    worker = threading.Thread(target=queue.run_worker, daemon=True)
    worker.start()

    job1.result(timeout=10)

    job2 = fast.delay()
    assert job2.id != job1.id


def test_idempotent_via_enqueue_kwarg(queue: Queue) -> None:
    """Per-call idempotent=True works without a registered task default."""

    @queue.task()
    def process(data: str) -> str:
        return data

    job1 = process.apply_async(args=("x",), idempotent=True)
    job2 = process.apply_async(args=("x",), idempotent=True)
    job3 = process.apply_async(args=("y",), idempotent=True)

    assert job1.id == job2.id
    assert job1.id != job3.id


def test_idempotent_unique_key_takes_precedence(queue: Queue) -> None:
    """An explicit unique_key beats both auto-derivation and idempotency_key."""

    @queue.task(idempotent=True)
    def process(data: str) -> str:
        return data

    explicit = process.apply_async(args=("payload",), unique_key="explicit-uk")
    auto = process.delay("payload")

    # The auto key is "auto:<sha256...>" and the explicit one is "explicit-uk",
    # so they must differ.
    assert explicit.id != auto.id
