"""Tests for namespace-based routing and isolation."""

import threading
from pathlib import Path

from flexiq import Queue


def test_namespace_enqueue_sets_namespace(tmp_path: Path) -> None:
    """Jobs enqueued on a namespaced Queue carry the namespace."""
    queue = Queue(db_path=str(tmp_path / "test.db"), namespace="team-a")

    @queue.task()
    def add(x: int, y: int) -> int:
        return x + y

    job = add.delay(1, 2)
    py_job = queue._inner.get_job(job.id)
    assert py_job is not None
    assert py_job.namespace == "team-a"


def test_no_namespace_jobs_have_none(tmp_path: Path) -> None:
    """Jobs enqueued without a namespace have namespace=None."""
    queue = Queue(db_path=str(tmp_path / "test.db"))

    @queue.task()
    def noop() -> None:
        pass

    job = noop.delay()
    py_job = queue._inner.get_job(job.id)
    assert py_job is not None
    assert py_job.namespace is None


def test_namespace_isolation_worker(tmp_path: Path) -> None:
    """A namespaced worker only processes jobs from its namespace."""
    db = str(tmp_path / "test.db")

    # Create two queues sharing the same DB but different namespaces
    q_a = Queue(db_path=db, namespace="team-a")
    q_b = Queue(db_path=db, namespace="team-b")

    results: list[str] = []

    @q_a.task()
    def task_a() -> str:
        results.append("a")
        return "a"

    @q_b.task()
    def task_b() -> str:
        results.append("b")
        return "b"

    # Enqueue one job on each namespace
    job_a = task_a.delay()
    job_b = task_b.delay()

    # Run worker for team-a only
    worker = threading.Thread(target=q_a.run_worker, daemon=True)
    worker.start()

    # Wait for team-a's job to complete
    job_a.result(timeout=10)

    # team-b's job should still be pending
    job_b.refresh()
    assert job_b.status == "pending"

    # Shut down team-a worker (daemon thread exits on its own)
    q_a._inner.request_shutdown()


def test_namespace_list_jobs_scoped(tmp_path: Path) -> None:
    """list_jobs defaults to the queue's namespace."""
    db = str(tmp_path / "test.db")
    q_a = Queue(db_path=db, namespace="ns-a")
    q_b = Queue(db_path=db, namespace="ns-b")

    @q_a.task()
    def task_x() -> None:
        pass

    @q_b.task()
    def task_y() -> None:
        pass

    task_x.delay()
    task_x.delay()
    task_y.delay()

    # Each queue sees only its own jobs by default
    assert len(q_a.list_jobs()) == 2
    assert len(q_b.list_jobs()) == 1

    # Passing namespace=None shows all
    assert len(q_a.list_jobs(namespace=None)) == 3


def test_namespace_preserved_in_job_result(tmp_path: Path) -> None:
    """JobResult.to_dict() includes the namespace."""
    queue = Queue(db_path=str(tmp_path / "test.db"), namespace="my-ns")

    @queue.task()
    def greet(name: str) -> str:
        return f"hi {name}"

    job = greet.delay("world")

    worker = threading.Thread(target=queue.run_worker, daemon=True)
    worker.start()

    result = job.result(timeout=10)
    assert result == "hi world"

    queue._inner.request_shutdown()


def test_namespace_scopes_the_id_addressed_surface(tmp_path: Path) -> None:
    """A job id from another namespace reads as missing and cannot be mutated.

    The namespace is a tenancy boundary, so a caller scoped to one must learn
    nothing about ids outside it — not through a read, and not through the
    effect of a write.
    """
    db = str(tmp_path / "test.db")
    q_a = Queue(db_path=db, namespace="ns-a")
    q_b = Queue(db_path=db, namespace="ns-b")
    unscoped = Queue(db_path=db)

    @q_a.task()
    def task_x() -> None:
        pass

    job = task_x.delay()

    assert q_a.get_job(job.id) is not None
    assert q_b.get_job(job.id) is None
    assert unscoped.get_job(job.id) is not None

    assert not q_b.cancel_job(job.id)
    assert not q_b.cancel_running_job(job.id)
    assert q_a.get_job(job.id) is not None, "the cross-namespace cancels must not have landed"

    assert q_b.job_errors(job.id) == []
    assert q_b.task_logs(job.id) == []

    assert q_a.cancel_job(job.id), "the owning namespace still cancels"
