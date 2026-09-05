"""A registered worker reports a fingerprint of the tasks it can run.

Deferred registration builds the registry by importing a tree, so a worker that
imported part of it looks healthy and dead-letters every job for the rest. The
fingerprint on the registry row is what makes that worker visible without going
host by host.
"""

from __future__ import annotations

import threading
from typing import Any

from conftest import join_worker
from flexiq import Queue

# The value `crates/flexiq-core/BINDING_CONTRACT.md` pins for this task set.
# Hard-coded rather than recomputed here: a test that reimplemented the hash
# would agree with any drift in it, and the reason the constant matters is that
# a Python worker and an executor written in another SDK have to produce the
# same string for the same registry.
INVOICES_AND_REPORTS = "fafd30ef8ebcb7de"


def test_worker_reports_its_task_registry_fingerprint(tmp_path: Any, poll_until: Any) -> None:
    queue = Queue(db_path=str(tmp_path / "q.db"), workers=1)

    @queue.task(name="invoices.send")
    def send_invoice() -> None:
        pass

    @queue.task(name="reports.build")
    def build_report() -> None:
        pass

    thread = threading.Thread(target=queue.run_worker, daemon=True)
    thread.start()

    try:
        poll_until(lambda: bool(queue.workers()), timeout=10, message="worker did not register")
        worker: dict[str, Any] = queue.workers()[0]

        assert worker["registry_fingerprint"] == INVOICES_AND_REPORTS
    finally:
        queue._inner.request_shutdown()
        join_worker(thread)


def test_registration_order_does_not_change_the_fingerprint(
    tmp_path: Any, poll_until: Any
) -> None:
    """The same tasks declared in the other order are the same registry.

    Decorator order follows import order, which discovery decides — so a
    fingerprint that depended on it would report divergence on every worker
    that happened to import its modules differently.
    """
    queue = Queue(db_path=str(tmp_path / "q.db"), workers=1)

    @queue.task(name="reports.build")
    def build_report() -> None:
        pass

    @queue.task(name="invoices.send")
    def send_invoice() -> None:
        pass

    thread = threading.Thread(target=queue.run_worker, daemon=True)
    thread.start()

    try:
        poll_until(lambda: bool(queue.workers()), timeout=10, message="worker did not register")

        assert queue.workers()[0]["registry_fingerprint"] == INVOICES_AND_REPORTS
    finally:
        queue._inner.request_shutdown()
        join_worker(thread)
