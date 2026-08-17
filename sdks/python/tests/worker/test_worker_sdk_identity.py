"""A registered worker reports which SDK and release it runs.

In a polyglot deployment the registry is the only place an operator can tell a
stale worker from a current one without going host by host.
"""

from __future__ import annotations

import threading
from typing import Any

from flexiq import Queue, __version__


def test_worker_reports_its_sdk_and_version(tmp_path: Any, poll_until: Any) -> None:
    queue = Queue(db_path=str(tmp_path / "q.db"), workers=1)

    @queue.task()
    def noop() -> None:
        pass

    thread = threading.Thread(target=queue.run_worker, daemon=True)
    thread.start()

    try:
        poll_until(lambda: bool(queue.workers()), timeout=10, message="worker did not register")
        worker: dict[str, Any] = queue.workers()[0]

        assert worker["sdk"] == "python"
        # Compared against the installed release rather than a literal, so the
        # assertion survives a version bump.
        assert worker["sdk_version"] == __version__
    finally:
        queue._inner.request_shutdown()
        thread.join(timeout=10)
