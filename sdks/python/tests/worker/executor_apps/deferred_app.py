"""An app whose task module is imported below the ``Queue`` it builds.

The order is the point: constructing the queue claims nothing, because the
declarations only happen on the import further down.
"""

from __future__ import annotations

import os

from flexiq import Queue

queue = Queue(db_path=os.environ.get("FLEXIQ_EXECUTOR_TEST_DB", "/tmp/flexiq-executor.db"))


@queue.task(max_retries=0)
def bound() -> str:
    """Declared on the queue, so it is claimed as it is decorated."""
    return "bound"


import deferred_tasks  # type: ignore[import-not-found]  # noqa: E402,F401
