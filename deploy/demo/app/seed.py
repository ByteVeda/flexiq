"""Seed the demo with a job in every state the dashboard shows.

Runs once, before the server starts, against the SQLite file the server then
opens. The executor moves each job into its state: ``hello`` succeeds,
``flaky`` keeps retrying, ``doomed`` is dead-lettered, ``nap`` parks in
``step.sleep`` and the release workflow waits at its approval gate.
"""

from __future__ import annotations

import os

from demo_app import build, doomed, flaky, hello, nap, queue, ship

from flexiq.workflows import Workflow

# The server image runs as distroless `nonroot`; this container runs as root
# so it can hand the volume over once the file exists.
NONROOT = 65532

# The server reads task overrides when it starts, which is after this runs. Its
# retry wait is jittered up to five minutes; a high base keeps it near the cap.
FLAKY_BACKOFF_SECS = 300


def seed() -> None:
    queue.set_task_override("demo.flaky", retry_backoff=FLAKY_BACKOFF_SECS)

    for _ in range(5):
        hello.delay()
    flaky.delay()
    doomed.delay()
    nap.delay()

    release = Workflow(name="release")
    release.step("build", build)
    release.gate("approve", after="build", timeout=86_400)
    release.step("ship", ship, after="approve")
    queue.submit_workflow(release)


def hand_over(directory: str) -> None:
    for root, _, files in os.walk(directory):
        os.chown(root, NONROOT, NONROOT)
        for name in files:
            os.chown(os.path.join(root, name), NONROOT, NONROOT)


if __name__ == "__main__":
    seed()
    hand_over(os.path.dirname(os.environ.get("FLEXIQ_DEMO_DB", "/data/flexiq.db")))
    print("seeded")
