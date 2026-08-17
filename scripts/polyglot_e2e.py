#!/usr/bin/env python3
"""Run examples/polyglot end to end and assert the pipeline drained.

Three runtimes share one jobs table and one CBOR wire format, so any cross-SDK
break — a payload that no longer decodes, a result the producer cannot read —
shows up here as a job that never completes or one that lands in the dead-letter
queue. Assertions read the shared database rather than worker stdout: a log line
proves a worker printed something, not that the payload survived the hop.

Requires the three SDKs already built from this working tree (see the "Running
against a local build" section of the example's README) and this interpreter to
be the one the Python SDK is installed into.

    python scripts/polyglot_e2e.py --orders 3
"""

from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import tempfile
import time
from contextlib import ExitStack
from pathlib import Path

from flexiq import Queue
from flexiq.result import JobResult
from flexiq.serializers import CborSerializer

REPO_ROOT = Path(__file__).resolve().parent.parent
EXAMPLE_DIR = REPO_ROOT / "examples" / "polyglot"
JAVA_WORKER_DIR = EXAMPLE_DIR / "java-worker"
NODE_WORKER_DIR = EXAMPLE_DIR / "node-worker"

PROCESS_TASK = "orders.process"
NOTIFY_TASK = "orders.notify"

# How long a worker gets to shut down on SIGTERM before it is killed outright.
STOP_GRACE_SECONDS = 10


class PipelineError(RuntimeError):
    """The pipeline did not drain — the SDKs disagree or a worker died."""


def build_java_worker() -> Path:
    """Install the Java worker and return its start script.

    The README documents `./gradlew run`, which is right for a human at a
    terminal. Here we install first and launch the script directly: Gradle's
    JavaExec child does not die with the Gradle process, so an unattended run
    would leave a JVM holding the database.
    """
    subprocess.run(
        ["./gradlew", "installDist", "--no-daemon", "-q"],
        cwd=JAVA_WORKER_DIR,
        check=True,
    )
    start_script = (
        JAVA_WORKER_DIR
        / "build"
        / "install"
        / "flexiq-polyglot-java-worker"
        / "bin"
        / "flexiq-polyglot-java-worker"
    )
    if not start_script.is_file():
        raise PipelineError(f"gradle installDist produced no start script at {start_script}")
    return start_script


class Worker:
    """A worker subprocess and the log file its output is captured to."""

    def __init__(self, name: str, argv: list[str], cwd: Path, db: Path, log_path: Path):
        self.name = name
        self.log_path = log_path
        self._log = log_path.open("w")
        env = {**os.environ, "FLEXIQ_DB": str(db)}
        try:
            # Own session so the whole tree (Gradle's JVM, node's threads) can be
            # signalled as one group at teardown.
            self._process = subprocess.Popen(
                argv,
                cwd=cwd,
                env=env,
                stdout=self._log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        except OSError:
            self._log.close()
            raise

    def __enter__(self) -> Worker:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.stop()

    def died(self) -> bool:
        return self._process.poll() is not None

    def stop(self) -> None:
        if self._process.poll() is None:
            os.killpg(os.getpgid(self._process.pid), signal.SIGTERM)
            try:
                self._process.wait(timeout=STOP_GRACE_SECONDS)
            except subprocess.TimeoutExpired:
                os.killpg(os.getpgid(self._process.pid), signal.SIGKILL)
                self._process.wait()
        self._log.close()

    def tail(self, lines: int = 40) -> str:
        return "\n".join(self.log_path.read_text().splitlines()[-lines:])


def start_workers(
    stack: ExitStack, db: Path, java_start_script: Path, workdir: Path
) -> list[Worker]:
    """Start both workers, registering each with the caller's exit stack.

    The stack unwinds what it already holds if a later worker fails to start, so
    a half-started run never leaves a process holding the database open.
    """
    return [
        stack.enter_context(
            Worker("node", ["node", "worker.mjs"], NODE_WORKER_DIR, db, workdir / "node.log")
        ),
        stack.enter_context(
            Worker("java", [str(java_start_script)], JAVA_WORKER_DIR, db, workdir / "java.log")
        ),
    ]


def run_producer(db: Path, orders: int) -> None:
    subprocess.run(
        [sys.executable, "producer.py", "--db", str(db), "--orders", str(orders)],
        cwd=EXAMPLE_DIR,
        check=True,
    )


def completed(queue: Queue, task_name: str, limit: int) -> list[JobResult]:
    return queue.list_jobs(task_name=task_name, status="complete", limit=limit)


def wait_for_drain(queue: Queue, orders: int, workers: list[Worker], timeout: float) -> None:
    """Poll until both stages completed every order, or fail with a reason.

    A dead-lettered job is checked on every pass because that is what a wire
    incompatibility looks like: the payload arrives, fails to decode, exhausts
    its retries. Waiting out the full timeout for that would hide the cause.
    """
    deadline = time.monotonic() + timeout
    while True:
        dead = queue.dead_letters(limit=orders * 2)
        if dead:
            raise PipelineError(f"{len(dead)} job(s) dead-lettered: {dead}")
        for worker in workers:
            if worker.died():
                raise PipelineError(f"the {worker.name} worker exited before the queue drained")

        processed = len(completed(queue, PROCESS_TASK, orders * 2))
        notified = len(completed(queue, NOTIFY_TASK, orders * 2))
        if processed >= orders and notified >= orders:
            return

        if time.monotonic() >= deadline:
            raise PipelineError(
                f"pipeline did not drain within {timeout:.0f}s — "
                f"{processed}/{orders} processed, {notified}/{orders} notified"
            )
        time.sleep(0.5)


def assert_results_round_trip(queue: Queue, orders: int) -> None:
    """Read back what the Java worker returned, decoded by the Python SDK.

    The drain check only proves each hop's payload decoded. This closes the
    loop on the other direction: a result written by one runtime, read by
    another.
    """
    for job in completed(queue, NOTIFY_TASK, orders * 2):
        result = job.result(timeout=5)
        if not isinstance(result, dict) or result.get("notified") is not True:
            raise PipelineError(
                f"job {job.id} returned {result!r}, expected a dict with notified=True"
            )


def dump_worker_logs(workers: list[Worker]) -> None:
    """Print what each worker said. The reason for the failure is printed by the
    caller — this is the context that explains it."""
    for worker in workers:
        print(f"\n--- {worker.name} worker ({worker.log_path}) ---", file=sys.stderr)
        print(worker.tail(), file=sys.stderr)


def run(db: Path, orders: int, timeout: float, workdir: Path) -> None:
    java_start_script = build_java_worker()
    with ExitStack() as stack:
        workers = start_workers(stack, db, java_start_script, workdir)
        try:
            run_producer(db, orders)
            # Same serializer as every other process in the example: each SDK's
            # own default is same-language-only.
            queue = Queue(str(db), serializer=CborSerializer())
            wait_for_drain(queue, orders, workers, timeout)
            assert_results_round_trip(queue, orders)
        except Exception:
            dump_worker_logs(workers)
            raise


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--orders", type=int, default=3, help="how many orders to push through")
    parser.add_argument(
        "--timeout", type=float, default=180.0, help="seconds to wait for the drain"
    )
    parser.add_argument(
        "--workdir", help="where to put the database and worker logs (default: a temp dir)"
    )
    args = parser.parse_args()

    workdir = (
        Path(args.workdir).resolve()
        if args.workdir
        else Path(tempfile.mkdtemp(prefix="flexiq-polyglot-"))
    )
    workdir.mkdir(parents=True, exist_ok=True)
    db = workdir / "flexiq.db"

    print(f"running the polyglot pipeline in {workdir}")
    try:
        run(db, args.orders, args.timeout, workdir)
    # OSError covers a runtime that is missing entirely (no `node` on PATH, no
    # start script) — a setup mistake deserves the same one-line report as a
    # broken payload, not a traceback.
    except (PipelineError, subprocess.CalledProcessError, OSError) as error:
        print(f"polyglot pipeline failed: {error}", file=sys.stderr)
        return 1

    print(f"polyglot pipeline OK — {args.orders} order(s) through Python, Node and Java")
    return 0


if __name__ == "__main__":
    sys.exit(main())
