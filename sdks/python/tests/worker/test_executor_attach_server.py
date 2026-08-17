"""The same attach assertions, against the real ``flexiq-server`` binary.

Gated on ``FLEXIQ_SERVER_BIN`` so the default suite needs no Rust build. Its
job is to keep `test_executor_attach.py`'s hand-rolled scheduler honest: that
file proves the executor speaks the protocol it was told to, this one proves the
protocol it was told to is the one the server actually speaks.

Build the binary with::

    cargo build -p flexiq-server
    FLEXIQ_SERVER_BIN=target/debug/flexiq-server uv run pytest tests/worker
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from collections.abc import Iterator
from pathlib import Path

import pytest

# tests/worker is not a package, so pytest's rootdir insertion is what makes
# this a plain module import rather than a relative one.
from test_executor_attach import (
    APP_DIR,
    APP_PATH,
    BOOM,
    ECHO,
    MIDDLEWARED,
    REPORTS,
    SLOW,
    read_stderr,
    spawn_executor,
    terminate,
    wait_started,
)

from flexiq import Queue

SERVER_BIN = os.environ.get("FLEXIQ_SERVER_BIN")

pytestmark = [
    pytest.mark.skipif(
        not SERVER_BIN,
        reason="set FLEXIQ_SERVER_BIN to a built flexiq-server to run these",
    ),
    pytest.mark.skipif(
        sys.platform == "win32",
        reason="the executor runs tasks on prefork children, which Windows does not support",
    ),
]

SETTLE = 60.0


def free_port() -> int:
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


@pytest.fixture
def scheduler(tmp_path: Path) -> Iterator[tuple[int, Path]]:
    """Run a real scheduler over a temp SQLite database."""
    db_path = tmp_path / "server.db"
    port = free_port()

    env = dict(os.environ)
    env["FLEXIQ_BACKEND"] = "sqlite"
    env["FLEXIQ_DSN"] = str(db_path)
    env["FLEXIQ_LISTEN"] = f"127.0.0.1:{port}"
    # Unset, not "off": the dashboard is disabled by having no bind address.
    env.pop("FLEXIQ_DASHBOARD", None)
    env.pop("FLEXIQ_ATTACH_TOKEN", None)

    assert SERVER_BIN is not None
    process = subprocess.Popen(
        [SERVER_BIN],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        wait_for_port(port, process)
        yield port, db_path
    finally:
        terminate(process)


def wait_for_port(port: int, process: subprocess.Popen[str], timeout: float = SETTLE) -> None:
    """Block until the attach listener accepts connections."""
    import socket

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise AssertionError(f"the server exited early: {read_stderr(process)}")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                return
        except OSError:
            time.sleep(0.05)
    raise AssertionError(f"the server never bound port {port}")


def enqueue(db_path: Path, task_name: str, args: tuple = ()) -> str:
    """Enqueue through a Queue over the same database the server reads."""
    queue = Queue(db_path=str(db_path))
    return str(queue.enqueue(task_name, args).id)


def wait_for_attach(db_path: Path) -> None:
    """Block until an executor is attached and dispatching.

    The scheduler starts on the first attach, and nothing records that in
    storage, so the only non-racy proof is a job the executor has to run. A
    fixed sleep would be a guess about app import and prefork spawn times on a
    loaded runner.
    """
    probe = enqueue(db_path, ECHO, ("ready",))
    wait_for_status(db_path, probe, "complete")


def wait_for_status(db_path: Path, job_id: str, status: str, timeout: float = SETTLE) -> None:
    queue = Queue(db_path=str(db_path))
    deadline = time.monotonic() + timeout
    seen = None
    while time.monotonic() < deadline:
        job = queue.get_job(job_id)
        seen = None if job is None else job.status
        if seen == status:
            return
        time.sleep(0.05)
    raise AssertionError(f"job {job_id} was {seen}, expected {status}")


def test_a_real_scheduler_dispatches_to_an_attached_executor(
    scheduler: tuple[int, Path], tmp_path: Path
) -> None:
    port, db_path = scheduler
    process = spawn_executor(port, db_path)
    try:
        wait_for_attach(db_path)
        job_id = enqueue(db_path, ECHO, ("hello",))
        wait_for_status(db_path, job_id, "complete")
    finally:
        terminate(process)


def test_a_failure_is_retried_by_the_real_scheduler(
    scheduler: tuple[int, Path], tmp_path: Path
) -> None:
    port, db_path = scheduler
    process = spawn_executor(port, db_path)
    try:
        wait_for_attach(db_path)
        job_id = enqueue(db_path, BOOM)

        # `boom` always raises. The error reaching storage is what proves the
        # executor's failure crossed the wire and the scheduler applied it.
        queue = Queue(db_path=str(db_path))
        deadline = time.monotonic() + SETTLE
        while time.monotonic() < deadline:
            job = queue.get_job(job_id)
            if job is not None and job.error:
                assert "deliberate failure" in job.error
                return
            time.sleep(0.05)
        raise AssertionError("the failure never reached storage")
    finally:
        terminate(process)


def test_progress_and_logs_reach_storage_through_a_real_scheduler(
    scheduler: tuple[int, Path], tmp_path: Path
) -> None:
    """The done-when for #589, against the binary an operator actually runs.

    The executor holds no database credentials, so a progress bar and task logs
    appearing in storage can only have got there through the scheduler.
    """
    port, db_path = scheduler
    process = spawn_executor(port, db_path)
    try:
        wait_for_attach(db_path)
        job_id = enqueue(db_path, REPORTS)
        wait_for_status(db_path, job_id, "complete")

        # The side-channel is fire-and-forget, so both the last progress value
        # and the logs may land just after the result. The job completing is
        # not a barrier for them, and must not be: making it one would put a
        # database write between a task and its own result.
        queue = Queue(db_path=str(db_path))
        deadline = time.monotonic() + SETTLE
        logs: list = []
        progress = None
        while time.monotonic() < deadline and not (progress == 100 and len(logs) >= 2):
            job = queue.get_job(job_id)
            progress = None if job is None else job.progress
            logs = queue.task_logs(job_id)
            time.sleep(0.05)

        assert progress == 100, f"the task's final progress must reach storage, saw {progress}"

        levels = {entry["level"]: entry for entry in logs}
        assert "info" in levels, f"the task's log line must reach storage: {logs}"
        assert levels["info"]["message"] == "halfway"
        assert levels["info"]["task_name"] == REPORTS
        # `publish` is a `result`-level log, which is what `job.stream()` reads.
        assert "result" in levels, f"the published partial must reach storage: {logs}"
        assert json.loads(levels["result"]["extra"]) == {"stage": "halfway"}
    finally:
        terminate(process)


def test_a_dashboard_toggle_reaches_an_attached_executor(
    scheduler: tuple[int, Path], tmp_path: Path
) -> None:
    """An executor cannot read settings, so the scheduler has to carry them."""
    port, db_path = scheduler
    process = spawn_executor(port, db_path)
    try:
        wait_for_attach(db_path)
        queue = Queue(db_path=str(db_path))

        ran = enqueue(db_path, MIDDLEWARED)
        wait_for_status(db_path, ran, "complete")
        job = queue.get_job(ran)
        assert job is not None
        assert job.result(timeout=SETTLE) == "recorder", "the middleware should run by default"

        queue.disable_middleware_for_task(MIDDLEWARED, "recorder")
        # The scheduler resolves the list per dispatch behind a short cache, so
        # a toggle takes effect within it rather than instantly.
        deadline = time.monotonic() + SETTLE
        while time.monotonic() < deadline:
            # Each wait gets what is left of the outer budget rather than a
            # fresh `SETTLE`, or one slow attempt would spend the whole thing
            # and leave no room for the retry this loop exists to make.
            remaining = max(deadline - time.monotonic(), 0.1)
            job_id = enqueue(db_path, MIDDLEWARED)
            wait_for_status(db_path, job_id, "complete", timeout=remaining)
            toggled = queue.get_job(job_id)
            assert toggled is not None
            if toggled.result(timeout=remaining) == "":
                return
            time.sleep(0.5)
        raise AssertionError("a middleware disabled in the dashboard still ran on the executor")
    finally:
        terminate(process)


def test_sigterm_drains_against_a_real_scheduler(
    scheduler: tuple[int, Path], tmp_path: Path
) -> None:
    import signal

    port, db_path = scheduler
    markers = tmp_path / "markers"
    process = spawn_executor(port, db_path, markers=markers)
    try:
        wait_for_attach(db_path)
        job_id = enqueue(db_path, SLOW, (600,))
        wait_started(markers, job_id)

        process.send_signal(signal.SIGTERM)
        (markers / "release").write_text("1")

        wait_for_status(db_path, job_id, "complete")
        assert process.wait(timeout=SETTLE) == 0
    finally:
        terminate(process)


def test_a_bad_token_is_refused_by_the_real_listener(tmp_path: Path) -> None:
    """The security gate, against the listener that actually enforces it."""
    db_path = tmp_path / "server.db"
    port = free_port()

    env = dict(os.environ)
    env["FLEXIQ_BACKEND"] = "sqlite"
    env["FLEXIQ_DSN"] = str(db_path)
    env["FLEXIQ_LISTEN"] = f"127.0.0.1:{port}"
    env.pop("FLEXIQ_DASHBOARD", None)
    env["FLEXIQ_ATTACH_TOKEN"] = "correct-token-0123456789"

    assert SERVER_BIN is not None
    server = subprocess.Popen(
        [SERVER_BIN], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )
    try:
        wait_for_port(port, server)
        executor = spawn_executor(port, db_path, token="wrong-token-0123456789")
        try:
            assert executor.wait(timeout=SETTLE) != 0
            assert "token" in read_stderr(executor).lower()
        finally:
            terminate(executor)
    finally:
        terminate(server)


def test_the_app_dir_is_the_one_under_test() -> None:
    """Guards the import above: these tests share the other file's fixtures."""
    assert (APP_DIR / "attach_app.py").exists()
    assert APP_PATH == "attach_app:queue"
