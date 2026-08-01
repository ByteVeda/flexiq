"""The same attach assertions, against the real ``taskito-server`` binary.

Gated on ``TASKITO_SERVER_BIN`` so the default suite needs no Rust build. Its
job is to keep `test_executor_attach.py`'s hand-rolled scheduler honest: that
file proves the executor speaks the protocol it was told to, this one proves the
protocol it was told to is the one the server actually speaks.

Build the binary with::

    cargo build -p taskito-server
    TASKITO_SERVER_BIN=target/debug/taskito-server uv run pytest tests/worker
"""

from __future__ import annotations

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
    SLOW,
    read_stderr,
    spawn_executor,
    terminate,
    wait_started,
)

from taskito import Queue

SERVER_BIN = os.environ.get("TASKITO_SERVER_BIN")

pytestmark = [
    pytest.mark.skipif(
        not SERVER_BIN,
        reason="set TASKITO_SERVER_BIN to a built taskito-server to run these",
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
    env["TASKITO_BACKEND"] = "sqlite"
    env["TASKITO_DSN"] = str(db_path)
    env["TASKITO_LISTEN"] = f"127.0.0.1:{port}"
    # Unset, not "off": the dashboard is disabled by having no bind address.
    env.pop("TASKITO_DASHBOARD", None)
    env.pop("TASKITO_ATTACH_TOKEN", None)

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
        # The scheduler starts on the first attach, so enqueue after attaching.
        time.sleep(1.0)
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
        time.sleep(1.0)
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


def test_sigterm_drains_against_a_real_scheduler(
    scheduler: tuple[int, Path], tmp_path: Path
) -> None:
    import signal

    port, db_path = scheduler
    markers = tmp_path / "markers"
    process = spawn_executor(port, db_path, markers=markers)
    try:
        time.sleep(1.0)
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
    env["TASKITO_BACKEND"] = "sqlite"
    env["TASKITO_DSN"] = str(db_path)
    env["TASKITO_LISTEN"] = f"127.0.0.1:{port}"
    env.pop("TASKITO_DASHBOARD", None)
    env["TASKITO_ATTACH_TOKEN"] = "correct-token-0123456789"

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
