"""End-to-end tests for ``flexiq executor``.

A scheduler is played by a plain socket speaking the frame protocol, so these
run in every CI job without building the Rust server binary. ``flexiq-server``
is the real peer, and `test_executor_attach_server.py` runs the same assertions
against it when one is available — that pairing is what keeps this fake honest.

The executor runs as a real subprocess rather than in-process: prefork children
and ``SIGTERM`` handling are most of what is under test, and neither is
observable from inside the test interpreter.
"""

from __future__ import annotations

import contextlib
import json
import os
import signal
import socket
import subprocess
import sys
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from flexiq.worker_protocol import WORKER_PROTOCOL_VERSION, read_frame, write_frame

pytestmark = pytest.mark.skipif(
    sys.platform == "win32",
    reason="the executor runs tasks on prefork children, which Windows does not support",
)

APP_DIR = Path(__file__).parent / "executor_apps"
APP_PATH = "attach_app:queue"

# Task names are module-qualified in the registry, and it is those names the
# scheduler routes on.
ECHO = "attach_app.echo"
BOOM = "attach_app.boom"
SLOW = "attach_app.slow"
REPORTS = "attach_app.reports"
MIDDLEWARED = "attach_app.middlewared"

# Generous: a cold subprocess import of the app plus a prefork child spawn.
ATTACH_TIMEOUT = 60.0
FRAME_TIMEOUT = 60.0


class FakeScheduler:
    """The scheduler end of an attach, driven frame by frame."""

    def __init__(self) -> None:
        self._listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listener.bind(("127.0.0.1", 0))
        self._listener.listen(1)
        self.port: int = self._listener.getsockname()[1]
        self._conn: socket.socket | None = None
        self._rfile: Any = None
        self._wfile: Any = None

    def accept(
        self,
        timeout: float = ATTACH_TIMEOUT,
        capabilities: list[str] | None = None,
    ) -> dict[str, Any]:
        """Accept the attach and complete the handshake, returning the hello.

        ``capabilities`` is what this scheduler promises to do on the
        executor's behalf. The default — nothing — is a scheduler built before
        the side-channel existed, which is the compatibility case worth having
        as the default rather than the exception.
        """
        self._listener.settimeout(timeout)
        self._conn, _ = self._listener.accept()
        self._conn.settimeout(FRAME_TIMEOUT)
        self._rfile = self._conn.makefile("rb")
        self._wfile = self._conn.makefile("wb")

        hello, _ = read_frame(self._rfile)
        assert hello["type"] == "hello", f"expected hello, got {hello}"
        self.send(
            {
                "type": "hello_ack",
                "scheduler_id": "fake-scheduler",
                "protocol_version": WORKER_PROTOCOL_VERSION,
                "capabilities": capabilities or [],
            }
        )
        return hello

    def refuse(self, timeout: float = ATTACH_TIMEOUT) -> dict[str, Any]:
        """Accept, read the hello, then close without acking — a rejected peer."""
        self._listener.settimeout(timeout)
        conn, _ = self._listener.accept()
        conn.settimeout(FRAME_TIMEOUT)
        with conn.makefile("rb") as rfile:
            hello, _ = read_frame(rfile)
        conn.close()
        return hello

    def send(self, header: dict[str, Any], payload: bytes = b"") -> None:
        write_frame(self._wfile, header, payload)

    def send_job(
        self,
        job_id: str,
        task_name: str,
        payload: bytes,
        *,
        retry_count: int = 0,
        max_retries: int = 3,
        timeout_ms: int = 30_000,
        disabled_middleware: list[str] | None = None,
    ) -> None:
        self.send(
            {
                "type": "job",
                "id": job_id,
                "task_name": task_name,
                "payload_len": len(payload),
                "retry_count": retry_count,
                "max_retries": max_retries,
                "queue": "default",
                "timeout_ms": timeout_ms,
                "namespace": None,
                # Resolved by the scheduler, because an executor has no
                # settings store of its own to read the toggle list from.
                "disabled_middleware": disabled_middleware or [],
                "metadata": None,
            },
            payload,
        )

    def next_result(self, timeout: float = FRAME_TIMEOUT) -> tuple[dict[str, Any], bytes]:
        """The next frame that is not a heartbeat."""
        deadline = time.monotonic() + timeout
        while True:
            assert time.monotonic() < deadline, "no result frame arrived"
            header, payload = read_frame(self._rfile)
            if header.get("type") != "heartbeat":
                return header, payload

    def collect_until_result(
        self, timeout: float = FRAME_TIMEOUT
    ) -> tuple[dict[str, Any], list[tuple[dict[str, Any], bytes]]]:
        """Every side-channel frame a job produced, plus its result.

        The result is ordered behind them on one connection, so its arrival is
        what proves the collection is complete rather than merely early.
        """
        deadline = time.monotonic() + timeout
        side_channel: list[tuple[dict[str, Any], bytes]] = []
        while True:
            assert time.monotonic() < deadline, "no result frame arrived"
            header, payload = read_frame(self._rfile)
            kind = header.get("type")
            if kind == "heartbeat":
                continue
            if kind in ("progress", "task_log"):
                side_channel.append((header, payload))
                continue
            return header, side_channel

    def next_heartbeat(self, free_slots: int, timeout: float = FRAME_TIMEOUT) -> None:
        """Block until the executor reports exactly ``free_slots`` free."""
        deadline = time.monotonic() + timeout
        while True:
            assert time.monotonic() < deadline, f"no heartbeat reporting {free_slots} slots"
            header, _ = read_frame(self._rfile)
            if header.get("type") == "heartbeat" and header.get("free_slots") == free_slots:
                return

    def close(self) -> None:
        # A test that already tore the connection down leaves these broken;
        # teardown must not turn that into a second, confusing failure.
        for handle in (self._rfile, self._wfile, self._conn, self._listener):
            if handle is not None:
                with contextlib.suppress(OSError):
                    handle.close()


@pytest.fixture
def scheduler() -> Iterator[FakeScheduler]:
    fake = FakeScheduler()
    try:
        yield fake
    finally:
        fake.close()


def spawn_executor(
    port: int,
    db_path: Path,
    *,
    slots: int = 1,
    token: str | None = None,
    executor_id: str | None = None,
    markers: Path | None = None,
) -> subprocess.Popen[str]:
    """Run ``flexiq executor`` against ``port`` as a real subprocess."""
    env = dict(os.environ)
    env["FLEXIQ_EXECUTOR_TEST_DB"] = str(db_path)
    if markers is not None:
        markers.mkdir(parents=True, exist_ok=True)
        env["FLEXIQ_EXECUTOR_MARKERS"] = str(markers)
    # Prefork children default to `python` on PATH; point them at this
    # interpreter so they import the same flexiq build the test does.
    env["FLEXIQ_PYTHON"] = sys.executable
    env.pop("FLEXIQ_ATTACH_TOKEN", None)
    if token is not None:
        env["FLEXIQ_ATTACH_TOKEN"] = token

    command = [
        sys.executable,
        "-m",
        "flexiq.cli",
        "executor",
        "--app",
        APP_PATH,
        "--attach",
        f"127.0.0.1:{port}",
        "--slots",
        str(slots),
    ]
    if executor_id is not None:
        command += ["--executor-id", executor_id]

    return subprocess.Popen(
        command,
        # The CLI puts the working directory on `sys.path`, and so does each
        # prefork child, so this is what makes `attach_app` importable in both.
        cwd=str(APP_DIR),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def read_stderr(process: subprocess.Popen[str]) -> str:
    """Drain a process's stderr, which every spawn here pipes."""
    assert process.stderr is not None, "the process was spawned without a stderr pipe"
    text: str = process.stderr.read()
    return text


def terminate(process: subprocess.Popen[str]) -> None:
    """Best-effort teardown for a process a test did not stop itself."""
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=30)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)


def wait_started(markers: Path, job_id: str, timeout: float = FRAME_TIMEOUT) -> None:
    """Block until the task for ``job_id`` reports that it is running.

    A heartbeat cannot serve here: they are seconds apart, so a sub-second job
    starts and finishes between two of them.
    """
    deadline = time.monotonic() + timeout
    marker = markers / f"{job_id}.started"
    while not marker.exists():
        assert time.monotonic() < deadline, f"{job_id} never started"
        time.sleep(0.02)


def release_tasks(markers: Path) -> None:
    """Let every parked task return."""
    (markers / "release").write_text("1")


def payload_for(task_name: str, *args: Any, **kwargs: Any) -> bytes:
    """Encode a call the way the enqueue path does."""
    from attach_app import queue  # type: ignore[import-not-found]

    payload: bytes = queue._get_serializer(task_name).dumps((args, kwargs))
    return payload


def decode_result(task_name: str, payload: bytes) -> Any:
    """Decode a success frame's blob with the task's own serializer."""
    from attach_app import queue

    return queue._get_serializer(task_name).loads(payload)


@pytest.fixture(autouse=True)
def _app_importable() -> Iterator[None]:
    """Put the app dir on `sys.path` so the test can use its serializer."""
    sys.path.insert(0, str(APP_DIR))
    try:
        yield
    finally:
        if str(APP_DIR) in sys.path:
            sys.path.remove(str(APP_DIR))


def test_executor_announces_itself_and_its_tasks(scheduler: FakeScheduler, tmp_path: Path) -> None:
    """The handshake carries what the scheduler routes on."""
    process = spawn_executor(scheduler.port, tmp_path / "t.db", slots=2, executor_id="exec-test")
    try:
        hello = scheduler.accept()

        assert hello["executor_id"] == "exec-test"
        assert hello["sdk"] == "python"
        assert hello["slots"] == 2
        assert hello["protocol_version"] == WORKER_PROTOCOL_VERSION
        # Only advertised tasks are ever dispatched, so a missing name here is
        # a job that silently never runs.
        assert set(hello["tasks"]) >= {ECHO, BOOM, SLOW}
        # A token that was never configured must not appear on the wire.
        assert "token" not in hello
    finally:
        terminate(process)


def test_a_job_runs_on_the_executor_and_returns_its_result(
    scheduler: FakeScheduler, tmp_path: Path
) -> None:
    process = spawn_executor(scheduler.port, tmp_path / "t.db")
    try:
        scheduler.accept()
        scheduler.send_job("job-1", ECHO, payload_for(ECHO, "hello"))

        header, payload = scheduler.next_result()
        assert header["type"] == "success", header
        assert header["job_id"] == "job-1"
        assert header["task_name"] == ECHO

        from attach_app import queue

        assert queue._get_serializer(ECHO).loads(payload) == "echo:hello"
    finally:
        terminate(process)


def test_a_failing_task_reports_a_retryable_failure(
    scheduler: FakeScheduler, tmp_path: Path
) -> None:
    """The retry verdict is the executor's to make — only it sees the exception."""
    process = spawn_executor(scheduler.port, tmp_path / "t.db")
    try:
        scheduler.accept()
        scheduler.send_job("job-1", BOOM, payload_for(BOOM), retry_count=1)

        header, _ = scheduler.next_result()
        assert header["type"] == "failure", header
        assert header["job_id"] == "job-1"
        assert header["should_retry"] is True
        assert header["timed_out"] is False
        assert header["retry_count"] == 1, "the frame's retry count is echoed back"
        assert "deliberate failure" in header["error"]
    finally:
        terminate(process)


def test_a_retry_is_dispatched_to_the_same_executor(
    scheduler: FakeScheduler, tmp_path: Path
) -> None:
    """A second attempt reuses the live attachment rather than needing a reattach."""
    process = spawn_executor(scheduler.port, tmp_path / "t.db")
    try:
        scheduler.accept()

        scheduler.send_job("job-1", BOOM, payload_for(BOOM), retry_count=0)
        first, _ = scheduler.next_result()
        assert first["type"] == "failure"

        scheduler.send_job("job-1", ECHO, payload_for(ECHO, "retried"), retry_count=1)
        second, _ = scheduler.next_result()
        assert second["type"] == "success"
        assert second["job_id"] == "job-1"
    finally:
        terminate(process)


def test_a_cancel_stops_a_running_task(scheduler: FakeScheduler, tmp_path: Path) -> None:
    markers = tmp_path / "markers"
    process = spawn_executor(scheduler.port, tmp_path / "t.db", markers=markers)
    try:
        scheduler.accept()
        scheduler.send_job("job-1", SLOW, payload_for(SLOW, 600))
        wait_started(markers, "job-1")

        # The task polls `check_cancelled()`, so the cancel lands within a tick
        # rather than waiting out the whole loop.
        scheduler.send({"type": "cancel", "job_id": "job-1"})

        header, _ = scheduler.next_result()
        assert header["type"] == "cancelled", header
        assert header["job_id"] == "job-1"
    finally:
        terminate(process)


def test_sigterm_drains_in_flight_work_before_exiting(
    scheduler: FakeScheduler, tmp_path: Path
) -> None:
    """The container-shutdown path: a held job must still report its result."""
    markers = tmp_path / "markers"
    process = spawn_executor(scheduler.port, tmp_path / "t.db", markers=markers)
    try:
        scheduler.accept()
        scheduler.send_job("job-1", SLOW, payload_for(SLOW, 600))
        wait_started(markers, "job-1")

        process.send_signal(signal.SIGTERM)

        # The drain announces zero capacity in-protocol before disconnecting, so
        # the scheduler stops dispatching rather than racing the close.
        scheduler.next_heartbeat(free_slots=0)

        # The job is still running at this point; finishing it must still be
        # reported, or it would wait for a reap it never needed.
        release_tasks(markers)
        header, _ = scheduler.next_result()
        assert header["type"] == "success", header
        assert header["job_id"] == "job-1"

        assert process.wait(timeout=60) == 0, "a drained executor exits cleanly"
    finally:
        terminate(process)


def test_a_shutdown_frame_ends_the_session(scheduler: FakeScheduler, tmp_path: Path) -> None:
    """The scheduler's own teardown stops the executor without a signal."""
    process = spawn_executor(scheduler.port, tmp_path / "t.db")
    try:
        scheduler.accept()
        scheduler.send({"type": "shutdown"})
        assert process.wait(timeout=60) == 0
    finally:
        terminate(process)


def test_a_refused_attach_exits_nonzero_with_a_token_hint(
    scheduler: FakeScheduler, tmp_path: Path
) -> None:
    """A refusal must name the likely cause, not surface as a network error."""
    process = spawn_executor(scheduler.port, tmp_path / "t.db", token="attach-token-0123456789")
    try:
        hello = scheduler.refuse()
        # The token is presented for the scheduler to check, and read from the
        # environment rather than argv so it stays out of `ps`.
        assert hello["token"] == "attach-token-0123456789"

        assert process.wait(timeout=60) != 0
        assert "token" in read_stderr(process).lower()
    finally:
        terminate(process)


def test_an_unreachable_scheduler_exits_nonzero(tmp_path: Path) -> None:
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    closed_port = listener.getsockname()[1]
    listener.close()

    process = spawn_executor(closed_port, tmp_path / "t.db")
    try:
        assert process.wait(timeout=60) != 0
        assert "could not reach the scheduler" in read_stderr(process)
    finally:
        terminate(process)


def test_missing_attach_address_is_reported(tmp_path: Path) -> None:
    env = dict(os.environ)
    env["FLEXIQ_EXECUTOR_TEST_DB"] = str(tmp_path / "t.db")
    env.pop("FLEXIQ_ATTACH", None)

    result = subprocess.run(
        [sys.executable, "-m", "flexiq.cli", "executor", "--app", APP_PATH],
        cwd=str(APP_DIR),
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode != 0
    assert "FLEXIQ_ATTACH" in result.stderr


def test_the_attach_address_can_come_from_the_environment(
    scheduler: FakeScheduler, tmp_path: Path
) -> None:
    """Deployments configure by env, not flags — the contract shared with the other SDKs."""
    env = dict(os.environ)
    env["FLEXIQ_EXECUTOR_TEST_DB"] = str(tmp_path / "t.db")
    env["FLEXIQ_PYTHON"] = sys.executable
    env["FLEXIQ_ATTACH"] = f"127.0.0.1:{scheduler.port}"
    env["FLEXIQ_SLOTS"] = "3"
    env.pop("FLEXIQ_ATTACH_TOKEN", None)

    process = subprocess.Popen(
        [sys.executable, "-m", "flexiq.cli", "executor", "--app", APP_PATH],
        cwd=str(APP_DIR),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        hello = scheduler.accept()
        assert hello["slots"] == 3, "FLEXIQ_SLOTS must be honoured"
    finally:
        terminate(process)


def test_slots_run_jobs_concurrently(scheduler: FakeScheduler, tmp_path: Path) -> None:
    """Two slots means two prefork children, so two jobs run at once."""
    markers = tmp_path / "markers"
    process = spawn_executor(scheduler.port, tmp_path / "t.db", slots=2, markers=markers)
    try:
        scheduler.accept()
        scheduler.send_job("job-1", SLOW, payload_for(SLOW, 600))
        scheduler.send_job("job-2", SLOW, payload_for(SLOW, 600))

        # Both parked at once. A pool that serialized them would never let the
        # second start while the first is still holding its child.
        wait_started(markers, "job-1")
        wait_started(markers, "job-2")

        release_tasks(markers)
        finished = {scheduler.next_result()[0]["job_id"] for _ in range(2)}
        assert finished == {"job-1", "job-2"}
    finally:
        terminate(process)


def test_the_executor_opens_no_storage(scheduler: FakeScheduler, tmp_path: Path) -> None:
    """The point of the attach split: app code without database credentials.

    Pointed at a Postgres DSN nothing is listening on. An executor that opened
    storage could not even start; one that does not never notices.
    """
    env = dict(os.environ)
    env["FLEXIQ_PYTHON"] = sys.executable
    env["FLEXIQ_ATTACH"] = f"127.0.0.1:{scheduler.port}"
    env.pop("FLEXIQ_ATTACH_TOKEN", None)
    # Port 1 on loopback is reserved and nothing listens there.
    env["FLEXIQ_EXECUTOR_TEST_BACKEND"] = "postgres"
    env["FLEXIQ_EXECUTOR_TEST_DB"] = "postgres://flexiq:nope@127.0.0.1:1/absent"

    process = subprocess.Popen(
        [sys.executable, "-m", "flexiq.cli", "executor", "--app", APP_PATH],
        cwd=str(APP_DIR),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        hello = scheduler.accept()
        assert set(hello["tasks"]) >= {ECHO, BOOM, SLOW}

        # And it still runs jobs, on a prefork child that also opened nothing.
        scheduler.send_job("job-1", ECHO, payload_for(ECHO, "detached"))
        header, _ = scheduler.next_result()
        assert header["type"] == "success", header
    finally:
        terminate(process)


def test_progress_and_logs_degrade_rather_than_failing_the_job(
    scheduler: FakeScheduler, tmp_path: Path
) -> None:
    """A task calling `update_progress` must not fail for want of storage.

    This scheduler advertises no side-channel — an older `flexiq-server` —
    so the executor sends nothing it could not parse. Losing the progress bar
    is a degradation; failing the job over it would be a regression for anyone
    moving a worker to an executor.
    """
    process = spawn_executor(scheduler.port, tmp_path / "t.db")
    try:
        scheduler.accept()
        scheduler.send_job("job-1", REPORTS, payload_for(REPORTS))

        header, side_channel = scheduler.collect_until_result()
        assert header["type"] == "success", header
        assert header["job_id"] == "job-1"
        assert side_channel == [], (
            "an executor must send no frame a scheduler did not advertise support for"
        )
    finally:
        terminate(process)


def test_progress_and_logs_reach_a_scheduler_that_advertised_the_side_channel(
    scheduler: FakeScheduler, tmp_path: Path
) -> None:
    """The whole point of #589: a task on an executor is not silently poorer.

    Both hops are under test — the task writes to a prefork child, the child
    frames it to the executor, and the executor frames it to the scheduler.
    """
    process = spawn_executor(scheduler.port, tmp_path / "t.db")
    try:
        scheduler.accept(capabilities=["side_channel"])
        scheduler.send_job("job-1", REPORTS, payload_for(REPORTS))

        header, side_channel = scheduler.collect_until_result()
        assert header["type"] == "success", header

        progress = [frame["progress"] for frame, _ in side_channel if frame["type"] == "progress"]
        assert progress, "the task's progress must reach the scheduler"
        assert progress[-1] == 100, f"the final progress must be the last value: {progress}"

        logs = [(frame, payload) for frame, payload in side_channel if frame["type"] == "task_log"]
        assert [frame["job_id"] for frame, _ in logs] == ["job-1", "job-1"]

        info = next(frame for frame, _ in logs if frame["level"] == "info")
        assert info["message"] == "halfway"
        assert info["task_name"] == REPORTS

        # `publish` is a `result`-level log, which is what `job.stream()` reads.
        partial, extra = next((frame, blob) for frame, blob in logs if frame["level"] == "result")
        assert partial["extra_len"] == len(extra)
        assert json.loads(extra) == {"stage": "halfway"}
    finally:
        terminate(process)


def test_a_middleware_disabled_on_the_dispatch_does_not_run(
    scheduler: FakeScheduler, tmp_path: Path
) -> None:
    """A dashboard toggle has to reach a process that cannot read settings.

    It rides the job frame instead, so the executor honours it without ever
    touching the database the scheduler holds.
    """
    process = spawn_executor(scheduler.port, tmp_path / "t.db")
    try:
        scheduler.accept(capabilities=["side_channel"])

        scheduler.send_job("job-1", MIDDLEWARED, payload_for(MIDDLEWARED))
        header, payload = scheduler.next_result()
        assert header["type"] == "success", header
        assert decode_result(MIDDLEWARED, payload) == "recorder", "the middleware should run"

        scheduler.send_job(
            "job-2",
            MIDDLEWARED,
            payload_for(MIDDLEWARED),
            disabled_middleware=["recorder"],
        )
        header, payload = scheduler.next_result()
        assert header["type"] == "success", header
        assert decode_result(MIDDLEWARED, payload) == "", (
            "a middleware disabled on the dispatch frame must not run"
        )
    finally:
        terminate(process)
