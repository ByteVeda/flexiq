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

# An app that imports its task module below the queue it builds, so the
# declarations are pending when the constructor drains.
DEFERRED_APP_PATH = "deferred_app:queue"

# Task names are module-qualified in the registry, and it is those names the
# scheduler routes on.
ECHO = "attach_app.echo"
BOOM = "attach_app.boom"
SLOW = "attach_app.slow"
REPORTS = "attach_app.reports"
MIDDLEWARED = "attach_app.middlewared"
CHARGED = "attach_app.charged"
ACHARGED = "attach_app.acharged"
BOUND = "deferred_app.bound"
SEND_INVOICE = "deferred_tasks.send_invoice"
BUILD_REPORT = "deferred_tasks.build_report"

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

    def send_job_steps(self, job_id: str, steps: list[dict[str, Any]]) -> None:
        """Send the snapshot a dispatch carries, immediately before the job."""
        payload = encode_snapshot(steps)
        self.send({"type": "job_steps", "job_id": job_id, "payload_len": len(payload)}, payload)

    def next_step_commit(self, timeout: float = FRAME_TIMEOUT) -> tuple[dict[str, Any], bytes]:
        """The next step commit, skipping heartbeats.

        Anything else arriving first is the failure: a result before a commit
        means the step never reached this end.
        """
        deadline = time.monotonic() + timeout
        while True:
            assert time.monotonic() < deadline, "no step commit arrived"
            header, payload = read_frame(self._rfile)
            if header.get("type") == "heartbeat":
                continue
            assert header.get("type") == "step_commit", f"expected a step commit, got {header}"
            return header, payload

    def ack_step(
        self, job_id: str, seq: int, *, already: bool = False, wake_at: int | None = None
    ) -> None:
        """Confirm a commit, which is what unblocks the step."""
        ack: dict[str, Any] = {"type": "step_ack", "job_id": job_id, "seq": seq, "ok": True}
        if already:
            ack["already"] = True
        if wake_at is not None:
            ack["wake_at"] = wake_at
        self.send(ack)

    def refuse_step(self, job_id: str, seq: int, error: str, failure: str) -> None:
        """Refuse a commit, carrying the verdict only this end can make."""
        self.send(
            {
                "type": "step_ack",
                "job_id": job_id,
                "seq": seq,
                "ok": False,
                "error": error,
                "failure": failure,
            }
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
    app_path: str = APP_PATH,
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
        app_path,
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


def encode_snapshot(steps: list[dict[str, Any]]) -> bytes:
    """Frame a job's committed steps the way a scheduler does.

    A JSON metadata line, then every blob concatenated in ``seq`` order — the
    frame format's own shape, one level down. Written out here rather than
    borrowed from the native module, because a fake that encodes with the code
    under test proves nothing about the format.
    """
    meta = [
        {
            "seq": step["seq"],
            "step_key": step["step_key"],
            "kind": step.get("kind", "run"),
            # ``None`` is "no result", which is every sleep; ``0`` is an empty one.
            "result_len": None if step.get("result") is None else len(step["result"]),
            "wake_at": step.get("wake_at"),
            "created_at": step.get("created_at", 0),
        }
        for step in steps
    ]
    blobs = b"".join(step["result"] for step in steps if step.get("result") is not None)
    return json.dumps(meta, separators=(",", ":")).encode() + b"\n" + blobs


def app_queue() -> Any:
    """The app's own ``Queue``, importable once ``_app_importable`` has run.

    Imported inside the call because the app directory reaches ``sys.path`` from
    a fixture, which runs after this module is imported.
    """
    from attach_app import queue  # type: ignore[import-not-found]

    return queue


def step_blob(value: Any) -> bytes:
    """Encode a step result the way ``ctx.step`` stores it.

    The **queue's** serializer, not the task's: that is the chain a
    ``Queue(codecs=…)`` puts encryption in, and it is what reaches ``job_steps``.
    """
    blob: bytes = app_queue()._serializer.dumps(value)
    return blob


def failure_message(header: dict[str, Any]) -> str:
    """The human-readable half of a failure frame's structured error."""
    return str(json.loads(header["error"])["message"])


def payload_for(task_name: str, *args: Any, **kwargs: Any) -> bytes:
    """Encode a call the way the enqueue path does."""
    payload: bytes = app_queue()._get_serializer(task_name).dumps((args, kwargs))
    return payload


def decode_result(task_name: str, payload: bytes) -> Any:
    """Decode a success frame's blob with the task's own serializer."""
    return app_queue()._get_serializer(task_name).loads(payload)


def deferred_payload_for(task_name: str, *args: Any, **kwargs: Any) -> bytes:
    """``payload_for``, for the app whose tasks are declared after its queue."""
    from deferred_app import queue  # type: ignore[import-not-found]

    payload: bytes = queue._get_serializer(task_name).dumps((args, kwargs))
    return payload


def decode_deferred_result(task_name: str, payload: bytes) -> Any:
    """``decode_result``, for the same app."""
    from deferred_app import queue

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


def test_tasks_declared_after_the_queue_are_advertised(
    scheduler: FakeScheduler, tmp_path: Path
) -> None:
    """An app that imports its task module below the queue advertises it anyway.

    The CLI used to read the registry with only the constructor's drain behind
    it, which runs before that import. A name missing here is routed to nobody,
    so its jobs park until ``placement_timeout`` and then fail retryably.
    """
    process = spawn_executor(scheduler.port, tmp_path / "deferred.db", app_path=DEFERRED_APP_PATH)
    try:
        hello = scheduler.accept()

        assert set(hello["tasks"]) == {BOUND, SEND_INVOICE, BUILD_REPORT}
    finally:
        terminate(process)


def test_a_task_declared_after_the_queue_runs_on_a_child(
    scheduler: FakeScheduler, tmp_path: Path
) -> None:
    """Advertising the name is only half of it — the child has to hold it too.

    Each prefork child imports the app module in its own interpreter, so it
    claims the declarations on its own. A child that did not would fail the job
    as ``not registered``, and non-retryably.
    """
    process = spawn_executor(
        scheduler.port, tmp_path / "deferred_run.db", app_path=DEFERRED_APP_PATH
    )
    try:
        scheduler.accept()
        payload = deferred_payload_for(SEND_INVOICE, 7)
        scheduler.send_job("job-1", SEND_INVOICE, payload)

        header, result = scheduler.next_result()
        assert header["type"] == "success", header
        assert decode_deferred_result(SEND_INVOICE, result) == "sent:7"
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


# ------------------------------------------------------------- durable steps


def test_a_snapshot_on_the_dispatch_answers_a_memo_hit(
    scheduler: FakeScheduler, tmp_path: Path
) -> None:
    """§9.1: the executor replays without a storage read it has no credentials for.

    Two hops: the snapshot rides the socket to the executor and the pipe to the
    prefork child that holds the session. ``ran`` empty is what proves the
    closure never ran — a charge replayed rather than made twice.
    """
    process = spawn_executor(scheduler.port, tmp_path / "t.db")
    try:
        scheduler.accept(capabilities=["steps"])
        scheduler.send_job_steps(
            "job-1",
            [{"seq": 0, "step_key": "charge#0", "result": step_blob("receipt-from-attempt-0")}],
        )
        scheduler.send_job("job-1", CHARGED, payload_for(CHARGED, 500), retry_count=1)

        header, payload = scheduler.next_result()
        assert header["type"] == "success", header
        assert decode_result(CHARGED, payload) == {
            "receipt": "receipt-from-attempt-0",
            "ran": [],
        }
    finally:
        terminate(process)


def test_a_new_step_commits_through_the_scheduler(
    scheduler: FakeScheduler, tmp_path: Path
) -> None:
    """§9.2: the executor holds no database, so the write crosses to the one that does.

    The commit blocks the task until it is acknowledged — an unconfirmed commit
    is indistinguishable from one that never happened.
    """
    process = spawn_executor(scheduler.port, tmp_path / "t.db")
    try:
        scheduler.accept(capabilities=["steps"])
        scheduler.send_job("job-1", CHARGED, payload_for(CHARGED, 700))

        commit, blob = scheduler.next_step_commit()
        assert commit["job_id"] == "job-1"
        assert commit["seq"] == 0
        assert commit["step_key"] == "charge#0"
        assert commit["kind"] == "run"
        assert commit["payload_len"] == len(blob)
        assert step_blob("receipt-700") == blob, "the blob is the encoded result, verbatim"
        # No owner on the frame, and there must never be one: an owner an
        # executor fills in is an owner it can forge.
        assert "owner" not in commit, commit

        scheduler.ack_step("job-1", 0)
        header, payload = scheduler.next_result()
        assert header["type"] == "success", header
        assert decode_result(CHARGED, payload) == {
            "receipt": "receipt-700",
            # `{run_key}:{step_key}` — what the step hands a downstream API, and
            # the same string on every attempt.
            "ran": ["job-1:charge#0"],
        }
    finally:
        terminate(process)


def test_an_async_step_commits_the_same_way(scheduler: FakeScheduler, tmp_path: Path) -> None:
    """``await ctx.step.arun`` crosses the same wire, and still blocks on its ack."""
    process = spawn_executor(scheduler.port, tmp_path / "t.db")
    try:
        scheduler.accept(capabilities=["steps"])
        scheduler.send_job("job-1", ACHARGED, payload_for(ACHARGED, 42))

        commit, blob = scheduler.next_step_commit()
        assert commit["step_key"] == "charge#0"
        assert step_blob("receipt-42") == blob

        scheduler.ack_step("job-1", 0)
        header, payload = scheduler.next_result()
        assert header["type"] == "success", header
        assert decode_result(ACHARGED, payload)["receipt"] == "receipt-42"
    finally:
        terminate(process)


def test_a_refused_commit_carries_the_scheduler_s_own_verdict(
    scheduler: FakeScheduler, tmp_path: Path
) -> None:
    """The classification is made where storage is, and survives both hops.

    Getting it wrong either way is expensive: a retried permanent failure burns
    the whole budget, and a dead-lettered transient one throws work away.
    """
    process = spawn_executor(scheduler.port, tmp_path / "t.db")
    try:
        scheduler.accept(capabilities=["steps"])
        scheduler.send_job("job-1", CHARGED, payload_for(CHARGED, 700))

        scheduler.next_step_commit()
        scheduler.refuse_step("job-1", 0, "the step store is unreachable", "retryable")

        header, _ = scheduler.next_result()
        assert header["type"] == "failure", header
        assert header["should_retry"] is True
        assert failure_message(header) == "the step store is unreachable", (
            "the message reads whole — no wrapper, and nothing re-derived on the way"
        )
    finally:
        terminate(process)


def test_a_commit_the_scheduler_never_answers_fails_the_attempt_retryably(
    scheduler: FakeScheduler, tmp_path: Path
) -> None:
    """Silence ends the attempt rather than parking the job on it.

    Two deadlines bound it — the executor's ack budget, which the job's own
    timeout caps, and the prefork watchdog on that same timeout. They are the
    same instant by construction, so which one fires is not the assertion;
    that the attempt ends *and is retried* is.
    """
    process = spawn_executor(scheduler.port, tmp_path / "t.db")
    try:
        scheduler.accept(capabilities=["steps"])
        scheduler.send_job("job-1", CHARGED, payload_for(CHARGED, 700), timeout_ms=2_000)

        scheduler.next_step_commit()  # deliberately never acknowledged

        header, _ = scheduler.next_result()
        assert header["type"] == "failure", header
        assert header["should_retry"] is True, (
            "nothing confirmed the write landed, so the replay is safe and owed"
        )
    finally:
        terminate(process)


def test_a_scheduler_without_the_step_capability_refuses_the_step(
    scheduler: FakeScheduler, tmp_path: Path
) -> None:
    """§9.4. There is no version of "your charge step silently lost its memo".

    The refusal names the scheduler rather than a storage backend: there is no
    backend on this side, and that line would send an operator to the wrong
    process. Retryable, so a fleet mid-rollout can still place the next attempt
    somewhere that commits.
    """
    process = spawn_executor(scheduler.port, tmp_path / "t.db")
    try:
        scheduler.accept(capabilities=["side_channel"])
        scheduler.send_job("job-1", CHARGED, payload_for(CHARGED, 700))

        header, _ = scheduler.next_result()
        assert header["type"] == "failure", header
        assert header["should_retry"] is True
        assert "offers no step store" in failure_message(header), header
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
