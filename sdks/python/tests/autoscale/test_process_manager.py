"""ProcessManager unit tests with mocked subprocess.Popen.

Verifies the two-step termination path (SIGTERM → wait → SIGKILL on
timeout), the non-blocking reap, and emergency kill_all.
"""

from __future__ import annotations

import subprocess
from typing import Any, cast
from unittest.mock import MagicMock, patch

import pytest

from flexiq.autoscale import ProcessManager


class _FakePopen:
    """Minimal Popen mock — counts calls and supports timeout simulation."""

    def __init__(self, pid: int, wait_raises: bool = False) -> None:
        self.pid = pid
        self.returncode: int | None = None
        self._terminated = False
        self._killed = False
        self._wait_raises = wait_raises
        self.terminate_calls = 0
        self.kill_calls = 0
        self.wait_calls = 0
        self.poll_returns: int | None = None

    def terminate(self) -> None:
        self.terminate_calls += 1
        self._terminated = True

    def kill(self) -> None:
        self.kill_calls += 1
        self._killed = True
        self.returncode = -9

    def wait(self, timeout: float | None = None) -> int:
        self.wait_calls += 1
        if self._wait_raises and not self._killed:
            raise subprocess.TimeoutExpired(cmd="flexiq", timeout=timeout or 0)
        if self._terminated and not self._killed:
            self.returncode = 0
        return self.returncode or 0

    def poll(self) -> int | None:
        return self.poll_returns


def _install_fake(pm: ProcessManager, fake: _FakePopen) -> None:
    """Inject a fake Popen into the manager's tracking dict."""
    pm._processes[fake.pid] = cast(Any, fake)


def _make_pm_with_fake_proc(
    pid: int = 12345,
    wait_raises: bool = False,
) -> tuple[ProcessManager, _FakePopen]:
    pm = ProcessManager(app_path="myapp:queue", drain_timeout_sec=2)
    fake = _FakePopen(pid=pid, wait_raises=wait_raises)
    _install_fake(pm, fake)
    return pm, fake


def test_terminate_worker_graceful_path() -> None:
    pm, fake = _make_pm_with_fake_proc()
    clean = pm.terminate_worker(fake.pid)
    assert clean is True
    assert fake.terminate_calls == 1
    assert fake.kill_calls == 0
    assert pm.count_live() == 0


def test_terminate_worker_timeout_falls_back_to_kill() -> None:
    pm, fake = _make_pm_with_fake_proc(wait_raises=True)
    clean = pm.terminate_worker(fake.pid)
    assert clean is False
    assert fake.terminate_calls == 1
    assert fake.kill_calls == 1
    assert pm.count_live() == 0


def test_terminate_unknown_pid_returns_true() -> None:
    pm = ProcessManager(app_path="x:q")
    assert pm.terminate_worker(99999) is True


def test_reap_dead_returns_exited_pids() -> None:
    pm, fake_a = _make_pm_with_fake_proc(pid=100)
    fake_b = _FakePopen(pid=200)
    _install_fake(pm, fake_b)

    fake_a.poll_returns = 0  # exited
    fake_b.poll_returns = None  # still alive

    dead = pm.reap_dead()
    assert dead == [100]
    assert pm.count_live() == 1
    assert pm.live_pids() == [200]


def test_kill_all_force_terminates_everything() -> None:
    pm, fake_a = _make_pm_with_fake_proc(pid=100)
    fake_b = _FakePopen(pid=200)
    _install_fake(pm, fake_b)

    pm.kill_all()
    assert fake_a.kill_calls == 1
    assert fake_b.kill_calls == 1
    assert pm.count_live() == 0


@patch("flexiq.autoscale.process_manager.subprocess.Popen")
def test_spawn_worker_invokes_flexiq_worker_module(mock_popen: MagicMock) -> None:
    fake = MagicMock()
    fake.pid = 4242
    mock_popen.return_value = fake

    pm = ProcessManager(app_path="myapp:queue", drain_timeout_sec=30)
    pid = pm.spawn_worker()

    assert pid == 4242
    args, kwargs = mock_popen.call_args
    cmd: list[Any] = args[0]
    assert cmd[1:] == [
        "-m",
        "flexiq",
        "worker",
        "--app",
        "myapp:queue",
        "--drain-timeout",
        "30",
    ]
    assert kwargs.get("start_new_session") is True
    assert pm.count_live() == 1


def test_count_live_and_live_pids_consistent() -> None:
    pm = ProcessManager(app_path="x:q")
    fake_a = _FakePopen(pid=1)
    fake_b = _FakePopen(pid=2)
    _install_fake(pm, fake_a)
    _install_fake(pm, fake_b)
    assert pm.count_live() == 2
    assert set(pm.live_pids()) == {1, 2}


def test_shutdown_drains_all_in_parallel() -> None:
    pm, fake_a = _make_pm_with_fake_proc(pid=1)
    fake_b = _FakePopen(pid=2)
    _install_fake(pm, fake_b)
    pm.shutdown()
    assert fake_a.terminate_calls == 1
    assert fake_b.terminate_calls == 1
    assert pm.count_live() == 0


@pytest.mark.parametrize("wait_raises", [False, True])
def test_terminate_handles_already_dead_process(wait_raises: bool) -> None:
    """Race: process exited between our check and terminate()."""
    pm, fake = _make_pm_with_fake_proc(wait_raises=wait_raises)

    def raise_lookup() -> None:
        raise ProcessLookupError

    fake.terminate = raise_lookup  # type: ignore[method-assign]
    clean = pm.terminate_worker(fake.pid)
    assert clean is True
    assert pm.count_live() == 0
