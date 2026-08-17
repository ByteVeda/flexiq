"""Worker subprocess lifecycle for the bare-metal autoscaler.

The two-step termination (``terminate()`` → ``wait(timeout)`` → ``kill()``)
follows the canonical Python subprocess pattern documented at
https://docs.python.org/3/library/subprocess.html — give the child a chance
to clean up, then escalate if it ignores SIGTERM.

The autoscaler intentionally manages independent OS processes rather than
threads within a single worker. Threads share a GIL; processes don't. For
horizontal scaling we want true parallelism, and we want each worker to
heartbeat independently so the controller can detect and replace crashed
workers via ``reap_dead()``.
"""

from __future__ import annotations

import logging
import os
import signal
import subprocess
import sys
import threading
from collections.abc import Callable
from contextlib import suppress

logger = logging.getLogger("flexiq.autoscale")


class ProcessManager:
    """Spawn, terminate, and reap worker subprocesses by PID.

    Thread-safe — all state mutations go through ``_lock`` so the
    controller's poll loop and signal handlers can both call into the
    manager without racing.
    """

    def __init__(
        self,
        app_path: str,
        drain_timeout_sec: int = 30,
        python_executable: str | None = None,
    ) -> None:
        self.app_path = app_path
        self.drain_timeout_sec = drain_timeout_sec
        self.python_executable = python_executable or sys.executable
        self._lock = threading.Lock()
        self._processes: dict[int, subprocess.Popen] = {}

    def spawn_worker(self) -> int:
        """Spawn a new worker subprocess and return its PID.

        The child invokes ``python -m flexiq worker --app ... --drain-timeout N``
        which boots a normal flexiq worker bound to the same backend as
        the controller. Children inherit stdout / stderr so logs flow to
        the parent's terminal.
        """
        cmd = [
            self.python_executable,
            "-m",
            "flexiq",
            "worker",
            "--app",
            self.app_path,
            "--drain-timeout",
            str(self.drain_timeout_sec),
        ]
        proc = subprocess.Popen(
            cmd,
            start_new_session=True,  # detach from controller's signal group
        )
        with self._lock:
            self._processes[proc.pid] = proc
        logger.info("autoscale: spawned worker pid=%s", proc.pid)
        return proc.pid

    def terminate_worker(self, pid: int) -> bool:
        """Gracefully terminate one worker by PID.

        Sends SIGTERM, waits up to ``drain_timeout_sec + 5`` seconds for
        the worker to exit on its own, then escalates to SIGKILL. Returns
        ``True`` if the worker exited cleanly (SIGTERM did the job),
        ``False`` if we had to SIGKILL.
        """
        with self._lock:
            proc = self._processes.get(pid)
        if proc is None:
            return True

        try:
            proc.terminate()
        except ProcessLookupError:
            self._drop(pid)
            return True

        grace = self.drain_timeout_sec + 5
        try:
            proc.wait(timeout=grace)
            clean = True
        except subprocess.TimeoutExpired:
            logger.warning(
                "autoscale: worker pid=%s did not drain within %ss; SIGKILL",
                pid,
                grace,
            )
            with suppress(ProcessLookupError):
                proc.kill()
            with suppress(subprocess.TimeoutExpired):
                proc.wait(timeout=5)
            clean = False

        self._drop(pid)
        return clean

    def reap_dead(self) -> list[int]:
        """Return PIDs of workers that have exited (cleanly or otherwise).

        Polls each tracked process non-blockingly via ``Popen.poll()``.
        Removes finished processes from the tracking dict. The controller
        calls this every tick to spawn replacements for crashed workers.
        """
        dead: list[int] = []
        with self._lock:
            pids = list(self._processes.keys())
        for pid in pids:
            with self._lock:
                proc = self._processes.get(pid)
            if proc is None:
                continue
            if proc.poll() is not None:
                dead.append(pid)
                self._drop(pid)
                logger.info("autoscale: worker pid=%s exited (rc=%s)", pid, proc.returncode)
        return dead

    def count_live(self) -> int:
        """Number of workers currently tracked (excluding reaped ones)."""
        with self._lock:
            return len(self._processes)

    def live_pids(self) -> list[int]:
        with self._lock:
            return list(self._processes.keys())

    def kill_all(self) -> None:
        """Hard-kill every tracked worker. Used on emergency shutdown."""
        with self._lock:
            procs = list(self._processes.values())
        for proc in procs:
            with suppress(ProcessLookupError):
                proc.kill()
        # Reap any zombies to give the OS a chance to clean up.
        for proc in procs:
            with suppress(subprocess.TimeoutExpired):
                proc.wait(timeout=2)
        with self._lock:
            self._processes.clear()

    def shutdown(self) -> None:
        """Graceful shutdown — SIGTERM all workers in parallel, then reap.

        Spawns one thread per worker so workers drain in parallel rather
        than serially (would otherwise take ``N * drain_timeout`` seconds).
        """
        with self._lock:
            pids = list(self._processes.keys())
        threads: list[threading.Thread] = []
        for pid in pids:
            t = threading.Thread(target=self.terminate_worker, args=(pid,), daemon=True)
            t.start()
            threads.append(t)
        for t in threads:
            t.join(timeout=self.drain_timeout_sec + 10)

    def _drop(self, pid: int) -> None:
        with self._lock:
            self._processes.pop(pid, None)


def install_signal_handlers(on_signal: Callable[[], None]) -> None:
    """Install SIGTERM/SIGINT handlers that call ``on_signal`` then re-raise.

    Per Python's multiprocessing graceful-shutdown best practice: replace
    the main process's signal handlers with our own so we can cleanly
    drain children before exiting.
    """

    def _handler(signum: int, _frame: object) -> None:
        logger.info("autoscale: received %s, draining workers", signal.Signals(signum).name)
        on_signal()

    signal.signal(signal.SIGTERM, _handler)
    signal.signal(signal.SIGINT, _handler)


def get_pgid_of_self() -> int:
    """Helper for tests that want to assert children are in their own pgid."""
    return os.getpgid(os.getpid())
