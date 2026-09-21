"""Worker subprocesses, started and stopped the way the polyglot e2e does it.

Every framework here is measured out-of-process, because that is how all five
are deployed and because an in-process FlexiQ worker would be competing against
four subprocess round trips. The lifecycle rules are the ones
``scripts/polyglot_e2e.py`` already paid for: a new session per worker so a
signal reaches the whole tree, a stack-scoped teardown so a half-started run
still cleans up, and logs on disk so a failure has something to show.
"""

from __future__ import annotations

import os
import signal
import subprocess
import time
from collections.abc import Callable, Sequence
from contextlib import ExitStack, suppress
from pathlib import Path
from types import TracebackType
from typing import IO

#: Seconds a worker gets to honour SIGTERM before it is killed outright.
STOP_GRACE_SECONDS = 15


class BenchError(RuntimeError):
    """A run could not produce a trustworthy measurement."""


class Worker:
    """One worker process, its log file, and its whole process group."""

    def __init__(
        self,
        name: str,
        argv: Sequence[str],
        cwd: Path,
        env: dict[str, str],
        log_path: Path,
    ) -> None:
        self.name = name
        self.argv = list(argv)
        self.cwd = cwd
        self.env = env
        self.log_path = log_path
        self._proc: subprocess.Popen[bytes] | None = None
        self._log: IO[bytes] | None = None

    def start(self) -> Worker:
        self._log = self.log_path.open("wb")
        self._proc = subprocess.Popen(
            self.argv,
            cwd=self.cwd,
            env={**os.environ, **self.env},
            stdout=self._log,
            stderr=subprocess.STDOUT,
            # Its own session, so `killpg` reaches a Celery prefork pool or a
            # Node thread pool rather than only the process we hold.
            start_new_session=True,
        )
        return self

    @property
    def returncode(self) -> int | None:
        return None if self._proc is None else self._proc.poll()

    def stop(self) -> None:
        proc, self._proc = self._proc, None
        if proc is not None and proc.poll() is None:
            # A worker that has already gone is not an error here: the run may
            # be tearing down *because* it died.
            with suppress(ProcessLookupError, PermissionError):
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
            try:
                proc.wait(timeout=STOP_GRACE_SECONDS)
            except subprocess.TimeoutExpired:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                proc.wait(timeout=STOP_GRACE_SECONDS)
        if self._log is not None:
            self._log.close()
            self._log = None

    def pids(self) -> list[int]:
        """The process group's members — what the idle sampler watches."""
        if self._proc is None:
            return []
        return [self._proc.pid]

    def __enter__(self) -> Worker:
        return self.start()

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.stop()


def start_workers(stack: ExitStack, workers: Sequence[Worker]) -> list[Worker]:
    """Start each worker under ``stack`` so a failure part-way still tears down."""
    return [stack.enter_context(worker) for worker in workers]


def assert_alive(workers: Sequence[Worker]) -> None:
    """Fail the moment a worker dies rather than waiting out the drain timeout."""
    for worker in workers:
        code = worker.returncode
        if code is not None:
            raise BenchError(f"worker {worker.name!r} exited with code {code} mid-run")


def wait_until(
    predicate: Callable[[], bool],
    *,
    workers: Sequence[Worker],
    timeout_s: float,
    interval_s: float = 0.25,
    what: str = "condition",
) -> None:
    """Poll ``predicate`` on a monotonic deadline, failing fast on a dead worker."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        assert_alive(workers)
        if predicate():
            return
        time.sleep(interval_s)
    raise BenchError(f"timed out after {timeout_s:.0f}s waiting for {what}")


def tail(path: Path, lines: int = 40) -> str:
    if not path.exists():
        return "(no log)"
    return "\n".join(path.read_text(errors="replace").splitlines()[-lines:])
