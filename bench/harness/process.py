"""Starting a worker, watching what it costs, and making sure it dies.

Two jobs. The first is idle cost: a worker with an empty queue is a worker a
reader is paying for all night, and it is the axis where a polling scheduler is
expected to look worst. The second is cleanup — every runner here is spawned in
its own process group, because a Celery prefork parent that survives the run
holds a broker connection and quietly joins the *next* system's measurement.
"""

from __future__ import annotations

import contextlib
import os
import signal
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

import psutil


class WorkerDied(RuntimeError):
    """The worker exited before it was asked to. Its output is the message."""


@dataclass(frozen=True)
class IdleCost:
    rss_mb_mean: float
    rss_mb_max: float
    cpu_percent: float
    processes: int

    def as_dict(self) -> dict[str, object]:
        return {
            "rss_mb_mean": round(self.rss_mb_mean, 1),
            "rss_mb_max": round(self.rss_mb_max, 1),
            "cpu_percent": round(self.cpu_percent, 2),
            "processes": self.processes,
        }


class Worker:
    """A spawned worker process tree, stopped on the way out of the `with`."""

    def __init__(self, argv: list[str], cwd: Path, env: dict[str, str], log: Path):
        self._argv = argv
        self._cwd = cwd
        self._env = env
        self._log = log
        self._handle: subprocess.Popen[bytes] | None = None

    def __enter__(self) -> Worker:
        self._log.parent.mkdir(parents=True, exist_ok=True)
        self._stream = self._log.open("wb")
        self._handle = subprocess.Popen(
            self._argv,
            cwd=self._cwd,
            env=self._env,
            stdout=self._stream,
            stderr=subprocess.STDOUT,
            # Its own process group: SIGTERM then reaches the prefork children
            # and the thread pools, not just the launcher that outlived them.
            start_new_session=True,
        )
        return self

    def __exit__(self, *_exc: object) -> None:
        handle = self._handle
        if handle is not None and handle.poll() is None:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(os.getpgid(handle.pid), signal.SIGTERM)
            try:
                handle.wait(timeout=20)
            except subprocess.TimeoutExpired:
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(os.getpgid(handle.pid), signal.SIGKILL)
                handle.wait(timeout=10)
        self._stream.close()

    def check_alive(self) -> None:
        handle = self._handle
        assert handle is not None
        if handle.poll() is not None:
            tail = self._log.read_text(errors="replace")[-4000:]
            raise WorkerDied(
                f"{self._argv[0]} exited with {handle.returncode}\n--- worker log ---\n{tail}"
            )

    def _tree(self) -> list[psutil.Process]:
        handle = self._handle
        assert handle is not None
        try:
            parent = psutil.Process(handle.pid)
        except psutil.NoSuchProcess:
            return []
        alive = [parent]
        with contextlib.suppress(psutil.Error):
            alive.extend(parent.children(recursive=True))
        return alive

    def idle_cost(self, seconds: int) -> IdleCost:
        """Sample RSS and CPU with the queue empty.

        Called before the worker has run a job, so this is what a freshly
        provisioned worker costs to keep waiting — not what one retains after
        draining a backlog, which is a different (and also interesting) number
        this harness does not yet take.

        CPU comes from the difference in cumulative CPU time across the window
        rather than from `cpu_percent`, because a prefork pool's children come
        and go and a sampler that only knows the pids it saw first reports a
        fraction of the truth. RSS is sampled every 500 ms and reported as both
        mean and max: a worker that allocates in bursts is a different
        proposition from one that sits flat, and one number hides that.
        """
        self.check_alive()
        start = {p.pid: _cpu_seconds(p) for p in self._tree()}
        samples: list[float] = []
        counts: list[int] = []

        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            tree = self._tree()
            counts.append(len(tree))
            samples.append(sum(_rss_bytes(p) for p in tree) / 1024 / 1024)
            time.sleep(0.5)

        end = {p.pid: _cpu_seconds(p) for p in self._tree()}
        # Children born inside the window count from zero; ones that died take
        # their time with them. Both are the truth about an idle worker that
        # churns processes, which is itself an idle cost.
        burned = sum(after - start.get(pid, 0.0) for pid, after in end.items())
        self.check_alive()

        return IdleCost(
            rss_mb_mean=sum(samples) / len(samples) if samples else 0.0,
            rss_mb_max=max(samples, default=0.0),
            cpu_percent=100.0 * burned / seconds,
            processes=max(counts, default=0),
        )


def _cpu_seconds(proc: psutil.Process) -> float:
    try:
        times = proc.cpu_times()
    except psutil.Error:
        return 0.0
    return times.user + times.system


def _rss_bytes(proc: psutil.Process) -> int:
    try:
        return proc.memory_info().rss
    except psutil.Error:
        return 0


def run_json(argv: list[str], cwd: Path, env: dict[str, str], timeout: int) -> dict:
    """Run a runner's one-shot command and parse the JSON it prints.

    Runners print their result as the last line of stdout so that a framework
    that logs a banner on import — most of them do — cannot corrupt the
    measurement it is reporting.
    """
    import json

    done = subprocess.run(argv, cwd=cwd, env=env, capture_output=True, text=True, timeout=timeout)
    if done.returncode != 0:
        raise RuntimeError(
            f"{' '.join(argv)} exited {done.returncode}\n"
            f"--- stdout ---\n{done.stdout[-2000:]}\n--- stderr ---\n{done.stderr[-4000:]}"
        )
    for line in reversed(done.stdout.strip().splitlines()):
        if line.startswith("{"):
            return json.loads(line)
    raise RuntimeError(f"{' '.join(argv)} printed no JSON result\n{done.stdout[-2000:]}")
