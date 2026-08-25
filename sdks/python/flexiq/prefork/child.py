"""Prefork child worker process.

Each child is an independent Python interpreter that:
1. Imports the app module and builds the task registry.
2. Initializes resources (if any).
3. Completes the ``hello``/``hello_ack`` handshake with the parent.
4. Runs a stdin reader thread that demultiplexes ``job``, ``cancel``, and
   ``shutdown`` frames from the parent. Jobs go on an internal queue;
   cancels populate a local set that ``current_job.check_cancelled()`` reads
   via a registered hook.
5. Pulls jobs off the internal queue on the main thread, executes them,
   and writes result frames to stdout.

Spawned by the Rust ``PreforkPool`` via ``python -m flexiq.prefork <app_path>``.
"""

from __future__ import annotations

import importlib
import logging
import os
import queue as _queue_mod
import signal
import sys
import threading
import time
import traceback
from typing import Any

from flexiq import __version__
from flexiq.async_support.helpers import run_maybe_async
from flexiq.context import (
    _clear_context,
    _set_context,
    _set_queue_ref,
    clear_local_cancel_check,
    set_local_cancel_check,
)
from flexiq.detached import (
    clear_sink,
    install_sink,
    is_detached,
    set_disabled_middleware,
)
from flexiq.exceptions import TaskCancelledError
from flexiq.log_config import silence_asyncio_pipe_noise
from flexiq.steps import StepError, StepSleepSignal
from flexiq.task_errors import encode_task_error
from flexiq.worker_protocol import (
    WORKER_PROTOCOL_VERSION,
    ProtocolError,
    read_frame,
    write_frame,
)

logger = logging.getLogger("flexiq.prefork.child")

# One job at a time per child: each is a whole interpreter.
_SLOTS = 1


def _import_queue(app_path: str) -> Any:
    """Import and return the Queue instance from a dotted path like 'myapp:queue'."""
    if ":" not in app_path:
        raise ValueError(f"Invalid app path '{app_path}': expected 'module:attribute' format")
    module_path, attr_name = app_path.rsplit(":", 1)
    module = importlib.import_module(module_path)
    queue = getattr(module, attr_name)
    # This interpreter is the child's own, so it claims the app's deferred
    # declarations itself. The parent draining them says nothing about here,
    # and a task missing from this registry fails its job non-retryably.
    queue._drain_pending_tasks()
    return queue


# Results are written by the main thread, but a task reporting progress may be
# on any thread it started. Two frames interleaved on one pipe would desync the
# parent's reader for good, so every write takes this.
_stdout_lock = threading.Lock()


def _write_message(header: dict[str, Any], payload: bytes = b"") -> None:
    """Write one frame to the parent."""
    with _stdout_lock:
        write_frame(sys.stdout.buffer, header, payload)


class _ParentSink:
    """Sends this child's progress and task logs on to its parent.

    A detached child has no storage — that is the point of an executor — so
    these travel one hop to the pool and a second to the scheduler, which owns
    the database and applies them. Failures are swallowed: the pipe breaking is
    the parent going away, which the main loop discovers on its own, and a task
    reporting progress must not be the thing that fails.
    """

    def update_progress(self, job_id: str, progress: int) -> None:
        self._send({"type": "progress", "job_id": job_id, "progress": progress})

    def write_task_log(
        self,
        job_id: str,
        task_name: str,
        level: str,
        message: str,
        extra: str | None,
    ) -> None:
        payload = b"" if extra is None else extra.encode()
        self._send(
            {
                "type": "task_log",
                "job_id": job_id,
                "task_name": task_name,
                "level": level,
                "message": message,
                # ``None`` and an empty blob are different, so the length is
                # read off the value rather than off the encoded bytes.
                "extra_len": None if extra is None else len(payload),
            },
            payload,
        )

    @staticmethod
    def _send(header: dict[str, Any], payload: bytes = b"") -> None:
        try:
            _write_message(header, payload)
        except (OSError, EOFError, ValueError, ProtocolError):
            # `OSError` rather than `BrokenPipeError` alone: a closed stream
            # fails with `EBADF`, and `flush` can surface `EPIPE` on its own.
            # Either would otherwise escape into the task body this exists to
            # keep whole.
            logger.debug("could not forward %s to the parent", header["type"], exc_info=True)


def _handshake(queue: Any) -> None:
    """Announce what this child can run and check the parent speaks our version.

    Runs before the stdin reader thread starts so the ack is not consumed by it.
    """
    _write_message(
        {
            "type": "hello",
            "executor_id": f"prefork-{os.getpid()}",
            "sdk": "python",
            "version": __version__,
            "tasks": sorted(queue._task_registry),
            "slots": _SLOTS,
            "protocol_version": WORKER_PROTOCOL_VERSION,
        }
    )

    ack, _ = read_frame(sys.stdin.buffer)
    if ack.get("type") != "hello_ack":
        raise ProtocolError(f"expected hello_ack, got {ack.get('type')!r}")

    theirs = ack.get("protocol_version")
    if theirs != WORKER_PROTOCOL_VERSION:
        raise ProtocolError(
            f"worker protocol mismatch: parent speaks {theirs}, "
            f"we speak {WORKER_PROTOCOL_VERSION} — check FLEXIQ_PYTHON points "
            f"at the interpreter holding the same flexiq install"
        )


class _CancelSignal:
    """Thread-safe set of job IDs the parent has asked us to cancel.

    Cancel messages may arrive before, during, or after the job they target;
    keeping the IDs around until the corresponding result is written means
    a cancel that races a job's start still fires deterministically.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._ids: set[str] = set()

    def request(self, job_id: str) -> None:
        with self._lock:
            self._ids.add(job_id)

    def is_requested(self, job_id: str) -> bool:
        with self._lock:
            return job_id in self._ids

    def discard(self, job_id: str) -> None:
        with self._lock:
            self._ids.discard(job_id)


def _execute_job(
    queue: Any,
    job: dict[str, Any],
    payload: bytes,
) -> tuple[dict[str, Any], bytes]:
    """Execute a single job and return its result frame and result payload."""
    task_name = job["task_name"]
    job_id = job["id"]
    retry_count = job.get("retry_count", 0)
    max_retries = job.get("max_retries", 3)

    logger.debug("executing %s[%s]", task_name, job_id)
    wrapper = queue._task_registry.get(task_name)
    if wrapper is None:
        return {
            "type": "failure",
            "job_id": job_id,
            "error": f"task '{task_name}' not registered",
            "retry_count": retry_count,
            "max_retries": max_retries,
            "task_name": task_name,
            "wall_time_ns": 0,
            "should_retry": False,
            "timed_out": False,
        }, b""

    _set_context(
        job_id,
        task_name,
        retry_count,
        job.get("queue", "default"),
        job.get("namespace"),
    )
    # Resolved by the scheduler and carried on the frame, because an executor
    # has no settings store of its own to read the toggle list from. Empty from
    # an in-process worker's parent, which reads storage directly instead.
    set_disabled_middleware(job.get("disabled_middleware") or ())

    start_ns = time.monotonic_ns()
    try:
        args, kwargs = queue._deserialize_payload(task_name, payload)
        result = run_maybe_async(wrapper(*args, **kwargs))
        result_bytes = queue._serialize_result(task_name, result) if result is not None else None
        wall_time_ns = time.monotonic_ns() - start_ns

        return {
            "type": "success",
            "job_id": job_id,
            "result_len": None if result_bytes is None else len(result_bytes),
            "task_name": task_name,
            "wall_time_ns": wall_time_ns,
        }, result_bytes or b""

    except TaskCancelledError:
        wall_time_ns = time.monotonic_ns() - start_ns
        return {
            "type": "cancelled",
            "job_id": job_id,
            "task_name": task_name,
            "wall_time_ns": wall_time_ns,
        }, b""

    except StepSleepSignal as sleep:
        # The attempt ended in a step.sleep: the row is committed and the job is
        # already Pending at its deadline, so this frame only tells the parent
        # where the job went. ``wake_at`` is the deadline storage settled on,
        # not the one this attempt proposed.
        return {
            "type": "slept",
            "job_id": job_id,
            "task_name": task_name,
            "wake_at": sleep.wake_at,
            "wall_time_ns": time.monotonic_ns() - start_ns,
        }, b""

    except StepError as step_failure:
        # A step failure carries the core's own retry decision, which outranks
        # the task's retry filters below.
        logger.error("task %s[%s] step failed: %s", task_name, job_id, step_failure)
        return {
            "type": "failure",
            "job_id": job_id,
            "error": encode_task_error(step_failure),
            "retry_count": retry_count,
            "max_retries": max_retries,
            "task_name": task_name,
            "wall_time_ns": time.monotonic_ns() - start_ns,
            "should_retry": step_failure.flexiq_should_retry,
            "timed_out": False,
        }, b""

    except Exception:
        wall_time_ns = time.monotonic_ns() - start_ns
        exc = sys.exc_info()[1]
        error_msg = encode_task_error(exc) if exc is not None else traceback.format_exc()
        last_line = traceback.format_exc().splitlines()[-1]
        logger.error("task %s[%s] failed: %s", task_name, job_id, last_line)

        should_retry = True
        filters = queue._task_retry_filters.get(task_name)
        if filters:
            dont_retry_on = filters.get("dont_retry_on", [])
            for cls in dont_retry_on:
                if isinstance(exc, cls):
                    should_retry = False
                    break
            if should_retry:
                retry_on = filters.get("retry_on", [])
                if retry_on:
                    should_retry = any(isinstance(exc, cls) for cls in retry_on)

        return {
            "type": "failure",
            "job_id": job_id,
            "error": error_msg,
            "retry_count": retry_count,
            "max_retries": max_retries,
            "task_name": task_name,
            "wall_time_ns": wall_time_ns,
            "should_retry": should_retry,
            "timed_out": False,
        }, b""

    finally:
        _clear_context()
        set_disabled_middleware(())


def _spawn_stdin_reader(
    job_queue: _queue_mod.Queue[tuple[dict[str, Any], bytes] | None],
    cancels: _CancelSignal,
) -> threading.Thread:
    """Run a background thread that demultiplexes parent → child frames.

    The main thread is blocked inside ``_execute_job`` while a job is
    running, so reading stdin must happen elsewhere. This thread turns the
    frame stream into queue items + cancel-set updates.
    """

    def reader() -> None:
        try:
            while True:
                try:
                    msg, payload = read_frame(sys.stdin.buffer)
                except ProtocolError as e:
                    # A desynced stream cannot be resynchronised: the payload
                    # boundary is lost, so every later frame would be garbage.
                    logger.error("invalid frame from parent: %s", e)
                    return

                msg_type = msg.get("type")
                if msg_type == "shutdown":
                    return
                if msg_type == "job":
                    job_queue.put((msg, payload))
                elif msg_type == "cancel":
                    job_id = msg.get("job_id")
                    if isinstance(job_id, str):
                        cancels.request(job_id)
                else:
                    logger.warning("unknown frame type from parent: %r", msg_type)
        except (BrokenPipeError, EOFError, KeyboardInterrupt):
            logger.debug("child stdin closed")
        finally:
            # Wake the main loop even if stdin closed without a shutdown
            # frame (e.g. the parent died).
            job_queue.put(None)

    thread = threading.Thread(target=reader, name="flexiq-prefork-stdin", daemon=True)
    thread.start()
    return thread


def _install_shutdown_signal_handler() -> None:
    """Mute asyncio's spurious 'pipe closed by peer' WARNING when SIGINT
    arrives, then re-raise ``KeyboardInterrupt`` so the main loop's existing
    cleanup path still runs. Subprocesses the user task spawned (Playwright
    browsers etc.) get the same SIGINT via the foreground process group and
    flood the asyncio logger as they die; the warning is informational only
    (asyncio already swallowed the underlying error) so demoting it to DEBUG
    keeps the child's output readable."""
    if threading.current_thread() is not threading.main_thread():
        return

    def handler(signum: int, frame: Any) -> None:
        silence_asyncio_pipe_noise()
        raise KeyboardInterrupt

    signal.signal(signal.SIGINT, handler)


def main() -> None:
    """Child process main loop. Called via ``python -m flexiq.prefork <app_path>``."""
    if len(sys.argv) < 2:
        sys.stderr.write("Usage: python -m flexiq.prefork <app_path>\n")
        sys.exit(1)

    _install_shutdown_signal_handler()

    app_path = sys.argv[1]

    # Ensure the working directory is on sys.path so module imports
    # resolve the same way as in the parent process.
    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)

    queue = _import_queue(app_path)
    _set_queue_ref(queue)

    runtime = queue._resource_runtime
    if runtime is not None:
        runtime.initialize()

    _handshake(queue)

    job_queue: _queue_mod.Queue[tuple[dict[str, Any], bytes] | None] = _queue_mod.Queue()
    cancels = _CancelSignal()
    set_local_cancel_check(cancels.is_requested)
    # Only under an executor: a child of an in-process worker holds real storage
    # and writes its own progress and logs, so it has nothing to forward.
    if is_detached():
        install_sink(_ParentSink())
    _spawn_stdin_reader(job_queue, cancels)

    logger.info("child ready (app=%s, pid=%d)", app_path, os.getpid())

    try:
        while True:
            item = job_queue.get()
            if item is None:
                break
            job, payload = item
            result, result_payload = _execute_job(queue, job, payload)
            _write_message(result, result_payload)
            # Drop the cancel marker once the result is written so a future
            # job with the same ID (extremely unlikely, but possible across
            # ID-reuse boundaries) does not auto-cancel.
            cancels.discard(result.get("job_id", ""))
    except (BrokenPipeError, EOFError, KeyboardInterrupt):
        logger.debug("child output pipe closed or interrupted")

    finally:
        clear_local_cancel_check()
        clear_sink()
        if runtime is not None:
            try:
                runtime.teardown()
            except Exception:
                logger.warning("resource teardown error", exc_info=True)
