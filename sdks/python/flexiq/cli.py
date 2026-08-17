"""CLI entry point for flexiq worker and info commands."""

from __future__ import annotations

import argparse
import importlib
import os
import signal as sig
import sys
import threading
import time

from flexiq._flexiq import Executor as _Executor
from flexiq.app import Queue
from flexiq.autoscale import AutoscaleConfig, serve_autoscaler
from flexiq.dashboard import serve_dashboard
from flexiq.detached import DETACHED_ENV
from flexiq.log_config import configure as configure_logging
from flexiq.migration_gate import withhold_migrations
from flexiq.scaler import serve_scaler

# How long the executor's main loop blocks between signal checks. Short enough
# that Ctrl-C feels immediate, long enough not to spin.
_EXECUTOR_POLL_MS = 200

# One job at a time unless asked otherwise: each slot is a whole interpreter.
_EXECUTOR_DEFAULT_SLOTS = 1


def _build_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser with all subcommands."""
    parser = argparse.ArgumentParser(
        prog="flexiq",
        description="flexiq — Rust-powered task queue for Python",
    )
    subparsers = parser.add_subparsers(dest="command")

    # worker subcommand
    worker_parser = subparsers.add_parser("worker", help="Start a worker process")
    worker_parser.add_argument(
        "--app",
        required=True,
        help="Python path to the Queue instance (e.g., 'myapp.tasks:queue')",
    )
    worker_parser.add_argument(
        "--queues",
        default=None,
        help="Comma-separated list of queues to process (default: all registered)",
    )
    worker_parser.add_argument(
        "--drain-timeout",
        type=int,
        default=None,
        help="Seconds to wait for in-flight jobs during shutdown (default: 30)",
    )
    worker_parser.add_argument(
        "--pool",
        choices=["thread", "prefork"],
        default="thread",
        help="Worker pool: 'thread' (default) or 'prefork' for true CPU parallelism",
    )

    # executor subcommand
    exec_parser = subparsers.add_parser(
        "executor",
        help="Run tasks for a detached scheduler instead of polling storage",
        description=(
            "Attach to a flexiq-server scheduler and run its jobs in this process. "
            "Needs no database credentials: everything a task needs arrives on the wire."
        ),
    )
    exec_parser.add_argument(
        "--app",
        required=True,
        help="Python path to the Queue instance (e.g., 'myapp.tasks:queue')",
    )
    exec_parser.add_argument(
        "--attach",
        default=None,
        help=(
            "Scheduler address: host:port, :port, or unix:/run/flexiq.sock "
            "(default: $FLEXIQ_ATTACH)"
        ),
    )
    exec_parser.add_argument(
        "--slots",
        type=int,
        default=None,
        help=("Jobs to run concurrently, one prefork child each (default: $FLEXIQ_SLOTS, or 1)"),
    )
    exec_parser.add_argument(
        "--executor-id",
        default=None,
        help="Identity announced to the scheduler (default: generated per process)",
    )

    # dashboard subcommand
    dash_parser = subparsers.add_parser("dashboard", help="Start the web dashboard")
    dash_parser.add_argument(
        "--app",
        required=True,
        help="Python path to the Queue instance (e.g., 'myapp.tasks:queue')",
    )
    dash_parser.add_argument(
        "--host", default="127.0.0.1", help="Bind address (default: 127.0.0.1)"
    )
    dash_parser.add_argument("--port", type=int, default=8080, help="Bind port (default: 8080)")
    dash_parser.add_argument(
        "--auth",
        action="store_true",
        default=False,
        help="Enable session authentication (login/setup, CSRF, RBAC); off by default",
    )
    dash_parser.add_argument(
        "--insecure-cookies",
        action="store_true",
        default=False,
        help="Drop the Secure flag on session cookies (local HTTP dev only)",
    )

    # info subcommand
    info_parser = subparsers.add_parser("info", help="Show queue statistics")
    info_parser.add_argument(
        "--app",
        required=True,
        help="Python path to the Queue instance (e.g., 'myapp.tasks:queue')",
    )
    info_parser.add_argument(
        "--watch",
        action="store_true",
        default=False,
        help="Continuously refresh stats every 2 seconds",
    )

    # migrate subcommand
    migrate_parser = subparsers.add_parser(
        "migrate",
        help="Apply pending schema changes (for a deployment that gates DDL)",
    )
    migrate_parser.add_argument(
        "--app",
        required=True,
        help="Python path to the Queue instance (e.g., 'myapp.tasks:queue')",
    )

    # pause subcommand
    pause_parser = subparsers.add_parser("pause", help="Pause a queue")
    pause_parser.add_argument(
        "--app",
        required=True,
        help="Python path to the Queue instance",
    )
    pause_parser.add_argument("queue_name", help="Queue name to pause")

    # resume subcommand
    resume_parser = subparsers.add_parser("resume", help="Resume a paused queue")
    resume_parser.add_argument(
        "--app",
        required=True,
        help="Python path to the Queue instance",
    )
    resume_parser.add_argument("queue_name", help="Queue name to resume")

    # scaler subcommand
    scaler_parser = subparsers.add_parser("scaler", help="Start a lightweight KEDA metrics server")
    scaler_parser.add_argument(
        "--app",
        required=True,
        help="Python path to the Queue instance (e.g., 'myapp.tasks:queue')",
    )
    scaler_parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Bind address (default: 127.0.0.1; use 0.0.0.0 only behind a network boundary)",
    )
    scaler_parser.add_argument("--port", type=int, default=9091, help="Bind port (default: 9091)")
    scaler_parser.add_argument(
        "--target-queue-depth",
        type=int,
        default=10,
        help="Scaling target hint for KEDA (default: 10)",
    )

    # autoscale subcommand
    auto_parser = subparsers.add_parser("autoscale", help="Start the bare-metal autoscaler")
    auto_parser.add_argument(
        "--app",
        required=True,
        help="Python path to the Queue instance (e.g., 'myapp.tasks:queue')",
    )
    auto_parser.add_argument(
        "--min-workers", type=int, default=1, help="Minimum worker count (default: 1)"
    )
    auto_parser.add_argument(
        "--max-workers", type=int, default=10, help="Maximum worker count (default: 10)"
    )
    auto_parser.add_argument(
        "--target-queue-depth",
        type=int,
        default=15,
        help="Target queue depth per worker (default: 15)",
    )
    auto_parser.add_argument(
        "--target-utilisation",
        type=float,
        default=0.75,
        help="Target worker utilisation 0.0-1.0 (default: 0.75)",
    )
    auto_parser.add_argument(
        "--scale-up-window",
        type=int,
        default=0,
        help="Seconds of sustained demand before scaling up (default: 0)",
    )
    auto_parser.add_argument(
        "--scale-down-window",
        type=int,
        default=300,
        help="Seconds of low demand before scaling down (default: 300)",
    )
    auto_parser.add_argument(
        "--tolerance", type=float, default=0.1, help="Scaling tolerance band (default: 0.1)"
    )
    auto_parser.add_argument(
        "--poll-interval",
        type=int,
        default=5,
        help="Seconds between scaling decisions (default: 5)",
    )
    auto_parser.add_argument(
        "--drain-timeout",
        type=int,
        default=30,
        help="Seconds to wait for in-flight jobs during scale-down (default: 30)",
    )
    auto_parser.add_argument(
        "--threads-per-worker",
        type=int,
        default=4,
        help="Worker threads per process (default: 4)",
    )

    # resources subcommand
    res_parser = subparsers.add_parser("resources", help="Show registered resources")
    res_parser.add_argument(
        "--app",
        required=True,
        help="Python path to the Queue instance (e.g., 'myapp.tasks:queue')",
    )

    # reload subcommand
    reload_parser = subparsers.add_parser(
        "reload", help="Reload resources on a running worker via SIGHUP"
    )
    reload_parser.add_argument(
        "--pid", type=int, required=True, help="PID of the running worker process"
    )
    reload_parser.add_argument(
        "--resource",
        default=None,
        help="Reload a specific resource (default: all reloadable)",
    )

    return parser


def main() -> None:
    """Parse CLI arguments and dispatch to the appropriate subcommand."""
    # Configure the central flexiq logger before any subcommand runs so
    # ``info``, ``dashboard``, ``scaler``, etc. all share the same sink.
    configure_logging()

    parser = _build_parser()
    args = parser.parse_args()

    if args.command == "worker":
        run_worker(args)
    elif args.command == "executor":
        run_executor(args)
    elif args.command == "dashboard":
        run_dashboard(args)
    elif args.command == "info":
        run_info(args)
    elif args.command == "migrate":
        run_migrate(args)
    elif args.command == "pause":
        queue = _load_queue(args.app)
        queue.pause(args.queue_name)
        print(f"Queue '{args.queue_name}' paused")
    elif args.command == "resume":
        queue = _load_queue(args.app)
        queue.resume(args.queue_name)
        print(f"Queue '{args.queue_name}' resumed")
    elif args.command == "scaler":
        run_scaler(args)
    elif args.command == "autoscale":
        run_autoscale(args)
    elif args.command == "resources":
        run_resources(args)
    elif args.command == "reload":
        run_reload(args)
    else:
        parser.print_help()
        sys.exit(1)


def _load_queue(app_path: str) -> Queue:
    """Import and return a Queue instance from a 'module:attribute' path."""
    if ":" not in app_path:
        print(
            f"Error: --app must be in 'module:attribute' format, got '{app_path}'",
            file=sys.stderr,
        )
        sys.exit(1)

    module_path, attr_name = app_path.rsplit(":", 1)

    # Ensure cwd is on sys.path so local modules can be imported
    if "" not in sys.path and "." not in sys.path:
        sys.path.insert(0, "")

    try:
        module = importlib.import_module(module_path)
    except ImportError as e:
        print(f"Error: could not import module '{module_path}': {e}", file=sys.stderr)
        sys.exit(1)

    try:
        queue = getattr(module, attr_name)
    except AttributeError:
        print(
            f"Error: module '{module_path}' has no attribute '{attr_name}'",
            file=sys.stderr,
        )
        sys.exit(1)

    if not isinstance(queue, Queue):
        print(
            f"Error: '{app_path}' is not a Queue instance (got {type(queue).__name__})",
            file=sys.stderr,
        )
        sys.exit(1)

    return queue


def run_worker(args: argparse.Namespace) -> None:
    """Import the user's Queue instance and start the worker."""
    queue = _load_queue(args.app)
    if args.drain_timeout is not None:
        queue._drain_timeout = args.drain_timeout
    queues = args.queues.split(",") if args.queues else None
    queue.run_worker(queues=queues, pool=args.pool, app=args.app)


def run_executor(args: argparse.Namespace) -> None:
    """Attach to a detached scheduler and run its jobs in this process.

    The inverse of ``run_worker``: instead of polling storage for work, the
    executor dials a scheduler that already holds the database connection and
    runs whatever it is sent. Only the transport differs — jobs execute on the
    same prefork pool, one child per slot.
    """
    address = args.attach or os.environ.get("FLEXIQ_ATTACH")
    if not address:
        print(
            "Error: --attach is required (or set FLEXIQ_ATTACH), e.g. --attach scheduler:7777",
            file=sys.stderr,
        )
        sys.exit(1)

    slots = _executor_slots(args.slots)

    # Set before the app is imported: building a Queue is what opens storage,
    # and an executor must not. Inherited by the prefork children, which import
    # the same app module in their own interpreters.
    os.environ[DETACHED_ENV] = "1"

    queue = _load_queue(args.app)
    tasks = sorted(queue._task_registry)

    try:
        executor = _Executor(
            address,
            args.app,
            tasks,
            slots,
            # Env only, never a flag: a token in argv is visible in `ps` output
            # and lands in shell history.
            os.environ.get("FLEXIQ_ATTACH_TOKEN"),
            args.executor_id,
        )
    except (RuntimeError, ValueError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)

    print(
        f"flexiq executor {executor.executor_id} attached to "
        f"{executor.scheduler_id} at {executor.peer} "
        f"({slots} slot(s), {len(tasks)} task(s)) — Ctrl-C to stop"
    )

    stopping = threading.Event()

    def request_stop(signum: int, frame: object) -> None:
        # Returns immediately; the drain runs on the executor's own threads.
        stopping.set()
        executor.stop()

    sig.signal(sig.SIGTERM, request_stop)
    sig.signal(sig.SIGINT, request_stop)

    # Polled rather than blocked on: a Python signal handler only runs when the
    # main thread holds the GIL, which it cannot do inside a blocking call.
    while not stopping.is_set():
        if executor.wait(_EXECUTOR_POLL_MS):
            break

    executor.shutdown()
    print("flexiq executor detached")


def _executor_slots(flag: int | None) -> int:
    """Resolve the slot count from the flag, then the env, then the default."""
    if flag is not None:
        value = flag
    else:
        raw = os.environ.get("FLEXIQ_SLOTS")
        if raw is None:
            return _EXECUTOR_DEFAULT_SLOTS
        try:
            value = int(raw)
        except ValueError:
            print(
                f"Error: FLEXIQ_SLOTS must be an integer, got '{raw}'",
                file=sys.stderr,
            )
            sys.exit(1)

    if value < 1:
        print(f"Error: slots must be at least 1, got {value}", file=sys.stderr)
        sys.exit(1)
    return value


def run_dashboard(args: argparse.Namespace) -> None:
    """Start the web dashboard."""
    queue = _load_queue(args.app)
    serve_dashboard(
        queue,
        host=args.host,
        port=args.port,
        secure_cookies=not args.insecure_cookies,
        auth_enabled=args.auth,
    )


def run_migrate(args: argparse.Namespace) -> None:
    """Apply pending schema changes and report what ran."""
    # Withheld before the import: the application module builds its own queue,
    # which would otherwise migrate on open and leave this command reporting
    # work it did not do.
    withhold_migrations()
    queue = _load_queue(args.app)
    report = queue.migrate()

    if report["schemaless"]:
        print("Nothing to migrate: this backend stores no schema.")
        return

    for version in report["applied"]:
        print(f"applied {version}")
    for version in report["workflow_applied"]:
        print(f"applied {version} (workflows)")
    if report["archived_jobs"]:
        print(f"archived {report['archived_jobs']} terminal job(s) left by an older version")
    if not report["applied"] and not report["workflow_applied"] and not report["archived_jobs"]:
        print("Already up to date.")


def run_info(args: argparse.Namespace) -> None:
    """Print queue statistics."""
    queue = _load_queue(args.app)

    if args.watch:
        _watch_stats(queue)
    else:
        _print_stats(queue)


def _print_stats(queue: Queue) -> None:
    """Print stats once."""
    stats = queue.stats()
    print("flexiq queue statistics")
    print("-" * 30)
    for key in ("pending", "running", "completed", "failed", "dead", "cancelled"):
        print(f"  {key:<12} {stats.get(key, 0)}")
    total = sum(stats.values())
    print("-" * 30)
    print(f"  {'total':<12} {total}")


def _watch_stats(queue: Queue) -> None:
    """Refresh stats every 2 seconds with throughput."""
    prev_completed = 0
    try:
        while True:
            print("\033[2J\033[H", end="")
            stats = queue.stats()
            completed = stats.get("completed", 0)
            throughput = (completed - prev_completed) / 2.0
            prev_completed = completed

            _print_stats(queue)
            if throughput > 0:
                print(f"\n  throughput   {throughput:.1f} jobs/s")
            print("\nRefreshing every 2s... (Ctrl+C to stop)")
            time.sleep(2)
    except KeyboardInterrupt:
        pass


def run_scaler(args: argparse.Namespace) -> None:
    """Start the lightweight KEDA metrics server."""
    queue = _load_queue(args.app)
    serve_scaler(
        queue,
        host=args.host,
        port=args.port,
        target_queue_depth=args.target_queue_depth,
    )


def run_autoscale(args: argparse.Namespace) -> None:
    """Start the bare-metal autoscaler."""
    queue = _load_queue(args.app)
    try:
        config = AutoscaleConfig(
            app_path=args.app,
            min_workers=args.min_workers,
            max_workers=args.max_workers,
            target_queue_depth_per_worker=args.target_queue_depth,
            target_utilisation=args.target_utilisation,
            scale_up_window_sec=args.scale_up_window,
            scale_down_window_sec=args.scale_down_window,
            tolerance=args.tolerance,
            poll_interval_sec=args.poll_interval,
            drain_timeout_sec=args.drain_timeout,
            threads_per_worker=args.threads_per_worker,
        )
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)
    serve_autoscaler(queue, config)


def run_resources(args: argparse.Namespace) -> None:
    """Print resource status from registered definitions or live runtime."""
    queue = _load_queue(args.app)
    resources = queue.resource_status()
    if not resources:
        print("No resources registered.")
        return

    header = (
        f"{'RESOURCE':<20} {'SCOPE':<10} {'HEALTH':<16} "
        f"{'INIT (ms)':<12} {'RECREATIONS':<14} DEPENDS ON"
    )
    print(header)
    print("-" * len(header))
    for r in resources:
        deps = ", ".join(r["depends_on"]) if r["depends_on"] else "-"
        line = (
            f"{r['name']:<20} {r['scope']:<10} {r['health']:<16} "
            f"{r['init_duration_ms']:<12.2f} {r['recreations']:<14} {deps}"
        )
        print(line)
        # Show pool stats for task-scoped resources
        pool = r.get("pool")
        if pool:
            print(
                f"  {'':>20} pool: "
                f"active={pool['active']} idle={pool['idle']} "
                f"max={pool['size']} "
                f"timeouts={pool['total_timeouts']}"
            )


def run_reload(args: argparse.Namespace) -> None:
    """Send SIGHUP to a running worker to reload resources."""
    if not hasattr(sig, "SIGHUP"):
        print("Error: SIGHUP is not available on this platform", file=sys.stderr)
        sys.exit(1)

    try:
        os.kill(args.pid, sig.SIGHUP)
        print(f"Sent SIGHUP to worker (PID {args.pid})")
    except ProcessLookupError:
        print(f"Error: no process with PID {args.pid}", file=sys.stderr)
        sys.exit(1)
    except PermissionError:
        print(
            f"Error: permission denied sending signal to PID {args.pid}",
            file=sys.stderr,
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
