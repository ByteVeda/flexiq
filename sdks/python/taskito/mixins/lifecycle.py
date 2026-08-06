"""Worker startup banner, run_worker loop, heartbeat, resource status, test mode."""

from __future__ import annotations

import contextlib
import json
import logging
import signal
import sys
import threading
import time
import urllib.parse
import uuid
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

import taskito
from taskito._taskito import PyQueue, PyTaskConfig
from taskito.context import _set_queue_ref
from taskito.dashboard.overrides_store import OverridesStore
from taskito.events import EventType
from taskito.log_config import configure as configure_logging
from taskito.log_config import restore_asyncio_pipe_noise, silence_asyncio_pipe_noise
from taskito.resources.health import HealthChecker
from taskito.resources.runtime import ResourceRuntime
from taskito.testing import TestMode

if TYPE_CHECKING:
    from collections.abc import Callable

    from taskito.mesh import MeshWorker
    from taskito.resources.definition import ResourceDefinition


logger = logging.getLogger("taskito")


class QueueLifecycleMixin:
    """Worker startup, heartbeat, resource status aggregation, and test mode."""

    _inner: PyQueue
    _backend: str
    _db_path: str
    _db_url: str | None
    _schema: str
    _workers: int
    _drain_timeout: int
    _async_concurrency: int
    _periodic_configs: list[dict[str, Any]]
    _resource_definitions: dict[str, ResourceDefinition]
    _resource_runtime: ResourceRuntime | None
    _task_registry: dict[str, Callable]
    _task_configs: list[PyTaskConfig]
    _queue_configs: dict[str, dict[str, Any]]

    def _print_banner(self, queues: list[str]) -> None:
        """Print ASCII startup banner."""
        banner = rf"""
 _            _    _ _
| |_ __ _ ___| | _(_) |_ ___
| __/ _` / __| |/ / | __/ _ \
| || (_| \__ \   <| | || (_) |
 \__\__,_|___/_|\_\_|\__\___/  v{taskito.__version__}
"""
        lines = [banner]
        lines.append(f"> Backend:     {self._backend}")
        if self._backend == "sqlite":
            lines.append(f"> DB:          {self._db_path}")
        else:
            # Mask password in connection URL for display
            url = self._db_url or ""
            parsed_url = urllib.parse.urlparse(url)
            if parsed_url.password:
                masked = parsed_url._replace(
                    netloc=f"{parsed_url.username}:****@{parsed_url.hostname}"
                    + (f":{parsed_url.port}" if parsed_url.port else "")
                )
                url = urllib.parse.urlunparse(masked)
            lines.append(f"> DB:          {url}")
            lines.append(f"> Schema:      {self._schema}")
        lines.append(f"> Concurrency: {self._workers} (threads)")
        lines.append(f"> Queues:      {', '.join(queues)}")
        lines.append("")

        task_names = sorted(self._task_registry.keys())
        if task_names:
            lines.append("[tasks]")
            for name in task_names:
                lines.append(f"  . {name}")
            lines.append("")

        if self._periodic_configs:
            lines.append("[periodic]")
            for pc in self._periodic_configs:
                lines.append(f"  . {pc['name']}  ({pc['cron_expr']})")
            lines.append("")

        if self._resource_definitions:
            lines.append("[resources]")
            for rname, rdef in sorted(self._resource_definitions.items()):
                deps = f" (depends: {', '.join(rdef.depends_on)})" if rdef.depends_on else ""
                lines.append(f"  . {rname}{deps}")
            lines.append("")

        print("\n".join(lines))

    def shutdown(self) -> None:
        """Request graceful worker shutdown (drain running tasks, then stop).

        Programmatic equivalent of SIGINT/SIGTERM: the scheduler stops
        dispatching and :meth:`run_worker` returns once running tasks finish,
        bounded by the drain timeout. Non-blocking and safe to call from any
        thread; a no-op when no worker is running.
        """
        self._inner.request_shutdown()

    def run_worker(
        self,
        queues: Sequence[str] | None = None,
        tags: list[str] | None = None,
        pool: str = "thread",
        app: str | None = None,
        mesh: MeshWorker | None = None,
    ) -> None:
        """Start the worker loop. Blocks until interrupted.

        Args:
            queues: List of queue names to consume from. ``None`` consumes
                from all queues.
            tags: Optional tags for worker specialization / routing.
            pool: Worker pool type — ``"thread"`` (default) or ``"prefork"``.
                Prefork spawns child processes with independent GILs for
                true parallelism on CPU-bound tasks.
            app: Import path to the Queue instance (e.g. ``"myapp:queue"``).
                Required when ``pool="prefork"``.
            mesh: Mesh scheduling config. Pass a ``MeshWorker`` instance to
                enable gossip-based worker discovery, task affinity, and
                work-stealing. Requires the ``mesh`` cargo feature.
        """
        if pool == "prefork":
            if sys.platform == "win32":
                raise NotImplementedError(
                    "pool='prefork' is not supported on Windows. "
                    "Use pool='thread' (default) or run on Linux/macOS."
                )
            if not app:
                raise ValueError("app= is required when pool='prefork' (e.g. app='myapp:queue')")
        queue_list = list(queues) if queues else None

        # Make queue accessible from job context (for current_job.update_progress())
        _set_queue_ref(self)

        # Register periodic tasks with Rust scheduler
        for pc in self._periodic_configs:
            self._inner.register_periodic(
                name=pc["name"],
                task_name=pc["task_name"],
                cron_expr=pc["cron_expr"],
                args=pc["payload"],
                queue=pc["queue"],
                timezone=pc.get("timezone"),
            )

        configure_logging()

        worker_queues = queue_list or ["default"]
        self._print_banner(worker_queues)

        # Initialize worker resources (before Rust dispatches tasks)
        health_checker = None
        if self._resource_definitions:
            self._resource_runtime = ResourceRuntime(self._resource_definitions)
            self._resource_runtime.initialize()
            logger.info(
                "Initialized %d resource(s): %s",
                len(self._resource_definitions),
                ", ".join(self._resource_runtime._init_order),
            )
            health_checker = HealthChecker(self._resource_runtime)
            health_checker.start()

        # Set up signal handlers for graceful shutdown (only in main thread)
        is_main = threading.current_thread() is threading.main_thread()
        original_sigint = None
        original_sigterm = None

        if is_main:
            original_sigint = signal.getsignal(signal.SIGINT)
            original_sigterm = signal.getsignal(signal.SIGTERM)

            def shutdown_handler(signum: int, frame: Any) -> None:
                logger.info("Warm shutdown (waiting for running tasks to finish)...")
                # Subprocesses spawned by user tasks (e.g. Playwright browsers)
                # receive the same SIGINT via the foreground process group; the
                # resulting broken-pipe spam from asyncio would otherwise drown
                # out the drain window. Suppressed only for the drain — the
                # filter is removed in the finally block below.
                silence_asyncio_pipe_noise()
                with contextlib.suppress(Exception):
                    self._inner.set_worker_status(worker_id, "draining")
                self._inner.request_shutdown()
                # Restore original handlers so a second signal force-kills
                signal.signal(signal.SIGINT, original_sigint)
                signal.signal(signal.SIGTERM, original_sigterm)

            signal.signal(signal.SIGINT, shutdown_handler)
            signal.signal(signal.SIGTERM, shutdown_handler)

            # SIGHUP handler for hot-reloading resources (Unix only)
            if hasattr(signal, "SIGHUP"):

                def sighup_handler(signum: int, frame: Any) -> None:
                    logger.info("SIGHUP received — reloading reloadable resources")
                    if self._resource_runtime is not None:
                        results = self._resource_runtime.reload()
                        for rname, success in results.items():
                            logger.info(
                                "Reload %s: %s",
                                rname,
                                "OK" if success else "FAILED",
                            )

                signal.signal(signal.SIGHUP, sighup_handler)

        # Serialize resource names for worker advertisement
        resources_json: str | None = None
        if self._resource_definitions:
            resources_json = json.dumps(sorted(self._resource_definitions.keys()))

        # Generate worker ID and start Python-side heartbeat thread
        worker_id = str(uuid.uuid4())

        # Flush topic subscriptions now that the ephemeral ones have an owner.
        self._declare_worker_subscriptions(worker_id)  # type: ignore[attr-defined]

        # Managed log-topic consumers: daemon threads that pull, invoke, and ack.
        stop_log_consumers = threading.Event()
        log_consumer_threads = self._build_log_consumer_threads(stop_log_consumers)  # type: ignore[attr-defined]
        for consumer in log_consumer_threads:
            consumer.start()

        stop_heartbeat = threading.Event()
        heartbeat_thread = threading.Thread(
            target=self._run_heartbeat,
            args=(worker_id, stop_heartbeat),
            daemon=True,
            name="taskito-heartbeat",
        )
        heartbeat_thread.start()

        self._emit_event(  # type: ignore[attr-defined]
            EventType.WORKER_STARTED,
            {"worker_id": worker_id, "queues": worker_queues},
        )
        self._emit_event(  # type: ignore[attr-defined]
            EventType.WORKER_ONLINE,
            {"worker_id": worker_id, "queues": worker_queues, "pool": pool},
        )

        try:
            overrides = OverridesStore(self)  # type: ignore[arg-type]
            # Mutate the in-memory PyTaskConfig list so the Rust scheduler
            # sees the override values; merge queue-level overrides into
            # the JSON blob passed to run_worker. Paused tasks/queues get
            # their pause state propagated to the existing paused_queues
            # mechanism for tasks-by-queue, but per-task pause is left to
            # the application-level guard in enqueue (out of scope here).
            paused_tasks = overrides.apply_task_overrides(self._task_configs)
            if paused_tasks:
                logger.info("Paused task overrides in effect: %s", paused_tasks)
            merged_queue_configs = overrides.apply_queue_overrides(self._queue_configs)
            for queue_name, slot in merged_queue_configs.items():
                if slot.get("paused"):
                    try:
                        self.pause(queue_name)  # type: ignore[attr-defined]
                    except Exception:
                        logger.exception("Failed to apply paused state for queue %s", queue_name)
            queue_configs_json = json.dumps(merged_queue_configs) if merged_queue_configs else None
            mesh_config_json = mesh.to_json() if mesh is not None else None
            self._inner.run_worker(
                task_registry=self._task_registry,
                task_configs=self._task_configs,
                queues=queue_list,
                drain_timeout_secs=self._drain_timeout,
                tags=",".join(tags) if tags else None,
                worker_id=worker_id,
                resources=resources_json,
                threads=self._workers,
                async_concurrency=self._async_concurrency,
                queue_configs=queue_configs_json,
                pool=pool if pool != "thread" else None,
                app_path=app,
                mesh_config=mesh_config_json,
            )
        except KeyboardInterrupt:
            logger.info("Cold shutdown (terminating immediately)")
        finally:
            self._emit_event(  # type: ignore[attr-defined]
                EventType.WORKER_STOPPED,
                {"worker_id": worker_id},
            )
            # Stop the consumer poll loops before waiting on anything else: their
            # per-interval reads otherwise keep contending for the storage lock
            # while the heartbeat makes its final round trip.
            stop_log_consumers.set()
            stop_heartbeat.set()
            heartbeat_thread.join(timeout=6)
            # Join managed log consumers before tearing down resources a handler
            # might still hold: give them a shared deadline (the worker drain
            # timeout) to finish their in-flight message, not a fixed slice each.
            consumer_deadline = time.monotonic() + self._drain_timeout
            for consumer in log_consumer_threads:
                consumer.join(timeout=max(0.0, consumer_deadline - time.monotonic()))
            # Tear down resources before stopping async loop
            if health_checker is not None:
                health_checker.stop()
            if self._resource_runtime is not None:
                self._resource_runtime.teardown()
                self._resource_runtime = None
            restore_asyncio_pipe_noise()
            logger.info("Worker stopped.")
            if is_main:
                if original_sigint is not None:
                    signal.signal(signal.SIGINT, original_sigint)
                if original_sigterm is not None:
                    signal.signal(signal.SIGTERM, original_sigterm)

    def _build_resource_health_json(self) -> str | None:
        """Snapshot current resource health as JSON for heartbeat."""
        if not self._resource_definitions:
            return None
        runtime = self._resource_runtime
        health: dict[str, str] = {}
        for name in self._resource_definitions:
            if runtime is not None and name in runtime._unhealthy:
                health[name] = "unhealthy"
            else:
                health[name] = "healthy"
        return json.dumps(health)

    def _run_heartbeat(
        self,
        worker_id: str,
        stop_event: threading.Event,
    ) -> None:
        """Send periodic heartbeats to storage with current resource health."""
        prev_unhealthy: set[str] = set()
        while not stop_event.is_set():
            resource_health = self._build_resource_health_json()
            try:
                reaped_ids = self._inner.worker_heartbeat(worker_id, resource_health)
                # Emit WORKER_OFFLINE events for reaped dead workers
                for rid in reaped_ids:
                    self._emit_event(EventType.WORKER_OFFLINE, {"worker_id": rid})  # type: ignore[attr-defined]
                # Dead workers gone from the registry → drop their ephemeral
                # topic subscriptions on the same cadence, under the same reaper
                # election so only one worker sweeps.
                self._inner.reap_ephemeral_subscriptions(worker_id)
            except Exception:
                logger.debug("Heartbeat failed", exc_info=True)

            # Detect health transitions → emit WORKER_UNHEALTHY
            runtime = self._resource_runtime
            if runtime is not None:
                current_unhealthy = set(runtime._unhealthy)
                new_unhealthy = current_unhealthy - prev_unhealthy
                if new_unhealthy:
                    self._emit_event(  # type: ignore[attr-defined]
                        EventType.WORKER_UNHEALTHY,
                        {
                            "worker_id": worker_id,
                            "resources": sorted(new_unhealthy),
                        },
                    )
                prev_unhealthy = current_unhealthy

            stop_event.wait(timeout=5.0)

    # -- Resource Status --

    def resource_status(self) -> list[dict[str, Any]]:
        """Return per-resource status info.

        Each entry contains: name, scope, health, init_duration_ms,
        recreations, depends_on. If this process is running the worker, the
        live in-process runtime is authoritative. Otherwise (e.g. the
        dashboard is a separate process), health is reconstructed from the
        latest heartbeat each worker pushed via ``worker_heartbeat``.
        Returns an empty list when nothing is registered and no worker has
        reported yet.
        """
        if self._resource_runtime is not None:
            return self._resource_runtime.status()
        return self._resource_status_from_heartbeats()

    def _resource_status_from_heartbeats(self) -> list[dict[str, Any]]:
        """Fallback path when the runtime isn't in this process.

        Aggregates each worker's ``resource_health`` JSON snapshot into a
        status list shaped like ``ResourceRuntime.status()``. The worst
        report wins: any ``unhealthy`` → ``unhealthy``; all ``healthy`` →
        ``healthy``; anything else → ``degraded``; no worker reporting a
        given resource → ``not_initialized``.
        """
        observed: dict[str, list[str]] = {}
        try:
            workers = self._inner.list_workers()
        except Exception:
            logger.warning("resource_status: failed to list workers", exc_info=True)
            workers = []

        for worker in workers:
            raw = worker.get("resource_health")
            if not raw:
                continue
            try:
                report = json.loads(raw)
            except (TypeError, ValueError):
                continue
            if not isinstance(report, dict):
                continue
            for name, health in report.items():
                observed.setdefault(str(name), []).append(str(health).lower())

        # Build an entry for every registered definition, joined with any
        # resource a live worker reports (covers the case where the
        # dashboard process has no definitions registered at all).
        names = set(self._resource_definitions.keys()) | set(observed.keys())
        result: list[dict[str, Any]] = []
        for name in sorted(names):
            defn = self._resource_definitions.get(name)
            healths = observed.get(name, [])
            if not healths:
                health = "not_initialized"
            elif any(h == "unhealthy" for h in healths):
                health = "unhealthy"
            elif all(h == "healthy" for h in healths):
                health = "healthy"
            else:
                health = "degraded"
            result.append(
                {
                    "name": name,
                    "scope": defn.scope.value if defn is not None else "unknown",
                    "health": health,
                    "init_duration_ms": 0,
                    "recreations": 0,
                    "depends_on": defn.depends_on if defn is not None else [],
                }
            )
        return result

    # -- Test Mode --

    def test_mode(
        self,
        propagate_errors: bool = False,
        resources: dict[str, Any] | None = None,
    ) -> TestMode:
        """Return a context manager that runs tasks synchronously (no worker needed).

        Args:
            propagate_errors: If True, re-raise task exceptions immediately.
            resources: Dict of resource name → mock instance for injection
                during test mode.

        Returns:
            A :class:`~taskito.testing.TestMode` context manager.
        """
        return TestMode(self, propagate_errors=propagate_errors, resources=resources)  # type: ignore[arg-type]
