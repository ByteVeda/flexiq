"""Main Queue class.

The Queue class is composed from multiple mixins for organization. Methods
shared across the inspection, operations, lock, decorator, resource, event,
and lifecycle surfaces live in the corresponding mixin under
``flexiq.mixins.*``. Async wrappers come from ``flexiq.async_support.mixins``.

The Queue class itself (this file) handles only:
- Constructor and storage backend initialization
- enqueue() / enqueue_many() job submission
- Internal helpers (``_get_serializer``, ``_deserialize_payload``)

Task-body wrapping (``_wrap_task``) and the per-task middleware chain
(``_get_middleware_chain``) live on ``QueueDecoratorMixin`` —
``_wrap_task`` is invoked from the decorator hot path, so co-locating it
with the decorator keeps the interactions in one file.
"""

from __future__ import annotations

import hashlib
import inspect
import logging
import os
from collections import Counter
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Any, cast

from flexiq._flexiq import PyQueue
from flexiq.async_support.mixins import AsyncQueueMixin
from flexiq.batching import BatchAccumulator, BatchConfig
from flexiq.codecs import CodecSerializer, PayloadCodec
from flexiq.debounce import (
    DebounceConfig,
    Duration,
    normalize_debounce,
    queue_full_counts,
    resolve_debounce_key,
)
from flexiq.detached import DetachedNative, is_detached
from flexiq.enums import StorageBackend, coerce_enum
from flexiq.events import EventBus, EventType
from flexiq.exceptions import QueueFullError, SerializationError
from flexiq.interception import ArgumentInterceptor, InterceptionMode
from flexiq.interception.built_in import build_default_registry
from flexiq.interception.metrics import InterceptionMetrics
from flexiq.middleware import TaskMiddleware
from flexiq.migration_gate import migrations_withheld
from flexiq.mixins import (
    QueueDecoratorMixin,
    QueueEventsMixin,
    QueueInspectionMixin,
    QueueLifecycleMixin,
    QueueLockMixin,
    QueueMiddlewareAdminMixin,
    QueueOperationsMixin,
    QueueOverridesMixin,
    QueuePeriodicMixin,
    QueuePredicateMixin,
    QueuePubSubMixin,
    QueueResourceMixin,
    QueueRuntimeConfigMixin,
    QueueSettingsMixin,
)
from flexiq.notes import validate_and_encode_notes
from flexiq.proxies import ProxyRegistry
from flexiq.proxies.built_in import BuiltInProxy, register_builtin_handlers
from flexiq.proxies.metrics import ProxyMetrics
from flexiq.registry import ClaimedTask
from flexiq.result import JobResult
from flexiq.retention import Retention
from flexiq.serializers import Serializer, SmartSerializer
from flexiq.webhooks import WebhookManager

if TYPE_CHECKING:
    from flexiq._flexiq import PyTaskConfig
    from flexiq.resources.definition import ResourceDefinition
    from flexiq.resources.runtime import ResourceRuntime

try:
    from flexiq.workflows.mixins import QueueWorkflowMixin
    from flexiq.workflows.tracker import WorkflowTracker

    _WORKFLOWS_AVAILABLE = True
except ImportError:  # pragma: no cover - workflows feature not compiled in

    class QueueWorkflowMixin:  # type: ignore[no-redef]
        pass

    WorkflowTracker = None  # type: ignore[assignment,misc]
    _WORKFLOWS_AVAILABLE = False

logger = logging.getLogger("flexiq")

# Hard cap on a single serialized task payload. Unbounded payloads let any
# producer exhaust DB/Redis storage and spike per-job worker memory, so reject
# oversized ones at enqueue time. Override per-Queue via ``max_payload_bytes``
# (set to 0 to disable).
DEFAULT_MAX_PAYLOAD_BYTES = 1 * 1024 * 1024  # 1 MiB


class Queue(
    QueueDecoratorMixin,
    QueuePredicateMixin,
    QueuePubSubMixin,
    QueueRuntimeConfigMixin,
    QueueResourceMixin,
    QueueEventsMixin,
    QueueLifecycleMixin,
    QueueInspectionMixin,
    QueueOperationsMixin,
    QueuePeriodicMixin,
    QueueLockMixin,
    QueueMiddlewareAdminMixin,
    QueueOverridesMixin,
    QueueSettingsMixin,
    QueueWorkflowMixin,
    AsyncQueueMixin,
):
    """
    Rust-powered task queue with embedded SQLite storage.

    Usage::

        queue = Queue()

        @queue.task()
        def send_email(to, subject, body):
            ...

        job = send_email.delay("user@example.com", "Hello", "World")
        queue.run_worker()  # in another process/thread
    """

    def __init__(
        self,
        db_path: str = ".flexiq/flexiq.db",
        workers: int = 0,
        default_retry: int = 3,
        default_timeout: int = 300,
        default_priority: int = 0,
        result_ttl: int | None = None,
        serializer: Serializer | None = None,
        codec: PayloadCodec | Sequence[PayloadCodec] | None = None,
        codecs: dict[str, PayloadCodec] | None = None,
        middleware: list[TaskMiddleware] | None = None,
        backend: StorageBackend | str = StorageBackend.SQLITE,
        db_url: str | None = None,
        schema: str = "flexiq",
        pool_size: int | None = None,
        drain_timeout: int = 30,
        interception: InterceptionMode | str = InterceptionMode.OFF,
        max_intercept_depth: int = 10,
        recipe_signing_key: str | None = None,
        max_reconstruction_timeout: int = 10,
        file_path_allowlist: list[str] | None = None,
        disabled_proxies: Sequence[BuiltInProxy | str] | None = None,
        async_concurrency: int = 100,
        event_workers: int = 4,
        scheduler_poll_interval_ms: int = 50,
        scheduler_reap_interval: int = 100,
        scheduler_cleanup_interval: int = 1200,
        scheduler_batch_size: int = 1,
        namespace: str | None = None,
        push_dispatch: bool = False,
        dlq_auto_retry_delay: int | None = None,
        dlq_auto_retry_max: int = 1,
        retention: Retention | None = None,
        max_pending: dict[str, int] | None = None,
        auto_migrate: bool = True,
    ):
        """Initialize a new task queue.

        Args:
            db_path: Path to the SQLite database file. Defaults to
                ``.flexiq/flexiq.db``. Parent directories are created
                automatically. Ignored when backend is ``"postgres"``.
            workers: Number of worker threads (0 = auto-detect CPU count).
            default_retry: Default max retry attempts for tasks.
            default_timeout: Default task timeout in seconds.
            default_priority: Default task priority (higher = more urgent).
            result_ttl: Auto-cleanup completed/dead jobs older than this many
                seconds. Must be non-negative. None disables queue-wide
                auto-cleanup; per-job TTLs are still honored. Superseded by
                ``retention`` — prefer that for new code.
            retention: Per-table retention windows (:class:`Retention`). When
                set, wins over ``result_ttl``. None falls back to ``result_ttl``.
            serializer: Serializer for task payloads. Defaults to SmartSerializer
                (msgpack with cloudpickle fallback).
            codec: Global payload codec chain — a single :class:`PayloadCodec`
                or a sequence applied in order after serialization (and in
                reverse before deserialization). Wraps the queue serializer,
                so it covers every payload and result. Jobs persisted before
                the chain was enabled cannot be decoded through it. Note:
                nondeterministic codecs (e.g. ``AesGcmCodec``) make
                ``idempotent=True`` auto-keys nondeterministic per call —
                pass an explicit ``idempotency_key`` instead.
            codecs: Named codec registry for per-task codecs. Tasks opt in
                via ``@queue.task(codecs=["name", ...])``; applies to task
                payloads only (results stay on the queue serializer).
            middleware: List of global middleware instances applied to all tasks.
            backend: Storage backend — a
                :class:`~flexiq.enums.StorageBackend` or its string:
                ``"sqlite"`` (default), ``"postgres"``, or ``"redis"``.
            db_url: PostgreSQL connection URL (required when backend is ``"postgres"``).
                Example: ``"postgresql://user:pass@localhost/flexiq"``.
            schema: PostgreSQL schema name for all flexiq tables. Defaults to
                ``"flexiq"``. Ignored when backend is ``"sqlite"``.
            pool_size: Maximum number of connections in the database connection
                pool. Defaults to 10. Useful for managed services like Supabase
                that have low connection limits.
            drain_timeout: Seconds to wait for in-flight jobs to finish during
                graceful shutdown. Defaults to 30.
            interception: Argument interception mode — an
                :class:`~flexiq.interception.InterceptionMode` or its string:
                ``"strict"`` (reject non-serializable args), ``"lenient"`` (warn
                and drop), or ``"off"`` (disabled, default). See
                :mod:`flexiq.interception`.
            max_intercept_depth: Maximum recursion depth for argument walking.
                Defaults to 10.
            recipe_signing_key: HMAC-SHA256 key for proxy recipe integrity.
                Falls back to ``FLEXIQ_RECIPE_SECRET`` env var.
            max_reconstruction_timeout: Max seconds for proxy reconstruction.
            file_path_allowlist: Allowed file paths for the file proxy handler.
            disabled_proxies: Built-in proxy handlers to skip registering —
                :class:`~flexiq.proxies.built_in.BuiltInProxy` members or their
                string ids. An unknown id raises rather than silently leaving
                the handler registered.
            async_concurrency: Maximum number of async tasks running concurrently
                on the native async executor. Defaults to 100.
            event_workers: Thread pool size for the event bus (default 4).
            scheduler_poll_interval_ms: Milliseconds between scheduler poll
                cycles (default 50).
            scheduler_reap_interval: Reap stale jobs every N poll iterations
                (default 100).
            scheduler_cleanup_interval: Cleanup old jobs every N poll iterations
                (default 1200).
            scheduler_batch_size: Maximum number of jobs the scheduler claims
                per dispatch round. ``1`` (default) preserves one-job-per-round
                behavior; higher values batch-claim for greater throughput.
            push_dispatch: Opt into event-driven dispatch, where an enqueue
                wakes the scheduler immediately instead of waiting for the next
                poll. Honored only when the native module was built with the
                ``push-dispatch`` cargo feature; otherwise accepted and ignored
                (polling is kept). Defaults to ``False``.
            dlq_auto_retry_delay: Minimum age in seconds before a DLQ entry
                is automatically retried. ``None`` disables auto-retry.
            dlq_auto_retry_max: Maximum number of DLQ auto-retries per entry
                before giving up. Defaults to 1.
            auto_migrate: Whether opening applies pending schema changes.
                ``True`` (default) keeps the existing behavior. ``False`` gates
                every schema change behind :meth:`migrate`, for a deployment
                whose database credentials do not permit DDL at runtime. On
                SQLite and PostgreSQL a database that has never been migrated
                has no tables, so queries fail until ``migrate`` has run —
                typically from a separate process, after which an application
                opened this way works normally. Redis stores no schema and is
                unaffected either way.
            max_pending: Opt-in per-queue admission cap, mapping queue name to a
                maximum pending backlog. When set, ``enqueue``/``enqueue_many``
                raise :class:`QueueFullError` once a queue's pending count
                reaches its cap. Queues absent from the map are uncapped (zero
                overhead). The check is a non-atomic count-then-insert, so brief
                overshoot is possible under concurrent producers — the same soft
                guarantee as the rate limiter. Also settable at runtime via
                ``set_queue_max_pending``.
        """
        # Unwrap the enum to its wire string; a raw string still passes through
        # so the native layer's aliases (e.g. "postgresql") keep working.
        if isinstance(backend, StorageBackend):
            backend = backend.value

        # An executor imports this app only to find its handlers; opening
        # storage here would put the database credentials back in the app image
        # that the attach split exists to keep them out of.
        detached = is_detached()

        # `flexiq migrate` imports the same app to reach its queue, so the
        # import itself would apply the DDL the command exists to gate. The
        # env var only ever withholds migrations, never grants them.
        if migrations_withheld():
            auto_migrate = False

        if backend == "sqlite" and not detached:
            # Ensure parent directory exists for SQLite
            db_dir = os.path.dirname(db_path)
            if db_dir:
                os.makedirs(db_dir, exist_ok=True)

        # `cast`, not a union: the stand-in answers the job-scoped calls a task
        # makes and raises on the rest, so widening the type would force all ~140
        # storage call sites to handle a case only an executor ever sees.
        self._inner: PyQueue = (
            cast("PyQueue", DetachedNative())
            if detached
            else PyQueue(
                db_path=db_path,
                workers=workers,
                default_retry=default_retry,
                default_timeout=default_timeout,
                default_priority=default_priority,
                result_ttl=result_ttl,
                backend=backend,
                db_url=db_url,
                schema=schema,
                pool_size=pool_size,
                scheduler_poll_interval_ms=scheduler_poll_interval_ms,
                scheduler_reap_interval=scheduler_reap_interval,
                scheduler_cleanup_interval=scheduler_cleanup_interval,
                scheduler_batch_size=scheduler_batch_size,
                namespace=namespace,
                push_dispatch=push_dispatch,
                dlq_auto_retry_delay=dlq_auto_retry_delay,
                dlq_auto_retry_max=dlq_auto_retry_max,
                retention=retention._as_map() if retention is not None else None,
                auto_migrate=auto_migrate,
            )
        )
        self._backend = backend
        self._namespace = namespace
        self._db_url = db_url
        self._schema = schema
        self._db_path = db_path
        self._workers = workers or os.cpu_count() or 1
        self._task_registry: dict[str, Callable] = {}
        self._task_configs: list[PyTaskConfig] = []
        self._executor = ThreadPoolExecutor(max_workers=2)
        self._periodic_configs: list[dict[str, Any]] = []
        self._subscription_configs: list[dict[str, Any]] = []
        self._log_consumer_configs: list[dict[str, Any]] = []
        self._hooks: dict[str, list[Callable]] = {
            "before_task": [],
            "after_task": [],
            "on_success": [],
            "on_failure": [],
        }
        base_serializer: Serializer = serializer or SmartSerializer()
        self._codec_chain: list[PayloadCodec] = (
            [codec] if isinstance(codec, PayloadCodec) else list(codec or [])
        )
        self._serializer: Serializer = (
            CodecSerializer(base_serializer, self._codec_chain)
            if self._codec_chain
            else base_serializer
        )
        self._codecs: dict[str, PayloadCodec] = dict(codecs or {})
        self._task_serializers: dict[str, Serializer] = {}
        self._task_codecs: dict[str, list[str]] = {}
        self._task_idempotent: dict[str, bool] = {}
        self._task_debounce: dict[str, DebounceConfig] = {}
        # Signatures of debounced tasks, so a key template can address
        # parameters by name even when the caller passed them positionally.
        # Cached because ``inspect.signature`` is far too slow to run per
        # enqueue, and populated lazily so a task registered after the first
        # submission still gets one.
        self._task_signatures: dict[str, inspect.Signature | None] = {}
        self._task_compensates: dict[str, str] = {}
        self._task_batch_configs: dict[str, BatchConfig] = {}
        # Per-task cooperative timeout, mirrored here like every other per-task
        # option so the lifecycle can reach it through the queue rather than a
        # closure only the blocking wrapper had.
        self._task_soft_timeouts: dict[str, float] = {}
        self._batch_accumulator: BatchAccumulator | None = None
        self._global_middleware: list[TaskMiddleware] = middleware or []
        self._task_middleware: dict[str, list[TaskMiddleware]] = {}
        # Per-task middleware-chain cache (chain, disable_version, computed_at).
        # Bumping ``_mw_disable_version`` invalidates same-process readers
        # instantly; the TTL bounds cross-process staleness. See
        # ``_get_middleware_chain``.
        self._mw_chain_cache: dict[str, tuple[list[TaskMiddleware], int, float]] = {}
        self._mw_disable_version: int = 0
        self._task_retry_filters: dict[str, dict[str, list[type[Exception]]]] = {}
        self._init_predicate_state()
        self._drain_timeout = drain_timeout
        self._queue_configs: dict[str, dict[str, Any]] = {}
        # Opt-in per-queue admission caps (queue -> max pending). Empty = uncapped.
        self._max_pending: dict[str, int] = dict(max_pending or {})
        # A negative cap would make `pending + incoming > cap` always true and
        # permanently reject every enqueue — reject it at construction instead.
        for _queue, _cap in self._max_pending.items():
            if _cap < 0:
                raise ValueError(f"max_pending for '{_queue}' must be non-negative")
        self._event_bus = EventBus(max_workers=event_workers)
        self._auto_migrate = auto_migrate
        # Gating migrations does not mean the schema is missing: the normal flow
        # is one process running `flexiq migrate` and the application starting
        # afterwards with `auto_migrate=False` against a database that is fully
        # migrated. Only a never-migrated store has nothing to read, and there
        # :meth:`migrate` loads the snapshot instead.
        self._webhook_manager = WebhookManager(
            queue_ref=self,
            preload=auto_migrate or self._inner.is_migrated(),
        )

        # Proxy handlers
        self._proxy_registry = ProxyRegistry()
        register_builtin_handlers(
            self._proxy_registry,
            disabled_proxies=disabled_proxies,
            file_path_allowlist=file_path_allowlist,
        )
        self._proxy_metrics = ProxyMetrics()
        self._recipe_signing_key = recipe_signing_key or os.environ.get("FLEXIQ_RECIPE_SECRET")
        self._max_reconstruction_timeout = max_reconstruction_timeout
        # Reject oversized serialized payloads at enqueue time (0 disables).
        self.max_payload_bytes: int = DEFAULT_MAX_PAYLOAD_BYTES

        # Argument interception
        self._interception_metrics: InterceptionMetrics | None = None
        interception_mode = coerce_enum(InterceptionMode, interception, param="interception")
        if interception_mode is not InterceptionMode.OFF:
            self._interception_metrics = InterceptionMetrics()
            registry = build_default_registry()
            self._interceptor: ArgumentInterceptor | None = ArgumentInterceptor(
                registry=registry,
                mode=interception_mode,
                max_depth=max_intercept_depth,
                proxy_registry=self._proxy_registry,
                metrics=self._interception_metrics,
            )
        else:
            self._interceptor = None

        # Worker resources (dependency injection)
        self._resource_definitions: dict[str, ResourceDefinition] = {}
        self._resource_runtime: ResourceRuntime | None = None
        self._task_inject_map: dict[str, list[str]] = {}

        # Native async concurrency limit
        self._async_concurrency = async_concurrency

        # Test mode flag (Phase M)
        self._test_mode_active = False

        # Workflow support
        self._workflow_registry: dict[str, Any] = {}
        self._workflow_tracker: Any = None
        if _WORKFLOWS_AVAILABLE and hasattr(self._inner, "submit_workflow"):
            self._workflow_tracker = WorkflowTracker(self)

        # Claim any ``@flexiq.task`` declared before this queue existed. Cheap
        # and idempotent, so it also runs from ``autodiscover`` and worker
        # start — between the three, neither import order needs a rule.
        self._drained_pending: dict[str, ClaimedTask] = {}
        self._drain_pending_tasks()

    def _get_serializer(self, task_name: str) -> Serializer:
        """Get the serializer for a task (per-task or queue-level fallback)."""
        return self._task_serializers.get(task_name, self._serializer)

    def _apply_task_codecs(self, task_name: str, payload: bytes) -> bytes:
        """Encode a serialized payload with the task's named codecs, in order."""
        for name in self._task_codecs.get(task_name, ()):
            codec = self._codecs.get(name)
            if codec is None:
                raise ValueError(f"no codec registered named {name!r}")
            payload = codec.encode(payload)
        return payload

    def _encode_payload(self, task_name: str, args: tuple, kwargs: dict) -> bytes:
        """Serialize ``(args, kwargs)`` and apply the task's named codecs."""
        payload = self._get_serializer(task_name).dumps((args, kwargs))
        return self._apply_task_codecs(task_name, payload)

    def _check_payload_size(self, task_name: str, size: int) -> None:
        """Reject a serialized payload that exceeds ``max_payload_bytes``."""
        limit = self.max_payload_bytes
        if limit and size > limit:
            raise ValueError(
                f"Serialized payload for task '{task_name}' is {size} bytes, exceeding the "
                f"{limit}-byte limit (adjust queue.max_payload_bytes)"
            )

    @staticmethod
    def _auto_idempotency_key(task_name: str, payload: bytes) -> str:
        """Derive a deterministic dedup key from task name + serialized payload."""
        # NUL separates the task name from the payload so the boundary is
        # unambiguous. A printable separator like "|" can occur in both fields,
        # letting one job's (name, payload) hash-collide with another's — a
        # job-suppression vector when task names are caller-influenced. NUL
        # cannot appear in a Python task name.
        digest = hashlib.sha256(task_name.encode("utf-8") + b"\x00" + payload).hexdigest()
        return f"auto:{digest[:32]}"

    def _resolve_unique_key(
        self,
        task_name: str,
        payload: bytes,
        unique_key: str | None,
        idempotency_key: str | None,
        idempotent: bool | None,
    ) -> str | None:
        """Resolve which dedup key (if any) to send to storage.

        Precedence: explicit unique_key > explicit idempotency_key > auto-key
        when the per-call ``idempotent`` flag (or per-task default) is True.
        A per-call ``idempotent=False`` disables auto-derivation even if the
        task is registered as idempotent.
        """
        if unique_key is not None:
            return unique_key
        if idempotency_key is not None:
            return idempotency_key
        if idempotent is False:
            return None
        if idempotent or self._task_idempotent.get(task_name, False):
            return self._auto_idempotency_key(task_name, payload)
        return None

    def _task_signature(self, task_name: str) -> inspect.Signature | None:
        """The registered task's signature, or ``None`` when it has none.

        A task can be submitted by name without ever being registered in this
        process (a producer that only enqueues, a workflow step naming a task
        that lives on the worker). Those calls still get positional
        (``{0}``) and keyword placeholders; only name-for-positional binding
        needs the signature.
        """
        fn = self._task_registry.get(task_name)
        if fn is None:
            # Not cached: registration may still be ahead of us, and a miss
            # costs one dict lookup rather than an ``inspect.signature`` call.
            return None
        if task_name in self._task_signatures:
            return self._task_signatures[task_name]
        signature: inspect.Signature | None
        try:
            signature = inspect.signature(fn)
        except (TypeError, ValueError):  # pragma: no cover - exotic callables
            signature = None
        self._task_signatures[task_name] = signature
        return signature

    def _resolve_debounce(
        self,
        task_name: str,
        args: tuple,
        kwargs: dict[str, Any],
        debounce: Duration | None,
        debounce_key: str | None,
        debounce_max_wait: Duration | None,
        debounce_replace_payload: bool | None,
    ) -> tuple[str, DebounceConfig] | None:
        """Resolve this call's debounce window and its key, or ``None`` when off.

        Per-call options override the registered window as a set: passing any
        of them means this call defines its own window, so the four are
        validated together rather than mixed field-by-field with the task's.
        That keeps a per-call ``debounce="1s"`` from inheriting a 30-minute
        ``max_wait`` that no longer makes sense next to it.
        """
        per_call = (
            debounce is not None
            or debounce_key is not None
            or debounce_max_wait is not None
            or debounce_replace_payload is not None
        )
        if per_call:
            config = normalize_debounce(
                debounce, debounce_key, debounce_max_wait, bool(debounce_replace_payload)
            )
        else:
            config = self._task_debounce.get(task_name)
        if config is None:
            return None

        key = resolve_debounce_key(
            config.key_template,
            task_name,
            self._task_signature(task_name),
            args,
            kwargs,
        )
        return key, config

    # ── Batching ──────────────────────────────────────────────────────

    def _ensure_batch_accumulator(self) -> BatchAccumulator:
        """Construct the accumulator on first use (lazy thread startup)."""
        if self._batch_accumulator is None:
            self._batch_accumulator = BatchAccumulator(
                dispatch=self._dispatch_batched_payload,
                queue=self,
            )
        return self._batch_accumulator

    def _dispatch_batched_payload(
        self,
        task_name: str,
        items: list[Any],
        config: BatchConfig,
    ) -> JobResult:
        """Callback the accumulator invokes when a batch reaches its limit.

        Serializes the list of items as a single ``(args, kwargs)`` payload
        where ``args=(items,)`` and ``kwargs={}``, then enqueues a normal job.
        The worker dispatch path is unchanged — the task function simply
        receives a list as its first positional arg.

        Returns the :class:`JobResult` for the enqueued batch job so the
        accumulator can stamp it onto every pending ``BatchedJobResult``
        handle (one per item that landed in this batch).
        """
        del config  # currently unused at dispatch time; reserved for telemetry
        payload = self._encode_payload(task_name, (items,), {})
        self._check_payload_size(task_name, len(payload))
        # A flush is a single-job enqueue onto the default queue, so it honors the
        # admission cap like any other producer path (no silent bypass).
        self._reject_if_queue_full("default")
        py_job = self._inner.enqueue(
            task_name=task_name,
            payload=payload,
            queue="default",
            priority=None,
            delay_seconds=None,
            max_retries=None,
            timeout=None,
            unique_key=None,
            metadata=None,
            notes=None,
            depends_on=None,
            expires=None,
            result_ttl=None,
        )
        self._emit_event(
            EventType.JOB_ENQUEUED,
            {
                "task_name": task_name,
                "job_id": py_job.id,
                "queue": "default",
                "batch_size": len(items),
            },
        )
        return JobResult(py_job=py_job, queue=self)

    def close(self) -> None:
        """Flush in-memory batches and shut down background helpers.

        Safe to call multiple times. If batched tasks have unflushed items
        when ``close`` is called, those items are dispatched as a final batch
        before the accumulator thread exits.
        """
        if self._batch_accumulator is not None:
            self._batch_accumulator.shutdown(flush=True)
            self._batch_accumulator = None

    def _deserialize_payload(self, task_name: str, payload: bytes) -> tuple:
        """Deserialize a job payload into an ``(args, kwargs)`` pair.

        Serializers without a native tuple type (msgpack, JSON) flatten the
        payload to ``[args, kwargs]`` with ``args`` as a list. The worker hands
        ``args`` straight to PyO3, which requires a real tuple, so the shape is
        normalized back to ``(tuple, dict)`` here regardless of serializer.
        """
        task_codecs = self._task_codecs.get(task_name)
        if task_codecs:
            for name in reversed(task_codecs):
                codec = self._codecs.get(name)
                if codec is None:
                    raise SerializationError(f"no codec registered named {name!r}")
                payload = codec.decode(payload)
        args, kwargs = self._get_serializer(task_name).loads(payload)
        return tuple(args), kwargs

    def _serialize_result(self, task_name: str, result: Any) -> bytes:
        """Serialize a task result with the queue-level serializer.

        Results always use the queue serializer, never per-task serializers:
        consumers (``JobResult.get``, workflow trackers) read results back
        with only the queue-level configuration. ``task_name`` is part of the
        contract with the Rust worker paths, which call this per job.
        """
        return self._serializer.dumps(result)

    def _reject_if_queue_full(self, queue_name: str, incoming: int = 1) -> None:
        """Enforce the opt-in ``max_pending`` admission cap for a queue.

        Raises :class:`QueueFullError` when admitting ``incoming`` jobs would push
        the queue's pending backlog past its configured cap. ``incoming`` is the
        batch size, so a batch is rejected as a whole rather than overshooting the
        cap by its full size. No-op (and no query) for uncapped queues. Non-atomic
        count-then-insert — brief overshoot under concurrent producers is accepted,
        like the rate limiter.
        """
        cap = self._max_pending.get(queue_name)
        if cap is None:
            return
        pending = self._inner.count_pending_by_queue(queue_name)
        if pending + incoming > cap:
            raise self._queue_full(queue_name, pending, cap, incoming)

    @staticmethod
    def _queue_full(queue_name: str, pending: int, cap: int, incoming: int = 1) -> QueueFullError:
        """The rejection this queue reports when its admission cap is reached.

        Shared with the debounced path, where storage decides the refusal and
        hands back the same two counts, so both read identically to a caller.
        """
        return QueueFullError(
            f"queue '{queue_name}' is full: {pending} pending + {incoming} "
            f"would exceed max_pending {cap}"
        )

    def enqueue(
        self,
        task_name: str,
        args: tuple = (),
        kwargs: dict | None = None,
        priority: int | None = None,
        delay: float | None = None,
        queue: str | None = None,
        max_retries: int | None = None,
        timeout: int | None = None,
        unique_key: str | None = None,
        metadata: str | None = None,
        notes: dict[str, Any] | None = None,
        depends_on: str | list[str] | None = None,
        expires: float | None = None,
        result_ttl: int | None = None,
        idempotency_key: str | None = None,
        idempotent: bool | None = None,
        debounce: Duration | None = None,
        debounce_key: str | None = None,
        debounce_max_wait: Duration | None = None,
        debounce_replace_payload: bool | None = None,
    ) -> JobResult:
        """Enqueue a task for background execution.

        Args:
            task_name: Registered task name.
            args: Positional arguments to pass to the task function.
            kwargs: Keyword arguments to pass to the task function.
            priority: Job priority (higher = more urgent).
            delay: Delay in seconds before the job becomes eligible to run.
            queue: Target queue name.
            max_retries: Max retry attempts.
            timeout: Timeout in seconds.
            unique_key: Deduplication key (alias of ``idempotency_key`` — wins
                if both are set, kept for backwards compatibility).
            metadata: Arbitrary JSON string to attach to the job.
            notes: Structured annotations dict (≤ 15 top-level fields). See
                :mod:`flexiq.notes` for the validation contract. Use this
                instead of ``metadata`` when you want dashboard-renderable
                key/value annotations with bounded size.
            depends_on: Job ID or list of job IDs that must complete first.
            expires: Seconds until the job expires (skipped if not started by then).
            result_ttl: Per-job result TTL in seconds. Overrides global result_ttl.
            idempotency_key: Explicit dedup key. A second enqueue with the same
                key while the first job is pending or running returns the
                existing job's ID instead of creating a duplicate.
            idempotent: Force-on (``True``) or force-off (``False``) the
                auto-derived dedup key for this single call. ``None`` (the
                default) falls back to the per-task setting from
                ``@queue.task(idempotent=True)``.
            debounce: Debounce window for this call — a duration string
                (``"5m"``) or a number of seconds. While a job with the same
                resolved ``debounce_key`` is pending and unclaimed, this
                submission slides its deadline forward instead of enqueuing a
                second job. Requires ``debounce_key`` and ``debounce_max_wait``.
            debounce_key: Format template resolved against ``args``/``kwargs``,
                e.g. ``"report:{user_id}"``. A placeholder the call does not
                provide raises rather than collapsing to one global key.
            debounce_max_wait: Ceiling on the total delay, measured from when
                the window opened. Mandatory alongside ``debounce`` — an
                unbounded debounce starves the job.
            debounce_replace_payload: Overwrite the pending job's payload with
                this call's when the submission lands on an open window.
                Defaults to keeping the payload the window opened with.

        Passing any ``debounce*`` option makes this call define its own window
        outright; passing none of them falls back to the task's registered
        window, if it has one. Either way the window owns the deadline, so
        ``delay`` cannot be combined with it and a predicate's deferral has no
        effect on a debounced submission.

        Raises:
            ValueError: the ``debounce*`` options are incomplete or
                contradictory, or ``debounce_key`` names something this call
                does not provide.
        """
        final_args = args
        final_kwargs = kwargs or {}

        # Run on_enqueue middleware hook (options dict is mutable). Skip the
        # whole build/read-back when no middleware is registered — the common
        # hot path — to avoid a needless dict allocation and 13 reads per call.
        if self._global_middleware:
            enqueue_options: dict[str, Any] = {
                "priority": priority,
                "delay": delay,
                "queue": queue,
                "max_retries": max_retries,
                "timeout": timeout,
                "unique_key": unique_key,
                "metadata": metadata,
                "notes": notes,
                "depends_on": depends_on,
                "expires": expires,
                "result_ttl": result_ttl,
                "idempotency_key": idempotency_key,
                "idempotent": idempotent,
                "debounce": debounce,
                "debounce_key": debounce_key,
                "debounce_max_wait": debounce_max_wait,
                "debounce_replace_payload": debounce_replace_payload,
            }
            for mw in self._global_middleware:
                if not mw._should_apply(None, task_name=task_name):
                    continue
                try:
                    mw.on_enqueue(task_name, final_args, final_kwargs, enqueue_options)
                except Exception:
                    logger.exception("middleware on_enqueue() error")

            # Apply any middleware mutations back
            priority = enqueue_options.get("priority")
            delay = enqueue_options.get("delay")
            queue = enqueue_options.get("queue")
            max_retries = enqueue_options.get("max_retries")
            timeout = enqueue_options.get("timeout")
            unique_key = enqueue_options.get("unique_key")
            metadata = enqueue_options.get("metadata")
            notes = enqueue_options.get("notes")
            depends_on = enqueue_options.get("depends_on")
            expires = enqueue_options.get("expires")
            result_ttl = enqueue_options.get("result_ttl")
            idempotency_key = enqueue_options.get("idempotency_key")
            idempotent = enqueue_options.get("idempotent")
            debounce = enqueue_options.get("debounce")
            debounce_key = enqueue_options.get("debounce_key")
            debounce_max_wait = enqueue_options.get("debounce_max_wait")
            debounce_replace_payload = enqueue_options.get("debounce_replace_payload")

        # Validation runs *after* middleware so a mutating hook still gets
        # the chance to reshape notes before we reject them.
        notes_encoded = validate_and_encode_notes(notes)

        # Resolved before interception, so the key comes from the arguments the
        # caller actually passed rather than the placeholder references the
        # interceptor swaps in for large values.
        debounce_plan = self._resolve_debounce(
            task_name,
            final_args,
            final_kwargs,
            debounce,
            debounce_key,
            debounce_max_wait,
            debounce_replace_payload,
        )
        if debounce_plan is not None and delay is not None:
            raise ValueError(
                f"cannot combine delay with debounce on task {task_name!r} — the "
                "window sets the deadline, so the delay would be silently dropped"
            )

        if self._interceptor is not None and not self._test_mode_active:
            final_args, final_kwargs = self._interceptor.intercept(final_args, final_kwargs)

        # Producer-side batching: instead of enqueueing one job, push the
        # call's first positional arg into the per-task accumulator. The
        # batch flushes as a single job whose payload is the list of
        # accumulated items.
        batch_cfg = self._task_batch_configs.get(task_name)
        if batch_cfg is not None:
            if debounce_plan is not None:
                raise ValueError(
                    f"cannot debounce batched task {task_name!r} — batched calls are "
                    "buffered in-process and never reach the queue individually, so "
                    "there is nothing to collapse"
                )
            if final_kwargs:
                raise ValueError(
                    f"batched task {task_name!r} does not accept kwargs — "
                    "pass each item as the first positional arg only"
                )
            if len(final_args) != 1:
                raise ValueError(
                    f"batched task {task_name!r} expects exactly one positional "
                    f"arg per call (the item), got {len(final_args)}"
                )
            handle = self._ensure_batch_accumulator().add(task_name, final_args[0], batch_cfg)
            return handle  # type: ignore[return-value]

        task_serializer = self._get_serializer(task_name)
        payload = task_serializer.dumps((final_args, final_kwargs))

        # Dedup keys hash the pre-codec bytes so nondeterministic codecs
        # (AES-GCM's random nonce) can't break idempotency for codec tasks.
        unique_key = self._resolve_unique_key(
            task_name=task_name,
            payload=payload,
            unique_key=unique_key,
            idempotency_key=idempotency_key,
            idempotent=idempotent,
        )

        if debounce_plan is not None and unique_key is not None:
            raise ValueError(
                f"cannot combine a dedup key with debounce on task {task_name!r} — "
                "a debounced submission slides the pending job's deadline instead of "
                "deduplicating against it, and the two keys would fight"
            )

        payload = self._apply_task_codecs(task_name, payload)
        self._check_payload_size(task_name, len(payload))

        # Evaluate enqueue-time predicate (if registered). Outcome may
        # adjust the delay (Defer / False+defer), raise (Cancel /
        # False+cancel), or pass through unchanged.
        predicate = self._task_predicates.get(task_name)
        if predicate is not None:
            delay = self._apply_enqueue_predicate(
                predicate=predicate,
                task_name=task_name,
                queue_name=queue or "default",
                priority=priority,
                args=final_args,
                kwargs=final_kwargs,
                payload_size=len(payload),
                delay=delay,
            )

        dep_ids = None
        if depends_on is not None:
            dep_ids = [depends_on] if isinstance(depends_on, str) else list(depends_on)

        queue_name = queue or "default"
        resolved_key, debounce_config = debounce_plan or (None, None)
        # A debounced enqueue carries the cap into storage instead of being
        # checked against it here: a call that lands on an open window inserts
        # nothing, and only the debounce write knows which of the two it is.
        if debounce_config is None:
            self._reject_if_queue_full(queue_name)

        try:
            py_job = self._inner.enqueue(
                task_name=task_name,
                payload=payload,
                queue=queue_name,
                priority=priority,
                delay_seconds=delay,
                max_retries=max_retries,
                timeout=timeout,
                unique_key=unique_key,
                metadata=metadata,
                notes=notes_encoded,
                depends_on=dep_ids,
                expires=expires,
                result_ttl=result_ttl,
                debounce_key=resolved_key,
                debounce_window_ms=debounce_config.window_ms if debounce_config else None,
                debounce_max_wait_ms=debounce_config.max_wait_ms if debounce_config else None,
                debounce_replace_payload=bool(debounce_config and debounce_config.replace_payload),
                debounce_max_pending=(
                    self._max_pending.get(queue_name) if debounce_config else None
                ),
            )
        except RuntimeError as exc:
            counts = queue_full_counts(str(exc)) if debounce_config else None
            if counts is None:
                raise
            raise self._queue_full(queue_name, *counts) from None

        self._emit_event(
            EventType.JOB_ENQUEUED,
            {
                "task_name": task_name,
                "job_id": py_job.id,
                "queue": queue_name,
            },
        )

        return JobResult(py_job=py_job, queue=self)

    def enqueue_many(
        self,
        task_name: str,
        args_list: list[tuple],
        kwargs_list: list[dict] | None = None,
        priority: int | None = None,
        queue: str | None = None,
        max_retries: int | None = None,
        timeout: int | None = None,
        delay: float | None = None,
        delay_list: list[float | None] | None = None,
        unique_keys: list[str | None] | None = None,
        metadata: str | None = None,
        metadata_list: list[str | None] | None = None,
        notes: dict[str, Any] | None = None,
        notes_list: list[dict[str, Any] | None] | None = None,
        expires: float | None = None,
        expires_list: list[float | None] | None = None,
        result_ttl: int | None = None,
        result_ttl_list: list[int | None] | None = None,
        idempotency_keys: list[str | None] | None = None,
        idempotent: bool | None = None,
    ) -> list[JobResult]:
        """Enqueue multiple jobs for the same task in a single transaction.

        Args:
            task_name: The registered task name.
            args_list: List of positional argument tuples, one per job.
            kwargs_list: Optional list of kwarg dicts, one per job.
            priority: Priority for all jobs (uses default if None).
            queue: Queue name for all jobs (uses "default" if None).
            max_retries: Max retries for all jobs (uses default if None).
            timeout: Timeout in seconds for all jobs (uses default if None).
            delay: Uniform delay in seconds for all jobs.
            delay_list: Per-job delays in seconds.
            unique_keys: Per-job deduplication keys (alias of
                ``idempotency_keys`` for backwards compatibility — wins per-job
                if both are set).
            metadata: Uniform metadata JSON string for all jobs.
            metadata_list: Per-job metadata JSON strings.
            notes: Uniform structured-notes dict for all jobs (see
                :mod:`flexiq.notes`).
            notes_list: Per-job notes dicts. Takes precedence over ``notes``
                when both are supplied.
            expires: Uniform expiry in seconds for all jobs.
            expires_list: Per-job expiry in seconds.
            result_ttl: Uniform result TTL in seconds for all jobs.
            result_ttl_list: Per-job result TTL in seconds.
            idempotency_keys: Per-job explicit dedup keys. Falls back to
                auto-derivation when an entry is ``None`` and the task is
                idempotent.
            idempotent: Force-on (``True``) or force-off (``False``)
                auto-derived dedup keys for this batch. ``None`` (the default)
                falls back to the per-task setting from
                ``@queue.task(idempotent=True)``.

        Returns:
            List of JobResult handles, one per enqueued job.
        """
        count = len(args_list)
        if kwargs_list is not None and len(kwargs_list) != len(args_list):
            raise ValueError(
                f"kwargs_list length ({len(kwargs_list)}) must match "
                f"args_list length ({len(args_list)})"
            )
        # Validate every per-job list up front so a length mismatch fails here
        # with a clear message rather than misaligning the batch arrays later.
        for field_name, values in (
            ("delay_list", delay_list),
            ("unique_keys", unique_keys),
            ("metadata_list", metadata_list),
            ("notes_list", notes_list),
            ("expires_list", expires_list),
            ("result_ttl_list", result_ttl_list),
            ("idempotency_keys", idempotency_keys),
        ):
            if values is not None and len(values) != count:
                raise ValueError(
                    f"{field_name} length ({len(values)}) must match args_list length ({count})"
                )
        if task_name in self._task_debounce:
            raise ValueError(
                f"cannot batch-enqueue debounced task {task_name!r} — a debounce "
                "window collapses submissions one at a time, and the batch insert "
                "has no equivalent. Submit them individually."
            )
        kw_list = kwargs_list or [{}] * count

        chain = self._get_middleware_chain(task_name)
        # ``_should_apply`` depends only on (task_name, middleware), which is
        # constant across the batch — evaluate it once, not per job.  [B4]
        applicable = [mw for mw in chain if mw._should_apply(None, task_name=task_name)]

        if applicable:
            # Middleware may mutate priority/queue/delay/etc. per job, so build
            # the mutable per-job options dicts and dispatch ``on_enqueue``
            # BEFORE enqueue_batch so mutations are applied.
            per_job_options: list[dict[str, Any]] = [
                {
                    "priority": priority,
                    "queue": queue,
                    "max_retries": max_retries,
                    "timeout": timeout,
                    "delay": (delay_list[i] if delay_list is not None else delay),
                    "unique_key": (unique_keys[i] if unique_keys is not None else None),
                    "metadata": (metadata_list[i] if metadata_list is not None else metadata),
                    "notes": (notes_list[i] if notes_list is not None else notes),
                    "expires": (expires_list[i] if expires_list is not None else expires),
                    "result_ttl": (
                        result_ttl_list[i] if result_ttl_list is not None else result_ttl
                    ),
                    "idempotency_key": (
                        idempotency_keys[i] if idempotency_keys is not None else None
                    ),
                    "idempotent": idempotent,
                }
                for i in range(count)
            ]
            for i in range(count):
                for mw in applicable:
                    try:
                        mw.on_enqueue(task_name, args_list[i], kw_list[i], per_job_options[i])
                    except Exception:
                        logger.exception("middleware on_enqueue() error")

            queues_list = [opt["queue"] or "default" for opt in per_job_options]
            priorities_list = [opt["priority"] for opt in per_job_options]
            retries_list = [opt["max_retries"] for opt in per_job_options]
            timeouts_list = [opt["timeout"] for opt in per_job_options]
            delays = [opt["delay"] for opt in per_job_options]
            metas = [opt["metadata"] for opt in per_job_options]
            notes_encoded = [validate_and_encode_notes(opt["notes"]) for opt in per_job_options]
            exp_list = [opt["expires"] for opt in per_job_options]
            ttl_list = [opt["result_ttl"] for opt in per_job_options]
            uk_src = [opt["unique_key"] for opt in per_job_options]
            ik_src = [opt["idempotency_key"] for opt in per_job_options]
            idem_src = [opt["idempotent"] for opt in per_job_options]
        else:
            # No applicable middleware — build the parallel lists the Rust batch
            # enqueue wants directly, skipping the per-job dict layer.  [B3]
            # Copy any per-job input lists so a later predicate mutation can't
            # corrupt the caller's arguments.
            default_queue = queue or "default"
            queues_list = [default_queue] * count
            priorities_list = [priority] * count
            retries_list = [max_retries] * count
            timeouts_list = [timeout] * count
            delays = list(delay_list) if delay_list is not None else [delay] * count
            metas = list(metadata_list) if metadata_list is not None else [metadata] * count
            if notes_list is None and notes is None:
                notes_encoded = [None] * count  # no per-job validate call needed
            else:
                notes_source = notes_list if notes_list is not None else [notes] * count
                notes_encoded = [validate_and_encode_notes(n) for n in notes_source]
            exp_list = list(expires_list) if expires_list is not None else [expires] * count
            ttl_list = (
                list(result_ttl_list) if result_ttl_list is not None else [result_ttl] * count
            )
            uk_src = list(unique_keys) if unique_keys is not None else [None] * count
            ik_src = list(idempotency_keys) if idempotency_keys is not None else [None] * count
            idem_src = [idempotent] * count

        task_serializer = self._get_serializer(task_name)
        if self._interceptor is not None:
            pairs = [
                self._interceptor.intercept(a, kw)
                for a, kw in zip(args_list, kw_list, strict=True)
            ]
            payloads = [task_serializer.dumps((a, kw)) for a, kw in pairs]
        else:
            payloads = [
                task_serializer.dumps((a, kw)) for a, kw in zip(args_list, kw_list, strict=True)
            ]
        task_names = [task_name] * count

        # Dedup keys hash the pre-codec bytes so nondeterministic codecs
        # (AES-GCM's random nonce) can't break idempotency for codec tasks.
        per_job_unique_keys = [
            self._resolve_unique_key(
                task_name=task_name,
                payload=payloads[i],
                unique_key=uk_src[i],
                idempotency_key=ik_src[i],
                idempotent=idem_src[i],
            )
            for i in range(count)
        ]

        payloads = [self._apply_task_codecs(task_name, payload) for payload in payloads]
        for payload in payloads:
            self._check_payload_size(task_name, len(payload))

        # Evaluate enqueue-time predicate per row. Cancel raises for the
        # whole batch — all-or-nothing semantics. Defer adjusts that row's
        # delay only.
        predicate = self._task_predicates.get(task_name)
        if predicate is not None:
            for i in range(count):
                delays[i] = self._apply_enqueue_predicate(
                    predicate=predicate,
                    task_name=task_name,
                    queue_name=queues_list[i],
                    priority=priorities_list[i],
                    args=args_list[i],
                    kwargs=kw_list[i],
                    payload_size=len(payloads[i]),
                    delay=delays[i],
                )

        # Admission cap is all-or-nothing for the batch: reject before inserting
        # any row if admitting this batch would push a target queue past its cap.
        # Count the rows per queue so the check accounts for the whole batch
        # rather than overshooting the cap by its size.
        batch_by_queue = Counter(queues_list)
        for capped_queue, incoming in batch_by_queue.items():
            self._reject_if_queue_full(capped_queue, incoming)

        py_jobs = self._inner.enqueue_batch(
            task_names=task_names,
            payloads=payloads,
            queues=queues_list,
            priorities=priorities_list,
            max_retries_list=retries_list,
            timeouts=timeouts_list,
            delay_seconds_list=delays,
            unique_keys=per_job_unique_keys,
            metadata_list=metas,
            notes_list=notes_encoded,
            expires_list=exp_list,
            result_ttl_list=ttl_list,
        )

        results = [JobResult(py_job=pj, queue=self) for pj in py_jobs]

        for job_result in results:
            self._emit_event(
                EventType.JOB_ENQUEUED,
                {"job_id": job_result.id, "task_name": task_name},
            )

        return results
