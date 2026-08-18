"""Task and periodic decorators, lifecycle hooks, and registration."""

from __future__ import annotations

import contextlib
import functools
import importlib
import inspect
import logging
import pkgutil
import time
import typing
from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Any

from flexiq._flexiq import PyTaskConfig
from flexiq.async_support.helpers import run_maybe_async
from flexiq.batching.config import BatchConfig
from flexiq.codecs import CodecSerializer
from flexiq.context import _clear_context
from flexiq.dashboard.middleware_store import MiddlewareDisableStore
from flexiq.debounce import DebounceConfig, Duration, normalize_debounce
from flexiq.detached import disabled_middleware as dispatched_disabled_middleware
from flexiq.detached import is_detached
from flexiq.enums import OnExcess, coerce_enum
from flexiq.exceptions import DuplicateTaskError
from flexiq.inject import Inject, _InjectAlias
from flexiq.middleware import middleware_key
from flexiq.predicates.core import coerce_predicate
from flexiq.predicates.outcomes import PredicateAction
from flexiq.registry import (
    ClaimedTask,
    _resolve_module_name,
    pending_tasks,
    set_task_options,
)
from flexiq.task import TaskWrapper
from flexiq.task_lifecycle import run_lifecycle

if TYPE_CHECKING:
    from flexiq.codecs import PayloadCodec
    from flexiq.interception import ArgumentInterceptor
    from flexiq.middleware import TaskMiddleware
    from flexiq.predicates import Predicate
    from flexiq.predicates.metrics import PredicateMetrics
    from flexiq.proxies import ProxyRegistry
    from flexiq.proxies.metrics import ProxyMetrics
    from flexiq.resources.runtime import ResourceRuntime
    from flexiq.serializers import Serializer


logger = logging.getLogger("flexiq")

# How long a cached middleware chain stays valid without a version bump. Bounds
# the worst-case lag for an out-of-process dashboard disable change.
_MW_CHAIN_TTL = 1.0


class QueueDecoratorMixin:
    """Task/periodic decorators, hooks, type registration, queue-level config."""

    _task_registry: dict[str, Callable]
    _task_configs: list[PyTaskConfig]
    _periodic_configs: list[dict[str, Any]]
    _hooks: dict[str, list[Callable]]
    _task_serializers: dict[str, Serializer]
    _codec_chain: list[PayloadCodec]
    _codecs: dict[str, PayloadCodec]
    _task_codecs: dict[str, list[str]]
    _task_idempotent: dict[str, bool]
    _task_debounce: dict[str, DebounceConfig]
    _task_signatures: dict[str, inspect.Signature | None]
    _task_compensates: dict[str, str]
    _task_batch_configs: dict[str, Any]
    _task_soft_timeouts: dict[str, float]
    _task_middleware: dict[str, list[TaskMiddleware]]
    _task_retry_filters: dict[str, dict[str, list[type[Exception]]]]
    _task_inject_map: dict[str, list[str]]
    _task_predicates: dict[str, Predicate]
    _task_predicate_on_false: dict[str, PredicateAction]
    _task_predicate_extras: dict[str, dict[str, Any]]
    _task_default_defer: dict[str, float]
    _task_predicate_serialized: dict[str, dict[str, Any] | None]
    _predicate_metrics: PredicateMetrics
    _interceptor: ArgumentInterceptor | None
    _proxy_registry: ProxyRegistry | None
    _proxy_metrics: ProxyMetrics
    _resource_runtime: ResourceRuntime | None
    _test_mode_active: bool
    _recipe_signing_key: str | None
    _max_reconstruction_timeout: int
    _global_middleware: list[TaskMiddleware]
    _queue_configs: dict[str, dict[str, Any]]
    # Pending-registry entries this queue has claimed, keyed by task name,
    # paired with the wrapper it built for them. Draining is idempotent, so
    # a re-drain has to tell 'already mine' apart from 'another task owns
    # this name', and has to re-bind the handle without re-registering.
    _drained_pending: dict[str, ClaimedTask]

    # ``_emit_event`` is provided by ``QueueEventsMixin`` on the composed
    # Queue. Declaring it as a class-level callable attribute (not a method)
    # lets mypy see it from this mixin without overriding the real
    # implementation through MRO.
    _emit_event: Callable[..., None]
    # ``_apply_dispatch_predicate`` is defined on the Queue itself
    # (alongside enqueue) since it needs ``_inner`` and the task
    # serializer. Declared here so mypy sees it through the mixin.
    _apply_dispatch_predicate: Callable[..., None]

    # Middleware-chain cache state (initialized in ``Queue.__init__``).
    _mw_chain_cache: dict[str, tuple[list[TaskMiddleware], int, float]]
    _mw_disable_version: int

    def _get_middleware_chain(self, task_name: str) -> list[TaskMiddleware]:
        """Get the combined global + per-task middleware list, minus any
        middleware the operator has disabled for this task from the dashboard.

        Cached per task to avoid a synchronous storage read on every job
        dispatch. The cache is invalidated immediately on same-process disable
        changes (via ``_mw_disable_version``) and expires after
        ``_MW_CHAIN_TTL`` so out-of-process changes still take effect promptly.

        An attached executor takes a different route entirely: it has no
        settings store to read, so the scheduler resolves the list and attaches
        it to the dispatch. That is per job rather than per task name, so it
        also bypasses the cache — there is nothing to save, the list arrived
        with the work.
        """
        if is_detached():
            return self._chain_without(task_name, dispatched_disabled_middleware())

        version = self._mw_disable_version
        cached = self._mw_chain_cache.get(task_name)
        if cached is not None:
            chain, cached_version, computed_at = cached
            if cached_version == version and time.monotonic() - computed_at < _MW_CHAIN_TTL:
                return chain

        try:
            disabled = MiddlewareDisableStore(self).get_for(task_name)  # type: ignore[arg-type]
        except Exception:  # pragma: no cover - storage read failure is non-fatal
            disabled = []
        chain = self._chain_without(task_name, disabled)

        self._mw_chain_cache[task_name] = (chain, version, time.monotonic())
        return chain

    def _chain_without(self, task_name: str, disabled: Sequence[str]) -> list[TaskMiddleware]:
        """This task's chain, minus ``disabled``.

        ``middleware_key`` matches admin discovery's keying (name, else class
        path) so a dashboard disable on an unnamed middleware works.
        """
        chain = self._global_middleware + self._task_middleware.get(task_name, [])
        if not disabled:
            return chain
        disabled_set = set(disabled)
        return [mw for mw in chain if middleware_key(mw) not in disabled_set]

    def _wrap_task(self, fn: Callable, task_name: str) -> Callable:
        """The blocking entry point Rust calls: drive the shared lifecycle to completion.

        Rust hands over deserialized args and expects a plain value back (or an
        exception), so the coroutine is driven here rather than awaited. It runs
        on a `spawn_blocking` thread with no event loop of its own, which is what
        makes that safe — `run_maybe_async` raises if one is already running.
        """
        queue_ref = self

        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                # The mixin is a Queue once composed; mypy only sees the mixin.
                return run_maybe_async(
                    run_lifecycle(
                        queue_ref,  # type: ignore[arg-type]
                        task_name,
                        fn,
                        args,
                        kwargs,
                    )
                )
            finally:
                # Rust clears the context too, but the classic pool relies on this
                # one. It is edge state, not lifecycle state, so it stays out of
                # the shared body — there it would null out the thread-local of
                # whichever thread the executor loop happens to run on.
                _clear_context()

        return wrapper

    def task(
        self,
        name: str | None = None,
        max_retries: int = 3,
        retry_backoff: float = 1.0,
        timeout: int = 300,
        expires: float | None = None,
        priority: int = 0,
        rate_limit: str | None = None,
        queue: str = "default",
        circuit_breaker: dict | None = None,
        retry_on: list[type[Exception]] | None = None,
        dont_retry_on: list[type[Exception]] | None = None,
        soft_timeout: float | None = None,
        middleware: list[TaskMiddleware] | None = None,
        retry_delays: list[float] | None = None,
        inject: list[str] | None = None,
        serializer: Serializer | None = None,
        codecs: list[str] | None = None,
        max_retry_delay: int | None = None,
        max_concurrent: int | None = None,
        idempotent: bool = False,
        compensates: TaskWrapper | str | None = None,
        batch: bool | dict[str, Any] | None = None,
        predicate: Predicate | Callable[..., Any] | None = None,
        on_false: PredicateAction | str = PredicateAction.DEFER,
        predicate_extras: dict[str, Any] | None = None,
        default_defer_seconds: float = 60.0,
        # Appended, not slotted next to their related options: this signature is
        # not keyword-only, so inserting mid-list would silently rebind the
        # positional arguments after it.
        max_in_flight_per_task: int | None = None,
        retry_budget: str | None = None,
        on_excess: OnExcess | str = OnExcess.DEFER,
        debounce: Duration | None = None,
        debounce_key: str | None = None,
        debounce_max_wait: Duration | None = None,
        debounce_replace_payload: bool = False,
    ) -> Callable[[Callable[..., Any]], TaskWrapper]:
        """Decorator to register a function as a background task.

        Args:
            name: Explicit task name. Defaults to ``module.qualname``.
            max_retries: Max retry attempts on failure before moving to DLQ.
            retry_backoff: Base delay in seconds for exponential backoff between retries.
            timeout: Max execution time in seconds before the task is killed.
            expires: Default job expiry in seconds — a job is skipped
                (cancelled and archived) if it hasn't started within this
                window after enqueue. Per-call ``apply_async(expires=...)``
                overrides it. ``None`` (default) means jobs never expire.
            priority: Priority level (higher = more urgent).
            rate_limit: Rate limit string, e.g. ``"100/m"``, ``"10/s"``, ``"3600/h"``.
            queue: Named queue to submit to.
            circuit_breaker: Optional dict with ``threshold``, ``window`` (seconds),
                and ``cooldown`` (seconds) keys.
            retry_on: List of exception classes that should trigger retries.
                If set, only these exceptions are retried.
            dont_retry_on: List of exception classes that should never be retried.
            soft_timeout: Soft timeout in seconds. Checked via ``current_job.check_timeout()``.
            middleware: Per-task middleware instances (in addition to global middleware).
            retry_delays: Explicit per-attempt delay schedule in seconds, e.g.
                ``[1.0, 5.0, 30.0]``. Overrides ``retry_backoff`` when set; the
                final value is reused for any further retries up to
                ``max_retries``.
            inject: List of resource names to inject as keyword arguments.
            serializer: Per-task serializer. Falls back to the queue-level serializer.
            codecs: Names of payload codecs (registered via ``Queue(codecs=...)``)
                applied in order to this task's serialized payload on enqueue
                and reversed on the worker. Payload only — results stay on the
                queue-level serializer.
            max_retry_delay: Maximum backoff delay in seconds. Defaults to 300
                (5 minutes) if not set.
            max_concurrent: Maximum number of concurrent running instances of
                this task. ``None`` means no limit.
            max_in_flight_per_task: Cap on this task's share of a single worker's
                dispatch slots, so one slow task cannot occupy the whole pool and
                starve the others. Unlike ``max_concurrent`` (a cluster-wide cap
                that costs a database read), this is in-process and free.
                ``None`` lets the task use the whole pool.
            retry_budget: Cap on how fast this task may *retry*, across all of its
                jobs — same syntax as ``rate_limit`` (e.g. ``"100/m"``). Once the
                budget is spent, further failures dead-letter instead of retrying,
                which stops a broken dependency turning into a retry storm.
                Distinct from ``max_retries``, which bounds one job rather than the
                rate, and from ``circuit_breaker``, which trips on hard failure
                rather than aggregate retry rate. ``None`` means no cap.
            on_excess: What a saturated rate limit does to this task's jobs.
                ``"defer"`` (the default) reschedules the job for a later
                dispatch cycle. ``"drop"`` sheds it instead: the job is
                dead-lettered on the spot with a reserved ``rate_limit:``
                reason, so shedding stays visible in the dashboard and
                countable in metrics, and the DLQ auto-retry sweep never
                resurrects it. Suits traffic whose value expires with the
                moment — metrics samples, cache warms — where a backlog is
                worth less than nothing. Applies to the limit on this task
                *and* to the one on the queue it runs in, since either
                rejecting means the same thing to the caller. A tripped
                ``circuit_breaker`` always defers regardless: that is
                downstream failure, not excess load. Dropping fires no
                middleware or event hooks — the job never ran — exactly like
                a CoDel shed.
            compensates: Optional reference to a task that compensates this
                one. When this task runs as part of a workflow saga and a
                later step fails, the framework enqueues the compensation
                task as part of reverse-order rollback. The compensation
                task is invoked with ``(forward_args, forward_kwargs,
                forward_result)`` so it can undo the original side effect.
                Accepts a :class:`TaskWrapper` (a sibling decorated task) or
                a task-name string. ``None`` (default) means no compensation
                — saga rollback skips this node if the workflow fails.
            idempotent: When ``True``, ``.delay()``/``.apply_async()`` calls
                automatically derive a deduplication key from
                ``sha256(task_name|serialized_payload)``. Two calls with
                identical arguments while a job is pending or running return
                the same job ID instead of producing a duplicate. The slot is
                released once the job leaves the active state. Per-call
                ``idempotency_key="..."`` overrides the derived key; per-call
                ``idempotent=False`` disables auto-derivation for that one
                submission.
            batch: Enable producer-side batching. ``True`` uses defaults
                (max_size=100, max_wait_ms=500); a dict overrides specific
                fields (e.g., ``batch={"max_size": 50, "max_wait_ms": 200}``).
                The task function must accept a ``list`` as its first
                positional arg — each ``.delay(item)`` adds one item to the
                in-memory buffer; the worker eventually runs the function
                once with the full list. Items in the buffer at process
                crash are lost; do not use for critical tasks. Cannot be
                combined with ``idempotent=True`` (rejected at decoration).
            predicate: A :class:`~flexiq.predicates.Predicate` (or plain
                callable receiving a :class:`~flexiq.predicates.PredicateContext`)
                evaluated both at enqueue time and at worker-dispatch time
                to decide whether the job runs.
            on_false: What to do when the predicate returns ``False`` —
                ``"defer"`` (re-schedule with ``default_defer_seconds``),
                ``"cancel"`` (terminally skip).
            predicate_extras: Arbitrary dict forwarded to the predicate via
                ``PredicateContext.extras``. Useful for passing static
                config without re-instantiating the predicate.
            default_defer_seconds: Default delay when ``on_false="defer"``
                and the predicate returns plain ``False`` (no explicit
                ``Defer(seconds=...)``). Ignored otherwise.
            debounce: Length of the debounce window — a duration string
                (``"500ms"``, ``"5m"``, ``"2h"``) or a number of seconds. While
                a job with the same resolved ``debounce_key`` is still pending
                and unclaimed, each further submission slides that job's
                deadline forward instead of enqueuing a second one, so a burst
                collapses into a single run. Requires ``debounce_key`` and
                ``debounce_max_wait``, and rules out ``idempotent=True``,
                ``batch=...``, ``.map()``/``enqueue_many``, and a per-call
                ``delay`` — each of those either decides the same thing or
                would be silently dropped. ``None`` (default) disables
                debouncing.
            debounce_key: Format template resolved against each call's
                arguments, e.g. ``"report:{user_id}"``. Parameters are
                addressable by name whether passed positionally or by keyword.
                A placeholder the call does not provide raises at enqueue time
                rather than silently collapsing to one global key. A template
                with no placeholder is a deliberate queue-wide window.
            debounce_max_wait: Hard ceiling on the total delay, measured from
                when the window opened — same syntax as ``debounce``, and never
                shorter than it. Mandatory: without it a caller who never stops
                submitting starves the job forever.
            debounce_replace_payload: When ``True``, a submission that lands on
                an open window also overwrites the pending job's payload with
                its own, so the run uses the newest arguments. The default
                ``False`` keeps the payload the window opened with — a repeat
                submission is a vote to run again soon, not a redefinition of
                the run.

        Returns:
            A decorator that wraps the function in a :class:`~flexiq.task.TaskWrapper`
            exposing ``.delay(*args, **kwargs)`` and
            ``.apply_async(args=..., kwargs=..., ...)`` for enqueueing.

        Raises:
            ValueError: ``on_false`` is neither a :class:`PredicateAction` nor one
                of its wire strings (``"defer"``/``"cancel"``), ``on_excess`` is
                neither an :class:`OnExcess` nor one of its wire strings
                (``"defer"``/``"drop"``), ``default_defer_seconds`` is negative,
                or the ``debounce*`` options are incomplete, contradictory, or
                combined with ``idempotent=True`` / ``batch=...``.
        """
        on_false_action = coerce_enum(PredicateAction, on_false, param="on_false")
        on_excess_action = coerce_enum(OnExcess, on_excess, param="on_excess")
        if default_defer_seconds < 0:
            raise ValueError("default_defer_seconds must be >= 0")
        batch_config = BatchConfig.normalize(batch)
        if batch_config is not None and idempotent:
            raise ValueError(
                "batch=... is incompatible with idempotent=True — the auto-derived "
                "dedup key would change for every batch flush. Use an explicit "
                "idempotency_key per .delay() call if you need dedup."
            )
        debounce_config = normalize_debounce(
            debounce, debounce_key, debounce_max_wait, debounce_replace_payload
        )
        if debounce_config is not None and idempotent:
            raise ValueError(
                "debounce=... is incompatible with idempotent=True — both decide what "
                "a repeat submission does, and they disagree: idempotency returns the "
                "pending job untouched, a debounce slides its deadline. Pick one."
            )
        if debounce_config is not None and batch_config is not None:
            raise ValueError(
                "debounce=... is incompatible with batch=... — batched calls never "
                "reach the queue individually, so there is nothing to debounce."
            )

        def decorator(fn: Callable) -> TaskWrapper:
            task_name = name or f"{_resolve_module_name(fn.__module__)}.{fn.__qualname__}"

            # Detect Inject["name"] annotations (Phase E)
            annotation_injects: list[str] = []
            try:
                hints: dict[str, Any] = {}
                with contextlib.suppress(Exception):
                    # get_type_hints evaluates string annotations
                    ns = getattr(fn, "__globals__", {})
                    ns = {**ns, "Inject": Inject}
                    hints = typing.get_type_hints(fn, globalns=ns, include_extras=True)
                # Fallback: check raw annotations if get_type_hints failed
                if not hints:
                    with contextlib.suppress(Exception):
                        hints = getattr(fn, "__annotations__", {})
                for _param_name, hint in hints.items():
                    if isinstance(hint, _InjectAlias):
                        annotation_injects.append(hint.resource_name)
            except Exception:
                pass

            # Merge explicit inject= with annotation-detected injects
            final_inject = list(inject or [])
            for res_name in annotation_injects:
                if res_name not in final_inject:
                    final_inject.append(res_name)

            # Store retry filters
            if retry_on or dont_retry_on:
                self._task_retry_filters[task_name] = {
                    "retry_on": retry_on or [],
                    "dont_retry_on": dont_retry_on or [],
                }

            # Store per-task middleware
            if middleware:
                self._task_middleware[task_name] = middleware

            # Store per-task serializer, wrapped in the global codec chain so a
            # per-task override can't silently bypass queue-wide codecs.
            if serializer is not None:
                self._task_serializers[task_name] = (
                    CodecSerializer(serializer, self._codec_chain)
                    if self._codec_chain
                    else serializer
                )

            # Store per-task codec names (validated lazily against the
            # registry at enqueue/decode time).
            if codecs:
                self._task_codecs[task_name] = list(codecs)

            # Store per-task idempotency flag (auto-derives unique_key on enqueue)
            if idempotent:
                self._task_idempotent[task_name] = True

            # Per-task debounce window. Re-registering a name replaces the task,
            # so an absent window has to clear any earlier one rather than leave
            # it for the new task to inherit.
            if debounce_config is None:
                self._task_debounce.pop(task_name, None)
            else:
                self._task_debounce[task_name] = debounce_config

            # Saga compensation: record the compensating task's name so the
            # workflow tracker can enqueue it during reverse-order rollback.
            if compensates is not None:
                if isinstance(compensates, TaskWrapper):
                    comp_name = compensates.name
                elif isinstance(compensates, str):
                    comp_name = compensates
                else:
                    raise TypeError(
                        "compensates= must be a TaskWrapper or a task-name "
                        f"string, got {type(compensates).__name__}"
                    )
                self._task_compensates[task_name] = comp_name

            # Store per-task batch config — producer-side accumulation enabled
            # for this task name; Queue.enqueue() routes items into the
            # accumulator instead of calling the Rust enqueue directly.
            if batch_config is not None:
                self._task_batch_configs[task_name] = batch_config

            # Store predicate (and its on_false/extras/default_defer).
            # Also serialize a JSON snapshot so the inspection API and
            # dashboard can show "gated by: ..." without keeping a live
            # Python reference. Bare callables can't be serialized; the
            # snapshot is None in that case.
            if predicate is not None:
                coerced = coerce_predicate(predicate)
                if coerced is not None:
                    self._task_predicates[task_name] = coerced
                    self._task_predicate_on_false[task_name] = on_false_action
                    if predicate_extras:
                        self._task_predicate_extras[task_name] = dict(predicate_extras)
                    self._task_default_defer[task_name] = default_defer_seconds
                    try:
                        self._task_predicate_serialized[task_name] = coerced.to_dict()
                    except Exception:
                        self._task_predicate_serialized[task_name] = None

            # Store inject map for resource injection
            if final_inject:
                self._task_inject_map[task_name] = final_inject

            # Wrap the function with hooks, middleware, and context. Re-registering
            # a name replaces the task, so an absent soft_timeout has to clear any
            # earlier one rather than leave it for the new task to inherit.
            if soft_timeout is None:
                self._task_soft_timeouts.pop(task_name, None)
            else:
                self._task_soft_timeouts[task_name] = soft_timeout
            wrapped = self._wrap_task(fn, task_name)

            # Native async dispatch keys off this registry entry: the pool reads
            # `_flexiq_is_async` to route, the executor reads `_flexiq_async_fn`
            # to find the coroutine — so both must live on `wrapped`, not only on
            # the TaskWrapper returned to the caller. On builds without the
            # native pool the flags are inert and async tasks run on the
            # blocking path via `run_maybe_async`.
            is_async = inspect.iscoroutinefunction(fn)
            wrapped._flexiq_is_async = is_async  # type: ignore[attr-defined]
            if is_async:
                wrapped._flexiq_async_fn = fn  # type: ignore[attr-defined]
            self._task_registry[task_name] = wrapped
            # Re-registering a name replaces the function, so the cached
            # signature a debounce key template binds against goes with it.
            self._task_signatures.pop(task_name, None)

            cb_threshold = None
            cb_window = None
            cb_cooldown = None
            cb_half_open_probes = None
            cb_half_open_success_rate = None
            if circuit_breaker:
                cb_threshold = circuit_breaker.get("threshold", 5)
                cb_window = circuit_breaker.get("window", 60)
                cb_cooldown = circuit_breaker.get("cooldown", 300)
                cb_half_open_probes = circuit_breaker.get("half_open_probes")
                cb_half_open_success_rate = circuit_breaker.get("half_open_success_rate")

            # Store config for worker startup
            config = PyTaskConfig(
                name=task_name,
                max_retries=max_retries,
                retry_backoff=retry_backoff,
                timeout=timeout,
                priority=priority,
                rate_limit=rate_limit,
                queue=queue,
                circuit_breaker_threshold=cb_threshold,
                circuit_breaker_window=cb_window,
                circuit_breaker_cooldown=cb_cooldown,
                retry_delays=retry_delays,
                max_retry_delay=max_retry_delay,
                max_concurrent=max_concurrent,
                circuit_breaker_half_open_probes=cb_half_open_probes,
                circuit_breaker_half_open_success_rate=cb_half_open_success_rate,
                max_in_flight_per_task=max_in_flight_per_task,
                retry_budget=retry_budget,
                on_excess=on_excess_action.value,
            )
            self._task_configs.append(config)

            # Return a TaskWrapper that has .delay() and .apply_async()
            wrapper = TaskWrapper(
                fn=fn,
                queue_ref=self,  # type: ignore[arg-type]
                task_name=task_name,
                default_priority=priority,
                default_queue=queue,
                default_max_retries=max_retries,
                default_timeout=timeout,
                default_expires=expires,
                inject=final_inject or None,
            )

            # Preserve function metadata
            functools.update_wrapper(wrapper, fn)

            # Mirror the async markers for introspection on the returned handle.
            wrapper._flexiq_is_async = is_async
            if is_async:
                wrapper._flexiq_async_fn = fn

            return wrapper

        return decorator

    def autodiscover(self, package: str = "tasks") -> list[str]:
        """Import every module under ``package`` and claim the tasks they declare.

        The import is the load-bearing half: ``@flexiq.task`` registers on
        import, so walking the tree is what makes the declarations happen. The
        walk alone would find nothing.

        Args:
            package: Dotted path to the package (or single module) holding the
                task declarations. Defaults to ``"tasks"``.

        Returns:
            Sorted names of every deferred task now registered on this queue.
            The list is the same on a second call — draining is idempotent, not
            destructive.

        Raises:
            ImportError: ``package``, or any module beneath it, failed to
                import. Never swallowed: an unregistered task is a fatal,
                non-retryable dispatch failure, so a module that quietly failed
                to import dead-letters every job it owns.
            DuplicateTaskError: A discovered task claims a name this queue
                already registered for a different function.
        """
        root = importlib.import_module(package)

        # ``walk_packages`` yields a subpackage before importing it to recurse,
        # and swallows the ImportError if that import fails — which would skip
        # the whole subtree in silence. Importing each yielded name here means
        # the failure surfaces on the yield, ahead of the swallowed one.
        paths = getattr(root, "__path__", None)
        if paths is not None:
            for _finder, mod_name, _ispkg in pkgutil.walk_packages(
                paths, prefix=f"{root.__name__}."
            ):
                importlib.import_module(mod_name)

        self._drain_pending_tasks()
        return sorted(self._drained_pending)

    def _drain_pending_tasks(self) -> None:
        """Claim every task in the module-global pending registry.

        Idempotent by construction: the registry is never emptied, so a second
        queue in the same process gets the same tasks, and a repeat drain on
        this queue re-binds without re-registering.

        Binding is last-drain-wins — the handle a task module exports submits
        to the queue that drained it most recently. Keeping the first binding
        would leave the handle pointing at a queue that has been torn down as
        soon as a second one appears.
        """
        for entry in pending_tasks():
            name = entry.name
            mine = self._drained_pending.get(name)
            if mine is not None and mine.entry.origin == entry.origin:
                # Already ours. Re-bind anyway: another queue may have drained
                # the same registry since, and the most recent drain wins.
                entry.handle._bind(mine.wrapper)
                continue
            if name in self._task_registry:
                owner = mine.entry.origin if mine else "@queue.task()"
                raise DuplicateTaskError(
                    f"deferred task {name!r} declared in {entry.origin} collides "
                    f"with the task this queue already registered from {owner} — "
                    "pass an explicit name= to one of them"
                )
            wrapper = self.task(**entry.options)(entry.fn)
            entry.handle._bind(wrapper)
            self._drained_pending[name] = ClaimedTask(entry, wrapper)

    def periodic(
        self,
        cron: str,
        name: str | None = None,
        args: tuple = (),
        kwargs: dict | None = None,
        queue: str = "default",
        timezone: str | None = None,
    ) -> Callable[[Callable[..., Any]], TaskWrapper]:
        """Decorator to register a periodic (cron-scheduled) task.

        Args:
            cron: Cron expression (6-field with seconds), e.g. ``"0 */5 * * * *"``
                for every 5 minutes.
            name: Explicit task name. Defaults to ``module.qualname``.
            args: Positional arguments to pass to the task on each run.
            kwargs: Keyword arguments to pass to the task on each run.
            queue: Named queue to submit to.
            timezone: IANA timezone name (e.g. ``"America/New_York"``) the cron
                expression is evaluated in. Defaults to UTC.

        Returns:
            A decorator that registers the function as both a normal task and
            a periodic schedule entry, returning the underlying
            :class:`~flexiq.task.TaskWrapper`.
        """

        def decorator(fn: Callable) -> TaskWrapper:
            # If fn is a WorkflowProxy (from @queue.workflow()), create a
            # launcher task that submits the workflow on each cron trigger.
            if getattr(fn, "_is_workflow_proxy", False):
                proxy: Any = fn
                launcher_name = f"_wf_launcher_{proxy._name}"

                @self.task(name=launcher_name, queue=queue)
                def _wf_launcher() -> str:
                    run = proxy.submit()
                    return f"submitted workflow run {run.id}"

                payload = self._encode_payload(launcher_name, (), {})  # type: ignore[attr-defined]
                self._periodic_configs.append(
                    {
                        "name": launcher_name,
                        "task_name": launcher_name,
                        "cron_expr": cron,
                        "payload": payload,
                        "queue": queue,
                        "timezone": timezone,
                    }
                )
                return fn  # type: ignore[return-value]

            # Register as a normal task first
            wrapper = self.task(name=name, queue=queue)(fn)

            # Store periodic config for registration at worker startup
            payload = self._encode_payload(wrapper.name, args, kwargs or {})  # type: ignore[attr-defined]
            self._periodic_configs.append(
                {
                    "name": name or f"{_resolve_module_name(fn.__module__)}.{fn.__qualname__}",
                    "task_name": wrapper.name,
                    "cron_expr": cron,
                    "payload": payload,
                    "queue": queue,
                    "timezone": timezone,
                }
            )

            return wrapper

        return decorator

    # -- Hooks / middleware --

    def before_task(self, fn: Callable) -> Callable:
        """Register a hook called before each task executes.

        Args:
            fn: Callback with signature ``fn(task_name, args, kwargs)``.
        """
        self._hooks["before_task"].append(fn)
        return fn

    def after_task(self, fn: Callable) -> Callable:
        """Register a hook called after each task completes or fails.

        Args:
            fn: Callback with signature
                ``fn(task_name, args, kwargs, result, error)``.
        """
        self._hooks["after_task"].append(fn)
        return fn

    def on_success(self, fn: Callable) -> Callable:
        """Register a hook called when a task completes successfully.

        Args:
            fn: Callback with signature
                ``fn(task_name, args, kwargs, result)``.
        """
        self._hooks["on_success"].append(fn)
        return fn

    def on_failure(self, fn: Callable) -> Callable:
        """Register a hook called when a task raises an exception.

        Args:
            fn: Callback with signature
                ``fn(task_name, args, kwargs, error)``.
        """
        self._hooks["on_failure"].append(fn)
        return fn


# ``@flexiq.task`` accepts exactly what ``Queue.task`` accepts. Deriving the
# names from the real signature here — rather than restating them in
# ``flexiq.registry`` — keeps the deferred decorator from drifting as options
# are added, without the registry having to import ``Queue``.
set_task_options(inspect.signature(QueueDecoratorMixin.task))
