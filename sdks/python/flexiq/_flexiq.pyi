"""Type stubs for the Rust extension module."""

from __future__ import annotations

from typing import Any

class PyTaskConfig:
    """Configuration for a registered task."""

    name: str
    max_retries: int
    retry_backoff: float
    timeout: int
    priority: int
    rate_limit: str | None
    queue: str

    circuit_breaker_threshold: int | None
    circuit_breaker_window: int | None
    circuit_breaker_cooldown: int | None

    retry_delays: list[float] | None
    max_retry_delay: int | None
    max_concurrent: int | None
    circuit_breaker_half_open_probes: int | None
    circuit_breaker_half_open_success_rate: float | None
    max_in_flight_per_task: int | None
    retry_budget: str | None
    on_excess: str | None

    def __init__(
        self,
        name: str,
        max_retries: int = 3,
        retry_backoff: float = 1.0,
        timeout: int = 300,
        priority: int = 0,
        rate_limit: str | None = None,
        queue: str = "default",
        circuit_breaker_threshold: int | None = None,
        circuit_breaker_window: int | None = None,
        circuit_breaker_cooldown: int | None = None,
        retry_delays: list[float] | None = None,
        max_retry_delay: int | None = None,
        max_concurrent: int | None = None,
        circuit_breaker_half_open_probes: int | None = None,
        circuit_breaker_half_open_success_rate: float | None = None,
        max_in_flight_per_task: int | None = None,
        retry_budget: str | None = None,
        on_excess: str | None = None,
    ) -> None: ...

class PyJob:
    """A job record from the database."""

    id: str
    queue: str
    task_name: str
    priority: int
    retry_count: int
    max_retries: int
    created_at: int
    scheduled_at: int
    started_at: int | None
    completed_at: int | None
    error: str | None
    timeout_ms: int
    unique_key: str | None
    progress: int | None
    metadata: str | None
    notes: str | None
    namespace: str | None

    @property
    def status(self) -> str: ...
    @property
    def payload_bytes(self) -> bytes: ...
    @property
    def result_bytes(self) -> bytes | None: ...
    def __repr__(self) -> str: ...

class PyQueue:
    """Low-level Rust queue interface backed by SQLite, PostgreSQL, or Redis."""

    def __init__(
        self,
        db_path: str = ".flexiq/flexiq.db",
        workers: int = 0,
        default_retry: int = 3,
        default_timeout: int = 300,
        default_priority: int = 0,
        result_ttl: int | None = None,
        backend: str = "sqlite",
        db_url: str | None = None,
        schema: str = "flexiq",
        pool_size: int | None = None,
        scheduler_poll_interval_ms: int = 50,
        scheduler_reap_interval: int = 100,
        scheduler_cleanup_interval: int = 1200,
        scheduler_batch_size: int = 1,
        namespace: str | None = None,
        push_dispatch: bool = False,
        dlq_auto_retry_delay: int | None = None,
        dlq_auto_retry_max: int = 1,
        retention: dict[str, int] | None = None,
        auto_migrate: bool = True,
    ) -> None: ...
    def request_shutdown(self) -> None: ...
    def enqueue(
        self,
        task_name: str,
        payload: bytes,
        queue: str = "default",
        priority: int | None = None,
        delay_seconds: float | None = None,
        max_retries: int | None = None,
        timeout: int | None = None,
        unique_key: str | None = None,
        metadata: str | None = None,
        notes: str | None = None,
        depends_on: list[str] | None = None,
        expires: float | None = None,
        result_ttl: int | None = None,
        debounce_key: str | None = None,
        debounce_window_ms: int | None = None,
        debounce_max_wait_ms: int | None = None,
        debounce_replace_payload: bool = False,
    ) -> PyJob: ...
    def enqueue_batch(
        self,
        task_names: list[str],
        payloads: list[bytes],
        queues: list[str] | None = None,
        priorities: list[int | None] | None = None,
        max_retries_list: list[int | None] | None = None,
        timeouts: list[int | None] | None = None,
        delay_seconds_list: list[float | None] | None = None,
        unique_keys: list[str | None] | None = None,
        metadata_list: list[str | None] | None = None,
        notes_list: list[str | None] | None = None,
        expires_list: list[float | None] | None = None,
        result_ttl_list: list[int | None] | None = None,
    ) -> list[PyJob]: ...
    def list_jobs(
        self,
        status: str | None = None,
        queue: str | None = None,
        task_name: str | None = None,
        limit: int = 50,
        offset: int = 0,
        namespace: str | None = None,
    ) -> list[PyJob]: ...
    def list_jobs_after(
        self,
        status: str | None = None,
        queue: str | None = None,
        task_name: str | None = None,
        limit: int = 50,
        after: str | None = None,
        namespace: str | None = None,
    ) -> tuple[list[PyJob], str | None]: ...
    def get_job(self, job_id: str) -> PyJob | None: ...
    def get_job_errors(self, job_id: str) -> list[dict[str, Any]]: ...
    def cancel_job(self, job_id: str) -> bool: ...
    def request_cancel(self, job_id: str) -> bool: ...
    def is_cancel_requested(self, job_id: str) -> bool: ...
    def get_dependencies(self, job_id: str) -> list[str]: ...
    def get_dependents(self, job_id: str) -> list[str]: ...
    def update_progress(self, job_id: str, progress: int) -> None: ...
    def purge_completed(self, older_than_seconds: int) -> int: ...
    def stats(self) -> dict[str, int]: ...
    def stats_by_queue(self, queue_name: str) -> dict[str, int]: ...
    def count_pending_by_queue(self, queue_name: str) -> int: ...
    def stats_all_queues(self) -> dict[str, dict[str, int]]: ...
    def list_jobs_filtered(
        self,
        status: str | None = None,
        queue: str | None = None,
        task_name: str | None = None,
        metadata_like: str | None = None,
        error_like: str | None = None,
        created_after: int | None = None,
        created_before: int | None = None,
        limit: int = 50,
        offset: int = 0,
        namespace: str | None = None,
    ) -> list[PyJob]: ...
    def list_jobs_filtered_after(
        self,
        status: str | None = None,
        queue: str | None = None,
        task_name: str | None = None,
        metadata_like: str | None = None,
        error_like: str | None = None,
        created_after: int | None = None,
        created_before: int | None = None,
        limit: int = 50,
        after: str | None = None,
        namespace: str | None = None,
    ) -> tuple[list[PyJob], str | None]: ...
    def dead_letters(self, limit: int = 10, offset: int = 0) -> list[dict[str, Any]]: ...
    def dead_letters_after(
        self, limit: int = 10, after: str | None = None
    ) -> tuple[list[dict[str, Any]], str | None]: ...
    def dead_letters_by_task(
        self, task_name: str, limit: int = 10, offset: int = 0
    ) -> list[dict[str, Any]]: ...
    def purge_dead_by_task(self, task_name: str) -> int: ...
    def retry_dead(self, dead_id: str) -> str: ...
    def requeue_job(self, job_id: str) -> bool: ...
    def purge_dead(self, older_than_seconds: int) -> int: ...
    def delete_dead(self, dead_id: str) -> bool: ...
    def register_periodic(
        self,
        name: str,
        task_name: str,
        cron_expr: str,
        args: bytes | None = None,
        kwargs: bytes | None = None,
        queue: str = "default",
        timezone: str | None = None,
    ) -> None: ...
    def list_periodic(self) -> list[dict[str, Any]]: ...
    def delete_periodic(self, name: str) -> bool: ...
    def pause_periodic(self, name: str) -> bool: ...
    def resume_periodic(self, name: str) -> bool: ...
    def register_subscription(
        self,
        topic: str,
        subscription_name: str,
        task_name: str,
        queue: str = "default",
        durable: bool = True,
        owner_worker_id: str | None = None,
        priority: int | None = None,
        max_retries: int | None = None,
        timeout_ms: int | None = None,
        mode: str = "fanout",
    ) -> None: ...
    def list_subscriptions(
        self, topic: str | None = None
    ) -> list[tuple[str, str, str, str, bool, bool]]: ...
    def unsubscribe(self, topic: str, subscription_name: str) -> bool: ...
    def set_subscription_active(
        self, topic: str, subscription_name: str, active: bool
    ) -> bool: ...
    def reap_ephemeral_subscriptions(self, worker_id: str | None = None) -> int: ...
    def topic_backlog_stats(
        self,
    ) -> list[tuple[str, str, str, str, bool, bool, int, int, int, int | None]]: ...
    def read_topic_messages(
        self, topic: str, subscription_name: str, limit: int
    ) -> list[tuple[str, bytes, str | None, str | None, int]]: ...
    def ack_topic_cursor(self, topic: str, subscription_name: str, cursor: str) -> bool: ...
    def lease_topic_messages(
        self, topic: str, subscription_name: str, limit: int, visibility_ms: int
    ) -> list[tuple[str, bytes, str | None, str | None, int]]: ...
    def ack_message(self, topic: str, subscription_name: str, message_id: str) -> bool: ...
    def nack_message(self, topic: str, subscription_name: str, message_id: str) -> bool: ...
    def topic_log_stats(
        self,
    ) -> list[tuple[str, str, str | None, int, int | None]]: ...
    def declare_topic(self, name: str, mode: str, retention_ms: int | None = None) -> None: ...
    def list_declared_topics(self) -> list[tuple[str, str, int | None, int]]: ...
    def publish(
        self,
        topic: str,
        payload: bytes,
        idempotency_key: str | None = None,
        metadata: str | None = None,
        notes: str | None = None,
        priority: int | None = None,
        delay_seconds: float | None = None,
        max_retries: int | None = None,
        timeout: int | None = None,
        expires: float | None = None,
        result_ttl: int | None = None,
    ) -> list[PyJob]: ...
    def run_worker(
        self,
        task_registry: dict[str, Any],
        task_configs: list[PyTaskConfig],
        queues: list[str] | None = None,
        drain_timeout_secs: int | None = None,
        tags: str | None = None,
        worker_id: str | None = None,
        resources: str | None = None,
        threads: int = 1,
        async_concurrency: int = 100,
        queue_configs: str | None = None,
        pool: str | None = None,
        app_path: str | None = None,
        mesh_config: str | None = None,
    ) -> None: ...
    def worker_heartbeat(
        self,
        worker_id: str,
        resource_health: str | None = None,
    ) -> list[str]: ...
    def set_worker_status(self, worker_id: str, status: str) -> None: ...
    def pause_queue(self, queue_name: str) -> None: ...
    def resume_queue(self, queue_name: str) -> None: ...
    def list_paused_queues(self) -> list[str]: ...
    def purge_queue(self, queue_name: str) -> int: ...
    def revoke_task(self, task_name: str) -> int: ...
    def get_setting(self, key: str) -> str | None: ...
    def set_setting(self, key: str, value: str) -> None: ...
    def set_setting_if(self, key: str, expected: str | None, value: str) -> bool: ...
    def delete_setting(self, key: str) -> bool: ...
    def migrate(self) -> dict[str, Any]: ...
    def is_migrated(self) -> bool: ...
    def min_contract(self) -> int: ...
    def set_min_contract(self, level: int) -> None: ...
    def list_settings(self) -> dict[str, str]: ...
    def effective_retention(self) -> str | None: ...
    def dry_run_retention(self, retention: dict[str, int] | None = None) -> str: ...
    def archive_old_jobs(self, older_than_seconds: int) -> int: ...
    def list_archived(self, limit: int = 50, offset: int = 0) -> list[PyJob]: ...
    def list_archived_after(
        self, limit: int = 50, after: str | None = None
    ) -> tuple[list[PyJob], str | None]: ...
    def get_metrics(
        self,
        task_name: str | None = None,
        since_seconds: int = 3600,
    ) -> list[dict[str, Any]]: ...
    def replay_job(self, job_id: str) -> str: ...
    def get_replay_history(self, job_id: str) -> list[dict[str, Any]]: ...
    def write_task_log(
        self,
        job_id: str,
        task_name: str,
        level: str,
        message: str,
        extra: str | None = None,
    ) -> None: ...
    def get_task_logs(self, job_id: str) -> list[dict[str, Any]]: ...
    def query_task_logs(
        self,
        task_name: str | None = None,
        level: str | None = None,
        since_seconds: int = 3600,
        limit: int = 100,
    ) -> list[dict[str, Any]]: ...
    def list_circuit_breakers(self) -> list[dict[str, Any]]: ...
    def list_workers(self) -> list[dict[str, Any]]: ...
    def acquire_lock(
        self,
        lock_name: str,
        owner_id: str,
        ttl_ms: int = 30000,
    ) -> bool: ...
    def release_lock(self, lock_name: str, owner_id: str) -> bool: ...
    def extend_lock(
        self,
        lock_name: str,
        owner_id: str,
        ttl_ms: int = 30000,
    ) -> bool: ...
    def get_lock_info(self, lock_name: str) -> dict[str, Any] | None: ...
    def submit_workflow(
        self,
        name: str,
        version: int,
        dag_bytes: bytes,
        step_metadata_json: str,
        node_payloads: dict[str, bytes],
        queue_default: str = "default",
        params_json: str | None = None,
        deferred_node_names: list[str] | None = None,
        parent_run_id: str | None = None,
        parent_node_name: str | None = None,
        cache_hit_nodes: dict[str, str] | None = None,
    ) -> PyWorkflowHandle: ...
    def get_workflow_run_status(self, run_id: str) -> PyWorkflowRunStatus: ...
    def list_workflow_runs(
        self,
        definition_name: str | None = None,
        state: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[PyWorkflowRun]: ...
    def list_workflow_runs_after(
        self,
        definition_name: str | None = None,
        state: str | None = None,
        limit: int = 50,
        after: str | None = None,
    ) -> tuple[list[PyWorkflowRun], str | None]: ...
    def get_workflow_run_detail(
        self, run_id: str
    ) -> tuple[PyWorkflowRun, list[PyWorkflowRunNode]]: ...
    def get_child_workflow_runs(self, parent_run_id: str) -> list[PyWorkflowRun]: ...
    def cancel_workflow_run(self, run_id: str) -> None: ...
    def mark_workflow_node_result(
        self,
        job_id: str,
        succeeded: bool,
        error: str | None = None,
        skip_cascade: bool = False,
        result_hash: str | None = None,
    ) -> tuple[str, str, str | None] | None: ...
    def get_base_run_node_data(self, base_run_id: str) -> list[tuple[str, str, str | None]]: ...
    def skip_workflow_node(self, run_id: str, node_name: str) -> None: ...
    def expand_fan_out(
        self,
        run_id: str,
        parent_node_name: str,
        child_names: list[str],
        child_payloads: list[bytes],
        task_name: str,
        queue: str,
        max_retries: int,
        timeout_ms: int,
        priority: int,
    ) -> list[str]: ...
    def create_deferred_job(
        self,
        run_id: str,
        node_name: str,
        payload: bytes,
        task_name: str,
        queue: str,
        max_retries: int,
        timeout_ms: int,
        priority: int,
    ) -> str: ...
    def check_fan_out_completion(
        self,
        run_id: str,
        parent_node_name: str,
    ) -> tuple[bool, list[str]] | None: ...
    def finalize_run_if_terminal(self, run_id: str) -> str | None: ...
    def set_workflow_node_waiting_approval(self, run_id: str, node_name: str) -> None: ...
    def set_workflow_node_running(self, run_id: str, node_name: str) -> None: ...
    def fail_workflow_node(self, run_id: str, node_name: str, error: str) -> None: ...
    def resolve_workflow_gate(
        self,
        run_id: str,
        node_name: str,
        approved: bool,
        error: str | None = None,
    ) -> None: ...
    def get_workflow_definition_dag(self, run_id: str) -> bytes: ...
    def set_workflow_node_fan_out_count(self, run_id: str, node_name: str, count: int) -> None: ...
    # Saga lifecycle (workflows feature only).
    def set_workflow_run_completed_with_failures(self, run_id: str) -> None: ...
    def set_workflow_run_compensating(self, run_id: str) -> None: ...
    def set_workflow_run_compensated(self, run_id: str, completed_at: int) -> None: ...
    def set_workflow_run_compensation_failed(
        self,
        run_id: str,
        completed_at: int,
        error: str | None = None,
    ) -> None: ...
    def set_workflow_node_compensation_job(
        self,
        run_id: str,
        node_name: str,
        compensation_job_id: str,
        started_at: int,
    ) -> None: ...
    def set_workflow_node_compensated(
        self, run_id: str, node_name: str, completed_at: int
    ) -> None: ...
    def set_workflow_node_compensation_failed(
        self, run_id: str, node_name: str, error: str, completed_at: int
    ) -> None: ...

class PyWorkflowBuilder:
    """Rust-side workflow DAG builder.

    Only available when built with the ``workflows`` feature.
    """

    def __init__(self) -> None: ...
    def add_step(
        self,
        name: str,
        task_name: str,
        after: list[str] | None = None,
        queue: str | None = None,
        max_retries: int | None = None,
        timeout_ms: int | None = None,
        priority: int | None = None,
        args_template: str | None = None,
        kwargs_template: str | None = None,
        fan_out: str | None = None,
        fan_in: str | None = None,
        condition: str | None = None,
    ) -> None: ...
    def step_count(self) -> int: ...
    def step_names(self) -> list[str]: ...
    def serialize(self) -> tuple[bytes, str]: ...

class PyWorkflowHandle:
    """Opaque handle returned from ``PyQueue.submit_workflow``.

    Only available when built with the ``workflows`` feature.
    """

    run_id: str
    name: str
    definition_id: str

class PyWorkflowRunStatus:
    """Snapshot of a workflow run's state and per-node status.

    Only available when built with the ``workflows`` feature.
    """

    run_id: str
    state: str
    started_at: int | None
    completed_at: int | None
    error: str | None

    def node_statuses(self) -> dict[str, dict[str, Any]]: ...

class PyWorkflowRun:
    """One workflow run row, dashboard-shaped.

    Only available when built with the ``workflows`` feature.
    """

    id: str
    definition_id: str
    state: str
    params: str | None
    started_at: int | None
    completed_at: int | None
    error: str | None
    parent_run_id: str | None
    parent_node_name: str | None
    created_at: int

class PyWorkflowRunNode:
    """One workflow node row with compensation fields included.

    Only available when built with the ``workflows`` feature.
    """

    node_name: str
    status: str
    job_id: str | None
    result_hash: str | None
    fan_out_count: int | None
    started_at: int | None
    completed_at: int | None
    error: str | None
    compensation_job_id: str | None
    compensation_started_at: int | None
    compensation_completed_at: int | None
    compensation_error: str | None

class PyResultSender:
    """Sends task results from Python async executor back to Rust scheduler.

    Only available when built with the ``native-async`` feature.
    """

    def try_report_success(
        self,
        job_id: str,
        task_name: str,
        result: bytes | None,
        wall_time_ns: int,
    ) -> bool: ...
    def try_report_failure(
        self,
        job_id: str,
        task_name: str,
        error: str,
        retry_count: int,
        max_retries: int,
        wall_time_ns: int,
        should_retry: bool,
    ) -> bool: ...
    def try_report_cancelled(
        self,
        job_id: str,
        task_name: str,
        wall_time_ns: int,
    ) -> bool: ...

class Executor:
    """A running attachment to a detached scheduler.

    Constructing one performs the handshake, so a bad token or an unreachable
    scheduler raises here rather than after the pool has been built. Jobs run on
    the prefork pool — one child per slot.
    """

    def __init__(
        self,
        address: str,
        app_path: str,
        tasks: list[str],
        slots: int,
        token: str | None = None,
        executor_id: str | None = None,
    ) -> None: ...
    @property
    def scheduler_id(self) -> str:
        """Identity the scheduler announced when it accepted this attach."""
        ...

    @property
    def executor_id(self) -> str:
        """Identity this executor attached under."""
        ...

    @property
    def peer(self) -> str:
        """Peer label of the scheduler connection."""
        ...

    def is_running(self) -> bool:
        """Whether the scheduler session is still open."""
        ...

    def wait(self, timeout_ms: int) -> bool:
        """Block up to `timeout_ms`, returning whether the session has ended.

        Loop on this rather than blocking indefinitely: each return hands the
        GIL back, which is the only moment a pending signal handler can run.
        """
        ...

    def stop(self) -> None:
        """Stop accepting work and finish what is in flight. Returns at once."""
        ...

    def shutdown(self) -> None:
        """Drain in-flight work, disconnect, and join. Idempotent."""
        ...

def _init_rust_logging() -> None:
    """Activate the Rust → Python `logging` bridge (idempotent)."""
    ...

def reserved_setting_prefixes() -> list[str]:
    """Settings-key prefixes the dashboard's generic KV surface must hide."""
    ...

WORKER_PROTOCOL_VERSION: int
"""Frame-format version an executor must announce to attach."""
