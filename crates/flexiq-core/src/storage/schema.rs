diesel::table! {
    jobs (id) {
        id -> Text,
        queue -> Text,
        task_name -> Text,
        payload -> Binary,
        status -> Integer,
        priority -> Integer,
        created_at -> BigInt,
        scheduled_at -> BigInt,
        started_at -> Nullable<BigInt>,
        completed_at -> Nullable<BigInt>,
        retry_count -> Integer,
        max_retries -> Integer,
        result -> Nullable<Binary>,
        error -> Nullable<Text>,
        timeout_ms -> BigInt,
        unique_key -> Nullable<Text>,
        progress -> Nullable<Integer>,
        metadata -> Nullable<Text>,
        notes -> Nullable<Text>,
        cancel_requested -> Integer,
        expires_at -> Nullable<BigInt>,
        result_ttl_ms -> Nullable<BigInt>,
        namespace -> Nullable<Text>,
        has_deps -> Bool,
        topic -> Nullable<Text>,
        subscription_name -> Nullable<Text>,
        debounce_key -> Nullable<Text>,
    }
}

diesel::table! {
    dead_letter (id) {
        id -> Text,
        original_job_id -> Text,
        queue -> Text,
        task_name -> Text,
        payload -> Binary,
        error -> Nullable<Text>,
        retry_count -> Integer,
        failed_at -> BigInt,
        metadata -> Nullable<Text>,
        notes -> Nullable<Text>,
        priority -> Integer,
        max_retries -> Integer,
        timeout_ms -> BigInt,
        result_ttl_ms -> Nullable<BigInt>,
        namespace -> Nullable<Text>,
        dlq_retry_count -> Integer,
        topic -> Nullable<Text>,
        subscription_name -> Nullable<Text>,
        shed -> Bool,
        origin_job_id -> Nullable<Text>,
    }
}

diesel::table! {
    rate_limits (key) {
        key -> Text,
        tokens -> Double,
        max_tokens -> Double,
        refill_rate -> Double,
        last_refill -> BigInt,
    }
}

diesel::table! {
    periodic_tasks (name) {
        name -> Text,
        task_name -> Text,
        cron_expr -> Text,
        args -> Nullable<Binary>,
        kwargs -> Nullable<Binary>,
        queue -> Text,
        enabled -> Bool,
        last_run -> Nullable<BigInt>,
        next_run -> BigInt,
        timezone -> Nullable<Text>,
    }
}

diesel::table! {
    job_errors (id) {
        id -> Text,
        job_id -> Text,
        attempt -> Integer,
        error -> Text,
        failed_at -> BigInt,
    }
}

diesel::table! {
    job_dependencies (id) {
        id -> Text,
        job_id -> Text,
        depends_on_job_id -> Text,
    }
}

diesel::table! {
    task_metrics (id) {
        id -> Text,
        task_name -> Text,
        job_id -> Text,
        wall_time_ns -> BigInt,
        memory_bytes -> BigInt,
        succeeded -> Bool,
        recorded_at -> BigInt,
        namespace -> Nullable<Text>,
    }
}

diesel::table! {
    replay_history (id) {
        id -> Text,
        original_job_id -> Text,
        replay_job_id -> Text,
        replayed_at -> BigInt,
        original_result -> Nullable<Binary>,
        replay_result -> Nullable<Binary>,
        original_error -> Nullable<Text>,
        replay_error -> Nullable<Text>,
    }
}

diesel::table! {
    task_logs (id) {
        id -> Text,
        job_id -> Text,
        task_name -> Text,
        level -> Text,
        message -> Text,
        extra -> Nullable<Text>,
        logged_at -> BigInt,
        namespace -> Nullable<Text>,
    }
}

diesel::table! {
    circuit_breakers (task_name) {
        task_name -> Text,
        state -> Integer,
        failure_count -> Integer,
        last_failure_at -> Nullable<BigInt>,
        opened_at -> Nullable<BigInt>,
        half_open_at -> Nullable<BigInt>,
        threshold -> Integer,
        window_ms -> BigInt,
        cooldown_ms -> BigInt,
        half_open_max_probes -> Integer,
        half_open_success_rate -> Double,
        half_open_probe_count -> Integer,
        half_open_success_count -> Integer,
        half_open_failure_count -> Integer,
    }
}

diesel::table! {
    workers (worker_id) {
        worker_id -> Text,
        last_heartbeat -> BigInt,
        queues -> Text,
        status -> Text,
        tags -> Nullable<Text>,
        resources -> Nullable<Text>,
        resource_health -> Nullable<Text>,
        threads -> Integer,
        started_at -> Nullable<BigInt>,
        hostname -> Nullable<Text>,
        pid -> Nullable<Integer>,
        pool_type -> Nullable<Text>,
        sdk -> Nullable<Text>,
        sdk_version -> Nullable<Text>,
        registry_fingerprint -> Nullable<Text>,
    }
}

diesel::table! {
    queue_state (queue_name) {
        queue_name -> Text,
        paused -> Bool,
        paused_at -> Nullable<BigInt>,
    }
}

diesel::table! {
    archived_jobs (id) {
        id -> Text,
        queue -> Text,
        task_name -> Text,
        payload -> Binary,
        status -> Integer,
        priority -> Integer,
        created_at -> BigInt,
        scheduled_at -> BigInt,
        started_at -> Nullable<BigInt>,
        completed_at -> Nullable<BigInt>,
        retry_count -> Integer,
        max_retries -> Integer,
        result -> Nullable<Binary>,
        error -> Nullable<Text>,
        timeout_ms -> BigInt,
        unique_key -> Nullable<Text>,
        progress -> Nullable<Integer>,
        metadata -> Nullable<Text>,
        notes -> Nullable<Text>,
        cancel_requested -> Integer,
        expires_at -> Nullable<BigInt>,
        result_ttl_ms -> Nullable<BigInt>,
        namespace -> Nullable<Text>,
    }
}

diesel::table! {
    distributed_locks (lock_name) {
        lock_name -> Text,
        owner_id -> Text,
        acquired_at -> BigInt,
        expires_at -> BigInt,
    }
}

diesel::table! {
    execution_claims (job_id) {
        job_id -> Text,
        worker_id -> Text,
        claimed_at -> BigInt,
    }
}

diesel::table! {
    dashboard_settings (key) {
        key -> Text,
        value -> Text,
        updated_at -> BigInt,
    }
}

diesel::table! {
    topic_subscriptions (topic, subscription_name) {
        topic -> Text,
        subscription_name -> Text,
        task_name -> Text,
        queue -> Text,
        active -> Bool,
        durable -> Bool,
        owner_worker_id -> Nullable<Text>,
        created_at -> BigInt,
        priority -> Nullable<Integer>,
        max_retries -> Nullable<Integer>,
        timeout_ms -> Nullable<BigInt>,
        mode -> Text,
        cursor -> Nullable<Text>,
    }
}

diesel::table! {
    topic_messages (id) {
        id -> Text,
        topic -> Text,
        payload -> Binary,
        metadata -> Nullable<Text>,
        notes -> Nullable<Text>,
        created_at -> BigInt,
        expires_at -> Nullable<BigInt>,
    }
}

diesel::table! {
    topics (name) {
        name -> Text,
        mode -> Text,
        retention_ms -> Nullable<BigInt>,
        created_at -> BigInt,
    }
}

diesel::table! {
    topic_deliveries (topic, subscription_name, message_id) {
        topic -> Text,
        subscription_name -> Text,
        message_id -> Text,
        acked -> Bool,
        attempts -> Integer,
        lease_expires_at -> BigInt,
        delivered_at -> BigInt,
    }
}

diesel::table! {
    job_steps (id) {
        id -> Text,
        job_id -> Text,
        namespace -> Nullable<Text>,
        step_key -> Text,
        seq -> Integer,
        kind -> Text,
        result -> Nullable<Binary>,
        result_len -> Integer,
        wake_at -> Nullable<BigInt>,
        created_at -> BigInt,
    }
}

diesel::allow_tables_to_appear_in_same_query!(jobs, job_dependencies);
diesel::allow_tables_to_appear_in_same_query!(topic_messages, topic_deliveries);
