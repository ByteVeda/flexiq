# flexiq-core

Embeddable task queue for Rust: durable jobs on SQLite (default), PostgreSQL,
or Redis, a scheduler with retries, rate limits, circuit breakers, cron
periodics, and pub/sub fan-out — and a native worker that runs your handlers.

This crate is the engine behind the FlexiQ SDKs; it works standalone in any
Rust program.

## Quick start

```rust,no_run
use flexiq_core::{
    now_millis, Job, NewJob, SqliteStorage, Storage, StorageBackend, Worker,
};

fn main() -> flexiq_core::Result<()> {
    let storage = StorageBackend::Sqlite(SqliteStorage::new("flexiq.db")?);

    // A worker executes registered handlers for dequeued jobs.
    let handle = Worker::new(storage.clone())
        .num_workers(4)
        .register("greet", |job: &Job| {
            println!("hello, {}!", String::from_utf8_lossy(&job.payload));
            Ok(None)
        })
        .spawn()?;

    // Producers enqueue jobs — from this process or any other.
    storage.enqueue(NewJob {
        queue: "default".to_string(),
        task_name: "greet".to_string(),
        payload: b"world".to_vec(),
        priority: 0,
        scheduled_at: now_millis(),
        max_retries: 3,
        timeout_ms: 30_000,
        unique_key: None,
        metadata: None,
        notes: None,
        depends_on: vec![],
        expires_at: None,
        result_ttl_ms: None,
        namespace: None,
        debounce_key: None,
    })?;

    std::thread::sleep(std::time::Duration::from_millis(500));
    handle.shutdown()
}
```

Async handlers work too — `register_async` spawns the future on the worker's
runtime. See `examples/hello.rs` for a runnable version.

## Storage backends

| Backend | Feature | Notes |
|---|---|---|
| SQLite | (default) | Zero-config, single file, bundled `libsqlite3` |
| PostgreSQL | `postgres` | `SKIP LOCKED` dequeue, `LISTEN/NOTIFY` push dispatch |
| Redis | `redis` | JSON blobs + sorted-set indexes, Lua-atomic claims |

All three pass one shared contract test suite; behavior is identical up to
backend-documented differences.

## What's in the box

- **Jobs**: priorities, delays, per-job timeouts, uniqueness/idempotency keys,
  dependencies, expiry, result TTLs, namespaces.
- **Scheduler**: adaptive polling (or event-driven wakeups via the
  `push-dispatch` feature), batch claiming, bounded in-flight dispatch,
  stale-job reaping, retention with per-table windows.
- **Resilience**: exponential-backoff retries, per-task rate limits and retry
  budgets, circuit breakers, a dead-letter queue with replay.
- **Workers**: `Worker` builder + `TaskRegistry` for sync and async Rust
  handlers, heartbeat/registry with elected cluster reaps.
- **Pub/sub**: topics with durable/ephemeral subscriptions and fan-out on
  publish.
- **Periodic tasks**: cron expressions with optional time zones.

## License

MIT
