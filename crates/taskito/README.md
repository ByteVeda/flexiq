# taskito

Embeddable task queue for Rust: durable jobs on SQLite (default), PostgreSQL, or
Redis, a scheduler with retries, rate limits, circuit breakers, cron periodics,
and pub/sub fan-out — and a native worker that runs your handlers.

This crate is the `taskito` entry point, matching the name the Python, Node, and
Java SDKs already use. It is a re-export of
[`taskito-core`](https://crates.io/crates/taskito-core) and adds nothing of its
own: `taskito::Worker` and `taskito_core::Worker` are the same type, so the two
can be mixed freely and either name works in a dependency graph that contains
both.

Reach for `taskito-core` directly if you prefer the explicit name; everything is
documented there.

## Quick start

```rust,no_run
use taskito::{now_millis, Job, NewJob, SqliteStorage, Storage, StorageBackend, Worker};

fn main() -> taskito::Result<()> {
    let storage = StorageBackend::Sqlite(SqliteStorage::new("taskito.db")?);

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
    })?;

    std::thread::sleep(std::time::Duration::from_millis(500));
    handle.shutdown()
}
```

## Features

| Feature | Effect |
| --- | --- |
| *(default)* | SQLite storage |
| `postgres` | PostgreSQL storage |
| `redis` | Redis storage |
| `push-dispatch` | event-driven scheduler wakeups instead of polling |

Each forwards to the identically named feature on `taskito-core`.

## Companion crates

- [`taskito-workflows`](https://crates.io/crates/taskito-workflows) — DAG workflows
- [`taskito-mesh`](https://crates.io/crates/taskito-mesh) — decentralized mesh scheduling

## License

MIT
