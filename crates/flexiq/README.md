# flexiq

Embeddable task queue for Rust: durable jobs on SQLite (default), PostgreSQL, or
Redis, a scheduler with retries, rate limits, circuit breakers, cron periodics,
and pub/sub fan-out — and a native worker that runs your handlers.

This crate is the `flexiq` entry point, matching the name the Python, Node, and
Java SDKs already use. It is a re-export of
[`flexiq-core`](https://crates.io/crates/flexiq-core) and adds nothing of its
own: `flexiq::Worker` and `flexiq_core::Worker` are the same type, so the two
can be mixed freely and either name works in a dependency graph that contains
both.

Reach for `flexiq-core` directly if you prefer the explicit name; everything is
documented there.

## Quick start

```rust,no_run
use flexiq::{now_millis, Job, NewJob, SqliteStorage, Storage, StorageBackend, Worker};

fn main() -> flexiq::Result<()> {
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

## Features

| Feature | Effect |
| --- | --- |
| *(default)* | SQLite storage |
| `postgres` | PostgreSQL storage |
| `redis` | Redis storage |
| `push-dispatch` | event-driven scheduler wakeups instead of polling |
| `workflows` | DAG workflows at `flexiq::workflows` |
| `mesh` | decentralized mesh scheduling at `flexiq::mesh` |

The storage features forward to the identically named feature on `flexiq-core`.

## Companion engines

[`flexiq-workflows`](https://crates.io/crates/flexiq-workflows) and
[`flexiq-mesh`](https://crates.io/crates/flexiq-mesh) are separate crates. Turn
them on here to get them under one dependency:

```toml
flexiq = { version = "0.22", features = ["workflows", "mesh"] }
```

```rust,ignore
use flexiq::workflows::WorkflowRun;
use flexiq::mesh::MeshNode;
```

Both are off by default. The other SDKs ship these inside one compiled artifact,
but Rust resolves source crates, so the equivalent is an opt-in dependency — a
consumer who only enqueues jobs should not compile a DAG engine and a gossip
mesh. Depending on `flexiq-workflows` or `flexiq-mesh` directly works exactly
the same; the re-export is a convenience, not a wrapper.

## License

MIT
