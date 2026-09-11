# flexiq

Embeddable task queue for Rust: durable jobs on SQLite (default), PostgreSQL, or
Redis, a scheduler with retries, rate limits, circuit breakers and cron
periodics, durable steps that survive a retry, and a worker that runs your
functions.

One process, no daemon, straight to SQLite. Nothing else has to be running.

This crate is the SDK: an attribute macro for registering tasks, a queue handle
for enqueuing them, and a worker for running them.
[`flexiq-core`](https://crates.io/crates/flexiq-core) is the engine underneath,
re-exported here in full — `flexiq::Storage` and `flexiq_core::Storage` are the
same trait — so anything the shell does not wrap is still one import away.

## Quick start

```rust,no_run
/// Charge an order. Runs at most five times, thirty seconds a try.
#[flexiq::task(max_retries = 5, timeout = "30s", queue = "billing")]
fn charge(order_id: String, cents: i64) -> flexiq::Outcome<i64> {
    println!("charging {order_id} {cents} cents");
    Ok(cents)
}

fn main() -> flexiq::Result<()> {
    let queue = flexiq::FlexiQ::open("flexiq.db")?;

    // Enqueue. The arguments are checked exactly as a direct call would be.
    queue.enqueue(charge::call("ord-1".into(), 4200))?;

    // Run. The worker owns the scheduler; `shutdown` drains and unregisters.
    let worker = queue.worker().register::<charge>().num_workers(4).spawn()?;
    std::thread::sleep(std::time::Duration::from_millis(500));
    worker.shutdown()
}
```

`#[flexiq::task]` replaces the function with a type of the same name carrying
three things: `charge::call(..)` builds an enqueueable value with the same
argument list, `charge::run(..)` is the original body and stays directly
callable from a unit test, and the `Task` implementation is what a worker
registers.

Per-call options override what the attribute declared:

```rust
# fn main() -> flexiq::Result<()> {
#[flexiq::task]
fn greet(name: String) -> flexiq::Outcome<()> { let _ = name; Ok(()) }

let queue = flexiq::FlexiQ::in_memory()?;
let job = queue.enqueue(
    greet::call("world".into())
        .priority(9)
        .delay_ms(60_000)
        .unique_key("greet-world"),
)?;

assert_eq!(job.task_name, "greet");
# Ok(())
# }
```

## Durable steps

A step runs once per durable run and is memoized afterwards, so a retry resumes
instead of repeating. A card charged once stays charged once, however many times
the work after it fails.

```rust,ignore
#[flexiq::task(max_retries = 5)]
fn checkout(order_id: String) -> flexiq::Outcome<()> {
    let mut step = flexiq::current_step();

    let receipt: String = step.run("charge", || Ok(charge_card(&order_id)?))?;
    step.sleep_ms("settle", 60_000)?;
    step.run("email", || Ok(send_receipt(&receipt)?))?;

    Ok(())
}
```

`sleep_ms` does not block a thread: the job is rescheduled and its claim
released, so the worker's slot is free while it waits. The attempt ends there —
what follows runs on the next attempt, from the top, with every committed step
memoized. A sleep is not a retry and does not spend one.

## Periodic tasks

```rust,ignore
// Six fields, seconds first.
#[flexiq::task(cron = "0 */5 * * * *", timezone = "Europe/Stockholm")]
fn sweep() -> flexiq::Outcome<()> {
    Ok(())
}
```

A worker registers every scheduled task it knows about at startup. Firing is
per-scheduler and not leader-elected, so register a periodic from one process
unless a fleet double-firing it is acceptable.

## Talking to the other SDKs

Task arguments travel in the cross-SDK envelope, byte for byte as
`contracts/wire-vectors.json` pins it, so a job enqueued here runs on a Python,
Node or Java worker and the reverse holds. Task names are the meeting point: a
task is named after its function unless `name = "..."` says otherwise.

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
flexiq = { version = "2", features = ["workflows", "mesh"] }
```

```rust,ignore
use flexiq::mesh::MeshNode;
use flexiq::workflows::WorkflowRun;
```

Both are off by default. The other SDKs ship these inside one compiled artifact,
but Rust resolves source crates, so the equivalent is an opt-in dependency — a
consumer who only enqueues jobs should not compile a DAG engine and a gossip
mesh. Depending on `flexiq-workflows` or `flexiq-mesh` directly works exactly
the same; the re-export is a convenience, not a wrapper.

## License

MIT
