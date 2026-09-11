//! The shell end to end: declare a task, enqueue it, run it, read the result.
//!
//! `cargo run -p flexiq --example quickstart`
//!
//! `flexiq-core` ships the same program written against the engine
//! (`examples/hello.rs`), where the producer hand-writes all fifteen `NewJob`
//! fields and the handler is a closure over raw bytes. The difference between
//! the two files is what this crate is for.

use std::time::{Duration, Instant};

use flexiq::{FlexiQ, JobStatus};

/// Charge an order, returning what was charged.
#[flexiq::task(max_retries = 5, timeout = "30s", queue = "billing")]
fn charge(order_id: String, cents: i64) -> flexiq::Outcome<i64> {
    println!("  charging {order_id}: {cents} cents");
    Ok(cents)
}

/// Add up a batch, to show a second task and a second shape of argument.
#[flexiq::task]
fn total(amounts: Vec<i64>) -> flexiq::Outcome<i64> {
    let sum = amounts.iter().sum();
    println!("  totalling {} amounts: {sum}", amounts.len());
    Ok(sum)
}

fn main() -> flexiq::Result<()> {
    // A private database that dies with the process. `FlexiQ::open("path.db")`
    // is the durable one.
    let queue = FlexiQ::in_memory()?;

    println!("enqueueing");
    let charged = queue.enqueue(charge::call("ord-1".into(), 4_200))?;
    let totalled = queue.enqueue(total::call(vec![1, 2, 3, 4]).priority(9))?;

    println!("starting a worker");
    let worker = queue
        .worker()
        .queues(["billing", "default"])
        .register::<charge>()
        .register::<total>()
        .num_workers(2)
        .spawn()?;

    for id in [&charged.id, &totalled.id] {
        let job = wait_for(&queue, id);
        println!(
            "{} finished as {:?}, result {} bytes",
            job.task_name,
            job.status,
            job.result.map(|r| r.len()).unwrap_or(0)
        );
    }

    // The body is still an ordinary function, callable without a queue — which
    // is how you unit-test one. Its error type is `Abort`, not `QueueError`:
    // "this attempt failed" and "the queue could not be reached" are different
    // things, so `?` deliberately does not bridge them.
    match charge::run("ord-2".into(), 7) {
        Ok(cents) => println!("calling the body directly: {cents}"),
        Err(abort) => println!("the body aborted: {abort:?}"),
    }

    worker.shutdown()
}

/// Poll until `job_id` finishes.
fn wait_for(queue: &FlexiQ, job_id: &str) -> flexiq::Job {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if let Some(job) = queue.get_job(job_id).expect("reads") {
            if matches!(job.status, JobStatus::Complete | JobStatus::Failed) {
                return job;
            }
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    panic!("job {job_id} did not finish");
}
