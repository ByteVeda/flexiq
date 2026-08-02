//! Minimal end-to-end run: enqueue two jobs, execute them on a native worker.
//!
//! ```sh
//! cargo run --example hello -p taskito-core
//! ```

use std::time::{Duration, Instant};

use taskito_core::{
    now_millis, Job, JobStatus, NewJob, SqliteStorage, Storage, StorageBackend, TaskError, Worker,
};

fn new_job(task_name: &str, payload: &[u8]) -> NewJob {
    NewJob {
        queue: "default".to_string(),
        task_name: task_name.to_string(),
        payload: payload.to_vec(),
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
    }
}

fn wait_until_completed(storage: &StorageBackend, job_id: &str) -> Job {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let job = storage
            .get_job(job_id, None)
            .expect("get_job")
            .expect("job exists");
        if job.status == JobStatus::Complete {
            return job;
        }
        assert!(Instant::now() < deadline, "job did not complete in time");
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn main() {
    let storage = StorageBackend::Sqlite(SqliteStorage::new("hello_taskito.db").expect("storage"));

    let handle = Worker::new(storage.clone())
        .num_workers(2)
        .register("greet", |job: &Job| {
            let name = String::from_utf8(job.payload.clone())
                .map_err(|invalid| TaskError::fatal(format!("payload not utf-8: {invalid}")))?;
            println!("hello, {name}!");
            Ok(Some(format!("greeted {name}").into_bytes()))
        })
        .register_async("greet_async", |job: Job| async move {
            let name = String::from_utf8(job.payload).unwrap_or_else(|_| "?".to_string());
            println!("hello (async), {name}!");
            Ok(None)
        })
        .spawn()
        .expect("worker spawn");

    let sync_job = storage
        .enqueue(new_job("greet", b"world"))
        .expect("enqueue");
    let async_job = storage
        .enqueue(new_job("greet_async", b"tokio"))
        .expect("enqueue");

    let done = wait_until_completed(&storage, &sync_job.id);
    println!(
        "sync result: {}",
        String::from_utf8_lossy(done.result.as_deref().unwrap_or_default())
    );
    wait_until_completed(&storage, &async_job.id);

    handle.shutdown().expect("clean shutdown");
    println!("both jobs completed.");
    let _ = std::fs::remove_file("hello_taskito.db");
}
