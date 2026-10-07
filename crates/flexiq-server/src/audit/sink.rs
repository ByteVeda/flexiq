//! Getting audit records into storage without putting storage on the call.
//!
//! The layer hands each record to a bounded channel and returns; one writer
//! task drains it and appends in batches. A call never waits on the audit
//! write and never fails because of it — refusing an enqueue because the audit
//! table is slow would make that table an availability dependency of every
//! write this door serves.
//!
//! What that costs is stated rather than hidden: a record the channel cannot
//! take, or an append that fails, is written to the log under
//! [`LOG_TARGET`] as one JSON line and counted in `flexiq_audit_records_total`.
//! It degrades to the log stream; it is never dropped silently.

use std::time::Duration;

use flexiq_core::{AuditRecord, Storage, StorageBackend};
use tokio::sync::mpsc::{self, error::TrySendError};
use tokio::task::JoinHandle;

use super::metrics;
use crate::runtime::shutdown::Shutdown;

/// Records the channel holds before a new one overflows to the log. Sized so
/// a storage stall of a few seconds under a busy door is absorbed.
pub const BUFFER: usize = 4096;

/// Records per append — one transaction on the Diesel backends, one pipeline
/// on Redis.
pub const BATCH: usize = 256;

/// The `log` target a record the table did not take is written under, so an
/// operator can route it apart from the rest of the log.
pub const LOG_TARGET: &str = "flexiq::audit";

/// The sending half. Cheap to clone; every clone feeds the one writer.
#[derive(Debug, Clone)]
pub struct AuditSink {
    records: mpsc::Sender<AuditRecord>,
}

/// How long a stopping listener waits for the writer to append what is
/// buffered. Bounded, so a storage outage cannot hold the process open.
pub const WRITER_GRACE: Duration = Duration::from_secs(10);

/// The writer task, for the listener to await on the way out — a detached
/// task would be dropped with the runtime, buffered records and all.
#[derive(Debug)]
pub struct AuditWriter(JoinHandle<()>);

impl AuditWriter {
    /// Wait for the writer to finish, at most [`WRITER_GRACE`]. It finishes
    /// once shutdown fires or every sink is dropped, and the buffer is empty.
    pub async fn finish(self) {
        match tokio::time::timeout(WRITER_GRACE, self.0).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => log::error!("audit: the writer task failed: {error}"),
            Err(_) => log::warn!(
                "audit: the writer was still appending {WRITER_GRACE:?} after shutdown; \
                 records it had not stored are lost"
            ),
        }
    }
}

impl AuditSink {
    /// Start the writer on `storage`. It drains what is buffered and stops
    /// when `shutdown` fires; await the [`AuditWriter`] to let it.
    pub fn start(storage: StorageBackend, shutdown: Shutdown) -> (Self, AuditWriter) {
        Self::with_capacity(storage, BUFFER, shutdown)
    }

    fn with_capacity(
        storage: StorageBackend,
        capacity: usize,
        shutdown: Shutdown,
    ) -> (Self, AuditWriter) {
        let (records, inbox) = mpsc::channel(capacity);
        let writer = tokio::spawn(write(storage, inbox, shutdown));
        (Self { records }, AuditWriter(writer))
    }

    /// Queue `record` for the table, or log it now if that cannot happen.
    pub fn record(&self, record: AuditRecord) {
        match self.records.try_send(record) {
            Ok(()) => {}
            Err(TrySendError::Full(record)) => {
                metrics::overflowed();
                to_log(&record, "audit buffer full");
            }
            Err(TrySendError::Closed(record)) => {
                metrics::failed(1);
                to_log(&record, "audit writer stopped");
            }
        }
    }
}

/// The writer: batch, append, repeat. On shutdown it appends whatever is
/// already buffered, so a clean stop loses nothing that was accepted.
async fn write(
    storage: StorageBackend,
    mut inbox: mpsc::Receiver<AuditRecord>,
    shutdown: Shutdown,
) {
    let mut batch = Vec::with_capacity(BATCH);
    loop {
        tokio::select! {
            // `recv_many` is cancel-safe: losing the race takes nothing.
            received = inbox.recv_many(&mut batch, BATCH) => {
                if received == 0 {
                    return;
                }
                append(&storage, std::mem::take(&mut batch)).await;
            }
            () = shutdown.wait() => {
                inbox.close();
                while inbox.recv_many(&mut batch, BATCH).await > 0 {
                    append(&storage, std::mem::take(&mut batch)).await;
                }
                return;
            }
        }
    }
}

/// Append one batch on the blocking pool; on failure, log every record in it.
async fn append(storage: &StorageBackend, records: Vec<AuditRecord>) {
    let storage = storage.clone();
    let count = records.len();
    let outcome = tokio::task::spawn_blocking(move || {
        let result = storage.append_audit(&records);
        (records, result)
    })
    .await;
    match outcome {
        Ok((_, Ok(()))) => metrics::written(count),
        Ok((records, Err(error))) => {
            metrics::failed(count);
            log::error!("audit: could not store {count} record(s): {error}");
            for record in &records {
                to_log(record, "audit write failed");
            }
        }
        // The append panicked or the runtime is stopping; the records went
        // with the task, so only their number is left to report.
        Err(error) => {
            metrics::failed(count);
            log::error!("audit: {count} record(s) lost with a failed write task: {error}");
        }
    }
}

/// One record as one JSON line under [`LOG_TARGET`]. A record names a token by
/// its public id only, so it is safe to log whole.
fn to_log(record: &AuditRecord, why: &str) {
    match serde_json::to_string(record) {
        Ok(json) => log::warn!(target: LOG_TARGET, "{why}: {json}"),
        Err(error) => log::error!(target: LOG_TARGET, "{why}: unserialisable record: {error}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flexiq_core::storage::sqlite::SqliteStorage;
    use flexiq_core::AuditFilter;
    use std::time::Duration;

    fn backend() -> StorageBackend {
        StorageBackend::Sqlite(SqliteStorage::in_memory().expect("in-memory sqlite"))
    }

    fn record(id: &str) -> AuditRecord {
        AuditRecord {
            id: id.to_string(),
            namespace: "prod".to_string(),
            at_ms: 1,
            principal_kind: "token".to_string(),
            token_id: "tok".to_string(),
            principal: "ci".to_string(),
            operation: "flexiq.v1.ProducerService/Enqueue".to_string(),
            target_kind: None,
            target: None,
            outcome: "OK".to_string(),
        }
    }

    fn stored(storage: &StorageBackend) -> usize {
        storage
            .list_audit_after("prod", &AuditFilter::default(), 1_000, None)
            .expect("list")
            .len()
    }

    #[tokio::test]
    async fn a_queued_record_reaches_the_table() {
        let storage = backend();
        let (sink, _writer) = AuditSink::start(storage.clone(), Shutdown::default());
        sink.record(record("r1"));
        for _ in 0..100 {
            if stored(&storage) == 1 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("the writer never appended the record");
    }

    #[tokio::test]
    async fn shutdown_appends_what_was_already_buffered() {
        let storage = backend();
        let shutdown = Shutdown::default();
        let (sink, writer) = AuditSink::start(storage.clone(), shutdown.clone());
        for i in 0..10 {
            sink.record(record(&format!("r{i}")));
        }
        shutdown.trigger();
        // Finished means stored: no polling, which is the listener's guarantee.
        writer.finish().await;
        assert_eq!(
            stored(&storage),
            10,
            "buffered records were lost on shutdown"
        );
    }

    /// A listener whose serve loop ended without a shutdown drops its sinks;
    /// the writer must end then too, or `finish` would sit out its grace.
    #[tokio::test]
    async fn dropping_every_sink_ends_the_writer() {
        let storage = backend();
        let (sink, writer) = AuditSink::start(storage.clone(), Shutdown::default());
        sink.record(record("r1"));
        drop(sink);
        tokio::time::timeout(Duration::from_secs(2), writer.finish())
            .await
            .expect("the writer ends once no sink is left");
        assert_eq!(stored(&storage), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_full_buffer_overflows_to_the_log_without_blocking() {
        // On a current-thread runtime the writer cannot run until this test
        // yields, so a capacity of one is full after the first record.
        let (sink, _writer) = AuditSink::with_capacity(backend(), 1, Shutdown::default());
        sink.record(record("kept"));
        let before = metrics::overflowed_total();
        sink.record(record("overflowed"));
        assert!(
            metrics::overflowed_total() > before,
            "the overflow is counted"
        );
    }
}
