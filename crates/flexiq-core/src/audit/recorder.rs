//! Getting an SDK dashboard's audit records into storage off the request
//! path (#1020).
//!
//! The same guarantee as `flexiq-server`'s sink, without a tokio runtime the
//! shells may not have: a request hands its records to a bounded channel and
//! returns; one thread drains it and appends in batches. An action is never
//! refused or slowed because the audit table is. A record the channel cannot
//! take, or an append that fails, is written to the log under [`LOG_TARGET`]
//! as one JSON line — degraded to the log stream, never dropped silently.
//!
//! The same thread prunes the namespace's trail on a timer when given a
//! window: a shell-only deployment has no server to do it. The delete is
//! idempotent, so a server or another dashboard pruning the same namespace
//! needs no election — whichever window is shortest wins.

use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TrySendError};
use std::sync::{Mutex, PoisonError};
use std::thread;
use std::time::{Duration, Instant};

use super::record::Actor;
use crate::job::now_millis;
use crate::storage::records::{AuditCutoffs, AuditRecord};
use crate::storage::{Storage, StorageBackend};

/// Records the channel holds before a new one overflows to the log. Sized so
/// a storage stall of a few seconds is absorbed.
pub const BUFFER: usize = 4096;

/// Records per append — one transaction on the Diesel backends, one pipeline
/// on Redis.
pub const BATCH: usize = 256;

/// How often the window is enforced. A record lives at most this much past
/// its window, which against a window of days is noise.
pub const PRUNE_EVERY: Duration = Duration::from_secs(3_600);

/// How long [`AuditRecorder::close`] waits for buffered records to be
/// appended. Bounded, so a storage outage cannot hold the process open.
pub const CLOSE_GRACE: Duration = Duration::from_secs(10);

/// The `log` target a record the table did not take is written under, so an
/// operator can route it apart from the rest of the log.
pub const LOG_TARGET: &str = "flexiq::audit";

/// The sending half and the writer thread behind it.
#[derive(Debug)]
pub struct AuditRecorder {
    records: Mutex<Option<SyncSender<AuditRecord>>>,
    finished: Mutex<Option<Receiver<()>>>,
}

impl AuditRecorder {
    /// Start the writer on `storage`. With a `retention` window it also
    /// prunes `namespace`'s trail now and every [`PRUNE_EVERY`].
    pub fn start(
        storage: StorageBackend,
        namespace: impl Into<String>,
        retention: Option<Duration>,
    ) -> std::io::Result<Self> {
        Self::with_capacity(storage, namespace.into(), retention, BUFFER)
    }

    fn with_capacity(
        storage: StorageBackend,
        namespace: String,
        retention: Option<Duration>,
        capacity: usize,
    ) -> std::io::Result<Self> {
        let (records, inbox) = mpsc::sync_channel(capacity);
        let (done, finished) = mpsc::channel();
        thread::Builder::new()
            .name("flexiq-audit".into())
            .spawn(move || {
                write(&storage, &namespace, retention, &inbox);
                // Release storage before signalling: `close` returning must
                // mean the database file is no longer held by this thread.
                drop(storage);
                let _ = done.send(());
            })?;
        Ok(Self {
            records: Mutex::new(Some(records)),
            finished: Mutex::new(Some(finished)),
        })
    }

    /// Queue `records` for the table, or log each now if that cannot happen.
    pub fn record(&self, records: Vec<AuditRecord>) {
        let sender = self
            .records
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        for record in records {
            let Some(sender) = &sender else {
                to_log(&record, "audit recorder closed");
                continue;
            };
            match sender.try_send(record) {
                Ok(()) => {}
                Err(TrySendError::Full(record)) => to_log(&record, "audit buffer full"),
                Err(TrySendError::Disconnected(record)) => {
                    to_log(&record, "audit writer stopped");
                }
            }
        }
    }

    /// Stop taking records and wait, at most [`CLOSE_GRACE`], for the writer
    /// to append what is buffered. Later records go to the log. Idempotent.
    pub fn close(&self) {
        drop(
            self.records
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take(),
        );
        let finished = self
            .finished
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(finished) = finished {
            if finished.recv_timeout(CLOSE_GRACE).is_err() {
                log::warn!(
                    "audit: the writer was still appending {CLOSE_GRACE:?} after close; \
                     records it had not stored are lost"
                );
            }
        }
    }
}

impl Drop for AuditRecorder {
    /// Closing on drop makes a queue handle's release wait for the writer, so
    /// its storage clone — an open database file on SQLite — is gone too.
    fn drop(&mut self) {
        self.close();
    }
}

/// The recorder an SDK dashboard owns: started when its server starts,
/// closed when it stops, and fed one call per answered request. Bindings
/// hold one of these and forward to it, so the record shape and the
/// off-path write live here, not in each shell.
#[derive(Debug, Default)]
pub struct DashboardAudit {
    running: Mutex<Option<Running>>,
}

#[derive(Debug)]
struct Running {
    recorder: std::sync::Arc<AuditRecorder>,
    namespace: String,
}

impl DashboardAudit {
    /// Start recording into `namespace` (`None` is the default namespace),
    /// pruning it to `retention` when given. A second start while running is
    /// a no-op: the first window stands.
    pub fn start(
        &self,
        storage: StorageBackend,
        namespace: Option<&str>,
        retention: Option<Duration>,
    ) -> std::io::Result<()> {
        let mut running = self.running.lock().unwrap_or_else(PoisonError::into_inner);
        if running.is_some() {
            return Ok(());
        }
        let namespace = namespace
            .unwrap_or(crate::scheduler::retention::DEFAULT_NAMESPACE)
            .to_string();
        let recorder = AuditRecorder::start(storage, namespace.clone(), retention)?;
        *running = Some(Running {
            recorder: std::sync::Arc::new(recorder),
            namespace,
        });
        Ok(())
    }

    /// Record one answered request: `username` is the signed-in user, `None`
    /// a dashboard with auth off. Anything but a state-changing route, or a
    /// call while not started, records nothing. Never blocks on storage.
    pub fn record(&self, method: &str, path: &str, status: u16, username: Option<&str>) {
        let running = self.running.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(running) = running.as_ref() else {
            return;
        };
        let actor = username.map_or_else(Actor::anonymous, Actor::user);
        running.recorder.record(super::dashboard::action_records(
            &running.namespace,
            &actor,
            method,
            path,
            status,
        ));
    }

    /// Stop recording and flush what is buffered — see
    /// [`AuditRecorder::close`]. A later [`Self::start`] starts afresh.
    pub fn close(&self) {
        let running = self
            .running
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(running) = running {
            running.recorder.close();
        }
    }
}

/// The writer: batch, append, prune when due, repeat — until every sender is
/// gone and the buffer is empty.
fn write(
    storage: &StorageBackend,
    namespace: &str,
    retention: Option<Duration>,
    inbox: &Receiver<AuditRecord>,
) {
    let mut next_prune = Instant::now();
    loop {
        let wait = match retention {
            Some(window) => {
                let now = Instant::now();
                if now >= next_prune {
                    prune(storage, namespace, window);
                    next_prune = now + PRUNE_EVERY;
                }
                next_prune.saturating_duration_since(now)
            }
            None => PRUNE_EVERY,
        };
        match inbox.recv_timeout(wait) {
            Ok(first) => {
                let mut batch = Vec::with_capacity(BATCH);
                batch.push(first);
                batch.extend(inbox.try_iter().take(BATCH - 1));
                append(storage, &batch);
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => return,
        }
    }
}

/// Append one batch; on failure, log every record in it.
fn append(storage: &StorageBackend, records: &[AuditRecord]) {
    if let Err(error) = storage.append_audit(records) {
        log::error!(
            "audit: could not store {} record(s): {error}",
            records.len()
        );
        for record in records {
            to_log(record, "audit write failed");
        }
    }
}

/// One pass: delete what the window has passed, and say so.
fn prune(storage: &StorageBackend, namespace: &str, window: Duration) {
    let cutoff = cutoff(now_millis(), window);
    match storage.purge_audit(namespace, &AuditCutoffs::uniform(cutoff)) {
        Ok(0) => {}
        Ok(removed) => log::info!(
            "audit: pruned {removed} record(s) past the retention window in '{namespace}'"
        ),
        Err(error) => log::warn!("audit: pruning '{namespace}' failed: {error}"),
    }
}

/// The instant a record must be newer than to survive. Saturating, so an
/// absurd window keeps everything rather than wrapping into deleting it.
fn cutoff(now_ms: i64, window: Duration) -> i64 {
    let window_ms = i64::try_from(window.as_millis()).unwrap_or(i64::MAX);
    now_ms.saturating_sub(window_ms)
}

/// One record as one JSON line under [`LOG_TARGET`]. A record names its
/// caller by public id or username only, so it is safe to log whole.
fn to_log(record: &AuditRecord, why: &str) {
    match serde_json::to_string(record) {
        Ok(json) => log::warn!(target: LOG_TARGET, "{why}: {json}"),
        Err(error) => log::error!(target: LOG_TARGET, "{why}: unserialisable record: {error}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audit::record::{records, Access, Actor};
    use crate::storage::records::AuditFilter;
    use crate::storage::sqlite::SqliteStorage;

    fn backend() -> StorageBackend {
        StorageBackend::Sqlite(SqliteStorage::in_memory().expect("sqlite"))
    }

    fn one(namespace: &str, at_ms: Option<i64>) -> Vec<AuditRecord> {
        let mut out = records(
            namespace,
            &Actor::anonymous(),
            Access::Write,
            "op",
            vec![],
            "OK",
        );
        if let Some(at_ms) = at_ms {
            out[0].at_ms = at_ms;
        }
        out
    }

    fn stored(storage: &StorageBackend, namespace: &str) -> usize {
        storage
            .list_audit_after(namespace, &AuditFilter::default(), 1_000, None)
            .expect("list")
            .len()
    }

    #[test]
    fn close_appends_everything_accepted() {
        let storage = backend();
        let recorder = AuditRecorder::start(storage.clone(), "prod", None).expect("start");
        for _ in 0..300 {
            recorder.record(one("prod", None));
        }
        recorder.close();
        assert_eq!(stored(&storage, "prod"), 300, "across more than one batch");

        // Closed: a late record goes to the log, not the table, and a second
        // close is a no-op.
        recorder.record(one("prod", None));
        recorder.close();
        assert_eq!(stored(&storage, "prod"), 300);
    }

    /// Dropping the recorder — a queue handle released without `close` —
    /// still flushes and lets go of storage before the drop returns.
    #[test]
    fn dropping_the_recorder_waits_for_the_writer() {
        let storage = backend();
        let recorder = AuditRecorder::start(storage.clone(), "prod", None).expect("start");
        recorder.record(one("prod", None));
        drop(recorder);
        assert_eq!(stored(&storage, "prod"), 1);

        let audit = DashboardAudit::default();
        audit
            .start(storage.clone(), Some("prod"), None)
            .expect("start");
        audit.record("POST", "/api/queues/emails/pause", 200, None);
        drop(audit);
        assert_eq!(stored(&storage, "prod"), 2);
    }

    #[test]
    fn a_full_buffer_never_blocks_the_caller() {
        let storage = backend();
        let recorder =
            AuditRecorder::with_capacity(storage.clone(), "prod".into(), None, 1).expect("start");
        let started = Instant::now();
        for _ in 0..1_000 {
            recorder.record(one("prod", None));
        }
        assert!(started.elapsed() < Duration::from_secs(5));
        recorder.close();
        assert!(stored(&storage, "prod") >= 1);
    }

    #[test]
    fn a_window_prunes_its_namespace_at_start() {
        let storage = backend();
        let old = now_millis() - 2 * 86_400_000;
        storage.append_audit(&one("prod", Some(old))).expect("seed");
        storage
            .append_audit(&one("staging", Some(old)))
            .expect("seed");

        let recorder =
            AuditRecorder::start(storage.clone(), "prod", Some(Duration::from_secs(86_400)))
                .expect("start");
        recorder.close();

        assert_eq!(stored(&storage, "prod"), 0);
        assert_eq!(
            stored(&storage, "staging"),
            1,
            "another namespace keeps its trail"
        );
    }

    #[test]
    fn a_dashboard_records_only_while_started() {
        let storage = backend();
        let audit = DashboardAudit::default();
        audit.record("POST", "/api/queues/emails/pause", 200, Some("alice"));

        audit
            .start(storage.clone(), Some("prod"), None)
            .expect("start");
        audit.record("POST", "/api/queues/emails/pause", 200, Some("alice"));
        audit.record("GET", "/api/queues", 200, Some("alice"));
        audit.record("POST", "/api/dead-letters/purge", 403, None);
        audit.close();
        audit.record("POST", "/api/queues/emails/resume", 200, Some("alice"));

        let trail = storage
            .list_audit_after("prod", &AuditFilter::default(), 10, None)
            .expect("list");
        assert_eq!(trail.len(), 2, "{trail:?}");
        let pause = trail
            .iter()
            .find(|r| r.target.as_deref() == Some("emails"))
            .expect("pause");
        assert_eq!(pause.token_id, "alice");
        assert_eq!(pause.target_kind.as_deref(), Some("queue"));
        let purge = trail.iter().find(|r| r.target.is_none()).expect("purge");
        assert_eq!(purge.principal_kind, "anonymous");
        assert_eq!(purge.outcome, "PERMISSION_DENIED");
    }

    #[test]
    fn an_absurd_window_keeps_everything() {
        assert!(cutoff(1_000, Duration::MAX) < 0, "before every record");
    }
}
