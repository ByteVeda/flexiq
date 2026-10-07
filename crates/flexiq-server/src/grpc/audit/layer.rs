//! The tower layer that turns an answered call into audit records.
//!
//! It sits **between** the metrics layer and [`AuthLayer`]: inside metrics,
//! which has nothing to add, and outside auth, so a call the gate refuses for
//! want of a scope is still answered through it and recorded. A layer inside
//! auth would never see that refusal, and a refused write is exactly what an
//! audit trail is asked about.
//!
//! Which calls it records is decided from the gate table, not listed here:
//! every path whose requirement is a write scope — `produce` or `admin`. So a
//! mutating RPC added later is recorded without anyone editing this file, the
//! same fail-closed default the scope check itself has. The executor door and
//! public paths pass straight through with no slot and no record.
//!
//! Reads — paths needing `read` or `inspect` — are recorded only when the
//! operator turns them on (#993), and then folded by [`ReadDedup`] so a poller
//! leaves one record per thing it looks at per window, not one per poll. A
//! stream is recorded once, when it opens: the layer settles on the response
//! head, and a stream's frames never pass through it.
//!
//! [`AuthLayer`]: crate::grpc::auth::AuthLayer

use std::borrow::Cow;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use flexiq_core::job::now_millis;
use flexiq_core::AuditRecord;
use tonic::Code;
use tower_layer::Layer;
use tower_service::Service;

use super::context::AuditContext;
use super::dedup::ReadDedup;
use super::sink::AuditSink;
use crate::grpc::auth::gate::{self, Requirement};
use crate::grpc::auth::Scope;
use crate::grpc::facade::error::code_name;
use crate::grpc::metrics;
use crate::grpc::metrics::layer::answered_code;

/// Records every authorised write the listener answers, and its reads when
/// those are turned on.
#[derive(Debug, Clone)]
pub struct AuditLayer {
    sink: AuditSink,
    reads: Option<Arc<ReadDedup>>,
}

impl AuditLayer {
    /// Record writes into `sink`, and reads too when `reads` is the window
    /// repeats are folded within.
    pub fn new(sink: AuditSink, reads: Option<Duration>) -> Self {
        Self {
            sink,
            reads: reads.map(|window| Arc::new(ReadDedup::new(window))),
        }
    }
}

impl<S> Layer<S> for AuditLayer {
    type Service = Audited<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Audited {
            inner,
            sink: self.sink.clone(),
            reads: self.reads.clone(),
        }
    }
}

/// A service whose writes, and optionally reads, are recorded.
#[derive(Debug, Clone)]
pub struct Audited<S> {
    inner: S,
    sink: AuditSink,
    reads: Option<Arc<ReadDedup>>,
}

/// What an auditable call does to the namespace.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Access {
    /// It needs `produce` or `admin`. Always recorded.
    Write,
    /// It needs `read` or `inspect`. Recorded when reads are turned on.
    Read,
}

/// Which kind of auditable call `path` is, or `None` for one the trail never
/// records: the executor door, public paths and the merely-authenticated ones.
pub fn access(method: &http::Method, path: &str) -> Option<Access> {
    // Every scope named, so a new one is a compile error here, not a silent
    // gap in the trail.
    match gate::requirement(method, path) {
        Requirement::Scoped(Scope::Produce | Scope::Admin) => Some(Access::Write),
        Requirement::Scoped(Scope::Read | Scope::Inspect) => Some(Access::Read),
        Requirement::Scoped(Scope::Execute) | Requirement::Authenticated | Requirement::Public => {
            None
        }
    }
}

impl<S, ReqBody, ResBody> Service<http::Request<ReqBody>> for Audited<S>
where
    S: Service<http::Request<ReqBody>, Response = http::Response<ResBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
    ReqBody: Send + 'static,
    ResBody: Send + 'static,
{
    type Response = http::Response<ResBody>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut request: http::Request<ReqBody>) -> Self::Future {
        // The ready service moves into the future; see `auth/layer.rs`.
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);

        // A read is folded through the dedup; a write never is.
        let dedup = match access(request.method(), request.uri().path()) {
            Some(Access::Write) => None,
            Some(Access::Read) if self.reads.is_some() => self.reads.clone(),
            Some(Access::Read) | None => return Box::pin(inner.call(request)),
        };

        let (operation, _) =
            metrics::labels(request.method(), request.uri().path(), request.headers());
        let context = AuditContext::default();
        request.extensions_mut().insert(context.clone());
        let mut pending = Pending {
            call: Some(Call {
                context,
                operation,
                sink: self.sink.clone(),
                dedup,
            }),
        };

        Box::pin(async move {
            let answered = inner.call(request).await;
            pending.settle(match &answered {
                Ok(response) => answered_code(response),
                // The service failed below the gRPC layer; no status was sent.
                Err(_) => Code::Unknown,
            });
            answered
        })
    }
}

/// A call whose records are still owed. Settled with the answer's code; if
/// the future is dropped first — a deadline, a client that hung up — the drop
/// settles it `CANCELLED`, because the handler may already have committed a
/// write that must not go unrecorded.
struct Pending {
    call: Option<Call>,
}

/// What settling one call needs.
struct Call {
    context: AuditContext,
    operation: Cow<'static, str>,
    sink: AuditSink,
    /// Set on a read: the repeats it folds away.
    dedup: Option<Arc<ReadDedup>>,
}

impl Pending {
    fn settle(&mut self, code: Code) {
        if let Some(call) = self.call.take() {
            for record in records(&call.context, &call.operation, code) {
                if call.dedup.as_ref().is_none_or(|dedup| dedup.admit(&record)) {
                    call.sink.record(record);
                }
            }
        }
    }
}

impl Drop for Pending {
    fn drop(&mut self) {
        self.settle(Code::Cancelled);
    }
}

/// The records one answered call leaves: one per target, or one with no
/// target when the call was refused before naming any. None at all when no
/// credential was believed — there is no trustworthy token id to record, and
/// the token store already logs those refusals.
fn records(context: &AuditContext, operation: &str, code: Code) -> Vec<AuditRecord> {
    let (Some(principal), targets) = context.take() else {
        return Vec::new();
    };
    let at_ms = now_millis();
    let record = |target: Option<(&'static str, String)>| {
        let (target_kind, target) = target.unzip();
        AuditRecord {
            id: uuid::Uuid::now_v7().to_string(),
            namespace: principal.namespace().to_string(),
            at_ms,
            principal_kind: "token".to_string(),
            token_id: principal.credential().to_string(),
            principal: principal.name().to_string(),
            operation: operation.to_string(),
            target_kind: target_kind.map(str::to_string),
            target,
            outcome: code_name(code).to_string(),
        }
    };
    if targets.is_empty() {
        return vec![record(None)];
    }
    targets
        .into_iter()
        .map(|(kind, id)| record(Some((kind.as_str(), id))))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::grpc::audit::TargetKind;
    use crate::grpc::auth::{Principal, ScopeSet};
    use crate::runtime::shutdown::Shutdown;
    use flexiq_core::storage::sqlite::SqliteStorage;
    use flexiq_core::{AuditFilter, Storage, StorageBackend};
    use tower::{service_fn, ServiceExt as _};

    const POST: http::Method = http::Method::POST;
    const GET: http::Method = http::Method::GET;

    #[test]
    fn writes_and_reads_are_told_apart() {
        let write = Some(Access::Write);
        assert_eq!(access(&POST, "/flexiq.v1.ProducerService/Enqueue"), write);
        assert_eq!(access(&POST, "/flexiq.v1.ProducerService/CancelJob"), write);
        assert_eq!(
            access(&POST, "/flexiq.admin.v1.AdminService/PauseQueue"),
            write
        );
        assert_eq!(access(&POST, "/v1/jobs"), write);
        assert_eq!(access(&POST, "/v1/admin/queues/emails/pause"), write);

        let read = Some(Access::Read);
        assert_eq!(access(&POST, "/flexiq.v1.ProducerService/GetJob"), read);
        assert_eq!(access(&POST, "/flexiq.v1.ProducerService/WatchJobs"), read);
        assert_eq!(
            access(&POST, "/flexiq.admin.v1.AdminService/ListQueues"),
            read
        );
        assert_eq!(access(&GET, "/v1/jobs/j1"), read);
        assert_eq!(access(&GET, "/v1/jobs:watch"), read);
        assert_eq!(access(&GET, "/v1/admin/queues"), read);
    }

    #[test]
    fn executor_public_and_unscoped_paths_are_never_audited() {
        assert_eq!(
            access(&POST, "/flexiq.executor.v1.ExecutorService/Attach"),
            None
        );
        assert_eq!(access(&POST, "/grpc.health.v1.Health/Check"), None);
        assert_eq!(access(&GET, "/metrics"), None);
    }

    /// A method added to a write package later is recorded by default.
    #[test]
    fn an_unheard_of_write_method_is_audited() {
        assert_eq!(
            access(&POST, "/flexiq.v1.ProducerService/SomethingNew"),
            Some(Access::Write)
        );
        assert_eq!(
            access(&POST, "/flexiq.admin.v1.AdminService/SomethingNew"),
            Some(Access::Write)
        );
    }

    fn operation() -> &'static str {
        "flexiq.v1.ProducerService/EnqueueBatch"
    }

    #[test]
    fn one_record_per_target_naming_the_token_by_id_and_name() {
        let context = AuditContext::default();
        context.identify(&Principal::new("tok_1", "prod", ScopeSet::ALL).named("ci"));
        context.target(TargetKind::Job, "j1");
        context.target(TargetKind::Job, "j2");

        let records = records(&context, operation(), Code::Ok);
        assert_eq!(records.len(), 2);
        for (record, job) in records.iter().zip(["j1", "j2"]) {
            assert_eq!(record.token_id, "tok_1");
            assert_eq!(record.principal, "ci");
            assert_eq!(record.namespace, "prod");
            assert_eq!(record.operation, "flexiq.v1.ProducerService/EnqueueBatch");
            assert_eq!(record.target_kind.as_deref(), Some("job"));
            assert_eq!(record.target.as_deref(), Some(job));
            assert_eq!(record.outcome, "OK");
        }
        assert_ne!(records[0].id, records[1].id);
    }

    #[test]
    fn a_refusal_before_any_target_is_one_untargeted_record() {
        let context = AuditContext::default();
        context.identify(&Principal::new("tok_1", "prod", ScopeSet::ALL));
        let records = records(&context, operation(), Code::PermissionDenied);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].target_kind, None);
        assert_eq!(records[0].target, None);
        assert_eq!(records[0].outcome, "PERMISSION_DENIED");
    }

    #[test]
    fn no_believed_credential_records_nothing() {
        let context = AuditContext::default();
        context.target(TargetKind::Job, "j1");
        assert!(records(&context, operation(), Code::Unauthenticated).is_empty());
    }

    /// The trail as the sink's writer stored it, once `count` records are in.
    async fn stored(storage: &StorageBackend, count: usize) -> Vec<AuditRecord> {
        for _ in 0..100 {
            let records = storage
                .list_audit_after("prod", &AuditFilter::default(), 100, None)
                .expect("list");
            if records.len() >= count {
                return records;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        panic!("the writer never stored {count} record(s)");
    }

    fn pending(storage: &StorageBackend) -> Pending {
        let context = AuditContext::default();
        context.identify(&Principal::new("tok", "prod", ScopeSet::ALL));
        context.target(TargetKind::Job, "j1");
        let (sink, _writer) = AuditSink::start(storage.clone(), Shutdown::default());
        Pending {
            call: Some(Call {
                context,
                operation: Cow::Borrowed(operation()),
                sink,
                dedup: None,
            }),
        }
    }

    /// A deadline or a client hanging up drops the call's future; a write the
    /// handler already committed must still be attributed.
    #[tokio::test]
    async fn a_dropped_call_is_recorded_cancelled() {
        let storage = StorageBackend::Sqlite(SqliteStorage::in_memory().expect("sqlite"));
        drop(pending(&storage));
        let records = stored(&storage, 1).await;
        assert_eq!(records[0].outcome, "CANCELLED");
        assert_eq!(records[0].target.as_deref(), Some("j1"));
    }

    #[tokio::test]
    async fn a_settled_call_is_recorded_once_with_its_code() {
        let storage = StorageBackend::Sqlite(SqliteStorage::in_memory().expect("sqlite"));
        let mut call = pending(&storage);
        call.settle(Code::Ok);
        drop(call);
        let records = stored(&storage, 1).await;
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let records_after = stored(&storage, 1).await;
        assert_eq!(records.len(), 1);
        assert_eq!(records, records_after, "the drop adds nothing");
        assert_eq!(records[0].outcome, "OK");
    }

    /// Send `paths` through an [`AuditLayer`] over a service standing in for
    /// auth and a handler: it names the caller and job `j1`, then answers with
    /// a head only — which is all a stream's open is to this layer. Returns
    /// every record stored once the writer has drained.
    async fn through_layer(reads: Option<Duration>, paths: &[&str]) -> Vec<AuditRecord> {
        let storage = StorageBackend::Sqlite(SqliteStorage::in_memory().expect("sqlite"));
        let shutdown = Shutdown::default();
        let (sink, writer) = AuditSink::start(storage.clone(), shutdown.clone());
        let layer = AuditLayer::new(sink, reads);
        let handler = service_fn(|request: http::Request<()>| async move {
            if let Some(audit) = AuditContext::of(request.extensions()) {
                audit.identify(&Principal::new("tok", "prod", ScopeSet::ALL));
                audit.target(TargetKind::Job, "j1");
            }
            Ok::<_, std::convert::Infallible>(http::Response::new(()))
        });
        for path in paths {
            let request = http::Request::post(*path).body(()).expect("request");
            layer
                .layer(handler)
                .oneshot(request)
                .await
                .expect("answered");
        }
        drop(layer);
        shutdown.trigger();
        writer.finish().await;
        storage
            .list_audit_after("prod", &AuditFilter::default(), 100, None)
            .expect("list")
    }

    const GET_JOB: &str = "/flexiq.v1.ProducerService/GetJob";
    const WATCH: &str = "/flexiq.v1.ProducerService/WatchJobs";
    const CANCEL: &str = "/flexiq.v1.ProducerService/CancelJob";
    const MINUTE: Option<Duration> = Some(Duration::from_secs(60));

    #[tokio::test]
    async fn reads_are_not_recorded_unless_turned_on() {
        let records = through_layer(None, &[GET_JOB, WATCH, CANCEL]).await;
        assert_eq!(records.len(), 1, "only the write: {records:?}");
        assert_eq!(records[0].operation, "flexiq.v1.ProducerService/CancelJob");
    }

    #[tokio::test]
    async fn a_polled_read_is_recorded_once_per_window() {
        let records = through_layer(MINUTE, &[GET_JOB, GET_JOB, GET_JOB]).await;
        assert_eq!(records.len(), 1, "{records:?}");
        assert_eq!(records[0].operation, "flexiq.v1.ProducerService/GetJob");
        assert_eq!(records[0].target.as_deref(), Some("j1"));
    }

    #[tokio::test]
    async fn a_zero_window_records_every_read() {
        let records = through_layer(Some(Duration::ZERO), &[GET_JOB, GET_JOB]).await;
        assert_eq!(records.len(), 2, "{records:?}");
    }

    /// Writes are never folded, whatever the read window.
    #[tokio::test]
    async fn repeated_writes_are_each_recorded() {
        let records = through_layer(MINUTE, &[CANCEL, CANCEL]).await;
        assert_eq!(records.len(), 2, "{records:?}");
    }

    /// A stream is recorded when its head is answered; its frames are body,
    /// which never passes back through the layer.
    #[tokio::test]
    async fn a_watch_is_recorded_once_when_it_opens() {
        let records = through_layer(MINUTE, &[WATCH]).await;
        assert_eq!(records.len(), 1, "{records:?}");
        assert_eq!(records[0].operation, "flexiq.v1.ProducerService/WatchJobs");
        assert_eq!(records[0].outcome, "OK");
    }
}
